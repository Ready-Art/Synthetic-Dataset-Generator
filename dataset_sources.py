"""Dataset readers for the optional dataset-binding foundation.

This module has no application, Tk, or configuration imports. ``schema()`` returns
column names and an opaque source fingerprint, never record values. JSON sources
must be read to discover their keys, but those values are not retained in schema
objects. Create fresh reader instances for each generation run.

CSV/TSV/JSON/JSONL use the standard library. Parquet requires ``pyarrow``; Hub and
``save_to_disk`` sources require the optional ``datasets`` 4.x backend. Importing
this module does not import or install those packages, make requests, or open data.
"""

from abc import ABC, abstractmethod
import csv
from dataclasses import asdict, dataclass
import hashlib
import importlib
import json
from pathlib import Path
import re
import sys
from threading import RLock
from typing import Any, Iterator, Mapping, Optional, Tuple


class DatasetSourceError(ValueError):
    """A source cannot be inspected or read as configured."""


class DatasetDependencyError(DatasetSourceError):
    """An optional reader dependency is absent or unsupported."""


_KINDS = frozenset(("csv", "tsv", "json", "jsonl", "parquet", "huggingface", "hf_disk"))


@dataclass(frozen=True)
class SourceSpec:
    """Serializable source settings. Paths and column names are never sanitized.

    ``config`` and ``revision`` apply only to Hub sources; ``split`` also applies
    to a local DatasetDict saved by Hugging Face. Credentials are intentionally
    excluded: the optional Hub backend uses its normal environment/login token.
    """

    source_id: str
    kind: str
    location: str
    config: Optional[str] = None
    split: Optional[str] = None
    revision: Optional[str] = None

    def __post_init__(self):
        for key in ("source_id", "kind", "location"):
            value = getattr(self, key)
            if not isinstance(value, str) or not value.strip():
                raise DatasetSourceError("Source {} must be a nonempty string.".format(key))
        if self.kind not in _KINDS:
            raise DatasetSourceError("Unsupported dataset kind: {}.".format(self.kind))
        for key in ("config", "split", "revision"):
            value = getattr(self, key)
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise DatasetSourceError("Source {} must be a nonempty string or None.".format(key))
        if self.kind == "huggingface":
            if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*/[A-Za-z0-9_][A-Za-z0-9_.-]*", self.location):
                raise DatasetSourceError("Use a Hugging Face repository ID such as owner/dataset, not a URL or file path.")
        else:
            if self.config is not None or self.revision is not None:
                raise DatasetSourceError("Only Hub sources accept config or revision settings.")
            if self.kind != "hf_disk" and self.split is not None:
                raise DatasetSourceError("A local file is one table; split applies only to Hub or saved DatasetDict sources.")


@dataclass(frozen=True)
class DatasetSchema:
    """Names only. Fingerprints identify this reader's source snapshot."""

    columns: Tuple[str, ...]
    fingerprint: str
    resolved_spec: Optional[SourceSpec] = None


@dataclass(frozen=True)
class DatasetRow:
    """One complete source row, before the binding layer copies mapped values."""

    row_id: str
    values: Mapping[str, Any]


class DatasetSource(ABC):
    """One source reader, owned by one run's binding manager."""

    def __init__(self, spec: SourceSpec):
        if not isinstance(spec, SourceSpec):
            raise DatasetSourceError("A source needs a SourceSpec.")
        self._spec = spec

    @property
    def spec(self) -> SourceSpec:
        return self._spec

    @abstractmethod
    def schema(self) -> DatasetSchema:
        """Discover column names without returning row values."""

    @abstractmethod
    def iter_rows(self) -> Iterator[DatasetRow]:
        """Read rows in source order. Every call starts a new iterator."""

    def close(self) -> None:
        """Release reader-owned resources after its row iterators are closed."""


def _columns(names) -> Tuple[str, ...]:
    columns = tuple(names)
    if any(not isinstance(name, str) or not name for name in columns):
        raise DatasetSourceError("Column names must be nonempty strings.")
    if len(columns) != len(set(columns)):
        raise DatasetSourceError("Duplicate column names cannot be mapped unambiguously.")
    return columns


def _csv_reader(handle, delimiter):
    # Python defaults to a roughly 128 KiB CSV cell limit. Text datasets can
    # legitimately exceed it. Raise (never lower) that process-wide parser limit
    # only when a CSV/TSV source is used. Windows may have a narrower C long.
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(max(csv.field_size_limit(), limit))
            break
        except OverflowError:
            limit //= 10
    return csv.reader(handle, delimiter=delimiter, strict=True)


def _fingerprint(spec: SourceSpec, snapshot: Any) -> str:
    payload = json.dumps({"source": asdict(spec), "snapshot": snapshot}, ensure_ascii=False,
                         sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise DatasetSourceError("Duplicate JSON object keys are not supported.")
        result[key] = value
    return result


def _reject_json_constant(_value):
    raise DatasetSourceError("JSON values must use finite numbers, not NaN or Infinity.")


def _load_json(text: str, where: str):
    try:
        return json.loads(text, object_pairs_hook=_json_object, parse_constant=_reject_json_constant)
    except (json.JSONDecodeError, DatasetSourceError) as exc:
        raise DatasetSourceError("Invalid JSON in {} ({}).".format(where, type(exc).__name__)) from exc


def _record(value: Any, where: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise DatasetSourceError("{} must contain a JSON object per row.".format(where))
    _columns(value.keys())
    return value


def _optional_module(name: str):
    try:
        return importlib.import_module(name)
    except ImportError as exc:
        raise DatasetDependencyError("This source requires the optional {} backend; see docs/dataset-bindings.md.".format(name)) from exc


def _datasets_backend():
    backend = _optional_module("datasets")
    version = getattr(backend, "__version__", "")
    if not re.match(r"^4\.", version):
        raise DatasetDependencyError("Hub and saved-HF sources require datasets 4.x (dataset scripts are not supported).")
    return backend


class LocalFileSource(DatasetSource):
    """A CSV/TSV, JSON array, JSONL, or Parquet file.

    JSONL schema discovery scans every record's keys, including late columns.
    JSON arrays are loaded in memory; prefer JSONL/Parquet for large data.
    Local fingerprints use file identity/size/mtime, not a content checksum.
    Ordinary changes during a run fail explicitly instead of mixing versions.
    """

    def __init__(self, spec: SourceSpec):
        super().__init__(spec)
        if spec.kind not in ("csv", "tsv", "json", "jsonl", "parquet"):
            raise DatasetSourceError("LocalFileSource requires a local file kind.")
        self._path = Path(spec.location).expanduser().absolute()
        self._schema = None
        self._stamp = None
        self._lock = RLock()

    def _stat(self):
        try:
            if not self._path.is_file():
                raise DatasetSourceError("Dataset source is not a readable file: {}.".format(self.spec.source_id))
            stat = self._path.stat()
            return (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
        except OSError as exc:
            raise DatasetSourceError("Cannot inspect local source {}.".format(self.spec.source_id)) from exc

    def _unchanged(self):
        if self._stat() != self._stamp:
            raise DatasetSourceError("Local source {} changed after inspection; start a new run with a fresh source.".format(self.spec.source_id))

    def _text_rows(self) -> Iterator[DatasetRow]:
        try:
            with self._path.open("r", encoding="utf-8-sig", newline="") as handle:
                if self.spec.kind in ("csv", "tsv"):
                    reader = _csv_reader(handle, "\t" if self.spec.kind == "tsv" else ",")
                    header = _columns(next(reader, ()))
                    for index, values in enumerate(reader):
                        if not values:  # Empty physical lines are not records.
                            continue
                        if len(values) != len(header):
                            raise DatasetSourceError("CSV/TSV row {} has a different number of fields from its header.".format(reader.line_num))
                        yield DatasetRow(str(index), dict(zip(header, values)))
                elif self.spec.kind == "jsonl":
                    index = 0
                    for line_number, line in enumerate(handle, 1):
                        if not line.strip():
                            continue
                        where = "JSONL line {}".format(line_number)
                        yield DatasetRow(str(index), _record(_load_json(line, where), where))
                        index += 1
                else:
                    rows = _load_json(handle.read(), "JSON file")
                    if not isinstance(rows, list):
                        raise DatasetSourceError("A JSON source must be an array of objects; use JSONL for one object per line.")
                    for index, value in enumerate(rows):
                        yield DatasetRow(str(index), _record(value, "JSON row {}".format(index)))
        except (OSError, UnicodeError, csv.Error) as exc:
            raise DatasetSourceError("Cannot parse source {} as UTF-8 {} ({}).".format(self.spec.source_id, self.spec.kind, type(exc).__name__)) from exc

    def schema(self) -> DatasetSchema:
        with self._lock:
            if self._schema is not None:
                self._unchanged()
                return self._schema
            self._stamp = self._stat()
            if self.spec.kind in ("csv", "tsv"):
                try:
                    with self._path.open("r", encoding="utf-8-sig", newline="") as handle:
                        reader = _csv_reader(handle, "\t" if self.spec.kind == "tsv" else ",")
                        columns = _columns(next(reader, ()))
                except (OSError, UnicodeError, csv.Error) as exc:
                    raise DatasetSourceError("Cannot read the CSV/TSV header for {}.".format(self.spec.source_id)) from exc
            elif self.spec.kind == "parquet":
                parquet = _optional_module("pyarrow.parquet")
                try:
                    columns = _columns(parquet.read_schema(self._path).names)
                except Exception as exc:
                    raise DatasetSourceError("Cannot read the Parquet schema for {}.".format(self.spec.source_id)) from exc
            else:
                names = {}  # Insertion ordered, without retaining any cell values.
                for row in self._text_rows():
                    names.update((name, None) for name in row.values)
                columns = _columns(names)
            self._unchanged()
            self._schema = DatasetSchema(columns, _fingerprint(self.spec, (str(self._path), self._stamp)))
            return self._schema

    def iter_rows(self) -> Iterator[DatasetRow]:
        self.schema()
        if self.spec.kind == "parquet":
            parquet = _optional_module("pyarrow.parquet")
            try:
                with parquet.ParquetFile(self._path) as reader:
                    index = 0
                    for batch in reader.iter_batches(batch_size=256):
                        for value in batch.to_pylist():
                            self._unchanged()
                            yield DatasetRow(str(index), value)
                            index += 1
                self._unchanged()
            except DatasetSourceError:
                raise
            except Exception as exc:
                raise DatasetSourceError("Cannot read Parquet source {}.".format(self.spec.source_id)) from exc
        else:
            iterator = self._text_rows()
            try:
                for row in iterator:
                    self._unchanged()
                    yield row
                self._unchanged()
            finally:
                iterator.close()


class HuggingFaceSource(DatasetSource):
    """Metadata discovery and streaming for a text/tabular Hub dataset.

    Resolve a branch/tag once to a commit and use that revision for subsequent
    metadata/row reads. No dataset scripts are executed. A source without declared
    features fails explicitly rather than using a data-row preview as its schema.
    Streaming does not guarantee zero downloads for every underlying file format.
    """

    def __init__(self, spec: SourceSpec):
        super().__init__(spec)
        if spec.kind != "huggingface":
            raise DatasetSourceError("HuggingFaceSource requires kind='huggingface'.")
        self._lock = RLock()
        self._schema = None
        self._revision = None
        self._config = None
        self._split = None

    def _check_location(self):
        # Datasets prefers an existing local directory over an identically named
        # Hub repository. Do not label local rows with a remote commit fingerprint.
        if Path(self.spec.location).exists():
            raise DatasetSourceError("A local path shadows Hub repository {}; move it or run from another directory.".format(self.spec.location))

    def _pin(self):
        if self._revision is None:
            hub = _optional_module("huggingface_hub")
            try:
                info = hub.HfApi().dataset_info(repo_id=self.spec.location, revision=self.spec.revision or "main", timeout=30)
            except Exception as exc:
                raise DatasetSourceError("Cannot resolve Hub source {}; check its repository, revision, and access.".format(self.spec.source_id)) from exc
            if not isinstance(info.sha, str) or not info.sha:
                raise DatasetSourceError("The Hub did not return a dataset commit ID.")
            self._revision = info.sha
        return self._revision

    @property
    def resolved_revision(self) -> Optional[str]:
        return self._revision

    @property
    def resolved_config(self) -> Optional[str]:
        return self._config

    @property
    def resolved_split(self) -> Optional[str]:
        return self._split

    def config_names(self) -> Tuple[str, ...]:
        with self._lock:
            self._check_location()
            backend = _datasets_backend()
            revision = self._pin()
            try:
                return tuple(backend.get_dataset_config_names(self.spec.location, revision=revision))
            except Exception as exc:
                raise DatasetSourceError("Cannot list configurations for Hub source {}.".format(self.spec.source_id)) from exc

    def schema(self) -> DatasetSchema:
        with self._lock:
            self._check_location()
            if self._schema is not None:
                return self._schema
            backend = _datasets_backend()
            revision = self._pin()
            configs = self.config_names()
            config = self.spec.config
            if config is None:
                if len(configs) != 1:
                    raise DatasetSourceError("Choose a dataset configuration from: {}.".format(", ".join(configs)))
                config = configs[0]
            if config not in configs:
                raise DatasetSourceError("Unknown dataset configuration; available configurations: {}.".format(", ".join(configs)))
            try:
                builder = backend.load_dataset_builder(self.spec.location, name=config, revision=revision)
                info = builder.info
                features = info.features
                if features is None:
                    raise DatasetSourceError("This revision has no declared column schema. Export it to a supported local file or choose a source with feature metadata.")
                columns = _columns(features.keys())
                splits = tuple(info.splits.keys()) if info.splits else tuple(backend.get_dataset_split_names(self.spec.location, config_name=config, revision=revision))
            except DatasetSourceError:
                raise
            except Exception as exc:
                raise DatasetSourceError("Cannot inspect Hub schema for {}; a supported data-file dataset is required.".format(self.spec.source_id)) from exc
            split = self.spec.split
            if split is None:
                if len(splits) != 1:
                    raise DatasetSourceError("Choose a dataset split from: {}.".format(", ".join(splits)))
                split = splits[0]
            if split not in splits:
                raise DatasetSourceError("Unknown dataset split; available splits: {}.".format(", ".join(splits)))
            self._config, self._split = config, split
            resolved = SourceSpec(self.spec.source_id, self.spec.kind, self.spec.location,
                                  config=config, split=split, revision=revision)
            self._schema = DatasetSchema(columns, _fingerprint(resolved, (revision, config, split)), resolved)
            return self._schema

    def iter_rows(self) -> Iterator[DatasetRow]:
        self.schema()
        backend = _datasets_backend()
        iterator = None
        try:
            dataset = backend.load_dataset(self.spec.location, name=self._config, split=self._split,
                                           revision=self._revision, streaming=True)
            iterator = iter(dataset)
            for index, row in enumerate(iterator):
                if not isinstance(row, Mapping):
                    raise DatasetSourceError("The Hub source did not return an object for each row.")
                yield DatasetRow(str(index), row)
        except DatasetSourceError:
            raise
        except Exception as exc:
            raise DatasetSourceError("Cannot stream Hub source {}; check access and its data format.".format(self.spec.source_id)) from exc
        finally:
            if iterator is not None and hasattr(iterator, "close"):
                iterator.close()


class SavedDatasetSource(DatasetSource):
    """Read a local Dataset or DatasetDict created with Dataset.save_to_disk."""

    def __init__(self, spec: SourceSpec):
        super().__init__(spec)
        if spec.kind != "hf_disk":
            raise DatasetSourceError("SavedDatasetSource requires kind='hf_disk'.")
        self._path = Path(spec.location).expanduser().absolute()
        self._schema = None
        self._dataset = None
        self._stamp = None
        self._lock = RLock()

    def _stat(self):
        if not self._path.is_dir():
            raise DatasetSourceError("Saved HF source must be a local save_to_disk directory.")
        try:
            result = []
            for path in sorted(self._path.rglob("*")):
                if path.is_file():
                    stat = path.stat()
                    result.append((str(path.relative_to(self._path)), stat.st_size, stat.st_mtime_ns))
            return tuple(result)
        except OSError as exc:
            raise DatasetSourceError("Cannot inspect saved source {}.".format(self.spec.source_id)) from exc

    def _unchanged(self):
        if self._stat() != self._stamp:
            raise DatasetSourceError("Saved HF source changed; create a fresh reader for a new run.")

    def schema(self) -> DatasetSchema:
        with self._lock:
            if self._schema is not None:
                self._unchanged()
                return self._schema
            backend = _datasets_backend()
            self._stamp = self._stat()
            try:
                loaded = backend.load_from_disk(str(self._path), keep_in_memory=False)
                split = None
                if isinstance(loaded, Mapping):
                    splits = tuple(loaded.keys())
                    split = self.spec.split
                    if split is None:
                        if len(splits) != 1:
                            raise DatasetSourceError("Choose a saved dataset split from: {}.".format(", ".join(splits)))
                        split = splits[0]
                    if split not in loaded:
                        raise DatasetSourceError("The chosen split does not exist in this saved DatasetDict.")
                    loaded = loaded[split]
                elif self.spec.split is not None:
                    raise DatasetSourceError("This directory contains one Dataset; do not set a split.")
                loaded = loaded.with_format(None)
                columns = _columns(loaded.column_names)
            except DatasetSourceError:
                raise
            except Exception as exc:
                raise DatasetSourceError("Cannot load saved source {}; use a Dataset.save_to_disk directory.".format(self.spec.source_id)) from exc
            self._unchanged()
            self._dataset = loaded
            resolved = SourceSpec(self.spec.source_id, self.spec.kind, self.spec.location, split=split)
            self._schema = DatasetSchema(columns, _fingerprint(resolved, (str(self._path), self._stamp)), resolved)
            return self._schema

    def iter_rows(self) -> Iterator[DatasetRow]:
        self.schema()
        for index, row in enumerate(self._dataset):
            self._unchanged()
            yield DatasetRow(str(index), row)
        self._unchanged()

    def close(self) -> None:
        self._dataset = None
        self._schema = None


def open_source(spec: SourceSpec) -> DatasetSource:
    """Create a reader without I/O; call schema or iterate only when needed."""
    if spec.kind == "huggingface":
        return HuggingFaceSource(spec)
    if spec.kind == "hf_disk":
        return SavedDatasetSource(spec)
    return LocalFileSource(spec)
