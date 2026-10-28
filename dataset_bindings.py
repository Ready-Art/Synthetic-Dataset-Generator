"""Freeze dataset values for one logical generation task, independently of workers.

This module does not import the GUI, generation worker, or application state. A
``DatasetBindingManager`` belongs to one run. Call ``bind(task_id)`` before a
task's first request, then retain that returned ``TaskBinding`` for every stage
and retry. Repeating the call with the same task ID returns the same object.

Each configured source advances sequentially, once per newly bound task. Rows
are staged until every source succeeds; exhaustion or invalid data never causes
silent cycling, skipping, or a partially published binding. The allocation lock
is released before the caller renders prompts or makes model API requests.

Task snapshots serialize actual values, not just row numbers. Their JSON format
is a foundation for later application save/resume integration; it does not itself
restore source cursors, pending jobs, or conversation progress.
"""

import json
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Dict, Iterable, Iterator, Optional, Tuple

from dataset_sources import DatasetRow, DatasetSchema, DatasetSource, SourceSpec
from dataset_templates import validate_alias


class DatasetBindingError(ValueError):
    """Configuration, source data, or a persisted binding is invalid."""


class DatasetExhaustedError(DatasetBindingError):
    """A source has no next row; no binding was committed for this task."""


def _text_identifier(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DatasetBindingError("{} must be a non-empty string.".format(label))
    return value


def _copy_aliases(column_aliases: Mapping) -> Mapping:
    if not isinstance(column_aliases, Mapping) or not column_aliases:
        raise DatasetBindingError("Each source must have at least one column-to-alias mapping.")
    copied = {}
    used = set()
    for column, alias in column_aliases.items():
        if not isinstance(column, str):
            raise DatasetBindingError("Column names must be strings.")
        try:
            validate_alias(alias)
        except (TypeError, ValueError) as exc:
            raise DatasetBindingError("Invalid alias for column {!r}: {}".format(column, exc)) from exc
        if alias in used:
            raise DatasetBindingError("Duplicate dataset alias {!r}.".format(alias))
        used.add(alias)
        copied[column] = alias
    return MappingProxyType(copied)


def _copy_spec(spec: SourceSpec) -> SourceSpec:
    """Detach source settings from an adapter or a mutable configuration owner."""
    try:
        copied = SourceSpec(
            source_id=spec.source_id, kind=spec.kind, location=spec.location,
            config=spec.config, split=spec.split, revision=spec.revision,
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise DatasetBindingError("Invalid dataset source specification: {}".format(exc)) from exc
    for name in ("source_id", "kind", "location"):
        _text_identifier(getattr(copied, name), "Source {}".format(name))
    for name in ("config", "split", "revision"):
        value = getattr(copied, name)
        if value is not None and not isinstance(value, str):
            raise DatasetBindingError("Source {} must be a string or null.".format(name))
    return copied


def _spec_dict(spec: SourceSpec) -> Dict[str, Any]:
    return {name: getattr(spec, name) for name in (
        "source_id", "kind", "location", "config", "split", "revision"
    )}


def _json_value(value: Any, seen: set) -> None:
    """Reject Python-only types and non-string object keys before JSON encoding."""
    if value is None or type(value) in (str, bool, int, float):
        return
    if type(value) not in (list, dict):
        raise DatasetBindingError("Unsupported cell type {}.".format(type(value).__name__))
    identity = id(value)
    if identity in seen:
        raise DatasetBindingError("A dataset cell contains a circular structure.")
    seen.add(identity)
    try:
        if isinstance(value, dict):
            if any(not isinstance(key, str) for key in value):
                raise DatasetBindingError("Dataset object keys must be strings.")
            items = value.values()
        else:
            items = value
        for item in items:
            _json_value(item, seen)
    finally:
        seen.remove(identity)


def cell_to_text(value: Any) -> str:
    """Preserve strings; map top-level null to empty text; JSON-encode other cells.

    JSON is compact, Unicode-preserving, and ordered by object key. Booleans use
    ``true``/``false``; nulls inside arrays/objects remain JSON ``null``. Non-finite
    numbers and Python-only objects fail explicitly rather than using ``repr``.
    """
    if isinstance(value, str):
        return value
    if value is None:
        return ""
    try:
        _json_value(value, set())
        return json.dumps(value, ensure_ascii=False, sort_keys=True,
                          separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, RecursionError) as exc:
        if isinstance(exc, DatasetBindingError):
            raise
        raise DatasetBindingError("Dataset cell cannot be encoded as JSON: {}".format(exc)) from exc


@dataclass(frozen=True)
class SourceBinding:
    """One reader and its exact column-name -> user-selected-alias mapping."""

    source: DatasetSource
    column_aliases: Mapping

    def __post_init__(self) -> None:
        object.__setattr__(self, "column_aliases", _copy_aliases(self.column_aliases))


@dataclass(frozen=True)
class SourceProvenance:
    """Frozen source identity and the row used by a task, without unused cells."""

    spec: SourceSpec
    fingerprint: str
    row_id: str
    column_aliases: Mapping

    def __post_init__(self) -> None:
        object.__setattr__(self, "spec", _copy_spec(self.spec))
        _text_identifier(self.fingerprint, "Source fingerprint")
        _text_identifier(self.row_id, "Row ID")
        object.__setattr__(self, "column_aliases", _copy_aliases(self.column_aliases))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "spec": _spec_dict(self.spec), "fingerprint": self.fingerprint,
            "row_id": self.row_id, "column_aliases": dict(self.column_aliases),
        }

    @classmethod
    def from_dict(cls, data: Mapping) -> "SourceProvenance":
        if not isinstance(data, Mapping):
            raise DatasetBindingError("Persisted source provenance must be an object.")
        try:
            spec = data["spec"]
            if not isinstance(spec, Mapping):
                raise DatasetBindingError("Persisted source specification must be an object.")
            return cls(SourceSpec(**dict(spec)), data["fingerprint"],
                       data["row_id"], data["column_aliases"])
        except (KeyError, TypeError, ValueError) as exc:
            if isinstance(exc, DatasetBindingError):
                raise
            raise DatasetBindingError("Invalid persisted source provenance: {}".format(exc)) from exc


@dataclass(frozen=True)
class TaskBinding:
    """Immutable, worker-independent values available to every stage of one task."""

    run_id: str
    task_id: str
    values: Mapping
    sources: Tuple[SourceProvenance, ...]

    def __post_init__(self) -> None:
        _text_identifier(self.run_id, "Run ID")
        _text_identifier(self.task_id, "Task ID")
        if not isinstance(self.values, Mapping):
            raise DatasetBindingError("Bound values must be an alias-to-string mapping.")
        values = dict(self.values)
        for alias, value in values.items():
            try:
                validate_alias(alias)
            except (TypeError, ValueError) as exc:
                raise DatasetBindingError("Invalid bound alias: {}".format(exc)) from exc
            if not isinstance(value, str):
                raise DatasetBindingError("Bound value for {!r} must be a string.".format(alias))
        sources = tuple(self.sources)
        expected = set()
        source_ids = set()
        for source in sources:
            if not isinstance(source, SourceProvenance):
                raise DatasetBindingError("Task sources must contain source provenance snapshots.")
            if source.spec.source_id in source_ids:
                raise DatasetBindingError("Duplicate source ID {!r}.".format(source.spec.source_id))
            source_ids.add(source.spec.source_id)
            for alias in source.column_aliases.values():
                if alias in expected:
                    raise DatasetBindingError("Duplicate dataset alias {!r}.".format(alias))
                expected.add(alias)
        if set(values) != expected:
            raise DatasetBindingError("Bound aliases must exactly match the recorded source mappings.")
        object.__setattr__(self, "values", MappingProxyType(values))
        object.__setattr__(self, "sources", sources)

    def to_dict(self) -> Dict[str, Any]:
        """Return a detached, JSON-serializable snapshot containing actual values."""
        return {
            "version": 1, "run_id": self.run_id, "task_id": self.task_id,
            "values": dict(self.values),
            "sources": [source.to_dict() for source in self.sources],
        }

    @classmethod
    def from_dict(cls, data: Mapping) -> "TaskBinding":
        """Read a snapshot without reopening or sampling any dataset."""
        if not isinstance(data, Mapping):
            raise DatasetBindingError("Persisted task binding must be an object.")
        if type(data.get("version")) is not int or data["version"] != 1:
            raise DatasetBindingError("Unsupported dataset binding snapshot version.")
        try:
            if not isinstance(data["sources"], list):
                raise DatasetBindingError("Persisted sources must be a list.")
            return cls(data["run_id"], data["task_id"], data["values"],
                       tuple(SourceProvenance.from_dict(item) for item in data["sources"]))
        except (KeyError, TypeError, ValueError) as exc:
            if isinstance(exc, DatasetBindingError):
                raise
            raise DatasetBindingError("Invalid persisted task binding: {}".format(exc)) from exc


class _SourceCursor:
    def __init__(self, selection: SourceBinding, spec: SourceSpec, schema: DatasetSchema):
        self.source = selection.source
        self.spec = spec
        self.fingerprint = schema.fingerprint
        self.column_aliases = selection.column_aliases
        self.iterator = None  # type: Optional[Iterator[DatasetRow]]
        self.pending = None  # type: Optional[Tuple[str, Mapping]]
        self.failure = None  # type: Optional[Tuple[type, str]]

    def peek(self) -> Tuple[str, Mapping]:
        if self.failure is not None:
            error_type, message = self.failure
            raise error_type(message)
        if self.pending is not None:
            return self.pending
        try:
            if self.iterator is None:
                self.iterator = iter(self.source.iter_rows())
            try:
                row = next(self.iterator)
            except StopIteration as exc:
                raise DatasetExhaustedError(
                    "Dataset source {!r} is exhausted; no row was assigned to this task."
                    .format(self.spec.source_id)
                ) from exc
            _text_identifier(row.row_id, "Row ID")
            if not isinstance(row.values, Mapping):
                raise DatasetBindingError("Dataset rows must contain a column-to-value mapping.")
            values = {}
            for column, alias in self.column_aliases.items():
                if column not in row.values:
                    raise DatasetBindingError(
                        "Source {!r}, row {!r} is missing mapped column {!r}."
                        .format(self.spec.source_id, row.row_id, column)
                    )
                try:
                    values[alias] = cell_to_text(row.values[column])
                except DatasetBindingError as exc:
                    raise DatasetBindingError(
                        "Source {!r}, row {!r}, column {!r}: {}"
                        .format(self.spec.source_id, row.row_id, column, exc)
                    ) from exc
            self.pending = (row.row_id, MappingProxyType(values))
            return self.pending
        except Exception as exc:
            error_type = DatasetExhaustedError if isinstance(exc, DatasetExhaustedError) else DatasetBindingError
            message = str(exc) if isinstance(exc, DatasetBindingError) else (
                "Could not read source {!r}: {}".format(self.spec.source_id, exc)
            )
            # A failed iterator cannot safely be replayed. Freeze the failure so
            # retries never silently move past a malformed or unreadable row.
            self.failure = (error_type, message)
            if isinstance(exc, error_type):
                raise
            raise error_type(message) from exc

    def commit(self) -> None:
        self.pending = None

    def close(self) -> None:
        iterator, self.iterator = self.iterator, None
        self.pending = None
        close = getattr(iterator, "close", None)
        if close is not None:
            close()


class DatasetBindingManager:
    """Allocate once per task within one run, regardless of worker concurrency.

    Source definitions and alias mappings must be supplied when constructing the
    manager; edits for another run must create another manager. Source reader
    instances are owned exclusively by this manager and must not be shared with
    another manager. Only mapped, enabled sources should be supplied. Zero
    sources is valid and yields empty task bindings. Call ``close()`` at run end
    (or use a context manager) to release file/stream handles. Previously bound
    tasks remain available after close; new allocations are rejected.
    """

    def __init__(self, run_id: str, sources: Iterable[SourceBinding]):
        self.run_id = _text_identifier(run_id, "Run ID")
        self._lock = threading.Lock()
        self._closed = False
        self._bindings = {}  # type: Dict[str, TaskBinding]
        self._cursors = []
        source_ids = set()
        aliases = set()
        inspected = []
        try:
            for supplied in sources:
                if not isinstance(supplied, SourceBinding):
                    raise DatasetBindingError("Sources must be SourceBinding objects.")
                selection = SourceBinding(supplied.source, supplied.column_aliases)
                spec = _copy_spec(selection.source.spec)
                if spec.source_id in source_ids:
                    raise DatasetBindingError("Duplicate source ID {!r}.".format(spec.source_id))
                source_ids.add(spec.source_id)
                for alias in selection.column_aliases.values():
                    if alias in aliases:
                        raise DatasetBindingError("Duplicate dataset alias {!r}.".format(alias))
                    aliases.add(alias)
                # Schema inspection can itself open a memory-mapped dataset.
                # Track the reader before calling it so failed inspection is
                # cleaned up alongside previously validated readers.
                inspected.append(selection.source)
                schema = selection.source.schema()
                resolved_spec = getattr(schema, "resolved_spec", None)
                if resolved_spec is not None:
                    resolved_spec = _copy_spec(resolved_spec)
                    if resolved_spec.source_id != spec.source_id:
                        raise DatasetBindingError("Resolved source settings must retain the configured source ID.")
                    spec = resolved_spec
                _text_identifier(schema.fingerprint, "Source fingerprint")
                columns = tuple(schema.columns)
                if any(not isinstance(column, str) for column in columns) or len(set(columns)) != len(columns):
                    raise DatasetBindingError("Dataset schema must contain distinct string column names.")
                missing = [column for column in selection.column_aliases if column not in columns]
                if missing:
                    raise DatasetBindingError(
                        "Source {!r} has no mapped column(s): {}."
                        .format(spec.source_id, ", ".join(repr(column) for column in missing))
                    )
                self._cursors.append(_SourceCursor(selection, spec, schema))
        except BaseException:
            self._closed = True
            for source in reversed(inspected):
                try:
                    close_source = getattr(source, "close", None)
                    if close_source is not None:
                        close_source()
                except Exception:
                    # Preserve the construction error; attempt every reader.
                    pass
            raise

    def bind(self, task_id: str) -> TaskBinding:
        """Return the original binding, or atomically allocate the next source rows."""
        _text_identifier(task_id, "Task ID")
        with self._lock:
            if task_id in self._bindings:
                return self._bindings[task_id]
            if self._closed:
                raise DatasetBindingError("Dataset binding manager is closed; no new tasks can be bound.")
            values = {}
            provenance = []
            for cursor in self._cursors:
                row_id, row_values = cursor.peek()
                values.update(row_values)
                provenance.append(SourceProvenance(
                    cursor.spec, cursor.fingerprint, row_id, cursor.column_aliases
                ))
            binding = TaskBinding(self.run_id, task_id, values, tuple(provenance))
            for cursor in self._cursors:
                cursor.commit()
            self._bindings[task_id] = binding
            return binding

    def get(self, task_id: str) -> Optional[TaskBinding]:
        """Look up a binding without selecting rows or creating a new task."""
        _text_identifier(task_id, "Task ID")
        with self._lock:
            return self._bindings.get(task_id)

    def snapshot(self) -> Tuple[TaskBinding, ...]:
        """Return immutable committed bindings in their allocation order."""
        with self._lock:
            return tuple(self._bindings.values())

    def close(self) -> None:
        """Release iterators and owned readers, retaining committed task bindings.

        Every close is attempted even if another fails. Closing readers as well
        as iterators releases resources opened during schema discovery, such as
        memory-mapped local Hugging Face datasets.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            failures = []
            for cursor in self._cursors:
                try:
                    cursor.close()
                except Exception as exc:
                    failures.append("{!r} iterator: {}".format(cursor.spec.source_id, exc))
                try:
                    close_source = getattr(cursor.source, "close", None)
                    if close_source is not None:
                        close_source()
                except Exception as exc:
                    failures.append("{!r} source: {}".format(cursor.spec.source_id, exc))
            if failures:
                raise DatasetBindingError("Could not close dataset reader(s): " + "; ".join(failures))

    def __enter__(self) -> "DatasetBindingManager":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
