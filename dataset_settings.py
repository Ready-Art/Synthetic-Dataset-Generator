"""Names-only settings for the optional Datasets configuration panel.

This module owns only the ``datasets`` configuration subtree, not the enclosing
application configuration. It imports no GUI code and performs no file or network
I/O. Settings contain source selectors, cached column names, and aliases; dataset
records and credentials are never part of the normalized configuration.

An enabled source with aliases must be ready to use. Disabled sources and sources
without aliases may remain unfinished drafts so a partially configured panel can
be saved. Hiding a column changes its presentation only, never its alias mapping.
"""

from collections.abc import Mapping
from typing import Any, Dict, Iterable, Optional, Tuple
from uuid import uuid4

from dataset_sources import DatasetSchema, SourceSpec
from dataset_templates import validate_alias


class DatasetSettingsError(ValueError):
    """Dataset settings are structurally invalid or an active mapping is stale."""


_SELECTOR_DEFAULTS = {
    "kind": "csv", "location": "", "config": None,
    "split": None, "revision": None,
}


def _mapping(value: Any, label: str) -> Mapping:
    if not isinstance(value, Mapping):
        raise DatasetSettingsError("{} must be an object.".format(label))
    return value


def _string(value: Any, label: str, optional: bool = False) -> Optional[str]:
    if optional and value is None:
        return None
    if not isinstance(value, str):
        suffix = " or null" if optional else ""
        raise DatasetSettingsError("{} must be a string{}.".format(label, suffix))
    return value


def _names(value: Any, label: str) -> list:
    if not isinstance(value, (list, tuple)):
        raise DatasetSettingsError("{} must be a list of column names.".format(label))
    result = []
    seen = set()
    for name in value:
        if not isinstance(name, str) or not name:
            raise DatasetSettingsError("{} must contain nonempty string names.".format(label))
        if name in seen:
            raise DatasetSettingsError("{} contains duplicate column {!r}.".format(label, name))
        seen.add(name)
        result.append(name)
    return result


def source_selector(source: Mapping) -> Dict[str, Any]:
    """Return detached, comparable kind/location/config/split/revision settings.

    This accepts unfinished selectors and does not construct a ``SourceSpec``.
    Empty or whitespace-only optional selectors become ``None``; every other
    string, including paths, is preserved exactly. The source ID is excluded.
    """
    source = _mapping(source, "Dataset source")
    result = {}
    for key, default in _SELECTOR_DEFAULTS.items():
        optional = key in ("config", "split", "revision")
        value = _string(source.get(key, default), "Source {}".format(key), optional)
        if optional and isinstance(value, str) and not value.strip():
            value = None
        result[key] = value
    return result


def _visibility(value: Any, columns: list) -> Optional[list]:
    if value is None:
        return None
    selected = _names(value, "Visible columns")
    # Selecting every current column means All, including future schema names.
    # An explicit empty selection on an empty schema must still mean None shown.
    if columns and set(selected) == set(columns):
        return None
    return selected


def _source(source: Mapping, index: int = 0) -> Dict[str, Any]:
    source = _mapping(source, "Dataset source")
    source_id = _string(source.get("source_id"), "Source ID")
    if not source_id.strip():
        raise DatasetSettingsError("Source ID must be a nonempty string.")
    enabled = source.get("enabled", True)
    if type(enabled) is not bool:
        raise DatasetSettingsError("Source enabled must be true or false.")
    # Preserve selector text in drafts. source_selector handles optional blanks
    # when selectors are compared or converted to a SourceSpec.
    result = {
        "source_id": source_id,
        "name": _string(source.get("name", "Dataset {}".format(index + 1)), "Source name"),
        "enabled": enabled,
    }
    for key, default in _SELECTOR_DEFAULTS.items():
        result[key] = _string(source.get(key, default), "Source {}".format(key),
                              key in ("config", "split", "revision"))
    columns = _names(source.get("columns", []), "Cached columns")
    aliases = _mapping(source.get("column_aliases", {}), "Column aliases")
    mappings = {}
    for column, alias in aliases.items():
        if not isinstance(column, str) or not column:
            raise DatasetSettingsError("Alias mapping columns must be nonempty strings.")
        if not isinstance(alias, str):
            raise DatasetSettingsError("An alias for {!r} must be a string.".format(column))
        if alias != "":  # A cleared GUI entry is an unmapped column.
            mappings[column] = alias
    selector = source.get("schema_selector")
    if selector is not None:
        selector = source_selector(_mapping(selector, "Imported schema selector"))
    fingerprint = _string(source.get("schema_fingerprint"), "Schema fingerprint", optional=True)
    result.update({
        "columns": columns,
        "column_aliases": mappings,
        "visible_columns": _visibility(source.get("visible_columns"), columns),
        "schema_selector": selector,
        "schema_fingerprint": fingerprint,
    })
    return result


def normalize_config(data: Optional[Mapping]) -> Dict[str, list]:
    """Copy the datasets subtree, checking structure without validating drafts.

    Missing settings normalize to ``{'sources': []}``. Only the recognized
    names-only dataset fields survive; this function must not receive the whole
    application configuration. IDs must be present and unique, including for
    disabled sources, and are never regenerated during a round trip.
    """
    if data is None:
        return {"sources": []}
    data = _mapping(data, "Datasets configuration")
    supplied = data.get("sources", [])
    if not isinstance(supplied, (list, tuple)):
        raise DatasetSettingsError("Dataset sources must be a list.")
    sources = []
    seen = set()
    for index, item in enumerate(supplied):
        source = _source(item, index)
        if source["source_id"] in seen:
            raise DatasetSettingsError("Duplicate source ID {!r}.".format(source["source_id"]))
        seen.add(source["source_id"])
        sources.append(source)
    return {"sources": sources}


def new_source(existing_sources: Iterable[Mapping]) -> Dict[str, Any]:
    """Create one blank source with a stable unique ID and unused Dataset N name."""
    if isinstance(existing_sources, (str, bytes, Mapping)):
        raise DatasetSettingsError("Existing sources must be a collection of source objects.")
    try:
        existing = normalize_config({"sources": list(existing_sources)})["sources"]
    except TypeError as exc:
        raise DatasetSettingsError("Existing sources must be a collection of source objects.") from exc
    names = {source["name"] for source in existing}
    source_ids = {source["source_id"] for source in existing}
    number = 1
    while "Dataset {}".format(number) in names:
        number += 1
    source_id = uuid4().hex
    while source_id in source_ids:
        source_id = uuid4().hex
    return _source({"source_id": source_id, "name": "Dataset {}".format(number)})


def source_spec(source: Mapping) -> SourceSpec:
    """Validate and construct a reader specification, without opening the source."""
    source = _mapping(source, "Dataset source")
    try:
        return SourceSpec(source_id=source.get("source_id"), **source_selector(source))
    except (TypeError, ValueError) as exc:
        if isinstance(exc, DatasetSettingsError):
            raise
        raise DatasetSettingsError(str(exc)) from exc


def validate_config(data: Optional[Mapping]) -> None:
    """Validate enabled mapped sources against their cached imported schema.

    Disabled and unmapped drafts need only be structurally valid. Hidden aliases
    remain active. No file existence, schema fetch, or credential check occurs.
    """
    normalized = normalize_config(data)
    used_aliases = {}
    for source in normalized["sources"]:
        mappings = source["column_aliases"]
        if not source["enabled"] or not mappings:
            continue
        label = source["name"] or source["source_id"]
        try:
            source_spec(source)
        except DatasetSettingsError as exc:
            raise DatasetSettingsError("Source {!r}: {}".format(label, exc)) from exc
        for column, alias in mappings.items():
            try:
                validate_alias(alias)
            except (TypeError, ValueError) as exc:
                raise DatasetSettingsError("Source {!r}, column {!r}: {}".format(label, column, exc)) from exc
            if alias in used_aliases:
                raise DatasetSettingsError(
                    "Dataset alias {!r} is used more than once among enabled sources ({!r} and {!r})."
                    .format(alias, used_aliases[alias], label)
                )
            used_aliases[alias] = label
        missing = [column for column in mappings if column not in source["columns"]]
        if missing:
            raise DatasetSettingsError(
                "Source {!r} has mapped columns absent from its imported schema: {}."
                .format(label, ", ".join(repr(column) for column in missing))
            )
        if (source["schema_selector"] is None or not (source["schema_fingerprint"] or "").strip()
                or source_selector(source) != source["schema_selector"]):
            raise DatasetSettingsError(
                "Source {!r}: import columns again for the current source settings before saving active mappings."
                .format(label)
            )


def apply_schema(source: Mapping, schema: DatasetSchema) -> Dict[str, Any]:
    """Attach imported names to a detached source, preserving aliases and selectors.

    A refresh that removes a mapped column is rejected. Clear that mapping in the
    old table before importing again. Resolved commit/config/split metadata does
    not replace the user's selectors, such as a branch name. Only names, the
    opaque fingerprint, and the current selectors are stored.
    """
    result = _source(source)
    if not isinstance(schema, DatasetSchema):
        raise DatasetSettingsError("Imported schema must be a DatasetSchema.")
    columns = _names(schema.columns, "Imported columns")
    if not isinstance(schema.fingerprint, str) or not schema.fingerprint.strip():
        raise DatasetSettingsError("Imported schema must include a nonempty fingerprint.")
    missing = [column for column in result["column_aliases"] if column not in columns]
    if missing:
        raise DatasetSettingsError(
            "The new schema is missing mapped columns: {}. Clear their aliases in the current table before importing again."
            .format(", ".join(repr(column) for column in missing))
        )
    visible = result["visible_columns"]
    if visible is not None:
        visible_set = set(visible)
        visible = [column for column in columns if column in visible_set]
    result.update({
        "columns": columns,
        "visible_columns": _visibility(visible, columns),
        "schema_selector": source_selector(result),
        "schema_fingerprint": schema.fingerprint,
    })
    return result


def visible_column_names(source: Mapping) -> Tuple[str, ...]:
    """Return visible names in schema order, without changing any aliases."""
    source = _source(source)
    columns = source["columns"]
    selected = source["visible_columns"]
    if selected is None:
        return tuple(columns)
    chosen = set(selected)
    return tuple(column for column in columns if column in chosen)


def with_visible_columns(source: Mapping, selection: Optional[Iterable[str]]) -> Dict[str, Any]:
    """Copy a source with a new visibility selection, leaving mappings intact.

    ``None`` means All, including columns imported later; ``[]`` means none.
    An explicit selection containing every current name also becomes All for a
    nonempty schema. Names are stored in schema order and must exist in it.
    """
    result = _source(source)
    if selection is None:
        result["visible_columns"] = None
        return result
    if isinstance(selection, (str, bytes, Mapping)):
        raise DatasetSettingsError("Visible columns must be a collection of column names or None.")
    try:
        selected = _names(list(selection), "Visible columns")
    except TypeError as exc:
        raise DatasetSettingsError("Visible columns must be a collection of column names or None.") from exc
    unknown = [column for column in selected if column not in result["columns"]]
    if unknown:
        raise DatasetSettingsError("Unknown visible columns: {}.".format(", ".join(repr(column) for column in unknown)))
    chosen = set(selected)
    ordered = [column for column in result["columns"] if column in chosen]
    result["visible_columns"] = _visibility(ordered, result["columns"])
    return result
