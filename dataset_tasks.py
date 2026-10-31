"""Optional dataset values attached to the generator's existing task identities.

One attachment service belongs to one existing queue. Configuration is copied
without source I/O; the first worker that needs a new binding opens the readers.
The original task tuple, task ID, retry rules, and generation-state file are not
changed. Attachments are in-memory only, including across same-process requeues.

Workers keep the returned immutable binding locally. The optional task-metadata
entry is a reference to that binding, not a mutable global "current row". Reader
cleanup follows the lifetime of the queue, so an old worker can finish using its
own queue without a replacement run closing that queue's readers underneath it.
"""

import threading
import weakref
from collections.abc import Mapping
from types import MappingProxyType
from uuid import uuid4

from dataset_bindings import (
    DatasetBindingError, DatasetBindingManager, DatasetExhaustedError, SourceBinding,
)
from dataset_settings import normalize_config, source_spec, validate_config
from dataset_sources import open_source
from dataset_templates import (
    DEFAULT_QUESTION_SEPARATOR, DatasetTemplateError, dataset_placeholders,
    render_question_line, render_template,
)


def _task_id(value):
    if not isinstance(value, str) or not value.strip():
        raise DatasetBindingError("Task ID must be a nonempty string.")
    return value


class _OwnedReader:
    """Close each reader once, including partially constructed managers.

    The binding manager already closes readers it has inspected on failure.
    This guard also lets the caller close readers not yet inspected, without
    asking a backend to release the same resource twice.
    """

    def __init__(self, reader):
        self.reader = reader
        self.closed = False

    @property
    def spec(self):
        return self.reader.spec

    def schema(self):
        return self.reader.schema()

    def iter_rows(self):
        return self.reader.iter_rows()

    def close(self):
        if not self.closed:
            self.closed = True
            close = getattr(self.reader, "close", None)
            if close is not None:
                close()


class DatasetTaskAttachments:
    """Allocate one frozen row per source for each original task ID.

    Only enabled sources with mappings participate. The allocation lock covers
    lazy reader setup and row selection, never model requests. Once assigned,
    an existing binding remains retrievable after exhaustion, source failure,
    or close. New runs need a new service; no cursor is shared between queues.
    """

    def __init__(self, settings, source_factory=open_source):
        normalized = normalize_config(settings)
        validate_config(normalized)
        self._sources = tuple(
            (source_spec(source), MappingProxyType(dict(source["column_aliases"])))
            for source in normalized["sources"]
            if source["enabled"] and source["column_aliases"]
        )
        self.run_id = uuid4().hex
        self._source_factory = source_factory
        self._lock = threading.Lock()
        self._manager = None
        self._failure = None
        self._closed = False

    @property
    def active(self):
        return bool(self._sources)

    def _new_manager(self):
        readers = []
        try:
            selections = []
            for spec, aliases in self._sources:
                reader = _OwnedReader(self._source_factory(spec))
                readers.append(reader)
                selections.append(SourceBinding(reader, aliases))
            return DatasetBindingManager(self.run_id, selections)
        except BaseException:
            for reader in reversed(readers):
                try:
                    reader.close()
                except Exception:
                    pass  # Retain the source/setup error; try every cleanup.
            raise

    def bind(self, task_id):
        """Return this task's original binding or select its source rows once."""
        task_id = _task_id(task_id)
        with self._lock:
            if self._manager is not None:
                existing = self._manager.get(task_id)
                if existing is not None:
                    return existing
            if self._closed:
                raise DatasetBindingError("Dataset task attachments are closed; no new rows can be assigned.")
            if self._failure is not None:
                error_type, message = self._failure
                raise error_type(message)
            try:
                if self._manager is None:
                    self._manager = self._new_manager()
                return self._manager.bind(task_id)
            except Exception as exc:
                error_type = DatasetExhaustedError if isinstance(exc, DatasetExhaustedError) else DatasetBindingError
                message = str(exc) if isinstance(exc, DatasetBindingError) else "Could not attach dataset values: {}".format(exc)
                self._failure = (error_type, message)
                if isinstance(exc, error_type):
                    raise
                raise error_type(message) from exc

    def get(self, task_id):
        """Look up an attachment without opening a source or selecting a row."""
        task_id = _task_id(task_id)
        with self._lock:
            return self._manager.get(task_id) if self._manager is not None else None

    def close(self):
        """Release readers; retain already assigned values for existing tasks."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._manager is not None:
                self._manager.close()


def configure_queue_datasets(queue, settings, source_factory=open_source):
    """Attach a lazy service to a new queue, or return None when unused.

    This is a configuration-only operation, safe before workers start. It does
    not iterate or replace queued tasks. Reconfiguring an existing service is
    rejected rather than replacing values belonging to live tasks.
    """
    if getattr(queue, "dataset_attachments", None) is not None:
        raise DatasetBindingError("This queue already has dataset attachments; configure a new queue for a new run.")
    service = DatasetTaskAttachments(settings, source_factory=source_factory)
    if not service.active:
        return None
    # finalize holds the service, but only a weak reference to the queue. No
    # worker joins, stop flags, queue sentinels, or recovery rules are changed.
    weakref.finalize(queue, service.close)
    queue.dataset_attachments = service
    return service


def bind_task_datasets(queue, task, task_metadata=None, metadata_lock=None):
    """Get task-local values and optionally expose the same binding in metadata.

    Metadata is updated only while it still describes this exact task object.
    An old queue's worker must not overwrite a newer queue's metadata merely
    because the original generator reused the same textual task ID.
    """
    service = getattr(queue, "dataset_attachments", None)
    if service is None:
        return None
    if not isinstance(task, tuple) or len(task) not in (4, 5):
        raise DatasetBindingError("Dataset attachment needs an original four- or five-field task tuple.")
    binding = service.bind(_task_id(task[0]))

    def attach():
        if task_metadata is None:
            return
        entry = task_metadata.get(task[0])
        if isinstance(entry, dict) and entry.get("task_data") is task:
            entry["dataset_binding"] = binding

    if metadata_lock is None:
        attach()
    else:
        with metadata_lock:
            attach()
    return binding


def render_task_inputs(inputs, values):
    """Make fresh authored-input containers, expanding dataset aliases only.

    The caller excludes templates whose native placeholders must be processed
    later. Mapping keys are field names, not prompt text, and remain unchanged.
    Neither the original inputs nor the frozen dataset values are mutated.
    """
    if isinstance(inputs, str):
        return render_template(inputs, values)
    if isinstance(inputs, Mapping):
        return {key: render_task_inputs(value, values) for key, value in inputs.items()}
    if isinstance(inputs, tuple):
        return tuple(render_task_inputs(value, values) for value in inputs)
    if isinstance(inputs, list):
        return [render_task_inputs(value, values) for value in inputs]
    return inputs


def validate_task_templates(templates, values):
    """Validate authored aliases without expanding stage-specific templates."""
    if isinstance(templates, str):
        for alias in dataset_placeholders(templates):
            if alias not in values:
                raise DatasetTemplateError("No dataset column is mapped to alias {!r} ({{{{ds:{}}}}}).".format(alias, alias))
    elif isinstance(templates, Mapping):
        for value in templates.values():
            validate_task_templates(value, values)
    elif isinstance(templates, (tuple, list)):
        for value in templates:
            validate_task_templates(value, values)


def render_question_task(task, values, append_alias="", separator=DEFAULT_QUESTION_SEPARATOR):
    """Expand one questions-file task's line for the task that owns it.

    ``task`` is an existing four-field questions-file task tuple
    ``(task_id, file_name, index, line)``; its identity, file name and index are returned
    untouched so task IDs, retries and recovery keep working as before. Only ``line`` is
    expanded, through :func:`dataset_templates.render_question_line`, which handles the
    ``{{ds:alias}}`` placeholders written into the line and the single appended alias value.

    ``values`` is that task's frozen binding. ``None`` means the run has no dataset
    attachments, and the line is then returned exactly as read.
    """
    if not isinstance(task, tuple) or len(task) not in (4, 5):
        raise DatasetTemplateError(
            "A questions-file task must be a four- or five-field tuple; got {!r}."
            .format(type(task).__name__)
        )
    if not isinstance(task[3], str):
        raise DatasetTemplateError(
            "The question line of task {!r} must be a string.".format(task[0])
        )
    line = render_question_line(task[3], values, append_alias=append_alias, separator=separator)
    return (task[0], task[1], task[2], line) + tuple(task[4:])
