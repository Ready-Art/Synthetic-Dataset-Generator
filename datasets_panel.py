"""Names-only dataset configuration panel, independent of generation workers.

The panel owns editable source definitions and alias mappings. Metadata inspection
runs on at most two daemon threads; Tk widgets are touched only by the UI thread.
Importing columns calls only the schema interface and never previews cell values.
Some formats, such as JSON, read records internally to discover their keys.
Settings can be loaded and collected without starting a network request.
"""

from queue import Empty, Full, Queue
import threading
import tkinter as tk
from tkinter import filedialog, ttk

from dataset_column_menu import ColumnVisibilityMenu
from dataset_settings import (
    apply_schema, new_source, normalize_config, source_selector, source_spec,
    validate_config, visible_column_names, with_visible_columns,
)
from dataset_sources import open_source
from dataset_templates import validate_alias


_KINDS = (
    ("csv", "CSV"),
    ("tsv", "TSV"),
    ("json", "JSON array"),
    ("jsonl", "JSONL"),
    ("parquet", "Parquet"),
    ("huggingface", "Hugging Face Hub"),
    ("hf_disk", "Saved HF dataset"),
)
_KIND_LABELS = dict(_KINDS)
_LABEL_KINDS = {label: kind for kind, label in _KINDS}


def _metadata_worker(requests, results, stopped):
    """Read immutable requests without keeping a panel or Tk object alive."""
    while not stopped.is_set():
        try:
            request = requests.get(timeout=0.2)
        except Empty:
            continue
        if request is None:
            return
        token, source_id, selector, spec, cancelled = request
        if stopped.is_set() or cancelled.is_set():
            continue
        source = None
        schema = None
        error = None
        try:
            source = open_source(spec)
            schema = source.schema()
        except Exception as exc:
            error = str(exc) or type(exc).__name__
        finally:
            if source is not None:
                try:
                    source.close()
                except Exception as exc:
                    if error is None:
                        error = "Could not close the dataset reader: {}".format(exc)
        if not stopped.is_set() and not cancelled.is_set():
            results.put((token, source_id, selector, schema, error))


class DatasetsPanel(ttk.Frame):
    """Edit a variable number of dataset sources without displaying their rows.

    ``load_config`` and ``get_config`` accept/return the dataset section, shaped
    as ``{"sources": [...]}``. ``validate`` performs local settings validation.
    ``import_columns`` is the sole entry point that performs source I/O, always
    in the background. Each source keeps its aliases and column visibility.
    """

    def __init__(self, master, **kwargs):
        kwargs.setdefault("padding", 8)
        super().__init__(master, **kwargs)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(3, weight=1)
        self._sources = []
        self._selected_source_id = None
        self._alias_column = None
        self._tree_columns = {}
        self._loading = False
        self._destroyed = False
        self._serial = 0
        self._requests = {}
        self._request_queue = Queue(maxsize=2)
        self._result_queue = Queue()
        self._stopped = threading.Event()
        self._workers = []
        self._poll_id = None

        self.name_var = tk.StringVar(self)
        self.enabled_var = tk.BooleanVar(self, value=True)
        self.kind_var = tk.StringVar(self, value="CSV")
        self.location_var = tk.StringVar(self)
        self.config_var = tk.StringVar(self)
        self.split_var = tk.StringVar(self)
        self.revision_var = tk.StringVar(self)
        self.selected_column_var = tk.StringVar(self)
        self.alias_var = tk.StringVar(self)
        self.count_var = tk.StringVar(self)
        self.status_var = tk.StringVar(self)
        self._build_widgets()

        for variable in (self.name_var, self.enabled_var, self.kind_var,
                         self.location_var, self.config_var, self.split_var,
                         self.revision_var):
            variable.trace_add("write", self._source_field_changed)
        self.bind("<Destroy>", self._on_destroy, add="+")
        self.bind("<Configure>", self._on_resize, add="+")
        self.load_config({})

    def _build_widgets(self):
        toolbar = ttk.Frame(self)
        toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        toolbar.columnconfigure(1, weight=1)
        ttk.Label(toolbar, text="Dataset").grid(row=0, column=0, padx=(0, 6))
        self.source_combo = ttk.Combobox(toolbar, state="readonly", width=20)
        self.source_combo.grid(row=0, column=1, sticky="ew")
        self.source_combo.bind("<<ComboboxSelected>>", self._source_selected)
        self.add_button = ttk.Button(toolbar, text="Add", command=self.add_source, width=6)
        self.add_button.grid(row=0, column=2, padx=(6, 0))
        self.remove_button = ttk.Button(toolbar, text="Remove", command=self.remove_source, width=8)
        self.remove_button.grid(row=0, column=3, padx=(6, 0))

        self.details_frame = ttk.Frame(self)
        self.details_frame.grid(row=1, column=0, sticky="ew")
        self.details_frame.columnconfigure(1, weight=1)
        name_row = ttk.Frame(self.details_frame)
        name_row.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 5))
        name_row.columnconfigure(1, weight=1)
        ttk.Label(name_row, text="Name").grid(row=0, column=0, padx=(0, 6))
        self.name_entry = ttk.Entry(name_row, textvariable=self.name_var, width=1)
        self.name_entry.grid(row=0, column=1, sticky="ew")
        self.enabled_check = ttk.Checkbutton(name_row, text="Enabled", variable=self.enabled_var)
        self.enabled_check.grid(row=0, column=2, padx=(10, 0))

        source_row = ttk.Frame(self.details_frame)
        source_row.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 5))
        source_row.columnconfigure(3, weight=1)
        ttk.Label(source_row, text="Type").grid(row=0, column=0, padx=(0, 6))
        self.kind_combo = ttk.Combobox(source_row, textvariable=self.kind_var,
                                       values=tuple(label for _, label in _KINDS),
                                       state="readonly", width=17)
        self.kind_combo.grid(row=0, column=1, sticky="w")
        ttk.Label(source_row, text="Location").grid(row=0, column=2, padx=(10, 6))
        self.location_entry = ttk.Entry(source_row, textvariable=self.location_var, width=1)
        self.location_entry.grid(row=0, column=3, sticky="ew")
        self.browse_button = ttk.Button(source_row, text="Browse", command=self._browse, width=7)
        self.browse_button.grid(row=0, column=4, padx=(6, 0))

        self.selectors_frame = ttk.Frame(self.details_frame)
        self.selectors_frame.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(0, 5))
        for column in (1, 3, 5):
            self.selectors_frame.columnconfigure(column, weight=1)
        self.config_label = ttk.Label(self.selectors_frame, text="Config")
        self.config_label.grid(row=0, column=0, padx=(0, 6))
        self.config_entry = ttk.Entry(self.selectors_frame, textvariable=self.config_var, width=8)
        self.config_entry.grid(row=0, column=1, sticky="ew")
        self.split_label = ttk.Label(self.selectors_frame, text="Split")
        self.split_label.grid(row=0, column=2, padx=(10, 6))
        self.split_entry = ttk.Entry(self.selectors_frame, textvariable=self.split_var, width=8)
        self.split_entry.grid(row=0, column=3, sticky="ew")
        self.revision_label = ttk.Label(self.selectors_frame, text="Revision")
        self.revision_label.grid(row=0, column=4, padx=(10, 6))
        self.revision_entry = ttk.Entry(self.selectors_frame, textvariable=self.revision_var, width=8)
        self.revision_entry.grid(row=0, column=5, sticky="ew")

        mapping_toolbar = ttk.Frame(self)
        mapping_toolbar.grid(row=2, column=0, sticky="ew", pady=(1, 5))
        mapping_toolbar.columnconfigure(2, weight=1)
        self.import_button = ttk.Button(mapping_toolbar, text="Import columns",
                                        command=self.import_columns, width=15)
        self.import_button.grid(row=0, column=0, sticky="w")
        self.column_menu = ColumnVisibilityMenu(mapping_toolbar, on_change=self._visibility_changed)
        self.column_menu.grid(row=0, column=1, padx=(6, 0), sticky="w")
        ttk.Label(mapping_toolbar, textvariable=self.count_var, width=1, anchor="e").grid(
            row=0, column=2, sticky="ew", padx=(8, 0)
        )

        self.table_frame = ttk.Frame(self)
        self.table_frame.grid(row=3, column=0, sticky="nsew")
        self.table_frame.columnconfigure(0, weight=1)
        self.table_frame.rowconfigure(0, weight=1)
        self.columns_tree = ttk.Treeview(self.table_frame,
                                         columns=("column", "alias", "placeholder"),
                                         show="headings", selectmode="browse", height=5)
        self.columns_tree.heading("column", text="Column name")
        self.columns_tree.heading("alias", text="Alias")
        self.columns_tree.heading("placeholder", text="Prompt placeholder")
        # Data never drives requested widget width. Long values scroll or remain
        # accessible in the single selected-row editor below the viewport.
        self.columns_tree.column("column", width=260, minwidth=110, stretch=True)
        self.columns_tree.column("alias", width=145, minwidth=80, stretch=False)
        self.columns_tree.column("placeholder", width=205, minwidth=110, stretch=False)
        self.columns_tree.grid(row=0, column=0, sticky="nsew")
        self.table_vscroll = ttk.Scrollbar(self.table_frame, orient="vertical", command=self.columns_tree.yview)
        self.table_vscroll.grid(row=0, column=1, sticky="ns")
        self.table_hscroll = ttk.Scrollbar(self.table_frame, orient="horizontal", command=self.columns_tree.xview)
        self.table_hscroll.grid(row=1, column=0, sticky="ew")
        self.columns_tree.configure(yscrollcommand=self.table_vscroll.set, xscrollcommand=self.table_hscroll.set)
        self.columns_tree.bind("<<TreeviewSelect>>", self._column_selected)
        self.columns_tree.bind("<Return>", self._focus_alias)
        self.columns_tree.bind("<Double-1>", self._focus_alias)

        editor = ttk.Frame(self)
        editor.grid(row=4, column=0, sticky="ew", pady=(5, 0))
        editor.columnconfigure(1, weight=1)
        ttk.Label(editor, text="Column").grid(row=0, column=0, padx=(0, 6))
        self.selected_column_entry = ttk.Entry(editor, textvariable=self.selected_column_var,
                                               state="readonly", width=1)
        self.selected_column_entry.grid(row=0, column=1, columnspan=4, sticky="ew")
        ttk.Label(editor, text="Alias").grid(row=1, column=0, padx=(0, 6), pady=(4, 0))
        self.alias_entry = ttk.Entry(editor, textvariable=self.alias_var, width=1)
        self.alias_entry.grid(row=1, column=1, sticky="ew", pady=(4, 0))
        self.alias_entry.bind("<Return>", self._apply_alias_event)
        self.alias_entry.bind("<FocusOut>", self._alias_focus_out)
        self.apply_alias_button = ttk.Button(editor, text="Apply", command=self.apply_alias, width=6)
        self.apply_alias_button.grid(row=1, column=2, padx=(6, 0), pady=(4, 0))
        self.clear_alias_button = ttk.Button(editor, text="Clear", command=self.clear_alias, width=6)
        self.clear_alias_button.grid(row=1, column=3, padx=(6, 0), pady=(4, 0))
        self.copy_button = ttk.Button(editor, text="Copy placeholder", command=self.copy_placeholder)
        self.copy_button.grid(row=1, column=4, padx=(6, 0), pady=(4, 0))
        self.status_label = ttk.Label(self, textvariable=self.status_var, wraplength=600,
                                       justify="left", anchor="w", width=1)
        self.status_label.grid(row=5, column=0, sticky="ew", pady=(5, 0))

    def _source(self, source_id=None):
        source_id = self._selected_source_id if source_id is None else source_id
        return next((source for source in self._sources if source["source_id"] == source_id), None)

    def _replace_source(self, replacement):
        for index, source in enumerate(self._sources):
            if source["source_id"] == replacement["source_id"]:
                self._sources[index] = replacement
                return

    def _kind(self):
        return _LABEL_KINDS.get(self.kind_var.get(), self.kind_var.get())

    def _set_status(self, message, error=False):
        # Bound error text as well as width, so a backend listing hundreds of
        # missing names cannot consume the mapping viewport.
        message = str(message).replace("\r", " ").replace("\n", " ")
        self.status_var.set(message if len(message) <= 260 else message[:257] + "...")
        self.status_label.configure(foreground="#b64242" if error else "")

    def _on_resize(self, event):
        if event.widget is self:
            self.status_label.configure(wraplength=max(120, event.width - 24))

    def _refresh_source_choices(self):
        labels = []
        selected_index = None
        for index, source in enumerate(self._sources):
            name = source["name"] or "Unnamed dataset"
            name = name.replace("\r", " ").replace("\n", " ")
            if len(name) > 65:
                name = name[:62] + "..."
            labels.append("{}. {}".format(index + 1, name))
            if source["source_id"] == self._selected_source_id:
                selected_index = index
        self.source_combo.configure(values=tuple(labels), state="readonly" if labels else "disabled")
        if selected_index is not None:
            self.source_combo.current(selected_index)
        else:
            self.source_combo.set("")

    def _update_kind_controls(self):
        source = self._source()
        active = source is not None
        kind = self._kind()
        hub = active and kind == "huggingface"
        saved = active and kind == "hf_disk"
        if hub or saved:
            self.selectors_frame.grid()
        else:
            self.selectors_frame.grid_remove()
        for widget in (self.config_label, self.config_entry, self.revision_label, self.revision_entry):
            if hub:
                widget.grid()
            else:
                widget.grid_remove()
        # Split expands across the selector row for a saved DatasetDict.
        self.split_label.grid_configure(column=2 if hub else 0, padx=(10, 6) if hub else (0, 6))
        self.split_entry.grid_configure(column=3 if hub else 1, columnspan=1 if hub else 5)
        self.browse_button.configure(state="normal" if active and not hub else "disabled")
        self.config_entry.configure(state="normal" if hub else "disabled")
        self.split_entry.configure(state="normal" if hub or saved else "disabled")
        self.revision_entry.configure(state="normal" if hub else "disabled")

    def _save_source_fields(self):
        source = self._source()
        if source is None or self._loading:
            return
        old_selector = source_selector(source)
        kind = self._kind()
        source.update({
            "name": self.name_var.get(), "enabled": bool(self.enabled_var.get()),
            "kind": kind, "location": self.location_var.get(),
            "config": (self.config_var.get() or None) if kind == "huggingface" else None,
            "split": (self.split_var.get() or None) if kind in ("huggingface", "hf_disk") else None,
            "revision": (self.revision_var.get() or None) if kind == "huggingface" else None,
        })
        if source_selector(source) != old_selector:
            self._cancel_request(source["source_id"])
            self._show_source_status()

    def _source_field_changed(self, *_args):
        if self._loading or self._destroyed:
            return
        self._save_source_fields()
        self._refresh_source_choices()
        self._update_kind_controls()
        self._update_import_button()

    def _commit_alias_editor(self):
        source = self._source()
        if self._loading or source is None or self._alias_column is None:
            return
        alias = self.alias_var.get()
        if alias:
            source["column_aliases"][self._alias_column] = alias
        else:
            source["column_aliases"].pop(self._alias_column, None)
        for item, column in self._tree_columns.items():
            if column == self._alias_column:
                self.columns_tree.item(item, values=(column, alias, self._placeholder(alias)))
                break
        self._update_count()

    @staticmethod
    def _placeholder(alias):
        if not alias:
            return ""
        try:
            validate_alias(alias)
        except ValueError:
            return "Invalid alias"
        return "{{{{ds:{}}}}}".format(alias)

    def _set_alias_editor(self, column):
        source = self._source()
        self._alias_column = column
        self.selected_column_var.set(column if column is not None else "")
        self.alias_var.set(source["column_aliases"].get(column, "") if source is not None and column is not None else "")
        active = source is not None and column is not None
        for widget in (self.alias_entry, self.apply_alias_button, self.clear_alias_button, self.copy_button):
            widget.configure(state="normal" if active else "disabled")
        self.selected_column_entry.configure(state="readonly" if active else "disabled")

    def _update_count(self):
        source = self._source()
        if source is None:
            self.count_var.set("")
        else:
            self.count_var.set("{} shown of {} · {} mapped".format(
                len(visible_column_names(source)), len(source["columns"]), len(source["column_aliases"])
            ))

    def _render_columns(self, selected=None):
        source = self._source()
        if selected is None:
            selected = self._alias_column
        children = self.columns_tree.get_children()
        if children:
            self.columns_tree.delete(*children)
        self._tree_columns = {}
        selected_item = None
        if source is not None:
            for index, column in enumerate(visible_column_names(source)):
                item = "column-{}".format(index)
                alias = source["column_aliases"].get(column, "")
                self.columns_tree.insert("", "end", iid=item, values=(column, alias, self._placeholder(alias)))
                self._tree_columns[item] = column
                if column == selected:
                    selected_item = item
        if selected_item is not None:
            self.columns_tree.selection_set(selected_item)
            self.columns_tree.focus(selected_item)
            self.columns_tree.see(selected_item)
            self._set_alias_editor(selected)
        else:
            self._set_alias_editor(None)
        self._update_count()

    def _load_selected(self):
        source = self._source()
        self._loading = True
        try:
            self.name_var.set(source["name"] if source else "")
            self.enabled_var.set(source["enabled"] if source else True)
            self.kind_var.set(_KIND_LABELS.get(source["kind"], source["kind"]) if source else "CSV")
            self.location_var.set(source["location"] if source else "")
            self.config_var.set((source["config"] or "") if source else "")
            self.split_var.set((source["split"] or "") if source else "")
            self.revision_var.set((source["revision"] or "") if source else "")
            self._alias_column = None
            self.column_menu.set_columns(source["columns"] if source else (),
                                         source["visible_columns"] if source else None)
            self._render_columns()
        finally:
            self._loading = False
        for widget in (self.name_entry, self.location_entry, self.enabled_check, self.remove_button):
            widget.configure(state="normal" if source else "disabled")
        self.kind_combo.configure(state="readonly" if source else "disabled")
        self.column_menu.configure(state="normal" if source and source["columns"] else "disabled")
        self._refresh_source_choices()
        self._update_kind_controls()
        self._update_import_button()
        self._show_source_status()

    def _show_source_status(self):
        source = self._source()
        if source is None:
            self._set_status("Add a dataset, import its column names, then choose aliases for the columns you need.")
        elif source["source_id"] in self._requests:
            self._set_status("Reading column names. Dataset records are not displayed.")
        elif source.get("schema_selector") is not None and source_selector(source) != source["schema_selector"]:
            self._set_status("Source settings changed. Import columns again before using these mappings.")
        elif source["columns"]:
            self._set_status("Select a column to edit its alias. Hiding a column keeps its alias mapping.")
        elif source.get("schema_fingerprint"):
            self._set_status("This source has no columns.")
        elif source["kind"] == "huggingface":
            self._set_status("Enter owner/dataset as the location. Set a config or split when the source has multiple choices.")
        else:
            self._set_status("Import column names to create mappings. Dataset records are not displayed.")

    def _source_selected(self, _event=None):
        index = self.source_combo.current()
        if 0 <= index < len(self._sources):
            self.select_source(self._sources[index]["source_id"])

    def select_source(self, source_id):
        """Select an existing source, preserving the outgoing source's edits."""
        if self._source(source_id) is None:
            raise ValueError("Unknown dataset source ID.")
        self._save_source_fields()
        self._commit_alias_editor()
        if self._selected_source_id != source_id:
            self._cancel_request(self._selected_source_id)
        self.column_menu.close()
        self._selected_source_id = source_id
        self._load_selected()

    def add_source(self):
        """Append a new draft source without importing metadata or records."""
        self._save_source_fields()
        self._commit_alias_editor()
        self._cancel_request(self._selected_source_id)
        self.column_menu.close()
        source = new_source(self._sources)
        self._sources.append(source)
        self._selected_source_id = source["source_id"]
        self._load_selected()
        self.name_entry.focus_set()
        self.name_entry.selection_range(0, tk.END)
        return source["source_id"]

    def remove_source(self):
        """Remove the selected source from the editor's unsaved configuration."""
        source = self._source()
        if source is None:
            return
        index = self._sources.index(source)
        self._cancel_request(source["source_id"])
        self.column_menu.close()
        self._sources.pop(index)
        self._selected_source_id = self._sources[min(index, len(self._sources) - 1)]["source_id"] if self._sources else None
        self._load_selected()

    def _column_selected(self, _event=None):
        if self._loading:
            return
        selection = self.columns_tree.selection()
        column = self._tree_columns.get(selection[0]) if selection else None
        if column != self._alias_column:
            self._commit_alias_editor()
            self._set_alias_editor(column)

    def _focus_alias(self, _event=None):
        if self._alias_column is not None:
            self.alias_entry.focus_set()
            self.alias_entry.selection_range(0, tk.END)
        return "break"

    def _apply_alias_event(self, _event=None):
        self.apply_alias()
        return "break"

    def _alias_focus_out(self, _event=None):
        if not self._destroyed:
            self._commit_alias_editor()

    def apply_alias(self):
        """Capture the selected mapping; report invalid alias syntax inline."""
        self._commit_alias_editor()
        alias = self.alias_var.get()
        if self._alias_column is None:
            return
        if alias:
            try:
                validate_alias(alias)
            except ValueError as exc:
                self._set_status(str(exc), error=True)
                self.alias_entry.focus_set()
                return
        self._set_status("Alias saved in this editor." if alias else "Alias mapping cleared.")

    def clear_alias(self):
        if self._alias_column is not None:
            self.alias_var.set("")
            self.apply_alias()

    def copy_placeholder(self):
        self._commit_alias_editor()
        alias = self.alias_var.get()
        try:
            validate_alias(alias)
        except ValueError as exc:
            self._set_status("Choose a valid alias before copying. {}".format(exc), error=True)
            return
        self.clipboard_clear()
        self.clipboard_append(self._placeholder(alias))
        self._set_status("Placeholder copied.")

    def _visibility_changed(self, selection):
        if self._loading or self._destroyed or self._source() is None:
            return
        self._commit_alias_editor()
        self._replace_source(with_visible_columns(self._source(), selection))
        self._render_columns()

    def _browse(self):
        kind = self._kind()
        if kind == "huggingface":
            return
        if kind == "hf_disk":
            location = filedialog.askdirectory(parent=self, title="Choose a saved Hugging Face dataset")
        else:
            patterns = {"csv": "*.csv", "tsv": "*.tsv", "json": "*.json",
                        "jsonl": "*.jsonl *.ndjson", "parquet": "*.parquet"}
            location = filedialog.askopenfilename(
                parent=self, title="Choose a dataset file",
                filetypes=((_KIND_LABELS.get(kind, "Dataset"), patterns.get(kind, "*")), ("All files", "*")),
            )
        if location:
            self.location_var.set(location)

    def _start_workers(self):
        if self._workers:
            return
        for index in range(2):
            worker = threading.Thread(target=_metadata_worker,
                                      args=(self._request_queue, self._result_queue, self._stopped),
                                      name="dataset-schema-{}".format(index + 1), daemon=True)
            worker.start()
            self._workers.append(worker)

    def _cancel_request(self, source_id):
        request = self._requests.pop(source_id, None)
        if request is not None:
            request[1].set()

    def _cancel_all(self):
        for _, cancelled in self._requests.values():
            cancelled.set()
        self._requests.clear()

    def _update_import_button(self):
        source = self._source()
        loading = source is not None and source["source_id"] in self._requests
        self.import_button.configure(text="Importing..." if loading else "Import columns",
                                     state="normal" if source and not loading else "disabled")

    def import_columns(self):
        """Start names-only metadata inspection using a frozen source definition."""
        if self._destroyed or self._source() is None:
            return
        self._save_source_fields()
        self._commit_alias_editor()
        source = self._source()
        if source["source_id"] in self._requests:
            return
        try:
            spec = source_spec(source)
            selector = source_selector(source)
        except ValueError as exc:
            self._set_status(str(exc), error=True)
            return
        self._serial += 1
        token = self._serial
        cancelled = threading.Event()
        request = (token, source["source_id"], selector, spec, cancelled)
        try:
            self._request_queue.put_nowait(request)
        except Full:
            self._set_status("Earlier imports are finishing. Try importing again shortly.")
            return
        self._requests[source["source_id"]] = (token, cancelled)
        self._start_workers()
        self._update_import_button()
        self._show_source_status()
        if self._poll_id is None:
            self._poll_id = self.after(80, self._poll_results)

    def _poll_results(self):
        self._poll_id = None
        if self._destroyed:
            return
        while True:
            try:
                token, source_id, selector, schema, error = self._result_queue.get_nowait()
            except Empty:
                break
            request = self._requests.get(source_id)
            source = self._source(source_id)
            if request is None or request[0] != token or source is None:
                continue
            self._requests.pop(source_id, None)
            if source_selector(source) != selector or source_id != self._selected_source_id:
                continue
            self._commit_alias_editor()
            selected = self._alias_column
            if error is None:
                try:
                    updated = apply_schema(self._source(source_id), schema)
                except ValueError as exc:
                    error = str(exc)
                else:
                    self._replace_source(updated)
                    self.column_menu.set_columns(updated["columns"], updated["visible_columns"])
                    self.column_menu.configure(state="normal" if updated["columns"] else "disabled")
                    self._render_columns(selected)
            if error is not None:
                self._set_status("Import failed: {}".format(error), error=True)
            else:
                self._set_status("Imported {} column names. Select a column to assign an alias.".format(len(updated["columns"])))
        self._update_import_button()
        if self._requests:
            self._poll_id = self.after(80, self._poll_results)

    def load_config(self, data):
        """Replace editor state locally; discard pending metadata results."""
        config = normalize_config(data)
        self._cancel_all()
        self.column_menu.close()
        self._sources = config["sources"]
        self._selected_source_id = self._sources[0]["source_id"] if self._sources else None
        self._load_selected()

    def get_config(self):
        """Return detached settings, including current edits, without I/O."""
        self._save_source_fields()
        self._commit_alias_editor()
        return normalize_config({"sources": self._sources})

    def validate(self):
        """Raise ValueError for invalid mappings/settings; never fetch metadata."""
        validate_config(self.get_config())

    def _on_destroy(self, event):
        if event.widget is not self or self._destroyed:
            return
        self._destroyed = True
        self._cancel_all()
        self._stopped.set()
        if self._poll_id is not None:
            try:
                self.after_cancel(self._poll_id)
            except tk.TclError:
                pass
            self._poll_id = None
        # Daemon workers either observe the event or finish their current read.
        # No join or executor shutdown can block closing the configuration UI.
        for _ in self._workers:
            try:
                self._request_queue.put_nowait(None)
            except Full:
                break
