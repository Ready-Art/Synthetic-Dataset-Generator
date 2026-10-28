"""A bounded, searchable column-visibility picker for the Datasets panel.

The button and popup contain column names only. Selection changes never alter
aliases or dataset records; those belong to the surrounding panel/model. A
single Treeview displays the names, so a thousand columns do not create a
thousand Tk widgets. No application-wide event bindings are installed.
"""

import sys
import tkinter as tk
from tkinter import font as tkfont, ttk


class ColumnVisibilityMenu(ttk.Button):
    """Select visible column names without allowing schema size to resize a UI.

    ``None`` means all columns, including names imported later. An explicit empty
    sequence means none. Other selections use original schema order and are
    canonicalized to ``None`` if they contain every current column. An explicit
    empty selection stays empty when the schema itself is empty.

    ``set_columns`` never calls ``on_change``. User selection methods and popup
    controls call ``on_change(None or tuple_of_names)`` when the selection changes.
    ``popup``, ``tree``, ``search_var``, ``search_entry``, ``all_var``, and
    ``all_button`` are available while the popup is open, otherwise ``None``.
    """

    def __init__(self, master, on_change, **kwargs):
        if on_change is not None and not callable(on_change):
            raise TypeError("on_change must be callable or None.")
        self._on_change = on_change
        self._columns = ()
        self._column_set = set()
        self._folded_columns = ()
        self._selected = None
        self._iid_names = {}
        self._focus_after = None
        self._search_trace = None
        self._previous_grab = None
        self._previous_grab_status = None
        self._previous_focus = None
        self._closing = False
        self._destroying = False
        self.popup = self.tree = self.search_var = self.search_entry = None
        self.all_var = self.all_button = self._status_var = None
        kwargs.pop("text", None)
        kwargs.pop("textvariable", None)
        kwargs["command"] = self._toggle_popup
        super().__init__(master, text="Columns: All \u25be", **kwargs)
        self.bind("<Destroy>", self._destroyed, add="+")

    def set_columns(self, columns, selection=None):
        """Replace the schema/selection silently; retain exact names and order."""
        if isinstance(columns, str):
            raise TypeError("Column names must be a sequence, not one string.")
        names = tuple(columns)
        if any(not isinstance(name, str) for name in names):
            raise TypeError("Column names must be strings.")
        if len(set(names)) != len(names):
            raise ValueError("Column names must be unique.")
        if isinstance(selection, str):
            raise TypeError("Selection must be a sequence of names or None.")
        selected = None if selection is None else set(selection)
        if selected is not None and any(not isinstance(name, str) for name in selected):
            raise TypeError("Selected column names must be strings.")
        self._columns = names
        self._column_set = set(names)
        self._folded_columns = tuple((name, name.casefold()) for name in names)
        self._selected = self._canonicalize(selected)
        self._update_caption()
        if self.popup is not None:
            self._populate_tree()

    def get_selection(self):
        """Return None for All, or visible names in their original schema order."""
        if self._selected is None:
            return None
        return tuple(name for name in self._columns if name in self._selected)

    def toggle_column(self, name):
        """Toggle one named column, including when it is hidden by the search."""
        if name not in self._column_set:
            raise ValueError("Unknown column name: {!r}.".format(name))
        selected = set(self._column_set if self._selected is None else self._selected)
        if name in selected:
            selected.remove(name)
        else:
            selected.add(name)
        self._change_selection(selected)

    def select_all(self):
        """Select the whole dataset, irrespective of the popup search query."""
        self._change_selection(None)

    def clear_selection(self):
        """Hide all columns, irrespective of the popup search query."""
        self._change_selection(set())

    def _canonicalize(self, selected):
        if selected is None:
            return None
        selected = selected.intersection(self._column_set)
        return None if self._columns and selected == self._column_set else selected

    def _change_selection(self, selected):
        selected = self._canonicalize(selected)
        if selected == self._selected:
            return
        self._selected = selected
        self._update_caption()
        self._refresh_checks()
        if self._on_change is not None:
            self._on_change(self.get_selection())

    def _update_caption(self):
        text = "Columns: All \u25be" if self._selected is None else "Columns: {} of {} \u25be".format(
            len(self._selected), len(self._columns)
        )
        self.configure(text=text)

    def _toggle_popup(self):
        if self.popup is None:
            self.open()
        else:
            self.close()

    def _pixels(self, value):
        return max(1, round(value * self.winfo_fpixels("1i") / 96.0))

    def _work_area(self):
        """Use the current Windows monitor's work area; portable Tk screen otherwise."""
        left, top = self.winfo_vrootx(), self.winfo_vrooty()
        bounds = (left, top, left + self.winfo_screenwidth(), top + self.winfo_screenheight())
        user32 = None
        if sys.platform == "win32":
            try:
                import ctypes
                from ctypes import wintypes

                class MonitorInfo(ctypes.Structure):
                    _fields_ = [("cbSize", wintypes.DWORD), ("rcMonitor", wintypes.RECT),
                                ("rcWork", wintypes.RECT), ("dwFlags", wintypes.DWORD)]

                user32 = ctypes.windll.user32
                user32.MonitorFromWindow.argtypes = [wintypes.HWND, wintypes.DWORD]
                user32.MonitorFromWindow.restype = wintypes.HANDLE
                user32.GetMonitorInfoW.argtypes = [wintypes.HANDLE, ctypes.POINTER(MonitorInfo)]
                monitor = user32.MonitorFromWindow(self.winfo_toplevel().winfo_id(), 2)
                info = MonitorInfo(cbSize=ctypes.sizeof(MonitorInfo))
                if user32.GetMonitorInfoW(monitor, ctypes.byref(info)):
                    area = info.rcWork
                    bounds = (area.left, area.top, area.right, area.bottom)
            except (AttributeError, OSError, tk.TclError):
                user32 = None
        return bounds, user32

    def _place_popup(self, popup):
        (left, top, right, bottom), user32 = self._work_area()
        margin = self._pixels(8)
        width = min(self._pixels(440), max(1, right - left - margin * 2))
        height = min(self._pixels(380), max(1, bottom - top - margin * 2))
        x = max(left, min(self.winfo_rootx(), right - width))
        below = self.winfo_rooty() + self.winfo_height()
        above = self.winfo_rooty() - height
        y = below if below + height <= bottom else above
        y = max(top, min(y, bottom - height))
        popup.geometry("{}x{}".format(width, height))
        popup.minsize(width, height)
        popup.maxsize(width, height)
        if user32 is not None:
            # Follow the existing layout helper's native positioning approach:
            # Tk's negative geometry offsets are not negative desktop coordinates.
            try:
                from ctypes import wintypes
                user32.GetAncestor.argtypes = [wintypes.HWND, wintypes.UINT]
                user32.GetAncestor.restype = wintypes.HWND
                user32.SetWindowPos.argtypes = [wintypes.HWND, wintypes.HWND, wintypes.INT,
                                               wintypes.INT, wintypes.INT, wintypes.INT, wintypes.UINT]
                popup.update_idletasks()
                if user32.SetWindowPos(user32.GetAncestor(popup.winfo_id(), 2), None,
                                       x, y, 0, 0, 0x0015):
                    return
            except (AttributeError, OSError, tk.TclError):
                pass
        popup.geometry("{}x{}+{}+{}".format(width, height, max(0, x), max(0, y)))

    def open(self):
        """Open a bounded popup; repeated calls focus the existing search field."""
        if self._destroying or not self.winfo_exists() or self.instate(["disabled"]):
            return
        if self.popup is not None:
            self.search_entry.focus_set()
            return
        self.update_idletasks()
        if not self.winfo_ismapped():
            return
        self._previous_grab = self.grab_current()
        self._previous_grab_status = self._previous_grab.grab_status() if self._previous_grab is not None else None
        self._previous_focus = self.focus_get()
        popup = tk.Toplevel(self)
        self.popup = popup
        popup.withdraw()
        popup.overrideredirect(True)
        popup.transient(self.winfo_toplevel())
        popup.columnconfigure(0, weight=1)
        popup.rowconfigure(0, weight=1)
        frame = ttk.Frame(popup, padding=self._pixels(6), relief="solid", borderwidth=1)
        frame.grid(row=0, column=0, sticky="nsew")
        frame.columnconfigure(1, weight=1)
        frame.rowconfigure(2, weight=1)
        self.all_var = tk.BooleanVar(master=popup)
        self.all_button = ttk.Checkbutton(frame, text="All", variable=self.all_var, command=self._all_changed)
        self.all_button.grid(row=0, column=0, sticky="w", pady=(0, 4))
        self._status_var = tk.StringVar(master=popup)
        ttk.Label(frame, textvariable=self._status_var, anchor="e").grid(row=0, column=1, sticky="e")
        ttk.Label(frame, text="Search:").grid(row=1, column=0, sticky="w", padx=(0, 6), pady=(0, 6))
        self.search_var = tk.StringVar(master=popup)
        self.search_entry = ttk.Entry(frame, textvariable=self.search_var, width=16)
        self.search_entry.grid(row=1, column=1, sticky="ew", pady=(0, 6))
        table = ttk.Frame(frame)
        table.grid(row=2, column=0, columnspan=2, sticky="nsew")
        table.columnconfigure(0, weight=1)
        table.rowconfigure(0, weight=1)
        self.tree = ttk.Treeview(table, columns=("selected", "name"), show="headings", selectmode="browse", height=10)
        self.tree.heading("selected", text="")
        self.tree.heading("name", text="Column name")
        self.tree.column("selected", width=self._pixels(32), minwidth=self._pixels(32), stretch=False, anchor="center")
        self.tree.column("name", width=self._pixels(320), minwidth=self._pixels(120), stretch=False)
        self.tree.grid(row=0, column=0, sticky="nsew")
        vertical = ttk.Scrollbar(table, orient="vertical", command=self.tree.yview)
        vertical.grid(row=0, column=1, sticky="ns")
        horizontal = ttk.Scrollbar(table, orient="horizontal", command=self.tree.xview)
        horizontal.grid(row=1, column=0, sticky="ew")
        self.tree.configure(yscrollcommand=vertical.set, xscrollcommand=horizontal.set)
        self.tree.bind("<Button-1>", self._row_clicked)
        self.tree.bind("<space>", self._row_space)
        self.search_entry.bind("<Down>", self._focus_tree)
        popup.bind("<Escape>", self._escape)
        popup.bind("<ButtonPress-1>", self._popup_clicked, add="+")
        popup.bind("<FocusOut>", self._focus_out, add="+")
        popup.bind("<Destroy>", self._popup_destroyed, add="+")
        self._search_trace = self.search_var.trace_add("write", self._search_changed)
        self._populate_tree()
        self._place_popup(popup)
        popup.deiconify()
        popup.update_idletasks()
        try:
            popup.lift()
            popup.grab_set()
            self.search_entry.focus_set()
        except tk.TclError:
            self.close()

    def _populate_tree(self):
        if self.tree is None:
            return
        query = self.search_var.get().casefold()
        previous_name = self._iid_names.get(self.tree.focus())
        children = self.tree.get_children()
        if children:
            self.tree.delete(*children)
        self._iid_names = {}
        tree_font = ttk.Style(self).lookup("Treeview", "font") or "TkDefaultFont"
        font = tkfont.Font(root=self, font=tree_font)
        width = self._pixels(320)
        focus_iid = None
        for index, (name, folded) in enumerate(self._folded_columns):
            if query and query not in folded:
                continue
            iid = "column-{}".format(index)
            self._iid_names[iid] = name
            self.tree.insert("", "end", iid=iid, values=(self._indicator(name), name))
            width = max(width, font.measure(name) + self._pixels(16))
            if name == previous_name:
                focus_iid = iid
        self.tree.column("name", width=min(width, 2 ** 30), stretch=False)
        children = self.tree.get_children()
        if children:
            focus_iid = focus_iid or children[0]
            self.tree.focus(focus_iid)
            self.tree.selection_set(focus_iid)
        count = len(self._iid_names)
        self._status_var.set("{} columns".format(len(self._columns)) if not query else
                             "{} matches / {}".format(count, len(self._columns)))
        self._refresh_checks()

    def _indicator(self, name):
        # ASCII markers remain visible when the active theme's font does not
        # provide Unicode checkbox glyphs. The native All control stays a real
        # checkbutton; individual rows remain lightweight Treeview items.
        return "[x]" if self._selected is None or name in self._selected else "[ ]"

    def _refresh_checks(self):
        if self.tree is None:
            return
        self.all_var.set(self._selected is None)
        self.all_button.state(["alternate"] if self._selected else ["!alternate"])
        for iid, name in self._iid_names.items():
            self.tree.set(iid, "selected", self._indicator(name))

    def _all_changed(self):
        if self.all_var.get():
            self.select_all()
        else:
            self.clear_selection()

    def _search_changed(self, *_):
        self._populate_tree()

    def _row_clicked(self, event):
        iid = self.tree.identify_row(event.y)
        if iid in self._iid_names and self.tree.identify_region(event.x, event.y) in ("cell", "tree"):
            self.tree.focus_set()
            self.tree.focus(iid)
            self.tree.selection_set(iid)
            self.toggle_column(self._iid_names[iid])
            return "break"
        return None

    def _row_space(self, _event):
        iid = self.tree.focus()
        if iid in self._iid_names:
            self.toggle_column(self._iid_names[iid])
        return "break"

    def _focus_tree(self, _event):
        self.tree.focus_set()
        return "break"

    def _escape(self, _event):
        self.close()
        return "break"

    def _popup_clicked(self, event):
        popup = self.popup
        if popup is not None and not (
            popup.winfo_rootx() <= event.x_root < popup.winfo_rootx() + popup.winfo_width()
            and popup.winfo_rooty() <= event.y_root < popup.winfo_rooty() + popup.winfo_height()
        ):
            self.close()
            return "break"
        return None

    @staticmethod
    def _inside(widget, ancestor):
        while widget is not None:
            if widget == ancestor:
                return True
            widget = getattr(widget, "master", None)
        return False

    def _focus_out(self, _event):
        if self.popup is not None and self._focus_after is None:
            self._focus_after = self.after_idle(self._check_focus, self.popup)

    def _check_focus(self, popup):
        self._focus_after = None
        if popup is not self.popup:
            return
        try:
            if not self._inside(self.focus_get(), popup):
                self.close()
        except tk.TclError:
            self.close()

    def close(self):
        """Dismiss the popup and restore any pre-existing local/modal grab."""
        popup = self.popup
        if popup is None or self._closing:
            return
        self._closing = True
        self.popup = None
        try:
            if self._focus_after is not None:
                self.after_cancel(self._focus_after)
                self._focus_after = None
            if self.search_var is not None and self._search_trace is not None:
                self.search_var.trace_remove("write", self._search_trace)
            self._search_trace = None
            current_grab = self.grab_current()
            restore_grab = current_grab is None or current_grab == popup
            restore_focus = self._inside(self.focus_get(), popup)
            if current_grab == popup:
                popup.grab_release()
            if popup.winfo_exists():
                popup.destroy()
            if restore_grab and self._previous_grab is not None and self._previous_grab.winfo_exists():
                if self._previous_grab_status == "global":
                    self._previous_grab.grab_set_global()
                else:
                    self._previous_grab.grab_set()
            if restore_focus and not self._destroying:
                target = self._previous_focus if self._previous_focus is not None else self
                if target.winfo_exists():
                    target.focus_set()
        except tk.TclError:
            # Ancestor destruction can remove windows before their Destroy event
            # reaches this button. Event callbacks must not outlive those windows.
            pass
        finally:
            self.tree = self.search_var = self.search_entry = None
            self.all_var = self.all_button = self._status_var = None
            self._previous_grab = self._previous_grab_status = self._previous_focus = None
            self._iid_names = {}
            self._closing = False

    def _popup_destroyed(self, event):
        if event.widget == self.popup:
            self.close()

    def _destroyed(self, event):
        if event.widget == self:
            self._destroying = True
            self.close()

    def state(self, statespec=None):
        result = super().state(statespec)
        if statespec is not None and self.instate(["disabled"]):
            self.close()
        return result

    def configure(self, cnf=None, **kwargs):
        result = super().configure(cnf, **kwargs)
        options = dict(cnf or {}) if isinstance(cnf, dict) else {}
        options.update(kwargs)
        if options.get("state") == "disabled":
            self.close()
        return result

    config = configure

    def destroy(self):
        self._destroying = True
        self.close()
        super().destroy()
