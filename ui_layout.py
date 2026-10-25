"""Small, shared layout helpers for resizable Tk interfaces.

Sizes passed to these helpers are preferred sizes at 96 DPI. Layout decisions
use the space allocated to a container, not the physical monitor resolution.
"""
import sys
import tkinter as tk
from tkinter import font, ttk


def _pixels(widget, value):
    return max(1, round(value * widget.winfo_fpixels("1i") / 96.0))


def fit_window(window, preferred=(1280, 900), minimum=(760, 520)):
    """Fit a window to its monitor, including taskbar space where available."""
    window.update_idletasks()
    left, top = window.winfo_vrootx(), window.winfo_vrooty()
    right = left + window.winfo_screenwidth()
    bottom = top + window.winfo_screenheight()
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
            anchor = window.master.winfo_toplevel() if window.master is not None else window
            monitor = user32.MonitorFromWindow(anchor.winfo_id(), 2)
            info = MonitorInfo(cbSize=ctypes.sizeof(MonitorInfo))
            if user32.GetMonitorInfoW(monitor, ctypes.byref(info)):
                work = info.rcWork
                left, top, right, bottom = work.left, work.top, work.right, work.bottom
        except (AttributeError, OSError, tk.TclError):
            user32 = None
    # Leave room for native window decorations; portable Tk has no work-area API.
    available_w = max(1, right - left - _pixels(window, 32))
    available_h = max(1, bottom - top - _pixels(window, 72))
    min_w, min_h = (min(_pixels(window, v), a)
                    for v, a in zip(minimum, (available_w, available_h)))
    width, height = (min(max(_pixels(window, v), m), a)
                     for v, m, a in zip(preferred, (min_w, min_h), (available_w, available_h)))
    window.minsize(min_w, min_h)
    x = left + max(0, (right - left - width) // 2)
    y = top + max(0, (bottom - top - height - _pixels(window, 40)) // 2)
    window.geometry(f"{width}x{height}")
    if user32 is not None:
        # Native positioning handles monitors with negative desktop coordinates.
        try:
            from ctypes import wintypes
            user32.GetAncestor.argtypes = [wintypes.HWND, wintypes.UINT]
            user32.GetAncestor.restype = wintypes.HWND
            user32.SetWindowPos.argtypes = [wintypes.HWND, wintypes.HWND,
                                           ctypes.c_int, ctypes.c_int, ctypes.c_int,
                                           ctypes.c_int, wintypes.UINT]
            window.update_idletasks()
            hwnd = user32.GetAncestor(window.winfo_id(), 2)
            user32.SetWindowPos(hwnd, None, x, y, 0, 0, 0x0015)
        except (AttributeError, OSError, tk.TclError):
            pass
    elif x >= 0 and y >= 0:
        window.geometry(f"{width}x{height}+{x}+{y}")


def scroll_to_widget(canvas, widget):
    """Bring a descendant near the top, using coordinates in canvas content."""
    canvas.update_idletasks()
    region = canvas.bbox("all")
    if not region or region[3] <= region[1]:
        return
    y = canvas.canvasy(widget.winfo_rooty() - canvas.winfo_rooty())
    fraction = (y - region[1] - _pixels(canvas, 8)) / (region[3] - region[1])
    canvas.yview_moveto(max(0.0, min(1.0, fraction)))


class _WheelRouter:
    """One scoped handler per toplevel; native text/table scrolling wins."""
    def __init__(self, toplevel):
        self.toplevel = toplevel
        self.canvases = {}
        self.bindings = [(sequence, toplevel.bind(sequence, self._wheel, add="+"))
                         for sequence in ("<MouseWheel>", "<Button-4>", "<Button-5>")]
        self.bindings.append(("<FocusIn>", toplevel.bind("<FocusIn>", self._focus, add="+")))

    def register(self, canvas):
        if canvas in self.canvases:
            return
        self.canvases[canvas] = 0.0
        canvas.bind("<Destroy>", lambda e: self._remove(canvas) if e.widget == canvas else None, add="+")

    def _remove(self, canvas):
        self.canvases.pop(canvas, None)
        if not self.canvases:
            for sequence, binding in self.bindings:
                try:
                    self.toplevel.unbind(sequence, binding)
                except tk.TclError:
                    pass
            self.toplevel._layout_wheel_router = None

    def _ancestors(self, widget):
        while isinstance(widget, tk.Misc):
            if widget in self.canvases:
                yield widget
            if widget == self.toplevel:
                break
            widget = widget.master

    def _wheel(self, event):
        target = event.widget
        try:
            if target.winfo_class() in ("Text", "Treeview", "TScrollbar", "Scrollbar", "Listbox",
                                        "TCombobox", "Combobox", "TSpinbox", "Spinbox"):
                return None
            num = getattr(event, "num", None)
            sequence = f"<Button-{num}>" if num in (4, 5) else "<MouseWheel>"
            delta = -1 if num == 4 else 1 if num == 5 else -event.delta / (1 if sys.platform == "darwin" else 120)
            if not delta:
                return None
            for canvas in self._ancestors(target):
                # Existing widget-specific handlers already received this event.
                if target == canvas and canvas.bind(sequence):
                    return None
                first, last = canvas.yview()
                if last - first >= 1 or (delta < 0 and first <= 0) or (delta > 0 and last >= 1):
                    continue
                self.canvases[canvas] += delta
                units = int(self.canvases[canvas])
                self.canvases[canvas] -= units
                if units:
                    canvas.yview_scroll(units, "units")
                return "break"
        except (AttributeError, tk.TclError):
            pass
        return None

    def _focus(self, event):
        try:
            for canvas in self._ancestors(event.widget):
                y = event.widget.winfo_rooty() - canvas.winfo_rooty()
                if y < 0 or y + event.widget.winfo_height() > canvas.winfo_height():
                    scroll_to_widget(canvas, event.widget)
        except (AttributeError, tk.TclError):
            pass


def bind_scroll_wheel(canvas):
    """Route descendant wheel events without application-wide binding changes."""
    top = canvas.winfo_toplevel()
    router = getattr(top, "_layout_wheel_router", None)
    if router is None:
        router = top._layout_wheel_router = _WheelRouter(top)
    router.register(canvas)
    canvas.configure(yscrollincrement=_pixels(canvas, 20))
    return router


class ScrollableFrame(ttk.Frame):
    """An expanding viewport containing a vertically scrollable .content frame."""
    def __init__(self, parent, horizontal=False, **kwargs):
        super().__init__(parent, **kwargs)
        self.horizontal = horizontal
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        self.canvas = tk.Canvas(self, width=1, height=1, highlightthickness=0)
        self.canvas.grid(row=0, column=0, sticky="nsew")
        self.content = ttk.Frame(self.canvas)
        self.window_id = self.canvas.create_window((0, 0), window=self.content, anchor="nw")
        scrollbar = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.canvas.configure(yscrollcommand=scrollbar.set)
        if horizontal:
            bar = ttk.Scrollbar(self, orient="horizontal", command=self.canvas.xview)
            bar.grid(row=1, column=0, sticky="ew")
            self.canvas.configure(xscrollcommand=bar.set)
        self.content.bind("<Configure>", self._content_changed, add="+")
        self.canvas.bind("<Configure>", self._viewport_changed, add="+")
        bind_scroll_wheel(self.canvas)

    def _viewport_changed(self, event):
        width = max(1, event.width)
        if self.horizontal:
            width = max(width, self.content.winfo_reqwidth())
        self.canvas.itemconfigure(self.window_id, width=width)

    def _content_changed(self, event):
        if self.horizontal:
            self.canvas.itemconfigure(self.window_id,
                                      width=max(self.canvas.winfo_width(), self.content.winfo_reqwidth()))
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))


class ResponsiveGrid:
    """Reflow existing child widgets while preserving their state and bindings.

    min_width is the readable width of a card/panel. Atomic controls also retain
    their natural requested width. The container should contain only these
    grid-managed widgets; use a separate frame for headings or spanning items.
    """
    def __init__(self, container, widgets, min_width=180, max_columns=4, padx=4, pady=4):
        self.container, self.widgets = container, list(widgets)
        self.min_width, self.max_columns = min_width, max(1, int(max_columns))
        self.padx, self.pady = padx, pady
        self.columns = 0
        self._pending = None
        self._binding = container.bind("<Configure>", self._configured, add="+")
        container.bind("<Destroy>", self._destroyed, add="+")
        self.refresh()

    def _configured(self, event):
        if event.widget == self.container:
            self.refresh()

    def refresh(self):
        if self._pending is None:
            self._pending = self.container.after_idle(self._layout)

    def _layout(self):
        self._pending = None
        if not self.container.winfo_exists() or not self.widgets:
            return
        padding_x, padding_y = _pixels(self.container, self.padx), _pixels(self.container, self.pady)
        minimum = _pixels(self.container, self.min_width)
        for widget in self.widgets:
            if widget.winfo_class() in ("TButton", "Button", "TCheckbutton", "TEntry", "TCombobox"):
                minimum = max(minimum, widget.winfo_reqwidth())
        columns = max(1, min(self.max_columns, len(self.widgets),
                             self.container.winfo_width() // (minimum + 2 * padding_x)))
        if columns == self.columns:
            return
        for col in range(max(self.columns, columns)):
            self.container.columnconfigure(col, weight=1 if col < columns else 0,
                                           minsize=0, uniform="responsive" if col < columns else "")
        for index, widget in enumerate(self.widgets):
            if widget.winfo_manager() == "pack":
                widget.pack_forget()
            widget.grid(row=index // columns, column=index % columns,
                        columnspan=1, rowspan=1, sticky="nsew", padx=padding_x, pady=padding_y)
        self.columns = columns

    def _destroyed(self, event):
        if event.widget == self.container and self._pending is not None:
            self.container.after_cancel(self._pending)
            self._pending = None


class NotebookNavigator(ttk.Frame):
    """Replace an overflowing tab strip with a compact, complete view selector.

    Grid this frame in a row immediately above the notebook. Other widgets must
    not share that row. refresh() may be called after adding/removing tabs.
    """
    def __init__(self, parent, notebook):
        super().__init__(parent)
        self.notebook = notebook
        self.columnconfigure(1, weight=1)
        ttk.Label(self, text="View:").grid(row=0, column=0, padx=(0, 8), sticky="w")
        self.selector = ttk.Combobox(self, state="readonly", width=18)
        self.selector.grid(row=0, column=1, sticky="ew")
        self.selector.bind("<<ComboboxSelected>>", self._selected, add="+")
        self.style = ttk.Style(notebook)
        self.base_style = notebook.cget("style") or "TNotebook"
        self.compact_style = f"Compact{id(self)}.{self.base_style}"
        self.style.layout(f"{self.compact_style}.Tab", [])
        self._pending = None
        self._tabs = []
        self._compact = None
        self._bindings = [(event, notebook.bind(event, self.refresh, add="+"))
                          for event in ("<Configure>", "<<NotebookTabChanged>>")]
        self.bind("<Destroy>", self._destroyed, add="+")
        self.refresh()

    def refresh(self, event=None):
        if self._pending is None:
            self._pending = self.after_idle(self._refresh)

    def _refresh(self):
        self._pending = None
        if not self.notebook.winfo_exists():
            return
        self._tabs = [tab for tab in self.notebook.tabs()
                      if self.notebook.tab(tab, "state") != "hidden"]
        labels = [self.notebook.tab(tab, "text") for tab in self._tabs]
        self.selector.configure(values=labels)
        selected = self.notebook.select()
        if selected in self._tabs:
            self.selector.current(self._tabs.index(selected))
        tab_style = f"{self.base_style}.Tab"
        tab_font = font.Font(root=self.notebook, font=self.style.lookup(tab_style, "font") or "TkDefaultFont")
        padding = self.style.lookup(tab_style, "padding") or (8, 4)
        values = self.tk.splitlist(padding)
        horizontal_padding = (self.winfo_pixels(values[0]) +
                              self.winfo_pixels(values[2] if len(values) > 2 else values[0])) if values else 16
        needed = sum(tab_font.measure(label) + horizontal_padding + _pixels(self, 8) for label in labels)
        compact = needed > max(1, self.notebook.winfo_width() - _pixels(self, 8))
        if compact != self._compact:
            self._compact = compact
            self.notebook.configure(style=self.compact_style if compact else self.base_style)
            if compact:
                self.grid()
            else:
                self.grid_remove()

    def _selected(self, event):
        index = self.selector.current()
        if 0 <= index < len(self._tabs):
            tab = self._tabs[index]
            if self.notebook.tab(tab, "state") != "disabled":
                self.notebook.select(tab)
            self.refresh()

    def _destroyed(self, event):
        if event.widget != self:
            return
        if self._pending is not None:
            self.after_cancel(self._pending)
        for sequence, binding in self._bindings:
            try:
                self.notebook.unbind(sequence, binding)
            except tk.TclError:
                pass


def wrap_label(label, container=None, padding=16):
    """Wrap a label to the allocated container width, without shrinking fonts."""
    container = container or label.master
    # Geometry runs before the first Configure/idle callback. A one-pixel wrap
    # can request one character per line and inflate an embedded native frame.
    if int(label.cget("wraplength") or 0) <= 1:
        label.configure(wraplength=_pixels(label, 320))
    def resize(event=None):
        if label.winfo_exists():
            width = max(1, container.winfo_width() - _pixels(container, padding))
            if width > 1 and int(label.cget("wraplength") or 0) != width:
                label.configure(wraplength=width)
    binding = container.bind("<Configure>", resize, add="+")
    def cleanup(event):
        if event.widget == label:
            try:
                container.unbind("<Configure>", binding)
            except tk.TclError:
                pass
    label.bind("<Destroy>", cleanup, add="+")
    label.after_idle(resize)
    return label
