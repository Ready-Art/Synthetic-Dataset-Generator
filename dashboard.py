# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Dashboard / presentation layer for the Synthetic Dataset Generator (refactor step 4).

The issue charts, the dashboard notebook tab, progress-bar styling, and the rate-limit / thread-status
displays, extracted from generate.py. Reads shared runtime state + the GUI-widget handles through
app_state (set when generate.py builds the main window); imports the existing helper modules. It does
NOT import generate.py (one-way dependency: generate.py imports dashboard).
"""
import re
import time

import matplotlib.ticker as ticker
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import tkinter as tk
from tkinter import ttk, scrolledtext, font, messagebox, filedialog

import app_state
import api_handler
from app_state import global_config
from logging_config import log_message, LOG_FILE_PATH
from api_handler import (
    RateLimiter, global_rate_limiter, get_cached_response, set_cached_response,
    api_response_times_per_slot, api_response_times_lock, MAX_RESPONSE_TIMES_TO_TRACK,
)

SPACING = 8  # UI padding constant (mirrors generate.py)


def _active_dashboard_tab_text():
    """Text label of the currently selected dashboard tab ('' if unavailable).

    Per-tick Treeview rebuilds and matplotlib redraws each allocate X server
    resources (pixmaps / GCs). Done for every tab at once, continuously, over a
    multi-hour run they exhaust the X server and the connection is dropped
    (``XIO: fatal IO error`` / errno 0). Refreshing only the visible tab keeps
    that churn bounded.
    """
    try:
        nb = app_state.dashboard_notebook
        selected = nb.select()
        return nb.tab(selected, "text") if selected else ""
    except Exception:
        return ""

def create_modern_issue_panel(panel_widget, columns, highlight_color="#ff6b6b"):
    """Keep issue columns readable; scroll when the panel is narrower than them."""
    style = ttk.Style()
    scale = panel_widget.winfo_fpixels("1i") / 96.0
    body_font = font.Font(root=panel_widget, font=('Segoe UI', 9))
    heading_font = font.Font(root=panel_widget, font=('Segoe UI', 10, 'bold'))
    row_height = body_font.metrics("linespace") + max(6, round(8 * scale))
    style.configure("Issue.Treeview",
                    background="#2a2a35",
                    fieldbackground="#2a2a35",
                    foreground="#e0e0e0",
                    rowheight=row_height,
                    font=('Segoe UI', 9))
    style.configure("Issue.Treeview.Heading",
                    background="#1e1e24",
                    foreground="#ffffff",
                    font=('Segoe UI', 10, 'bold'))
    style.map("Issue.Treeview",
              background=[('selected', '#3a3a45')],
              foreground=[('selected', '#ffffff')])

    tree = ttk.Treeview(panel_widget, columns=columns, show="headings",
                        height=8, style="Issue.Treeview")
    col_widths = {"Time": 80, "API": 60, "Phrase": 140, "Context": 380}
    min_widths = {"Time": 72, "API": 60, "Phrase": 120, "Context": 240}
    for col in columns:
        label = col.replace('_', ' ').title()
        minimum = max(round(min_widths.get(col, 100) * scale),
                      heading_font.measure(label) + round(16 * scale))
        tree.heading(col, text=label)
        tree.column(col, width=max(minimum, round(col_widths.get(col, 120) * scale)),
                    minwidth=minimum, stretch=col not in ("Time", "API"), anchor='w')

    scrollbar_options = dict(width=max(14, round(18 * scale)), background="#7a7a9a",
                             troughcolor="#16161c", activebackground="#b0b0d0",
                             highlightthickness=0, borderwidth=0, relief="flat")
    vsb = tk.Scrollbar(panel_widget, orient="vertical", command=tree.yview,
                       **scrollbar_options)
    hsb = tk.Scrollbar(panel_widget, orient="horizontal", command=tree.xview,
                       **scrollbar_options)
    tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
    panel_widget.columnconfigure(0, weight=1)
    panel_widget.rowconfigure(0, weight=1)
    tree.grid(row=0, column=0, sticky="nsew", padx=(5, 2), pady=(5, 2))
    vsb.grid(row=0, column=1, sticky="ns", padx=(2, 5), pady=(5, 2))
    hsb.grid(row=1, column=0, sticky="ew", padx=(5, 2), pady=(2, 5))

    tree.tag_configure("issue_highlight",
                       background="#3a3a45", foreground=highlight_color,
                       font=('Segoe UI', 9, 'italic'))
    return tree

def update_modern_issue_panel(tree, recent_items, highlight_tag="issue_highlight", is_total_tab=False):
    """Updates a modern Treeview panel with recent issues."""
    if not tree or not hasattr(tree, 'winfo_exists') or not tree.winfo_exists():
        return

    # Clear old data efficiently
    for item_id in tree.get_children():
        tree.delete(item_id)

    # Insert new items
    for item in recent_items:
        phrase = context = api_str = "N/A"
        ts = time.time()

        # Parse tuple formats from app_state
        if is_total_tab:
            if isinstance(item, tuple) and len(item) == 3:
                phrase, context, api_idx = item
                api_str = f"Slot {api_idx+1}"
                ts = time.time() # Or extract from item if stored
            elif isinstance(item, tuple) and len(item) == 2:
                context, api_idx = item
                api_str = f"Slot {api_idx+1}"
        else:
            if isinstance(item, tuple) and len(item) == 2:
                phrase, context = item
            else:
                context = str(item)

        # Format timestamp
        time_str = time.strftime("%H:%M:%S", time.localtime(ts))

        # Determine columns based on tab type
        if is_total_tab:
            tree.insert("", tk.END, values=(time_str, api_str, phrase, context), tags=(highlight_tag,))
        else:
            tree.insert("", tk.END, values=(time_str, phrase, context), tags=(highlight_tag,))

    # Auto-scroll to the bottom (latest issue)
    children = tree.get_children()
    if children:
        tree.see(children[-1])

def _fit_issue_graph(fig, ax):
    """Fit labels to the allocated figure without changing its requested size."""
    # Inches keep the breakpoint consistent when TkAgg changes the figure DPI.
    compact = fig.get_figwidth() < 6.4
    ax.title.set_fontsize(12 if compact else 15)
    ax.xaxis.label.set_fontsize(10 if compact else 12)
    ax.yaxis.label.set_fontsize(10 if compact else 12)
    ax.tick_params(axis='both', labelsize=8 if compact else 10)
    legend = ax.get_legend()
    if legend is not None:
        for label in legend.get_texts():
            label.set_fontsize(8 if compact else 10)
    # Tiny transient allocations occur while changing tabs or resizing a window.
    if fig.bbox.width >= 300 and fig.bbox.height >= 220:
        fig.tight_layout(pad=1.0)


def draw_issue_graph(canvas_widget, height=400):
    """Draws a modern, detailed time-series graph showing issue counts over the last 60 minutes."""
    canvas_widget.delete("all")

    # Modern dark theme setup
    fig = Figure(figsize=(7.5, max(280, min(height, 400)) / 100),
                 dpi=100, facecolor='#1e1e24')
    fig.patch.set_facecolor('#1e1e24')
    ax = fig.add_subplot(111)
    ax.set_facecolor('#2a2a35')

    # Time bins setup
    now = time.time()
    sixty_minutes_ago = now - 3600
    num_bins = 6
    bin_size = 3600 / num_bins

    # Initialize counts
    counts = {'refusals': [0]*num_bins, 'user_speaking': [0]*num_bins,
              'slop': [0]*num_bins, 'errors': [0]*num_bins, 'anti_slop': [0]*num_bins,
              'out_of_character': [0]*num_bins, 'logic_issues': [0]*num_bins,
              'regex_watch': [0]*num_bins, 'non_latin': [0]*num_bins,
              'scene_continuity': [0]*num_bins}

    with app_state.issue_timestamps_lock:
        for key in counts.keys():
            for ts in app_state.issue_timestamps.get(key, []):
                if sixty_minutes_ago <= ts <= now:
                    idx = min(int((ts - sixty_minutes_ago) / bin_size), num_bins - 1)
                    counts[key][idx] += 1

    # X-axis labels (stacked for readability)
    x_labels = []
    for i in range(num_bins):
        start = time.strftime('%H:%M', time.localtime(sixty_minutes_ago + i * bin_size))
        end = time.strftime('%H:%M', time.localtime(sixty_minutes_ago + (i + 1) * bin_size))
        x_labels.append(f"{start}\n{end}")

    x = range(num_bins)
    width = 0.15
    # One offset per issue series, symmetric around each bin center.
    # Computed from the series count so adding a new detection type
    # (which extends `counts`) can never run past the end of `offsets`.
    n_series = len(counts)
    offsets = [((i + 0.5) - n_series / 2) * width for i in range(n_series)]

    # Cohesive, accessible color palette
    colors = {'refusals': '#ff4d6d', 'user_speaking': '#4dabf7', 'slop': '#9775fa',
              'anti_slop': '#ffd43b', 'errors': '#fc8181', 'out_of_character': '#ff922b',
              'logic_issues': '#38d9a9', 'regex_watch': '#b197fc', 'non_latin': '#20c997',
              'scene_continuity': '#e599f7'}
    labels = {'refusals': 'Refusals', 'user_speaking': 'User Speak', 'slop': 'Slop',
              'anti_slop': 'Anti-Slop', 'errors': 'Errors', 'out_of_character': 'OOC Drift',
              'logic_issues': 'Logic Errors', 'regex_watch': 'Regex Watch',
              'non_latin': 'Non-Latin',
              'scene_continuity': 'Cast Drift'}

    # Plot bars & add value labels
    for i, key in enumerate(counts.keys()):
        bar = ax.bar([j + offsets[i] for j in x], counts[key], width, label=labels[key],
                     color=colors[key], edgecolor='#1e1e24', linewidth=0.8, alpha=0.9)
        for rect in bar:
            h = rect.get_height()
            if h > 0:
                ax.annotate(f'{int(h)}', xy=(rect.get_x() + rect.get_width()/2, h),
                            xytext=(0, 4), textcoords="offset points",
                            ha='center', va='bottom', fontsize=8, color='#e0e0e0', fontweight='bold')

    # Styling & Layout
    max_val = max(max(c) for c in counts.values()) if any(any(c) for c in counts.values()) else 5
    ax.set_ylim(0, max_val + 2)
    ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))

    ax.set_xlabel('Time Window (Last 60 Minutes)', fontsize=12, fontweight='bold', color='#e0e0e0', labelpad=10)
    ax.set_ylabel('Issue Count', fontsize=12, fontweight='bold', color='#e0e0e0', labelpad=10)
    ax.set_title('Issue Detection Dashboard', fontsize=15, fontweight='bold', color='#ffffff', pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, rotation=0, ha='center', fontsize=10, color='#c0c0c0')

    ax.grid(axis='y', linestyle='--', alpha=0.25, color='#555555')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_color('#555555')
    ax.spines['left'].set_color('#555555')
    ax.tick_params(axis='both', colors='#c0c0c0')

    ax.legend(loc='upper right', fontsize=10, framealpha=0.85, facecolor='#2a2a35',
              edgecolor='#444444', labelcolor='#e0e0e0')
    _fit_issue_graph(fig, ax)

    # Embed in Tkinter
    canvas = FigureCanvasTkAgg(fig, master=canvas_widget)
    canvas.draw()
    canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

    # FIX: Force Tkinter to process geometry events immediately so the
    # parent scrollable frame can calculate the correct scroll region
    canvas_widget.update_idletasks()
    canvas_widget.graph_canvas = canvas
    canvas_widget.graph_fig = fig
    canvas_widget.graph_ax = ax
    canvas_widget._graph_resize_after = None

    def fit_after_resize():
        canvas_widget._graph_resize_after = None
        if canvas_widget.winfo_exists():
            _fit_issue_graph(fig, ax)
            canvas.draw_idle()

    def schedule_fit(_event):
        if canvas_widget._graph_resize_after is not None:
            canvas_widget.after_cancel(canvas_widget._graph_resize_after)
        canvas_widget._graph_resize_after = canvas_widget.after(150, fit_after_resize)

    def cancel_fit(event):
        if event.widget == canvas_widget and canvas_widget._graph_resize_after is not None:
            canvas_widget.after_cancel(canvas_widget._graph_resize_after)
            canvas_widget._graph_resize_after = None

    # Preserve TkAgg's own Configure binding; it resizes the existing figure.
    canvas.get_tk_widget().bind("<Configure>", schedule_fit, add="+")
    canvas_widget.bind("<Destroy>", cancel_fit, add="+")
    fit_after_resize()


# Module-level throttle state for the issue graph.
_graph_last_draw = {"t": 0.0, "data_sig": None}

# Minimum seconds between full graph redraws. The graph is a 6-bin view of the
# last 60 minutes, so sub-minute refreshes add nothing but X server churn — each
# matplotlib redraw blits through a Tk PhotoImage and accumulates pixmaps.
_GRAPH_REDRAW_MIN_INTERVAL = 45.0


def update_issue_graph(canvas_widget):
    """Updates an existing issue graph with new data without recreating the figure.

    Throttled to at most one full redraw every ``_GRAPH_REDRAW_MIN_INTERVAL``
    seconds (or when the underlying timestamp data changes) to avoid flooding the
    X server with rasterise requests over a long run.
    """
    if not hasattr(canvas_widget, 'graph_canvas'):
        draw_issue_graph(canvas_widget)
        _graph_last_draw["t"] = time.time()
        return

    # --- Throttle: skip redraw if interval not elapsed AND data is unchanged ---
    now = time.time()
    with app_state.issue_timestamps_lock:
        sig = tuple(
            len(app_state.issue_timestamps.get(k, []))
            for k in ("refusals", "user_speaking", "slop", "errors", "anti_slop", "out_of_character",
                      "logic_issues", "regex_watch", "non_latin", "scene_continuity")
        )
    if (now - _graph_last_draw["t"]) < _GRAPH_REDRAW_MIN_INTERVAL and sig == _graph_last_draw["data_sig"]:
        return
    _graph_last_draw["t"] = now
    _graph_last_draw["data_sig"] = sig
    # ------------------------------------------------------------------------

    fig = canvas_widget.graph_fig
    ax = canvas_widget.graph_ax
    ax.clear()

    # Reuse the exact same drawing logic from draw_issue_graph
    # (We extract it into a shared helper in production, but for drop-in replacement:)
    fig.patch.set_facecolor('#1e1e24')
    ax.set_facecolor('#2a2a35')

    sixty_minutes_ago = now - 3600
    num_bins = 6
    bin_size = 3600 / num_bins
    counts = {'refusals': [0]*num_bins, 'user_speaking': [0]*num_bins,
              'slop': [0]*num_bins, 'errors': [0]*num_bins, 'anti_slop': [0]*num_bins,
              'out_of_character': [0]*num_bins, 'logic_issues': [0]*num_bins,
              'regex_watch': [0]*num_bins, 'non_latin': [0]*num_bins,
              'scene_continuity': [0]*num_bins}

    with app_state.issue_timestamps_lock:
        for key in counts.keys():
            for ts in app_state.issue_timestamps.get(key, []):
                if sixty_minutes_ago <= ts <= now:
                    idx = min(int((ts - sixty_minutes_ago) / bin_size), num_bins - 1)
                    counts[key][idx] += 1

    x_labels = []
    for i in range(num_bins):
        start = time.strftime('%H:%M', time.localtime(sixty_minutes_ago + i * bin_size))
        end = time.strftime('%H:%M', time.localtime(sixty_minutes_ago + (i + 1) * bin_size))
        x_labels.append(f"{start}\n{end}")

    x = range(num_bins)
    width = 0.15
    # Symmetric offsets computed from the series count so the offsets list
    # can never be shorter than `counts.keys()`.
    n_series = len(counts)
    offsets = [((i + 0.5) - n_series / 2) * width for i in range(n_series)]
    colors = {'refusals': '#ff4d6d', 'user_speaking': '#4dabf7', 'slop': '#9775fa',
              'anti_slop': '#ffd43b', 'errors': '#fc8181', 'out_of_character': '#ff922b',
              'logic_issues': '#38d9a9', 'regex_watch': '#b197fc', 'non_latin': '#20c997',
              'scene_continuity': '#e599f7'}
    labels = {'refusals': 'Refusals', 'user_speaking': 'User Speak', 'slop': 'Slop',
              'anti_slop': 'Anti-Slop', 'errors': 'Errors', 'out_of_character': 'OOC Drift',
              'logic_issues': 'Logic Errors', 'regex_watch': 'Regex Watch',
              'non_latin': 'Non-Latin',
              'scene_continuity': 'Cast Drift'}

    for i, key in enumerate(counts.keys()):
        bar = ax.bar([j + offsets[i] for j in x], counts[key], width, label=labels[key],
                     color=colors[key], edgecolor='#1e1e24', linewidth=0.8, alpha=0.9)
        for rect in bar:
            h = rect.get_height()
            if h > 0:
                ax.annotate(f'{int(h)}', xy=(rect.get_x() + rect.get_width()/2, h),
                            xytext=(0, 4), textcoords="offset points",
                            ha='center', va='bottom', fontsize=8, color='#e0e0e0', fontweight='bold')

    max_val = max(max(c) for c in counts.values()) if any(any(c) for c in counts.values()) else 5
    ax.set_ylim(0, max_val + 2)
    ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))

    ax.set_xlabel('Time Window (Last 60 Minutes)', fontsize=12, fontweight='bold', color='#e0e0e0', labelpad=10)
    ax.set_ylabel('Issue Count', fontsize=12, fontweight='bold', color='#e0e0e0', labelpad=10)
    ax.set_title('Issue Detection Dashboard', fontsize=15, fontweight='bold', color='#ffffff', pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, rotation=0, ha='center', fontsize=10, color='#c0c0c0')

    ax.grid(axis='y', linestyle='--', alpha=0.25, color='#555555')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_color('#555555')
    ax.spines['left'].set_color('#555555')
    ax.tick_params(axis='both', colors='#c0c0c0')

    ax.legend(loc='upper right', fontsize=10, framealpha=0.85, facecolor='#2a2a35',
              edgecolor='#444444', labelcolor='#e0e0e0')
    _fit_issue_graph(fig, ax)
    canvas_widget.graph_canvas.draw()


# ---------------------------------------------------------------------------
# vLLM Metrics tab (v13.1.0)
#
# One line per API slot that has a vLLM backend. Deliberately NOT aggregated: the tab exists so a
# user can see WHICH endpoint is slow or saturated, so every slot keeps its own colour and its own
# line, and a slot whose server is unreachable simply has a gap.
#
# Two panels share the time axis:
#   top    -- tokens/second (solid = token generation, dashed = prompt processing)
#   bottom -- the server's KV-cache load percentage, i.e. how full that endpoint is
# ---------------------------------------------------------------------------

# One colour per API slot index (0-6), matching the slot colours used elsewhere in the UI.
_VLLM_SERIES_COLORS = ('#4dabf7', '#51cf66', '#ffd43b', '#ff922b',
                       '#b197fc', '#f783ac', '#20c997')

# Redrawing both panels is the expensive part, so a redraw per poll is enough; this keeps a
# 500 ms UI tick from re-rasterising the same data.
#
# v13.5.0: 1.0 s, down from 5.0 s.  The poller now runs every 2 s, and a 5 s floor meant the
# graph skipped every other sample -- the panel lagged the table by up to 5 s and a short burst
# was drawn as a straight line between two points that were not adjacent.  The floor only has to
# be long enough to absorb the UI tick, not the poll interval.
_vllm_graph_last_draw = {"t": 0.0, "sig": None}
_VLLM_GRAPH_REDRAW_MIN_INTERVAL = 1.0


def _vllm_series(rows):
    """``[(label, colour, history), ...]`` for the rows that have any samples yet."""
    series = []
    for row in rows or []:
        history = row.get('history') or []
        if not history:
            continue
        slot_idx = row.get('slot_idx', 0)
        colour = _VLLM_SERIES_COLORS[slot_idx % len(_VLLM_SERIES_COLORS)]
        series.append((row.get('label') or 'API', colour, history))
    return series


def _vllm_graph_signature(rows):
    """A cheap fingerprint of the plotted data, so an unchanged poll does not redraw."""
    return tuple(
        (row.get('slot_idx'), len(row.get('history') or []),
         (row.get('history') or [{}])[-1].get('time'))
        for row in rows or []
    )


def draw_vllm_metrics_graph(canvas_widget, height=420):
    """Create the per-endpoint vLLM graph (speeds on top, KV-cache load below).

    Called once when the 📊 Metrics tab is built; ``update_vllm_metrics_graph()`` refreshes the
    same figure in place afterwards. An empty dataset still draws the axes and a hint, so the tab
    explains itself before the first poll rather than showing a blank rectangle.
    """
    canvas_widget.delete("all")

    fig = Figure(figsize=(7.5, max(300, min(height, 460)) / 100), dpi=100, facecolor='#1e1e24')
    fig.patch.set_facecolor('#1e1e24')
    speed_ax = fig.add_subplot(211)
    load_ax = fig.add_subplot(212, sharex=speed_ax)
    for ax in (speed_ax, load_ax):
        ax.set_facecolor('#2a2a35')
        ax.grid(axis='y', linestyle='--', alpha=0.25, color='#555555')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_color('#555555')
        ax.spines['left'].set_color('#555555')
        ax.tick_params(axis='both', colors='#c0c0c0')

    canvas = FigureCanvasTkAgg(fig, master=canvas_widget)
    canvas.draw()
    canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
    canvas_widget.update_idletasks()
    canvas_widget.graph_canvas = canvas
    canvas_widget.graph_fig = fig
    canvas_widget.graph_speed_ax = speed_ax
    canvas_widget.graph_load_ax = load_ax
    canvas_widget._graph_resize_after = None

    def fit_after_resize():
        canvas_widget._graph_resize_after = None
        if canvas_widget.winfo_exists():
            _fit_issue_graph(fig, speed_ax)
            _fit_issue_graph(fig, load_ax)
            canvas.draw_idle()

    def schedule_fit(_event):
        if canvas_widget._graph_resize_after is not None:
            canvas_widget.after_cancel(canvas_widget._graph_resize_after)
        canvas_widget._graph_resize_after = canvas_widget.after(150, fit_after_resize)

    def cancel_fit(event):
        if event.widget == canvas_widget and canvas_widget._graph_resize_after is not None:
            canvas_widget.after_cancel(canvas_widget._graph_resize_after)
            canvas_widget._graph_resize_after = None

    canvas.get_tk_widget().bind("<Configure>", schedule_fit, add="+")
    canvas_widget.bind("<Destroy>", cancel_fit, add="+")
    fit_after_resize()
    # Draw the empty state immediately, so the tab explains itself before the first poll
    # instead of showing a blank pair of axes.
    update_vllm_metrics_graph(canvas_widget, [], force=True)
    return canvas_widget


def update_vllm_metrics_graph(canvas_widget, rows, force=False):
    """Refresh the per-endpoint vLLM graph from ``vllm_metrics.endpoint_rows()``.

    Throttled on both the interval and a data fingerprint: the tab's 500 ms tick must not
    re-rasterise an unchanged figure (each matplotlib redraw allocates X server resources, and
    doing that continuously over a long run drops the X connection).
    """
    if not hasattr(canvas_widget, 'graph_canvas'):
        draw_vllm_metrics_graph(canvas_widget)
        if not hasattr(canvas_widget, 'graph_speed_ax'):
            return

    speed_ax = getattr(canvas_widget, 'graph_speed_ax', None)
    load_ax = getattr(canvas_widget, 'graph_load_ax', None)
    if speed_ax is None or load_ax is None:
        return

    signature = _vllm_graph_signature(rows)
    now = time.time()
    if (not force and _vllm_graph_last_draw["sig"] == signature
            and now - _vllm_graph_last_draw["t"] < _VLLM_GRAPH_REDRAW_MIN_INTERVAL):
        return
    _vllm_graph_last_draw["sig"] = signature
    _vllm_graph_last_draw["t"] = now

    series = _vllm_series(rows)
    speed_ax.clear()
    load_ax.clear()

    if not series:
        for ax, title in ((speed_ax, 'Tokens per Second (per API endpoint)'),
                           (load_ax, 'Endpoint Load — KV Cache Used')):
            ax.set_facecolor('#2a2a35')
            ax.grid(axis='y', linestyle='--', alpha=0.25, color='#555555')
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            ax.spines['bottom'].set_color('#555555')
            ax.spines['left'].set_color('#555555')
            ax.tick_params(axis='both', colors='#c0c0c0')
            ax.set_title(title, fontsize=12, fontweight='bold', color='#ffffff', pad=8)
            ax.set_yticks([])
            ax.text(0.5, 0.5, 'No vLLM metrics yet.\nConfigure an API slot that points at a vLLM '
                              'server; the tab polls its /metrics endpoint every 10 seconds.',
                    transform=ax.transAxes, ha='center', va='center', fontsize=10,
                    color='#8b949e', multialignment='center')
        canvas_widget.graph_fig.tight_layout(pad=1.0)
        canvas_widget.graph_canvas.draw_idle()
        return

    # A common time axis in seconds relative to the newest sample keeps the x labels readable
    # without depending on the machine's locale formatting.
    newest = max(sample['time'] for _label, _colour, history in series for sample in history)

    for label, colour, history in series:
        times = [(sample['time'] - newest) for sample in history]
        generation = [sample.get('generation_tps') for sample in history]
        prompt = [sample.get('prompt_tps') for sample in history]
        load = [sample.get('load_percent') for sample in history]

        speed_ax.plot(times, generation, color=colour, linewidth=1.8, marker='o', markersize=3,
                      label='%s — generation' % label)
        speed_ax.plot(times, prompt, color=colour, linewidth=1.4, linestyle='--', marker='s',
                      markersize=2.5, label='%s — prompt' % label)
        load_ax.plot(times, load, color=colour, linewidth=1.8, marker='o', markersize=3,
                     label=label)

    for ax, title in ((speed_ax, 'Tokens per Second (per API endpoint)'),
                      (load_ax, 'Endpoint Load — KV Cache Used (per API endpoint)')):
        ax.set_facecolor('#2a2a35')
        ax.grid(axis='y', linestyle='--', alpha=0.25, color='#555555')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_color('#555555')
        ax.spines['left'].set_color('#555555')
        ax.tick_params(axis='both', colors='#c0c0c0')
        ax.set_title(title, fontsize=12, fontweight='bold', color='#ffffff', pad=8)
        ax.legend(loc='upper left', fontsize=8, framealpha=0.85, facecolor='#2a2a35',
                  edgecolor='#444444', labelcolor='#e0e0e0', ncol=2)

    load_ax.set_ylim(0, 105)
    load_ax.set_xlabel('Seconds Ago', fontsize=10, fontweight='bold', color='#e0e0e0')
    _fit_issue_graph(canvas_widget.graph_fig, speed_ax)
    _fit_issue_graph(canvas_widget.graph_fig, load_ax)
    canvas_widget.graph_canvas.draw_idle()


# ---------------------------------------------------------------------------
# v13.4.0: the three extra Metrics-tab graph panels
#
# The v13.1.0 figure above answers "which endpoint is slow and how full is its KV cache?".  Three
# questions it could not answer get their own panel rather than another line on a crowded axis:
#
#   slot   -- how much throughput is THIS generator slot actually achieving?
#   proxy  -- what is the antislop proxy doing (requests/min, bans/min, back-tracks/min)?
#   queue  -- is a server queueing (running vs waiting), which explains a slow generation?
#
# Each panel is built lazily on first refresh and cached on the canvas widget, so the tab builds
# fast and a hidden panel costs nothing.
# ---------------------------------------------------------------------------

# Per-slot colours, matching _VLLM_SERIES_COLORS so a slot keeps one colour across every panel.
_PROXY_SERIES_COLORS = ('#4dabf7', '#51cf66', '#ffd43b', '#ff922b',
                        '#b197fc', '#f783ac', '#20c997')


def _style_metric_axes(ax, title, ylabel=None):
    """Apply the tab's dark panel styling to one axes."""
    ax.set_facecolor('#2a2a35')
    ax.grid(axis='y', linestyle='--', alpha=0.25, color='#555555')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_color('#555555')
    ax.spines['left'].set_color('#555555')
    ax.tick_params(axis='both', colors='#c0c0c0', labelsize=9)
    ax.set_title(title, fontsize=11, fontweight='bold', color='#ffffff', pad=6)
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=9, color='#c0c0c0')


def _draw_metric_hint(ax, message):
    """The empty state: axes with a centred explanation, never a blank rectangle."""
    ax.set_yticks([])
    ax.set_xticks([])
    ax.text(0.5, 0.5, message, transform=ax.transAxes, ha='center', va='center',
            fontsize=9, color='#8b949e', multialignment='center')


def _metric_panel(canvas_widget, attr, panels):
    """Create (once) and return ``{name: axes}`` for a canvas, plus its Figure.

    ``panels`` is ``[(name, title, ylabel, height_ratio), ...]``.  The figure is cached on the
    canvas widget under ``attr`` so a refresh reuses it instead of allocating a new matplotlib
    figure every poll -- that churn is what drops the X connection over a long run.
    """
    figure_attr = attr + '_fig'
    axes_attr = attr + '_axes'
    fig = getattr(canvas_widget, figure_attr, None)
    axes = getattr(canvas_widget, axes_attr, None)
    if fig is not None and axes is not None:
        return fig, axes

    canvas_widget.delete("all")
    fig = Figure(figsize=(7.5, 3.0), dpi=100, facecolor='#1e1e24')
    fig.patch.set_facecolor('#1e1e24')
    total = sum(panel[3] for panel in panels)
    bottom = 0.0
    axes = {}
    for name, title, ylabel, ratio in panels:
        height = ratio / total
        ax = fig.add_axes((0.09, bottom + 0.06, 0.88, height * 0.86))
        _style_metric_axes(ax, title, ylabel)
        axes[name] = ax
        bottom += height

    canvas = FigureCanvasTkAgg(fig, master=canvas_widget)
    canvas.draw()
    canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
    canvas_widget.update_idletasks()
    setattr(canvas_widget, attr + '_canvas', canvas)
    setattr(canvas_widget, figure_attr, fig)
    setattr(canvas_widget, axes_attr, axes)
    return fig, axes


def _metric_series_colour(slot_idx):
    return _PROXY_SERIES_COLORS[int(slot_idx or 0) % len(_PROXY_SERIES_COLORS)]


def _relative_times(history):
    """``[seconds before the newest sample, ...]`` so the x axis reads "seconds ago"."""
    newest = max(sample['time'] for sample in history)
    return [(sample['time'] - newest) for sample in history]


def _plot_metric_series(ax, history, key, colour, label, linestyle='-', marker='o'):
    """Plot one key out of a history ring, skipping ``None`` samples as gaps.

    ``None`` means "this endpoint does not report that figure" (a plain vLLM has no proxy
    counters), which must draw as a gap, not as a line pinned to zero.
    """
    if not history:
        return
    times = _relative_times(history)
    values = [sample.get(key) for sample in history]
    if all(value is None for value in values):
        return
    ax.plot(times, values, color=colour, linewidth=1.8, linestyle=linestyle,
            marker=marker, markersize=3, label=label)


def _finish_metric_panel(ax, legend=False, ylim=None):
    """Common tail: legend, y limits and the x label.

    Deliberately no ``tight_layout()``: these figures place their axes with explicit rectangles
    (see ``_metric_panel``), which ``tight_layout`` cannot reconcile -- it warns and then moves
    them anyway.  The rectangles already leave room for the labels.
    """
    if legend:
        handles, labels = ax.get_legend_handles_labels()
        if handles:
            ax.legend(loc='upper left', fontsize=7, framealpha=0.85, facecolor='#2a2a35',
                      edgecolor='#444444', labelcolor='#e0e0e0', ncol=2)
    if ylim is not None:
        ax.set_ylim(*ylim)
    ax.set_xlabel('Seconds Ago', fontsize=9, color='#c0c0c0')


# --- panel 2: per-slot achieved throughput -----------------------------------
_slot_graph_last_draw = {"t": 0.0, "sig": None}


def update_vllm_slot_graph(canvas_widget, rows, force=False):
    """Draw each API slot's OWN measured throughput, one line per slot.

    This is the figure the server cannot provide: vLLM's ``/metrics`` has no caller label, so two
    slots on one server are indistinguishable there.  Here each slot's own responses are counted,
    which is what that API is actually delivering.
    """
    fig, axes = _metric_panel(canvas_widget, 'slot_graph', [
        ('throughput', 'This Slot\'s Achieved Throughput (prompt + completion tokens ÷ wall clock)',
         'tok/s', 1.0),
    ])
    ax = axes['throughput']

    signature = tuple((row.get('slot_idx'), len(row.get('slot_history') or []))
                      for row in rows or [])
    now = time.time()
    if (not force and _slot_graph_last_draw["sig"] == signature
            and now - _slot_graph_last_draw["t"] < _VLLM_GRAPH_REDRAW_MIN_INTERVAL):
        return
    _slot_graph_last_draw["sig"] = signature
    _slot_graph_last_draw["t"] = now

    ax.clear()
    _style_metric_axes(ax, 'This Slot\'s Achieved Throughput '
                            '(prompt + completion tokens ÷ wall clock)', 'tok/s')
    drew = False
    for row in rows or []:
        history = row.get('slot_history') or []
        if not history:
            continue
        _plot_metric_series(ax, history, 'tokens_per_second',
                            _metric_series_colour(row.get('slot_idx')),
                            row.get('label') or 'API')
        drew = True
    if not drew:
        _draw_metric_hint(ax, 'No per-slot throughput yet.\nA slot records its own tokens once it '
                               'has completed a request.')
    else:
        _finish_metric_panel(ax, legend=True)
    canvas_widget.slot_graph_canvas.draw_idle()


# --- panel 3: what the proxy is doing ---------------------------------------
_proxy_graph_last_draw = {"t": 0.0, "sig": None}


def update_vllm_proxy_graph(canvas_widget, rows, force=False):
    """Draw the antislop proxy's activity: requests, bans and back-tracks per minute.

    Only rows that actually report ``antislop:`` metrics contribute.  A plain-vLLM tab therefore
    shows the panel's own hint rather than three empty axes, which is the honest reading: there is
    no proxy here.
    """
    fig, axes = _metric_panel(canvas_widget, 'proxy_graph', [
        ('requests', 'Proxy Requests per Minute', 'req/min', 1.0),
        ('bans', 'Bans and Back-tracks per Minute', '/min', 1.0),
    ])

    signature = tuple((row.get('slot_idx'), len(row.get('history') or []))
                      for row in rows or [])
    now = time.time()
    if (not force and _proxy_graph_last_draw["sig"] == signature
            and now - _proxy_graph_last_draw["t"] < _VLLM_GRAPH_REDRAW_MIN_INTERVAL):
        return
    _proxy_graph_last_draw["sig"] = signature
    _proxy_graph_last_draw["t"] = now

    request_ax = axes['requests']
    ban_ax = axes['bans']
    request_ax.clear()
    ban_ax.clear()
    _style_metric_axes(request_ax, 'Proxy Requests per Minute', 'req/min')
    _style_metric_axes(ban_ax, 'Bans and Back-tracks per Minute', '/min')

    drew = False
    drew_bans = False
    for row in rows or []:
        if not row.get('is_antislop'):
            continue
        history = row.get('history') or []
        if not history:
            continue
        colour = _metric_series_colour(row.get('slot_idx'))
        label = row.get('label') or 'API'
        # The request counter is cumulative, so its per-minute rate comes from the delta between
        # consecutive samples over the wall clock between them -- the same rule the proxy's own
        # generation speed uses.
        _plot_metric_series(request_ax, _rate_history(history, 'antislop_requests', 60.0),
                            'rate', colour, label)
        _plot_metric_series(ban_ax, history, 'antislop_ban_rate', colour,
                            '%s — bans' % label)
        _plot_metric_series(ban_ax, history, 'antislop_backtrack_rate', colour,
                            '%s — back-tracks' % label, linestyle='--', marker='s')
        drew = True
        if any(sample.get('antislop_ban_rate') is not None
               or sample.get('antislop_backtrack_rate') is not None for sample in history):
            drew_bans = True

    if not drew:
        _draw_metric_hint(request_ax, 'No antislop-vllm-fork proxy metrics.\nPoint an API slot at '
                                       'the fork to see its request, ban and back-track rates.')
        _draw_metric_hint(ban_ax, 'Bans and back-tracks need a proxied slot.')
    else:
        _finish_metric_panel(request_ax, legend=True)
        if drew_bans:
            _finish_metric_panel(ban_ax, legend=True)
        else:
            # A proxied slot with no bans in the window is GOOD news, not missing data -- say so
            # rather than leaving an empty axes that reads as a broken panel.
            _draw_metric_hint(ban_ax, 'No bans or back-tracks in this window.\n'
                                      'That is the healthy case; a rate appears here the moment a '
                                      'validator fires.')
    canvas_widget.proxy_graph_canvas.draw_idle()


def _rate_history(history, key, scale):
    """Turn a cumulative counter's history into a per-minute rate history.

    Returns a new history ring whose ``rate`` key holds ``delta / seconds * scale``.  A negative
    delta (a restarted proxy zeroed its counters) yields ``None`` for that sample, so the graph
    shows a gap instead of a spike -- the same rule ``vllm_metrics._delta()`` applies.
    """
    out = []
    previous = None
    previous_time = None
    for sample in history:
        value = sample.get(key)
        rate = None
        if value is not None and previous is not None and previous_time is not None:
            elapsed = sample['time'] - previous_time
            delta = value - previous
            if elapsed > 0 and delta >= 0:
                rate = delta / elapsed * scale
        out.append({"time": sample['time'], "rate": rate})
        previous = value
        previous_time = sample['time']
    return out


# --- panel 4: queue depth ---------------------------------------------------
_queue_graph_last_draw = {"t": 0.0, "sig": None}


def update_vllm_queue_graph(canvas_widget, rows, force=False):
    """Draw each server's running vs waiting request count.

    This is the panel that explains a slow generation: a high ``waiting`` line with a flat
    ``running`` one means the server is queueing, so the bottleneck is admission, not the GPU.
    """
    fig, axes = _metric_panel(canvas_widget, 'queue_graph', [
        ('queue', 'Requests Running vs Waiting per Server', 'requests', 1.0),
    ])
    ax = axes['queue']

    signature = tuple((row.get('slot_idx'), len(row.get('history') or []))
                      for row in rows or [])
    now = time.time()
    if (not force and _queue_graph_last_draw["sig"] == signature
            and now - _queue_graph_last_draw["t"] < _VLLM_GRAPH_REDRAW_MIN_INTERVAL):
        return
    _queue_graph_last_draw["sig"] = signature
    _queue_graph_last_draw["t"] = now

    ax.clear()
    _style_metric_axes(ax, 'Requests Running vs Waiting per Server', 'requests')
    drew = False
    for row in rows or []:
        history = row.get('history') or []
        if not history:
            continue
        colour = _metric_series_colour(row.get('slot_idx'))
        label = row.get('label') or 'API'
        _plot_metric_series(ax, history, 'running', colour, '%s — running' % label)
        _plot_metric_series(ax, history, 'waiting', colour, '%s — waiting' % label,
                            linestyle='--', marker='s')
        drew = True
    if not drew:
        _draw_metric_hint(ax, 'No queue data yet.\nA vLLM server reports running/waiting counts '
                              'on its /metrics endpoint.')
    else:
        _finish_metric_panel(ax, legend=True, ylim=(0, None))
    canvas_widget.queue_graph_canvas.draw_idle()


def update_dashboard():
    """Updates the dashboard labels and text areas with current statistics and recent issues."""

    # NEW: Update rate limit status
    update_rate_limit_status()

    total_attempts_for_calc = app_state.total_attempts_global if app_state.total_attempts_global > 0 else 1

    refusal_percent = (app_state.refusal_count_total / total_attempts_for_calc) * 100
    user_speaking_percent = (app_state.user_speaking_count_total / total_attempts_for_calc) * 100
    slop_percent = (app_state.slop_count_total / total_attempts_for_calc) * 100
    error_percent = (app_state.error_count_total / total_attempts_for_calc) * 100
    ooc_percent = (app_state.ooc_count_total / total_attempts_for_calc) * 100
    logic_percent = (app_state.logic_issue_count_total / total_attempts_for_calc) * 100
    regex_percent = (getattr(app_state, 'regex_watch_count_total', 0) / total_attempts_for_calc) * 100
    non_latin_percent = (getattr(app_state, 'non_latin_count_total', 0) / total_attempts_for_calc) * 100
    cast_percent = (getattr(app_state, 'scene_continuity_count_total', 0) / total_attempts_for_calc) * 100

    # Calculate cost (ensure you have the price per 1k tokens from config)
    price_per_token = global_config.get('api.pricing.cost_per_1k_tokens', 0) / 1000
    estimated_cost = (app_state.total_input_tokens + app_state.total_output_tokens) * price_per_token

    budget_limit = global_config.get('api.pricing.budget_limit', 0.0)
    if app_state.budget_label.winfo_exists():
        if budget_limit > 0:
            app_state.budget_label.config(text=f"Budget: ${estimated_cost:.2f} / ${budget_limit:.2f}",
                                foreground="red" if estimated_cost >= budget_limit else "lightgray")
        else:
            app_state.budget_label.config(text="Budget: Disabled", foreground="lightgray")

    if api_handler.valkey_client:
        try:
            api_handler.valkey_client.set("stats:refusal_count", app_state.refusal_count_total)
            api_handler.valkey_client.set("stats:total_attempts", app_state.total_attempts_global)
        except Exception as e:
            # Log the error but don't stop the dashboard from updating
            log_message(f"Error updating stats in Valkey: {e}", "WARNING")

    if hasattr(app_state.refusal_percent_label, 'winfo_exists') and app_state.refusal_percent_label.winfo_exists():
        app_state.refusal_percent_label.config(text=f"{app_state.refusal_count_total} Refusals encountered ({refusal_percent:.1f}%)")
        app_state.user_speaking_label.config(text=f"{app_state.user_speaking_count_total} User Speak instances ({user_speaking_percent:.1f}%)")
        app_state.slop_label.config(text=f"{app_state.slop_count_total} Slop instances detected ({slop_percent:.1f}%)")
        app_state.error_percent_label.config(text=f"{app_state.error_count_total} Total Errors logged ({error_percent:.1f}%)")
        if hasattr(app_state.ooc_label, 'winfo_exists') and app_state.ooc_label.winfo_exists():
            app_state.ooc_label.config(text=f"{app_state.ooc_count_total} OOC drift instances ({ooc_percent:.1f}%)")
        if getattr(app_state, 'logic_label', None) is not None and hasattr(app_state.logic_label, 'winfo_exists') and app_state.logic_label.winfo_exists():
            app_state.logic_label.config(text=f"{app_state.logic_issue_count_total} logic errors caught & rewritten ({logic_percent:.1f}%)")
        _regex_label = getattr(app_state, 'regex_watch_label', None)
        if _regex_label is not None and hasattr(_regex_label, 'winfo_exists') and _regex_label.winfo_exists():
            _regex_label.config(
                text=f"{getattr(app_state, 'regex_watch_count_total', 0)} replies with regex hits "
                     f"({regex_percent:.1f}%)")
        _non_latin_label = getattr(app_state, 'non_latin_label', None)
        if _non_latin_label is not None and hasattr(_non_latin_label, 'winfo_exists') and _non_latin_label.winfo_exists():
            _non_latin_label.config(
                text=f"{getattr(app_state, 'non_latin_count_total', 0)} non-Latin replies "
                     f"({non_latin_percent:.1f}%)")
        _cast_label = getattr(app_state, 'scene_continuity_label', None)
        if _cast_label is not None and hasattr(_cast_label, 'winfo_exists') and _cast_label.winfo_exists():
            _cast_label.config(
                text=f"{getattr(app_state, 'scene_continuity_count_total', 0)} cast-drift replies "
                     f"({cast_percent:.1f}%)")

        # NEW: Update token and cost labels (you need to create these labels in the UI first)
        app_state.token_label.config(text=f"Tokens: {app_state.total_input_tokens + app_state.total_output_tokens}")
        app_state.cost_label.config(text=f"Est. Cost: ${estimated_cost:.4f}")
        # NEW: Update API response time labels
        for slot_idx in range(6):
            slot_label_name = f"api_response_time_label_{slot_idx+1}"
            if hasattr(app_state.slot_widgets.get(slot_label_name), 'winfo_exists') and app_state.slot_widgets.get(slot_label_name).winfo_exists():
                with api_response_times_lock:
                    response_times = api_response_times_per_slot[slot_idx].copy()

                if response_times:
                    avg_response_time = sum(response_times) / len(response_times)
                    min_response_time = min(response_times)
                    max_response_time = max(response_times)
                    app_state.slot_widgets[slot_label_name].config(
                        text=f"API {slot_idx+1}: {avg_response_time:.2f}s (min: {min_response_time:.2f}s, max: {max_response_time:.2f}s, samples: {len(response_times)})"
                    )
                else:
                    app_state.slot_widgets[slot_label_name].config(text=f"API {slot_idx+1}: No data yet")

    def update_scrolled_text_widget_content(text_widget, recent_items_list, tag_name="highlight", is_total_tab_list=False):
        if not (hasattr(text_widget, 'winfo_exists') and text_widget.winfo_exists()): return

        text_widget.config(state=tk.NORMAL)
        text_widget.delete(1.0, tk.END)
        for item_idx, item in enumerate(recent_items_list):
            phrase_to_highlight = None
            sentence_context = None
            api_origin_idx = -1

            if is_total_tab_list:
                if isinstance(item, tuple) and len(item) == 3 and isinstance(item[0], str) and isinstance(item[1], str) and isinstance(item[2], int):
                    phrase_to_highlight, sentence_context, api_origin_idx = item
                elif isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], str) and isinstance(item[1], int):
                    sentence_context = item[0]
                    api_origin_idx = item[1]
                else:
                    sentence_context = str(item)
            else:
                if isinstance(item, tuple) and len(item) == 2:
                    phrase_to_highlight, sentence_context = item
                else:
                    sentence_context = str(item)

            prefix = f"- "
            if api_origin_idx != -1:
                prefix += f"[API {api_origin_idx+1}] "

            if phrase_to_highlight and sentence_context:
                start_idx = -1; end_idx = -1
                try:
                    match = re.search(r'\b' + re.escape(phrase_to_highlight) + r'\b', sentence_context, re.IGNORECASE)
                    if match:
                        start_idx = match.start(); end_idx = match.end()
                except re.error:
                    start_idx = sentence_context.lower().find(phrase_to_highlight.lower())
                    if start_idx != -1: end_idx = start_idx + len(phrase_to_highlight)

                text_widget.insert(tk.END, prefix)
                if start_idx != -1 and end_idx != -1:
                    text_widget.insert(tk.END, sentence_context[:start_idx])
                    text_widget.insert(tk.END, sentence_context[start_idx:end_idx], (tag_name, f"item_{item_idx}"))
                    text_widget.insert(tk.END, f"{sentence_context[end_idx:]}\n")
                else:
                    text_widget.insert(tk.END, f"{sentence_context} (Highlight failed for '{phrase_to_highlight}')\n")
            elif sentence_context:
                text_widget.insert(tk.END, f"{prefix}{sentence_context}\n")

        text_widget.config(state=tk.DISABLED)
        text_widget.yview(tk.END)

    # Only rebuild the issue panels / redraw the graph for the tab the user is
    # actually looking at. The always-visible metric labels above are already
    # updated unconditionally; these Treeview/matplotlib refreshes are the
    # expensive, X-resource-leaking part.
    active_tab = _active_dashboard_tab_text()

    if active_tab == "Totals":
        update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets["Totals"]["refusals"], app_state.recent_refusals_total, is_total_tab=True)
        update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets["Totals"]["user_speak"], app_state.recent_user_speaking_total, is_total_tab=True)
        update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets["Totals"]["slop"], app_state.recent_slop_total, is_total_tab=True)
        update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets["Totals"]["anti_slop"], app_state.recent_anti_slop_total, is_total_tab=True)
        update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets["Totals"]["out_of_character"], app_state.recent_ooc_total, is_total_tab=True)
        update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets["Totals"]["non_latin"], app_state.recent_non_latin_total, is_total_tab=True)
        update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets["Totals"]["scene_continuity"], app_state.recent_scene_continuity_total, is_total_tab=True)

    for i in range(6):
        api_tab_name = f"API {i+1}"
        if active_tab == api_tab_name and api_tab_name in app_state.dashboard_notebook.tabs_widgets:
            update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets[api_tab_name]["refusals"], app_state.recent_refusals_per_api.get(i,[]))
            update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets[api_tab_name]["user_speak"], app_state.recent_user_speaking_per_api.get(i,[]))
            update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets[api_tab_name]["slop"], app_state.recent_slop_per_api.get(i,[]))
            update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets[api_tab_name]["anti_slop"], app_state.recent_anti_slop_per_api.get(i,[]))
            update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets[api_tab_name]["out_of_character"], app_state.recent_ooc_per_api.get(i,[]))
            update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets[api_tab_name]["errors"], app_state.recent_errors_per_api.get(i,[]))
            update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets[api_tab_name]["non_latin"], app_state.recent_non_latin_per_api.get(i,[]))
            update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets[api_tab_name]["scene_continuity"], app_state.recent_scene_continuity_per_api.get(i,[]))

    # Update the graph on the Totals tab — only while that tab is visible.
    if active_tab == "Totals" and "Totals" in app_state.dashboard_notebook.tabs_widgets:
        graph_canvas_widget = app_state.dashboard_notebook.tabs_widgets["Totals"].get("graph_canvas")
        if graph_canvas_widget and hasattr(graph_canvas_widget, 'winfo_exists') and graph_canvas_widget.winfo_exists():
            try:
                update_issue_graph(graph_canvas_widget)
            except Exception as e_graph:
                log_message(f"Error updating issue graph: {e_graph}", "ERROR")


def update_dashboard_safe(): 
    """Safely updates the dashboard, checking if the root window still exists. Called from ConfigEditor."""
    if app_state.root.winfo_exists(): 
        update_dashboard()


def clear_dashboard():

    # Clear total recent lists
    app_state.recent_refusals_total = []
    app_state.recent_user_speaking_total = []
    app_state.recent_slop_total = []
    app_state.recent_anti_slop_total = []
    app_state.recent_errors_total = []
    app_state.recent_ooc_total = []
    app_state.recent_logic_total = []
    app_state.recent_regex_total = []
    app_state.recent_non_latin_total = []
    app_state.recent_scene_continuity_total = []
    with app_state.regex_watch_log_lock:
        app_state.regex_watch_log = []
    with app_state.cast_events_log_lock:
        app_state.cast_events_log = []

    # Clear per-API recent lists
    for i in range(6):
        app_state.recent_refusals_per_api[i] = []
        app_state.recent_user_speaking_per_api[i] = []
        app_state.recent_slop_per_api[i] = []
        app_state.recent_anti_slop_per_api[i] = []
        app_state.recent_errors_per_api[i] = []
        app_state.recent_ooc_per_api[i] = []
        app_state.recent_logic_per_api[i] = []
        app_state.recent_regex_per_api[i] = []
        app_state.recent_non_latin_per_api[i] = []
        app_state.recent_scene_continuity_per_api[i] = []

    # Clear graph timestamps safely
    with app_state.issue_timestamps_lock:
        for key in app_state.issue_timestamps:
            app_state.issue_timestamps[key] = []

    # Refresh the UI
    update_dashboard()
    log_message("Dashboard and issue graph cleared.", "INFO")


def search_in_dashboard_tab(tab_name):
    """Highlights matching text across all issue panels in the active tab."""
    query = app_state.dashboard_notebook.tabs_widgets[tab_name].get('search_var', tk.StringVar()).get().strip()
    if not query:
        clear_dashboard_search(tab_name)
        return

    clear_dashboard_search(tab_name)
    issue_keys = ["refusals", "user_speak", "slop", "anti_slop", "errors"]

    for key in issue_keys:
        text_widget = app_state.dashboard_notebook.tabs_widgets[tab_name].get(key)
        if text_widget and text_widget.winfo_exists():
            # Configure highlight tag (doesn't interfere with existing issue tags)
            text_widget.tag_configure("search_match", background="#FFD700", foreground="#000000", underline=False)

            start_pos = "1.0"
            while True:
                # Case-insensitive search
                pos = text_widget.search(query, start_pos, stopindex=tk.END, nocase=True)
                if not pos:
                    break
                end_pos = f"{pos}+{len(query)}c"
                text_widget.tag_add("search_match", pos, end_pos)
                start_pos = end_pos

            # Auto-scroll to first match if found
            if text_widget.tag_ranges("search_match"):
                text_widget.see(text_widget.tag_ranges("search_match")[0])


def clear_dashboard_search(tab_name):
    """Removes search highlights from all panels in the specified tab."""
    for key in ["refusals", "user_speak", "slop", "anti_slop", "errors"]:
        text_widget = app_state.dashboard_notebook.tabs_widgets[tab_name].get(key)
        if text_widget and text_widget.winfo_exists():
            text_widget.tag_remove("search_match", "1.0", tk.END)


def copy_dashboard_tab(tab_name):
    """Copies all text from the active tab's panels to clipboard."""
    clipboard_text = []
    for key in ["refusals", "user_speak", "slop", "anti_slop", "errors"]:
        text_widget = app_state.dashboard_notebook.tabs_widgets[tab_name].get(key)
        if text_widget and text_widget.winfo_exists():
            content = text_widget.get("1.0", tk.END).strip()
            if content and content != f"No recent {key}.":
                clipboard_text.append(f"--- {key.upper()} ---\n{content}\n")

    if clipboard_text:
        app_state.root.clipboard_clear()
        app_state.root.clipboard_append("\n".join(clipboard_text))
        app_state.root.update()
        app_state.status_bar.config(text="Dashboard text copied to clipboard!", foreground="green")
    else:
        app_state.status_bar.config(text="No data to copy.", foreground="orange")

def pulse_progress_bar(bar, bar_key, root_window):
    """Briefly flashes a progress bar to a brighter color when a milestone is crossed,
    then reverts it back after a short delay."""
    if not bar or not hasattr(bar, 'winfo_exists') or not bar.winfo_exists():
        return

    current_value = bar['value']
    previous_value = app_state._previous_progress_values.get(bar_key, 0)

    # Define milestones at which to pulse (every 25%)
    milestones = [25, 50, 75, 90, 100]

    for milestone in milestones:
        if previous_value < milestone <= current_value:
            # Milestone crossed! Apply pulse
            try:
                bar.configure(style="Pulse.Horizontal.TProgressbar")
                # Schedule revert after 600ms
                revert_style = get_progress_style_name(current_value)
                root_window.after(600, lambda b=bar, s=revert_style: (
                    b.configure(style=s) if b.winfo_exists() else None
                ))
            except tk.TclError:
                pass
            break

    app_state._previous_progress_values[bar_key] = current_value


def configure_animated_progress_styles(style_obj):
    """Configure custom progress bar styles with color-coded stages and animation support."""
    # Trough (background track) - dark theme consistent
    trough_color = '#2a2a35'
    border_color = '#1e1e24'

    # Stage 1: Low progress (0-25%) - Cool Blue
    style_obj.configure("Low.Horizontal.TProgressbar",
                        troughcolor=trough_color,
                        background='#3498db',
                        darkcolor='#2980b9',
                        lightcolor='#5dade2',
                        bordercolor=border_color,
                        thickness=22)

    # Stage 2: Medium progress (25-50%) - Cyan / Teal
    style_obj.configure("Medium.Horizontal.TProgressbar",
                        troughcolor=trough_color,
                        background='#17a2b8',
                        darkcolor='#138496',
                        lightcolor='#4dc8e8',
                        bordercolor=border_color,
                        thickness=22)

    # Stage 3: Progressing (50-75%) - Green
    style_obj.configure("Progressing.Horizontal.TProgressbar",
                        troughcolor=trough_color,
                        background='#28a745',
                        darkcolor='#1e7e34',
                        lightcolor='#5cb85c',
                        bordercolor=border_color,
                        thickness=22)

    # Stage 4: High progress (75-90%) - Amber / Yellow
    style_obj.configure("High.Horizontal.TProgressbar",
                        troughcolor=trough_color,
                        background='#ffc107',
                        darkcolor='#e0a800',
                        lightcolor='#ffd54f',
                        bordercolor=border_color,
                        thickness=22)

    # Stage 5: Complete (90-100%) - Bright Green with pulse
    style_obj.configure("Complete.Horizontal.TProgressbar",
                        troughcolor=trough_color,
                        background='#51cf66',
                        darkcolor='#40c057',
                        lightcolor='#69db7c',
                        bordercolor=border_color,
                        thickness=22)

    # Pulse state (briefly flashes brighter when milestones are hit)
    style_obj.configure("Pulse.Horizontal.TProgressbar",
                        troughcolor=trough_color,
                        background='#74c0fc',
                        darkcolor='#4dabf7',
                        lightcolor='#a5d8ff',
                        bordercolor=border_color,
                        thickness=22)

    # Error / Stalled state - Red
    style_obj.configure("Error.Horizontal.TProgressbar",
                        troughcolor=trough_color,
                        background='#ff6b6b',
                        darkcolor='#fa5252',
                        lightcolor='#ffa8a8',
                        bordercolor=border_color,
                        thickness=22)

    log_message("Animated progress bar styles configured.", "DEBUG")


def get_progress_style_name(progress_percent):
    """Returns the appropriate style name based on progress percentage."""
    if progress_percent < 0:
        return "Low.Horizontal.TProgressbar"
    elif progress_percent < 25:
        return "Low.Horizontal.TProgressbar"
    elif progress_percent < 50:
        return "Medium.Horizontal.TProgressbar"
    elif progress_percent < 75:
        return "Progressing.Horizontal.TProgressbar"
    elif progress_percent < 90:
        return "High.Horizontal.TProgressbar"
    else:
        return "Complete.Horizontal.TProgressbar"


def update_progress_bar_style(bar, progress_percent, error_state=False):
    """Updates the style of a progress bar based on its current progress percentage."""
    if not bar or not hasattr(bar, 'winfo_exists') or not bar.winfo_exists():
        return
    if error_state:
        style_name = "Error.Horizontal.TProgressbar"
    else:
        style_name = get_progress_style_name(progress_percent)
    try:
        bar.configure(style=style_name)
    except tk.TclError:
        pass  # Style might not exist yet during early init


def update_rate_limit_status():
    """Updates the rate limit status labels with progress-style visualization."""
    if not app_state.rate_limit_labels:
        return

    # Color constants matching the dark theme
    ACCENT_GREEN = "#51cf66"
    ACCENT_ORANGE = "#ff922b"
    ACCENT_RED = "#ff6b6b"

    with global_rate_limiter.lock:
        for slot_idx in range(6):
            try:
                limit = global_rate_limiter.rates_per_slot[slot_idx]
                used = len(global_rate_limiter.requests_per_slot[slot_idx])
                remaining = max(0, limit - used)
                usage_percent = (used / limit) * 100 if limit > 0 else 0

                if slot_idx in app_state.rate_limit_labels:
                    if usage_percent > 90:
                        icon = "🔴"
                        color = ACCENT_RED
                    elif usage_percent > 70:
                        icon = "🟡"
                        color = ACCENT_ORANGE
                    elif usage_percent > 40:
                        icon = "🟠"
                        color = ACCENT_ORANGE
                    else:
                        icon = "🟢"
                        color = ACCENT_GREEN

                    app_state.rate_limit_labels[slot_idx].config(
                        text=f"{icon} API {slot_idx+1}: {remaining}/{limit} ({usage_percent:.0f}% used)",
                        foreground=color
                    )
            except Exception as e:
                log_message(f"Error updating rate limit status for slot {slot_idx}: {e}", "ERROR")


def update_thread_status_display():
    """Periodically updates the thread count label in the GUI."""
    try:
        # Safely count spawned and active threads
        spawned = len(app_state.threads) if app_state.threads else 0
        active = sum(1 for t in app_state.threads if t.is_alive()) if spawned > 0 else 0
        app_state.thread_status_label.config(text=f"Threads: {spawned} spawned, {active} active")
    except Exception:
        app_state.thread_status_label.config(text="Threads: 0 spawned, 0 active")

    # Schedule next update every 1 second
    if app_state.root.winfo_exists():
        app_state.root.after(1000, update_thread_status_display)
