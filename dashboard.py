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

def create_modern_issue_panel(panel_widget, columns, highlight_color="#ff6b6b"):
    """Creates a modern, dark-themed Treeview inside an existing panel widget."""
    style = ttk.Style()

    # --- Treeview styles (unchanged) ---
    style.configure("Issue.Treeview",
                    background="#2a2a35",
                    fieldbackground="#2a2a35",
                    foreground="#e0e0e0",
                    rowheight=28,
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
    for col in columns:
        tree.heading(col, text=col.replace('_', ' ').title())
        tree.column(col, width=col_widths.get(col, 120), anchor='w')

    # --- Scrollbar (tk, not ttk — width is respected) ---
    vsb = tk.Scrollbar(
        panel_widget,
        orient="vertical",
        command=tree.yview,
        width=18,
        background="#7a7a9a",
        troughcolor="#16161c",
        activebackground="#b0b0d0",
        highlightthickness=0,
        borderwidth=0,
        relief="flat"
    )
    tree.configure(yscrollcommand=vsb.set)
    # ----------------------------------------------------

    tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(5, 2), pady=5)
    vsb.pack(side=tk.RIGHT, fill="y", padx=(2, 5), pady=5)

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

def draw_issue_graph(canvas_widget, height=400):
    """Draws a modern, detailed time-series graph showing issue counts over the last 60 minutes."""
    canvas_widget.delete("all")

    # Modern dark theme setup
    fig = Figure(figsize=(12, 4.5), dpi=120, facecolor='#1e1e24')
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
              'slop': [0]*num_bins, 'errors': [0]*num_bins, 'anti_slop': [0]*num_bins}

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
    offsets = [-2*width, -width, 0, width, 2*width]

    # Cohesive, accessible color palette
    colors = {'refusals': '#ff4d6d', 'user_speaking': '#4dabf7', 'slop': '#9775fa',
              'anti_slop': '#ffd43b', 'errors': '#fc8181'}
    labels = {'refusals': 'Refusals', 'user_speaking': 'User Speak', 'slop': 'Slop',
              'anti_slop': 'Anti-Slop', 'errors': 'Errors'}

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
    fig.tight_layout()

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


# Module-level throttle state for the issue graph (add near the top of dashboard.py,
# after the existing imports, before any function definitions):
_graph_last_draw = {"t": 0.0, "data_sig": None}


def update_issue_graph(canvas_widget):
    """Updates an existing issue graph with new data without recreating the figure.

    Throttled to at most one full redraw every 5 seconds (or when the underlying
    timestamp data changes) to avoid flooding the X server with millions of
    rasterise requests over a long run.
    """
    if not hasattr(canvas_widget, 'graph_canvas'):
        draw_issue_graph(canvas_widget)
        _graph_last_draw["t"] = time.time()
        return

    # --- Throttle: skip redraw if < 5 s elapsed AND data is unchanged ---
    now = time.time()
    with app_state.issue_timestamps_lock:
        sig = tuple(
            len(app_state.issue_timestamps.get(k, []))
            for k in ("refusals", "user_speaking", "slop", "errors", "anti_slop")
        )
    if (now - _graph_last_draw["t"]) < 5.0 and sig == _graph_last_draw["data_sig"]:
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
              'slop': [0]*num_bins, 'errors': [0]*num_bins, 'anti_slop': [0]*num_bins}

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
    offsets = [-2*width, -width, 0, width, 2*width]
    colors = {'refusals': '#ff4d6d', 'user_speaking': '#4dabf7', 'slop': '#9775fa',
              'anti_slop': '#ffd43b', 'errors': '#fc8181'}
    labels = {'refusals': 'Refusals', 'user_speaking': 'User Speak', 'slop': 'Slop',
              'anti_slop': 'Anti-Slop', 'errors': 'Errors'}

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
    fig.tight_layout()
    canvas_widget.graph_canvas.draw()


def update_dashboard():
    """Updates the dashboard labels and text areas with current statistics and recent issues."""

    # NEW: Update rate limit status
    update_rate_limit_status()

    total_attempts_for_calc = app_state.total_attempts_global if app_state.total_attempts_global > 0 else 1

    refusal_percent = (app_state.refusal_count_total / total_attempts_for_calc) * 100
    user_speaking_percent = (app_state.user_speaking_count_total / total_attempts_for_calc) * 100
    slop_percent = (app_state.slop_count_total / total_attempts_for_calc) * 100
    error_percent = (app_state.error_count_total / total_attempts_for_calc) * 100

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

    update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets["Totals"]["refusals"], app_state.recent_refusals_total, is_total_tab=True)
    update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets["Totals"]["user_speak"], app_state.recent_user_speaking_total, is_total_tab=True)
    update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets["Totals"]["slop"], app_state.recent_slop_total, is_total_tab=True)
    update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets["Totals"]["anti_slop"], app_state.recent_anti_slop_total, is_total_tab=True)

    for i in range(6):
        api_tab_name = f"API {i+1}"
        if api_tab_name in app_state.dashboard_notebook.tabs_widgets:
            update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets[api_tab_name]["refusals"], app_state.recent_refusals_per_api.get(i,[]))
            update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets[api_tab_name]["user_speak"], app_state.recent_user_speaking_per_api.get(i,[]))
            update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets[api_tab_name]["slop"], app_state.recent_slop_per_api.get(i,[]))
            update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets[api_tab_name]["anti_slop"], app_state.recent_anti_slop_per_api.get(i,[]))
            update_modern_issue_panel(app_state.dashboard_notebook.tabs_widgets[api_tab_name]["errors"], app_state.recent_errors_per_api.get(i,[]))

    # NEW: Update the graph on the Totals tab
    if "Totals" in app_state.dashboard_notebook.tabs_widgets:
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

    # Clear per-API recent lists
    for i in range(6):
        app_state.recent_refusals_per_api[i] = []
        app_state.recent_user_speaking_per_api[i] = []
        app_state.recent_slop_per_api[i] = []
        app_state.recent_anti_slop_per_api[i] = []
        app_state.recent_errors_per_api[i] = []

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
