#!/usr/bin/env python3
"""Check the real GUI at smaller window sizes without running generation.

Run with the application's normal Python dependencies and a desktop display:
    python tools/check_gui_layout.py --check --config-editor
Linux CI can use xvfb-run with the same command. Reports go to ./layout-check
unless --output specifies another directory. --screenshots adds PNG captures.
Only temporary fixture configuration is loaded. No API/DB connections or workers
are started, and no application configuration or generation state is saved.
Production source is executed in memory through its UI-construction boundary.
Tk scaling is recorded; --scale is a diagnostic, not Windows DPI emulation.
"""
from __future__ import annotations

import argparse
import ast
import copy
from contextlib import contextmanager
import hashlib
from importlib.metadata import version
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import sys
import tempfile
from types import SimpleNamespace


DEFAULT_SIZES = ["900x600", "1024x700", "1366x700", "1916x1126"]
SUPPORTED_MINIMUM_AT_96_DPI = (760, 520)


@contextmanager
def working_directory(path):
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        # Exit this scope before TemporaryDirectory cleanup, including on Windows.
        os.chdir(previous)


def parse_size(value):
    try:
        width, height = (int(part) for part in value.lower().split("x"))
        if min(width, height) < 1:
            raise ValueError
        return width, height
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Expected positive WIDTHxHEIGHT") from exc


def extract_ui(source, filename):
    """Keep real module construction, excluding the first startup action."""
    module = ast.parse(source, filename=filename)
    stop = next((index for index, node in enumerate(module.body)
                 if isinstance(node, ast.Expr)
                 and isinstance(node.value, ast.Call)
                 and isinstance(node.value.func, ast.Name)
                 and node.value.func.id == "reset_all_stats_and_history"), None)
    if stop is None:
        raise RuntimeError("Cannot find the expected startup boundary; refusing to run.")
    startup = module.body[stop]
    start_fn = next(node for node in module.body
                    if isinstance(node, ast.FunctionDef)
                    and node.name == "start_processing")
    progress = next((node for node in start_fn.body
                     if isinstance(node, ast.If)
                     and isinstance(node.test, ast.Name)
                     and node.test.id == "master_duplication_enabled"
                     and "api_processed_tasks" in ast.unparse(node)
                     and "ttk.Progressbar" in ast.unparse(node)), None)
    if progress is None:
        raise RuntimeError("Cannot identify the production progress-widget block.")
    helper = ast.parse("def _probe_construct_progress():\n    pass\n")
    helper.body[0].body = [copy.deepcopy(progress)]
    ast.fix_missing_locations(helper)
    ui = ast.Module(body=module.body[:stop], type_ignores=[])
    return (compile(ui, filename, "exec"), compile(helper, filename, "exec"),
            {"startup_excluded_at_line": startup.lineno,
             "progress_block_lines": [progress.lineno, progress.end_lineno]})


def rect(widget):
    return [widget.winfo_rootx(), widget.winfo_rooty(),
            widget.winfo_width(), widget.winfo_height()]


def intersect(first, second):
    x = max(first[0], second[0])
    y = max(first[1], second[1])
    right = min(first[0] + first[2], second[0] + second[2])
    bottom = min(first[1] + first[3], second[1] + second[3])
    return [x, y, max(0, right - x), max(0, bottom - y)]


def widget_info(widget, window):
    """Intersect with every ancestor; report mapping separately from old sizes."""
    own = rect(widget)
    window_rect = rect(window)
    visible = own
    ancestor = widget
    mapped = bool(widget.winfo_ismapped())
    while ancestor is not None:
        mapped = mapped and bool(ancestor.winfo_ismapped())
        visible = intersect(visible, rect(ancestor))
        if ancestor == window:
            break
        ancestor = ancestor.master
    visible = intersect(visible, window_rect)
    if not mapped:
        visible[2:] = [0, 0]
    screen_visible = intersect(visible, [0, 0, window.winfo_screenwidth(),
                                         window.winfo_screenheight()])
    own_area = max(1, own[2] * own[3])
    item = {
        "path": str(widget), "class": widget.winfo_class(),
        "mapped": mapped,
        "rect_in_window": [own[0] - window_rect[0], own[1] - window_rect[1],
                           own[2], own[3]],
        "requested_size": [widget.winfo_reqwidth(), widget.winfo_reqheight()],
        "visible_size_in_window": visible[2:],
        "visible_fraction_in_window": round(visible[2] * visible[3] / own_area, 4),
        "visible_size_on_screen": screen_visible[2:],
    }
    try:
        item["text"] = str(widget.cget("text"))
    except Exception:
        pass
    if widget.winfo_class() == "Canvas":
        item["scrollregion"] = str(widget.cget("scrollregion"))
        item["yview"] = list(widget.yview())
        item["content_bbox"] = widget.bbox("all")
    return item


def descendants(widget):
    for child in widget.winfo_children():
        yield child
        yield from descendants(child)


def settle(root):
    # Process geometry and tab-change events; no generation timers are installed.
    for _ in range(3):
        root.update_idletasks()
        root.update()


def select_tab(notebook, text):
    for tab in notebook.tabs():
        if notebook.tab(tab, "text") == text:
            notebook.select(tab)
            return notebook.nametowidget(tab)
    raise RuntimeError(f"Missing expected tab: {text}")


def take_screenshot(window, destination, errors):
    try:
        from PIL import ImageGrab
        x, y, width, height = rect(window)
        ImageGrab.grab(bbox=(x, y, x + width, y + height)).save(destination)
        return destination.name
    except Exception as exc:
        errors.append(f"Screenshot {destination.name}: {type(exc).__name__}: {exc}")
        return None


def check(report, name, passed, details=None):
    report["checks"].append({"name": name, "passed": bool(passed), "details": details})


def fully_visible(info):
    return (info["mapped"] and min(info["visible_size_in_window"]) > 0
            and info["visible_fraction_in_window"] >= 0.999)


def natural_size_fits(info):
    return all(actual >= requested for actual, requested in
               zip(info["rect_in_window"][2:], info["requested_size"]))


def supported_minimum_bound(window):
    scale = window.winfo_fpixels("1i") / 96.0
    screen = (window.winfo_screenwidth(), window.winfo_screenheight())
    return [min(round(target * scale), available)
            for target, available in zip(SUPPORTED_MINIMUM_AT_96_DPI, screen)]


def check_requested_size(report, window, requested, label):
    minimum = window.minsize()
    supported = supported_minimum_bound(window)
    actual = [window.winfo_width(), window.winfo_height()]
    check(report, f"{label}: minimum within supported bounds",
          all(minimum[i] <= supported[i] for i in (0, 1)),
          {"minimum": list(minimum), "supported_bound": supported})
    if all(requested[i] >= supported[i] for i in (0, 1)):
        check(report, f"{label}: fits requested client size",
              all(actual[i] <= requested[i] + 2 for i in (0, 1)),
              {"requested": requested, "actual": actual, "minimum": list(minimum)})


def check_tab_contents(report, root, page, label):
    """Exercise actual scroll handlers, without requiring offscreen rows to show."""
    for widget in descendants(page):
        if widget.winfo_class() == "Canvas":
            window_items = [item for item in widget.find_all() if widget.type(item) == "window"]
            if not window_items:
                continue
            content = widget.nametowidget(widget.itemcget(window_items[0], "window"))
            original = widget.cget("scrollregion")
            try:
                # A content Configure must refresh THIS canvas, not the last tab's.
                widget.configure(scrollregion=(0, 0, 1, 1))
                content.event_generate("<Configure>")
                settle(root)
                actual = tuple(float(value) for value in widget.tk.splitlist(widget.cget("scrollregion")))
                expected = widget.bbox("all")
                check(report, f"{label}: own canvas scrollregion {widget}",
                      expected is not None and len(actual) == 4
                      and all(abs(a - b) <= 1 for a, b in zip(actual, expected)),
                      {"actual": actual, "expected": expected})
            finally:
                widget.configure(scrollregion=original)
        elif widget.winfo_class() == "Treeview":
            # Empty tables still have column geometry and should remain navigable.
            original_x = widget.xview()[0]
            try:
                widget.xview_moveto(0)
                settle(root)
                overflow = widget.xview()[1] < 0.999
                widget.xview_moveto(1)
                settle(root)
                check(report, f"{label}: table horizontal end {widget}",
                      widget.xview()[1] >= 0.999
                      and (not overflow or bool(widget.cget("xscrollcommand"))),
                      {"overflow": overflow, "end_view": widget.xview()})
            finally:
                widget.xview_moveto(original_x)


def check_navigation(report, root, window, notebook, label):
    navigator = next((widget for widget in descendants(window)
                      if type(widget).__name__ == "NotebookNavigator"
                      and widget.notebook == notebook), None)
    check(report, f"{label}: view selector exists", navigator is not None)
    if navigator is None:
        return
    original = notebook.select()
    navigator.refresh()
    settle(root)
    tabs = [tab for tab in notebook.tabs() if notebook.tab(tab, "state") != "hidden"]
    expected_labels = [notebook.tab(tab, "text") for tab in tabs]
    actual_labels = list(navigator.selector.cget("values"))
    check(report, f"{label}: selector contains every view", actual_labels == expected_labels,
          {"expected": expected_labels, "actual": actual_labels})
    if navigator.winfo_ismapped():
        check(report, f"{label}: compact selector fully visible",
              fully_visible(widget_info(navigator.selector, window)))
    for index, tab in enumerate(tabs):
        if notebook.tab(tab, "state") == "disabled":
            continue
        navigator.selector.current(index)
        navigator.selector.event_generate("<<ComboboxSelected>>")
        settle(root)
        page = notebook.nametowidget(tab)
        tab_label = f"{label}/{notebook.tab(tab, 'text')}"
        selected = notebook.select() == tab
        visible = widget_info(page, window)
        check(report, f"{tab_label}: selectable and visible",
              selected and visible["mapped"] and min(visible["visible_size_in_window"]) > 0,
              visible["visible_size_in_window"])
        if selected:
            check_tab_contents(report, root, page, tab_label)
    notebook.select(original)
    settle(root)


def run(args):
    repo = args.repo.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    source_path = repo / "generate.py"
    source = source_path.read_text(encoding="utf-8")
    ui_code, progress_code, seams = extract_ui(source, str(source_path))
    sizes = args.size or [parse_size(value) for value in DEFAULT_SIZES]
    # Revisit the smallest size after growth to catch stale reflow/minimum state.
    sizes = sorted(sizes, key=lambda size: (size[0] * size[1], size[0]))
    if len(sizes) > 1:
        sizes = sizes + [sizes[0]]
    report = {
        "platform": platform.platform(), "python": sys.version,
        "repository": str(repo),
        "generate_sha256": hashlib.sha256(source_path.read_bytes()).hexdigest(),
        "source_sha256": {name: hashlib.sha256((repo / name).read_bytes()).hexdigest()
                          for name in ("generate.py", "config_editor.py", "dashboard.py", "ui_layout.py")
                          if (repo / name).is_file()},
        "seams": seams, "requested_tk_scale": args.scale,
        "minimum_size_override": args.allow_below_minimum,
        "limitations": [
            "Tk scale changes are not an emulation of Windows display scaling.",
            "Font substitution and platform widget metrics affect pixel measurements.",
            "Requested sizes are client sizes, excluding title bars and taskbars.",
        ],
        "errors": [], "dialogs": [], "blocked_network": [], "checks": [],
        "main_window_cases": [], "config_editor_cases": [],
    }
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(repo))

    # Fail closed if a future source revision unexpectedly attempts Python networking.
    real_connect = socket.socket.connect
    real_connect_ex = socket.socket.connect_ex
    def guarded_connect(sock, address):
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            report["blocked_network"].append("socket.connect")
            raise RuntimeError("Network disabled for isolated layout probe")
        return real_connect(sock, address)
    def guarded_connect_ex(sock, address):
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            report["blocked_network"].append("socket.connect_ex")
            raise RuntimeError("Network disabled for isolated layout probe")
        return real_connect_ex(sock, address)
    socket.socket.connect = guarded_connect
    socket.socket.connect_ex = guarded_connect_ex

    root = None
    try:
        with tempfile.TemporaryDirectory(prefix="sdg-layout-") as scratch, working_directory(scratch):
            Path("config").mkdir()
            # JSON is valid YAML; only harmless fixture values are loaded.
            fixture = {
                "database": {"enabled": False}, "valkey": {"enabled": False},
                "api": {"pricing": {"cost_per_1k_tokens": 0, "budget_limit": 0},
                        "apis": [{"enabled": True, "url": "https://example.invalid",
                                  "model": "Layout probe", "key": "", "threads": 1,
                                  "rate_limit_rpm": 60} for _ in range(6)]},
                "generation": {"max_character_cards": 1},
            }
            Path("config/config.yml").write_text(json.dumps(fixture), encoding="utf-8")
            if (repo / "config/api_profiles.yml").is_file():
                shutil.copyfile(repo / "config/api_profiles.yml", "config/api_profiles.yml")
            for slot in range(1, 7):
                for prefix in ("API_URL", "MODEL_NAME", "API_KEY"):
                    os.environ.pop(f"{prefix}_{slot}", None)

            import tkinter as tk
            from tkinter import font, messagebox
            import ttkbootstrap as ttkbs
            import requests
            import psycopg2
            from psycopg2 import pool

            def capture_tk_error(self, typ, value, trace):
                report["errors"].append(f"Tk callback: {typ.__name__}: {value}")
            tk.Tk.report_callback_exception = capture_tk_error
            def no_network(*unused_args, **unused_kwargs):
                report["blocked_network"].append("API or database connection")
                raise RuntimeError("Network disabled for isolated layout probe")
            requests.sessions.Session.request = no_network
            psycopg2.connect = no_network
            pool.ThreadedConnectionPool = no_network
            def capture_dialog(title=None, message=None, *unused_args, **unused_kwargs):
                report["dialogs"].append({"title": title, "message": message})
                return "cancel"
            messagebox._show = capture_dialog

            real_window = ttkbs.Window
            def probe_window(*window_args, **window_kwargs):
                window = real_window(*window_args, **window_kwargs)
                if args.scale is not None:
                    window.tk.call("tk", "scaling", args.scale)
                return window
            ttkbs.Window = probe_window
            namespace = {"__name__": "_isolated_sdg_layout", "__file__": str(source_path)}
            exec(ui_code, namespace)
            exec(progress_code, namespace)
            state = namespace["app_state"]
            root = state.root
            namespace["api_handler"].valkey_client = None
            if args.allow_below_minimum:
                root.minsize(1, 1)
            report["display"] = {
                "screen_size": [root.winfo_screenwidth(), root.winfo_screenheight()],
                "tk_scaling": float(root.tk.call("tk", "scaling")),
                "pixels_per_inch": root.winfo_fpixels("1i"),
                "tk_version": root.tk.call("package", "provide", "Tk"),
                "tcl_version": root.tk.call("info", "patchlevel"),
                "package_versions": {name: version(name) for name in
                                     ("ttkbootstrap", "matplotlib", "Pillow", "requests", "PyYAML")},
                "body_font": font.Font(root=root, family="Segoe UI", size=10).actual(),
                "code_font": font.Font(root=root, family="Consolas", size=10).actual(),
                "default_font": font.nametofont("TkDefaultFont", root=root).actual(),
                "body_font_metrics": font.Font(root=root, family="Segoe UI", size=10).metrics(),
                "root_minsize": list(root.minsize()),
                "supported_minimum_bound": supported_minimum_bound(root),
            }
            notebook = state.dashboard_notebook
            active_tab = select_tab(notebook, "Totals")
            for scenario in ("idle", "single", "four_apis"):
                state.master_duplication_enabled_var.set(scenario == "four_apis")
                for child in namespace["progress_frame"].winfo_children():
                    child.destroy()
                if scenario != "idle":
                    state.task_queue = SimpleNamespace(api_widgets={}, total_tasks_for_progress=100)
                    namespace.update(master_duplication_enabled=scenario == "four_apis",
                                     should_resume=False, current_num_turns=1,
                                     all_api_configs_runtime=fixture["api"]["apis"][:4])
                    namespace["_probe_construct_progress"]()
                else:
                    state.task_queue = None
                for case_index, (width, height) in enumerate(sizes):
                    root.geometry(f"{width}x{height}+0+0")
                    settle(root)
                    named = {"root": root, "dashboard_outer": namespace["dashboard_outer_frame"],
                             "notebook": notebook, "active_tab": active_tab,
                             "content_viewport": notebook.tabs_widgets["Totals"]["canvas"],
                             "graph": notebook.tabs_widgets["Totals"]["graph_canvas"],
                             "progress": namespace["progress_frame"], "status": state.status_bar}
                    for name in ("start_button", "pause_button", "stop_clear_button", "config_button",
                                 "quit_button", "export_button", "clear_db_button", "recovery_button"):
                        named[name] = namespace[name]
                    case = {"scenario": scenario, "resize_index": case_index,
                            "requested_client_size": [width, height],
                            "actual_client_size": [root.winfo_width(), root.winfo_height()],
                            "widgets": {name: widget_info(widget, root) for name, widget in named.items()},
                            "root_children": [widget_info(child, root) for child in root.winfo_children()]}
                    if args.screenshots:
                        case["screenshot"] = take_screenshot(root, output / f"{scenario}_{case_index}_{width}x{height}.png",
                                                             report["errors"])
                    report["main_window_cases"].append(case)
                    label = f"{scenario}/{case_index}/{width}x{height}"
                    check_requested_size(report, root, [width, height], label)
                    for name in ("start_button", "pause_button", "stop_clear_button", "config_button"):
                        info = case["widgets"][name]
                        check(report, f"{label}: {name} fully visible", fully_visible(info),
                              info["visible_size_in_window"])
                        check(report, f"{label}: {name} fits natural size", natural_size_fits(info),
                              {"actual": info["rect_in_window"][2:], "requested": info["requested_size"]})
                    viewport = case["widgets"]["content_viewport"]
                    check(report, f"{label}: Totals viewport at least 120px high",
                          viewport["mapped"] and viewport["visible_size_in_window"][1] >= 120,
                          viewport["visible_size_in_window"])
                check_navigation(report, root, root, notebook, scenario)

            idle = [case for case in report["main_window_cases"] if case["scenario"] == "idle"]
            for scenario in ("single", "four_apis"):
                cases = [case for case in report["main_window_cases"] if case["scenario"] == scenario]
                for baseline, case in zip(idle, cases):
                    before = baseline["widgets"]["content_viewport"]["visible_size_in_window"][1]
                    after = case["widgets"]["content_viewport"]["visible_size_in_window"][1]
                    check(report, f"{scenario}/{case['resize_index']}: progress preserves detail height",
                          after >= before - 2, {"idle_height": before, "height": after})
            if len(sizes) > 1:
                for scenario in ("idle", "single", "four_apis"):
                    cases = [case for case in report["main_window_cases"] if case["scenario"] == scenario]
                    first = cases[0]["widgets"]["content_viewport"]["visible_size_in_window"]
                    last = cases[-1]["widgets"]["content_viewport"]["visible_size_in_window"]
                    check(report, f"{scenario}: small-large-small restores viewport",
                          all(abs(a - b) <= 2 for a, b in zip(first, last)),
                          {"first": first, "last": last})

            if args.config_editor:
                editor = namespace["ConfigEditor"](root, state.global_config,
                    state.master_duplication_enabled_var, namespace["no_user_impersonation_var"])
                if args.allow_below_minimum:
                    editor.minsize(1, 1)
                report["editor_minsize"] = list(editor.minsize())
                for case_index, (width, height) in enumerate(sizes):
                    editor.geometry(f"{width}x{height}+0+0")
                    editor.lift()
                    settle(root)
                    selected = editor.nametowidget(editor.notebook.select())
                    case = {"requested_client_size": [width, height],
                            "resize_index": case_index,
                            "actual_client_size": [editor.winfo_width(), editor.winfo_height()],
                            "active_tab": editor.notebook.tab(editor.notebook.select(), "text"),
                            "widgets": {"editor": widget_info(editor, editor),
                                        "notebook": widget_info(editor.notebook, editor),
                                        "active_tab": widget_info(selected, editor)},
                            "buttons": [widget_info(widget, editor) for widget in descendants(editor)
                                        if widget.winfo_class() in ("TButton", "Button")]}
                    if args.screenshots:
                        case["screenshot"] = take_screenshot(editor, output / f"editor_{case_index}_{width}x{height}.png",
                                                             report["errors"])
                    report["config_editor_cases"].append(case)
                    label = f"editor/{case_index}/{width}x{height}"
                    check_requested_size(report, editor, [width, height], label)
                    for name in ("save_button", "validate_button"):
                        button = getattr(editor, name, None)
                        info = widget_info(button, editor) if button is not None else None
                        check(report, f"{label}: {name} fully visible",
                              info is not None and fully_visible(info))
                        check(report, f"{label}: {name} fits natural size",
                              info is not None and natural_size_fits(info),
                              None if info is None else
                              {"actual": info["rect_in_window"][2:], "requested": info["requested_size"]})
                check_navigation(report, root, editor, editor.notebook, "editor")
                editor.destroy()  # Deliberately bypass the close handler, which saves config.
            root.destroy()  # Likewise bypass Quit Application and its state-saving behavior.
            root = None
            check(report, "no Tk callback or capture errors", not report["errors"], report["errors"])
            check(report, "no network attempts", not report["blocked_network"], report["blocked_network"])
            check(report, "no unexpected dialogs", not report["dialogs"], report["dialogs"])
    finally:
        if root is not None:
            try:
                root.destroy()
            except Exception:
                pass
        socket.socket.connect = real_connect
        socket.socket.connect_ex = real_connect_ex
        (output / "layout_measurements.json").write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"Measurements: {output / 'layout_measurements.json'}")
    for case in report["main_window_cases"]:
        viewport = case["widgets"]["content_viewport"]
        print(f"{case['scenario']:10} requested={case['requested_client_size']} "
              f"actual={case['actual_client_size']} dashboard_viewport="
              f"{viewport['visible_size_in_window']} mapped={viewport['mapped']}")
    failed = [item for item in report["checks"] if not item["passed"]]
    print(f"Checks: {len(report['checks']) - len(failed)} passed, {len(failed)} failed")
    for item in failed:
        print(f"FAIL: {item['name']}: {item['details']}")
    return 1 if args.check and failed else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path,
                        default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--output", type=Path, default=Path("layout-check"),
                        help="Report directory (default: ./layout-check)")
    parser.add_argument("--check", action="store_true", help="Exit nonzero if a layout assertion fails")
    parser.add_argument("--size", type=parse_size, action="append", help="Client WIDTHxHEIGHT; repeatable")
    parser.add_argument("--scale", type=float, help="Tk pixels/point scale, not Windows DPI emulation")
    parser.add_argument("--screenshots", action="store_true")
    parser.add_argument("--config-editor", action="store_true")
    parser.add_argument("--allow-below-minimum", action="store_true",
                        help="Explicit diagnostic override of production window minima")
    args = parser.parse_args()
    if args.scale is not None and args.scale <= 0:
        parser.error("--scale must be positive")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
