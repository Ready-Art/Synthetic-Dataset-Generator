# SPDX-License-Identifier: AGPL-3.0-or-later
#
# ReadyArt Synthetic Dataset Generator
# Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors
#
# All code here is AGPLv3, and has been for some time. Very early versions were MIT by
# Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
# it is unlikely any of that code survives. Unless you have the original MIT-era code to
# compare against, treat this file as AGPLv3-only. See docs/licensing.md.

"""Display-dependent regression test for the 📊 Metrics dashboard tab (v13.1.0).

Run directly: ``xvfb-run python -B tests/test_metrics_tab_ui.py`` (needs a display; skips
cleanly without one).

This builds the REAL ``generate.py`` UI and pins the invariants that make the tab useful:

1. the tab exists, is registered in ``tabs_widgets``, and has no issue panels (it is a
   diagnostics area like Regex Watch, not an issue view);
2. the table uses the dedicated readable ``Treeview`` style and no column is narrower than its
   own heading text;
3. the table has one row PER API SLOT with a vLLM backend and never one aggregated row, and two
   slots on the same server still get two rows;
4. a slot whose endpoint is unreachable keeps its own row, marked as such, instead of vanishing;
5. the graph draws one line per endpoint, never one combined line.

Every HTTP call is stubbed, so no real vLLM is needed and nothing leaves the process.
"""

import atexit as _atexit
import os as _os
import shutil as _shutil
import sys as _sys
import tempfile as _tempfile
import unittest

_REPO = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
_sys.path.insert(0, _REPO)

SCRATCH = _tempfile.mkdtemp(prefix="sdg-metrics-tab-ui-")
_atexit.register(_shutil.rmtree, SCRATCH, ignore_errors=True)
_os.chdir(SCRATCH)
_shutil.copytree(_os.path.join(_REPO, "config"), _os.path.join(SCRATCH, "config"))

try:
    import tkinter as tk
    from tkinter import font, ttk
except ImportError as exc:
    tk = None
    TK_UNAVAILABLE = str(exc)
else:
    TK_UNAVAILABLE = None

MIN_ROW_SLACK = 6

# A body shaped like the real vLLM /metrics output this tab reads.
VLLM_BODY = """\
vllm:num_requests_running{engine="0",model_name="acme/model"} 12.0
vllm:num_requests_waiting{engine="0",model_name="acme/model"} 1.0
vllm:kv_cache_usage_perc{engine="0",model_name="acme/model"} 0.72
vllm:request_prefill_kv_computed_tokens_sum{engine="0",model_name="acme/model"} 90000.0
vllm:generation_tokens_total{engine="0",model_name="acme/model"} 12000.0
vllm:request_prefill_time_seconds_sum{engine="0",model_name="acme/model"} 300.0
vllm:request_decode_time_seconds_sum{engine="0",model_name="acme/model"} 150.0
vllm:request_success_total{engine="0",finished_reason="stop",model_name="acme/model"} 520.0
"""


class _StubResponse:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code


class _StubRequests:
    """Serve a canned /metrics body for any URL, so nothing leaves the process."""

    def __init__(self, body=VLLM_BODY):
        self.body = body
        self.calls = []

    def get(self, url, timeout=None):
        self.calls.append(url)
        return _StubResponse(self.body)


class _ConfigStub:
    """A plain-dict config that answers the dot-separated paths this module reads."""

    def __init__(self, data):
        self.data = data

    def get(self, path, default=None):
        value = self.data
        for key in path.split("."):
            if isinstance(value, dict) and key in value:
                value = value[key]
            elif isinstance(value, list) and key.isdigit() and int(key) < len(value):
                value = value[int(key)]
            else:
                return default
        return value


class MetricsTabLayoutTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if tk is None:
            raise unittest.SkipTest("Tk is unavailable: " + TK_UNAVAILABLE)
        try:
            import generate  # noqa: F401  (constructs the whole UI)
        except tk.TclError as exc:
            raise unittest.SkipTest("A working Tk display is required: {}".format(exc)) from exc
        except Exception as exc:  # pragma: no cover - environment problem
            raise unittest.SkipTest("Could not construct the UI: {}".format(exc)) from exc

        import app_state
        import vllm_metrics

        cls.app_state = app_state
        cls.vllm_metrics = vllm_metrics
        cls.root = app_state.root
        cls.notebook = app_state.dashboard_notebook
        cls.tab = "📊 Metrics"
        cls.settle()

        _os.makedirs("output", exist_ok=True)
        cls.widgets = cls.notebook.tabs_widgets[cls.tab]

        # A poller must never run inside a test: it would issue real requests.
        vllm_metrics.stop_polling()
        cls.REAL_CONFIG = app_state.global_config
        cls.REAL_REQUESTS = vllm_metrics.requests

    @classmethod
    def tearDownClass(cls):
        if tk is not None:
            try:
                cls.root.destroy()
            except tk.TclError:
                pass

    @classmethod
    def settle(cls):
        cls.root.update_idletasks()
        cls.root.update()

    def _select_tab(self):
        for tab_id in self.notebook.tabs():
            if "Metrics" in self.notebook.tab(tab_id, "text"):
                self.notebook.select(tab_id)
                break
        self.settle()

    def _stub_poll(self, slots, stub_class=None):
        """Poll the given ``{slot_idx: url}`` with stubbed HTTP, then refresh the tab.

        ``app_state.global_config`` is swapped for a plain-dict stand-in so the row builder sees
        exactly these slots, and both patches are restored after the test.
        """
        apis = [{"url": "", "model": "m%d" % i} for i in range(6)]
        for slot_idx, url in slots.items():
            apis[slot_idx]["url"] = url
        config = _ConfigStub({"api": {"apis": apis},
                              "quality": {"scoring_api": {"url": ""}}})

        stub = stub_class() if stub_class is not None else _StubRequests()
        self.addCleanup(setattr, self.app_state, "global_config", self.REAL_CONFIG)
        self.addCleanup(setattr, self.vllm_metrics, "requests", self.REAL_REQUESTS)

        self.vllm_metrics.reset_endpoints()
        self.vllm_metrics.reset_slot_usage()
        self.vllm_metrics.requests = stub
        self.app_state.global_config = config

        for _slot_idx, url in slots.items():
            metrics_url = self.vllm_metrics.metrics_url_for_api_url(url)
            self.vllm_metrics.poll_endpoint(metrics_url)

        import generate as g
        g._update_metrics_tab(force=True)
        self.settle()
        return stub

    def _rows(self):
        tree = self.widgets["metrics_tree"]
        return [tree.item(item, "values") for item in tree.get_children()]

    # --- the tab itself ---------------------------------------------------

    def test_tab_is_registered_with_its_widgets(self):
        for key in ("metrics_page", "metrics_tree", "metrics_graph_canvas",
                     "metrics_status_label"):
            self.assertIn(key, self.widgets, "missing widget registry key %r" % key)
        self.assertTrue(self.widgets["metrics_tree"].winfo_exists())

    def test_tab_has_no_issue_panels(self):
        """It is a diagnostics area like Regex Watch, not an issue view."""
        self.assertNotIn("refusals", self.widgets)
        self.assertNotIn("panel_layout", self.widgets)

    def test_other_tabs_still_have_their_panels(self):
        """Adding this tab must not have swallowed the issue panels of the others."""
        totals = self.notebook.tabs_widgets["Totals"]
        self.assertIn("refusals", totals)
        self.assertIn("scene_continuity", totals)

    # --- table readability -----------------------------------------------

    def test_table_uses_the_dedicated_style(self):
        regex_widgets = self.notebook.tabs_widgets["🔎 Regex Watch"]
        self.assertEqual(self.widgets["metrics_tree"].cget("style"),
                         regex_widgets["regex_pattern_tree"].cget("style"),
                         "the metrics table must use the shared readable Treeview style")

    def test_row_height_leaves_room_for_the_font(self):
        import generate as g
        style = ttk.Style()
        linespace = font.Font(root=self.root, font=g.REGEX_TREE_BODY_FONT).metrics("linespace")
        rowheight = int(style.lookup(self.widgets["metrics_tree"].cget("style"),
                                    "rowheight") or 0)
        self.assertGreaterEqual(rowheight, linespace + MIN_ROW_SLACK,
                                 "rowheight %d cannot fit a %dpx font" % (rowheight, linespace))

    def test_columns_are_wide_enough_for_their_headings(self):
        import generate as g
        scale = self.root.winfo_fpixels("1i") / 96.0
        heading_font = font.Font(root=self.root, font=g.REGEX_TREE_HEADING_FONT)
        tree = self.widgets["metrics_tree"]
        for col in tree.cget("columns"):
            width = int(tree.column(col, "width"))
            needed = heading_font.measure(col) + round(24 * scale)
            self.assertGreaterEqual(width, needed,
                                     "column %r is %dpx but its heading needs %dpx"
                                     % (col, width, needed))

    def test_the_three_required_figures_have_their_own_columns(self):
        """Prompt speed, generation speed and endpoint load are each a column."""
        columns = list(self.widgets["metrics_tree"].cget("columns"))
        for heading in ("Prompt Speed", "Generation Speed", "Endpoint Load"):
            self.assertIn(heading, columns)

    # --- v13.3.0: the antislop-vllm-fork's optional columns ------------------

    def test_the_antislop_columns_exist_but_are_hidden_by_default(self):
        """A default run's tab must be byte-identical, so the columns ship hidden."""
        tree = self.widgets["metrics_tree"]
        # Re-apply from the REAL shipped config: another test in this class may have switched the
        # key on, and `displaycolumns` is tree state that outlives it.
        import generate as g
        saved = self.app_state.global_config
        self.addCleanup(setattr, self.app_state, "global_config", saved)
        self.app_state.global_config = _ConfigStub(
            {"api": {"apis": [{"url": "", "model": ""} for _ in range(6)]},
             "quality": {"scoring_api": {"url": ""}},
             "metrics": {"antislop_columns": False}})
        g._apply_metrics_columns(tree)
        columns = list(tree.cget("columns"))
        for heading in g.METRICS_ANTISLOP_COLUMNS:
            self.assertIn(heading, columns, "the column must exist even when hidden")
        shown = list(tree.cget("displaycolumns"))
        for heading in g.METRICS_ANTISLOP_COLUMNS:
            self.assertNotIn(heading, shown, "hidden by default")

    def test_the_antislop_columns_are_shown_when_the_config_key_is_on(self):
        self._select_tab()
        import generate as g
        tree = self.widgets["metrics_tree"]
        saved = self.app_state.global_config
        self.addCleanup(setattr, self.app_state, "global_config", saved)
        self.app_state.global_config = _ConfigStub(
            {"api": {"apis": [{"url": "", "model": ""} for _ in range(6)]},
             "quality": {"scoring_api": {"url": ""}},
             "metrics": {"antislop_columns": True}})
        g._apply_metrics_columns(tree)
        shown = list(tree.cget("displaycolumns"))
        for heading in g.METRICS_ANTISLOP_COLUMNS:
            self.assertIn(heading, shown)
        # The vLLM columns are still there: this ADDS, it does not replace.
        for heading in ("Prompt Speed", "Generation Speed", "Endpoint Load"):
            self.assertIn(heading, shown)

    def test_every_row_has_a_value_for_every_column(self):
        """A row shorter than the column list silently shifts every cell after it."""
        self._select_tab()
        self._stub_poll({0: "http://a:8000/v1", 1: "http://dead:9000/v1"},
                         stub_class=self._mixed_stub())
        tree = self.widgets["metrics_tree"]
        expected = len(tree.cget("columns"))
        for item in tree.get_children():
            self.assertEqual(len(tree.item(item, "values")), expected,
                             tree.item(item, "values"))

    @staticmethod
    def _mixed_stub():
        class _MixedStub:
            def get(self, url, timeout=None):
                if "dead" in url:
                    raise IOError("connection refused")
                return _StubResponse(VLLM_BODY)
        return _MixedStub

    # --- one row per slot, never aggregated -------------------------------

    def test_one_row_per_configured_slot_never_one_aggregate(self):
        self._select_tab()

        class _PerHostStub:
            """Serve a different KV-cache load per host, so the rows cannot be merged."""

            def get(self, url, timeout=None):
                load = "0.12" if "a:8000" in url else "0.88"
                body = VLLM_BODY.replace(
                    'vllm:kv_cache_usage_perc{engine="0",model_name="acme/model"} 0.72',
                    'vllm:kv_cache_usage_perc{engine="0",model_name="acme/model"} ' + load)
                return _StubResponse(body)

        self._stub_poll({0: "http://a:8000/v1", 1: "http://a:8000/v1",
                         2: "http://b:9000/v1"}, stub_class=_PerHostStub)
        rows = self._rows()
        self.assertEqual(len(rows), 3, rows)
        self.assertEqual([row[0] for row in rows], ["API 1", "API 2", "API 3"], rows)
        # Two slots on ONE server agree (the KV cache is server-wide), but the third
        # slot's own server is reported separately -- never one summed row.
        self.assertEqual(rows[0][4], rows[1][4], rows)
        self.assertNotEqual(rows[0][4], rows[2][4], rows)
        self.assertEqual(rows[0][4], "12%", rows)
        self.assertEqual(rows[2][4], "88%", rows)

    def test_an_unreachable_endpoint_keeps_its_own_row(self):
        self._select_tab()

        class _MixedStub:
            def get(self, url, timeout=None):
                if "dead" in url:
                    raise IOError("connection refused")
                return _StubResponse(VLLM_BODY)

        self._stub_poll({0: "http://a:8000/v1", 1: "http://dead:9000/v1"},
                         stub_class=_MixedStub)
        rows = self._rows()
        self.assertEqual(len(rows), 2, rows)
        self.assertEqual(rows[0][0], "API 1")
        self.assertEqual(rows[1][0], "API 2")
        self.assertEqual(rows[1][1], "unreachable", rows[1])

    def test_no_backend_shows_a_single_explanatory_row(self):
        self._select_tab()
        self._stub_poll({})
        rows = self._rows()
        self.assertEqual(len(rows), 1, rows)
        self.assertEqual(rows[0][0], "-", rows)

    def test_status_line_names_the_poll_interval(self):
        self._select_tab()
        self._stub_poll({0: "http://a:8000/v1"})
        text = self.widgets["metrics_status_label"].cget("text")
        self.assertIn(str(int(self.vllm_metrics.POLL_INTERVAL_SECONDS)), text)

    # --- v13.4.0: the overhauled tab -------------------------------------

    def test_the_kpi_cards_exist_and_are_populated(self):
        """The four cards answer "is this run healthy?" without reading a 22-column row."""
        self._select_tab()
        self._stub_poll({0: "http://a:8000/v1"})
        cards = self.widgets["metrics_cards"]
        details = self.widgets["metrics_cards_detail"]
        for key in ("throughput", "load", "bans", "errors"):
            self.assertIn(key, cards, "missing KPI card %r" % key)
            self.assertTrue(cards[key].winfo_exists())
            self.assertNotEqual(cards[key].cget("text"), "", "card %r is blank" % key)
            self.assertNotEqual(details[key].cget("text"), "",
                                "card %r has no context line" % key)

    def test_the_kpi_cards_read_the_polled_figures(self):
        self._select_tab()
        self._stub_poll({0: "http://a:8000/v1"}, stub_class=self._fork_stub())
        cards = self.widgets["metrics_cards"]
        # 0.72 KV-cache load on the only endpoint.
        self.assertEqual(cards["load"].cget("text"), "72%")
        # 30 fixed of 40 total bans.
        self.assertEqual(cards["bans"].cget("text"), "40")
        self.assertIn("75%", self.widgets["metrics_cards_detail"]["bans"].cget("text"))

    def test_the_kpi_cards_do_not_double_count_two_slots_on_one_proxy(self):
        """Two slots on one proxy report the SAME counters; summing per row would double them."""
        self._select_tab()
        self._stub_poll({0: "http://a:8000/v1", 1: "http://a:8000/v1"},
                        stub_class=self._fork_stub())
        cards = self.widgets["metrics_cards"]
        self.assertEqual(cards["bans"].cget("text"), "40",
                         "the proxy's ban total was counted once per slot")

    @staticmethod
    def _fork_stub():
        """A fork body: the vLLM half plus the proxy's own counters."""
        body = VLLM_BODY + (
            'antislop:requests_total{endpoint="/v1/chat/completions",status="200"} 40.0\n'
            'antislop:ban_events_total{validator="slop",fixed="true"} 30.0\n'
            'antislop:ban_events_total{validator="slop",fixed="false"} 10.0\n'
            'antislop:backtracks_total 12.0\n'
            'antislop:generation_tokens_total 9000.0\n'
            'antislop:upstream_errors_total 1.0\n')

        class _ForkStub:
            def get(self, url, timeout=None):
                return _StubResponse(body)
        return _ForkStub

    def test_the_antislop_columns_show_themselves_when_a_slot_serves_them(self):
        """v13.4.0: "I pointed a slot at the fork and saw nothing" must not happen again."""
        self._select_tab()
        tree = self.widgets["metrics_tree"]
        saved = self.app_state.global_config
        self.addCleanup(setattr, self.app_state, "global_config", saved)
        # The key is OFF, and the endpoint IS a fork: the columns must still appear.
        self.app_state.global_config = _ConfigStub(
            {"api": {"apis": [{"url": "http://a:8000/v1"}] + [{"url": ""}] * 5},
             "quality": {"scoring_api": {"url": ""}},
             "metrics": {"antislop_columns": False}})
        self._stub_poll({0: "http://a:8000/v1"}, stub_class=self._fork_stub())
        shown = list(tree.cget("displaycolumns"))
        self.assertIn("Proxy Requests", shown,
                      "a proxied slot must show its columns even with the key off")

    def test_a_plain_vllm_slot_keeps_the_columns_hidden(self):
        """...and a plain vLLM must stay exactly as it was."""
        self._select_tab()
        tree = self.widgets["metrics_tree"]
        saved = self.app_state.global_config
        self.addCleanup(setattr, self.app_state, "global_config", saved)
        self.app_state.global_config = _ConfigStub(
            {"api": {"apis": [{"url": "http://a:8000/v1"}] + [{"url": ""}] * 5},
             "quality": {"scoring_api": {"url": ""}},
             "metrics": {"antislop_columns": False}})
        self._stub_poll({0: "http://a:8000/v1"})
        shown = list(tree.cget("displaycolumns"))
        self.assertNotIn("Proxy Requests", shown)

    def test_the_table_scrollbars_are_wide_enough_to_grab(self):
        """The theme's ~6px thumb is unusable; a real grab width is the whole point."""
        tree = self.widgets["metrics_tree"]
        bars = [child for child in tree.master.winfo_children()
                if child is not tree and child.winfo_class() == "Scrollbar"]
        self.assertEqual(len(bars), 2, "the table needs both a vertical and a horizontal bar")
        for bar in bars:
            self.assertGreaterEqual(int(bar.cget("width")), 16,
                                    "a %s scrollbar is too narrow to drag" % bar.cget("orient"))

    def test_the_graph_stack_scrollbar_is_wide_enough_to_grab(self):
        """The graph stack is ~1450px tall, so dragging its thumb is the only practical way."""
        scroll = self.widgets["metrics_graph_scroll"]
        self.assertEqual(scroll.scrollbar.winfo_class(), "Scrollbar",
                         "the graph stack must use an explicit tk.Scrollbar, not the theme's")
        self.assertGreaterEqual(int(scroll.scrollbar.cget("width")), 16,
                                "the graph stack's scrollbar is too narrow to drag")

    def test_the_graph_stack_scrolls_to_every_panel(self):
        """A panel the user cannot reach is a panel that does not exist."""
        scroll = self.widgets["metrics_graph_scroll"]
        self.settle()
        region = scroll.canvas.cget("scrollregion").split()
        self.assertEqual(len(region), 4, region)
        content_height = float(region[3])
        self.assertGreater(content_height, scroll.canvas.winfo_height(),
                           "the graph stack does not overflow, so nothing scrolls")
        for key in ("metrics_graph_canvas", "metrics_slot_graph_canvas",
                    "metrics_proxy_graph_canvas", "metrics_queue_graph_canvas"):
            self.assertTrue(self.widgets[key].winfo_ismapped(),
                            "%r is not laid out inside the scroll region" % key)

    def test_all_four_graph_panels_exist(self):
        for key in ("metrics_graph_canvas", "metrics_slot_graph_canvas",
                    "metrics_proxy_graph_canvas", "metrics_queue_graph_canvas"):
            self.assertIn(key, self.widgets, "missing graph panel %r" % key)
            self.assertTrue(self.widgets[key].winfo_exists(), key)

    def test_the_slot_graph_draws_one_line_per_slot(self):
        """Per-slot throughput is OUR measurement, so two slots on one server still differ."""
        self._select_tab()
        self._stub_poll({0: "http://a:8000/v1", 1: "http://a:8000/v1"})
        self.vllm_metrics.note_slot_usage(0, {"prompt_tokens": 100,
                                              "completion_tokens": 300}, 2.0)
        self.vllm_metrics.note_slot_usage(1, {"prompt_tokens": 100,
                                              "completion_tokens": 100}, 4.0)
        self.vllm_metrics.poll_once()
        import generate as g
        g._update_metrics_tab(force=True)
        self.settle()
        ax = self.widgets["metrics_slot_graph_canvas"].slot_graph_axes["throughput"]
        self.assertEqual(len(ax.get_lines()), 2, "one line per slot, never one combined line")

    def test_the_proxy_graph_draws_the_request_rate(self):
        self._select_tab()
        self._stub_poll({0: "http://a:8000/v1"}, stub_class=self._fork_stub())
        self.vllm_metrics.poll_once()
        import generate as g
        g._update_metrics_tab(force=True)
        self.settle()
        axes = self.widgets["metrics_proxy_graph_canvas"].proxy_graph_axes
        self.assertEqual(len(axes["requests"].get_lines()), 1, "no request-rate line")

    def test_the_queue_graph_draws_running_and_waiting(self):
        self._select_tab()
        self._stub_poll({0: "http://a:8000/v1"})
        self.vllm_metrics.poll_once()
        import generate as g
        g._update_metrics_tab(force=True)
        self.settle()
        ax = self.widgets["metrics_queue_graph_canvas"].queue_graph_axes["queue"]
        self.assertEqual(len(ax.get_lines()), 2, "running and waiting are two lines")

    # --- v13.5.0: collection is independent of the visible tab -------------

    def test_the_poller_collects_while_the_metrics_tab_is_closed(self):
        """The tab is a VIEW of the history, not the thing that collects it.

        Before v13.5.0 the graphs were empty for the first poll after every tab switch, and the
        history had holes wherever the user was looking at another tab.
        """
        self._select_tab()
        self._stub_poll({0: "http://a:8000/v1"})
        before = len(self._rows())
        # Switch away, then poll again with the tab hidden.
        for tab_id in self.notebook.tabs():
            if "Totals" in self.notebook.tab(tab_id, "text"):
                self.notebook.select(tab_id)
                break
        self.settle()
        self.vllm_metrics.poll_once()
        self.settle()
        # The history grew even though the tab was not on screen.
        rows = self.vllm_metrics.endpoint_rows()
        self.assertEqual(len(rows), 1, rows)
        self.assertGreaterEqual(len(rows[0]["history"]), 2,
                                "the poller did not collect while the tab was hidden")
        # Coming back re-renders from that history rather than starting empty.
        self._select_tab()
        self.assertEqual(len(self._rows()), before)

    def test_the_status_line_says_where_the_history_is_saved(self):
        self._select_tab()
        self._stub_poll({0: "http://a:8000/v1"})
        text = self.widgets["metrics_status_label"].cget("text")
        self.assertIn(self.vllm_metrics.METRICS_HISTORY_FILENAME, text,
                      "the status line does not say where the history goes: %r" % text)

    def test_the_status_line_shows_the_real_poll_interval(self):
        """The interval is 2 s now, so a truncated "%d" would read "2" -- but 1.5 s must not
        silently become "1"."""
        self._select_tab()
        self._stub_poll({0: "http://a:8000/v1"})
        text = self.widgets["metrics_status_label"].cget("text")
        self.assertIn("%.1f" % self.vllm_metrics.POLL_INTERVAL_SECONDS, text, text)

    def test_the_graph_redraw_floor_is_shorter_than_the_poll_interval(self):
        """A 5 s floor against a 2 s poll skipped every other sample."""
        import dashboard
        self.assertLess(dashboard._VLLM_GRAPH_REDRAW_MIN_INTERVAL,
                        self.vllm_metrics.POLL_INTERVAL_SECONDS,
                        "the graph would drop samples between polls")

    # --- the graph -------------------------------------------------------

    def test_graph_draws_one_line_per_endpoint(self):
        self._select_tab()
        self._stub_poll({0: "http://a:8000/v1", 1: "http://b:9000/v1"})
        canvas = self.widgets["metrics_graph_canvas"]
        # Two lines per endpoint in the speed panel (generation + prompt) and one in the
        # load panel -- never a single combined line for all endpoints.
        self.assertEqual(len(canvas.graph_speed_ax.get_lines()), 4,
                          "speed panel is not per-endpoint")
        self.assertEqual(len(canvas.graph_load_ax.get_lines()), 2,
                          "load panel is not per-endpoint")

    def test_graph_explains_itself_before_the_first_poll(self):
        self._select_tab()
        self.vllm_metrics.reset_endpoints()
        import generate as g
        g._update_metrics_tab(force=True)
        self.settle()
        canvas = self.widgets["metrics_graph_canvas"]
        texts = [text.get_text() for text in canvas.graph_speed_ax.texts]
        self.assertTrue(any("No vLLM metrics yet" in text for text in texts), texts)


if __name__ == "__main__":
    unittest.main(verbosity=2)
