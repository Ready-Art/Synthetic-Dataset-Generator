"""Real Tk checks for the Datasets panel and Configuration Editor integration.

Run separately from other GUI suites: ``python -B -m unittest test_datasets_panel``.
A desktop/virtual display and normal application requirements are needed; missing
Tk/display yields an explicit skip. Sources are temporary files or controlled
metadata fixtures. No network or model API is used. Optional screenshots go to
READYART_GUI_SCREENSHOTS; they contain synthetic column names, never record data.
"""

import csv
import json
import os
from pathlib import Path
import shutil
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

try:
    import tkinter as tk
    import ttkbootstrap
except ImportError:
    tk = None

if tk is not None:
    from datasets_panel import DatasetsPanel
    from dataset_settings import apply_schema, new_source
    from dataset_sources import DatasetSchema, open_source


def descendants(widget):
    result = []
    for child in widget.winfo_children():
        result.append(child)
        result.extend(descendants(child))
    return result


@unittest.skipIf(tk is None, "Tk and the normal application GUI dependencies are required.")
class DatasetsPanelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.root = tk.Tk()
        except tk.TclError as exc:
            raise unittest.SkipTest("A desktop/virtual display is required: {}".format(exc))
        cls.root.tk.call("tk", "scaling", 4 / 3)
        cls.style = ttkbootstrap.Style(theme="superhero")
        cls.root.title("Dataset configuration checks")

    @classmethod
    def tearDownClass(cls):
        cls.root.destroy()

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="readyart-dataset-gui-")
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.errors = []
        self.root.report_callback_exception = lambda kind, value, trace: self.errors.append((kind.__name__, str(value)))
        self.root.geometry("900x650+20+20")
        self.panel = DatasetsPanel(self.root)
        self.panel.pack(fill="both", expand=True)
        self.pump()

    def tearDown(self):
        if self.panel.winfo_exists():
            self.panel.destroy()
        self.pump()
        self.assertEqual(self.errors, [], "A Tk callback failed.")

    def pump(self):
        self.root.update_idletasks()
        self.root.update()

    def wait_for(self, predicate, timeout=5):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.pump()
            if predicate():
                return
            time.sleep(0.005)
        self.fail("The expected background/UI state did not arrive before the timeout.")

    def fixture(self, count=1000, name="Example records", location=r"C:\Datasets\records.csv"):
        names = ["record_id", "category", "description"][:count]
        names.extend("field_{:04d}".format(index) for index in range(len(names), count))
        if count > 5:
            names[-1] = "extended_column_" + "long_name_" * 70
        source = new_source([])
        source.update(name=name, kind="csv", location=location)
        source = apply_schema(source, DatasetSchema(tuple(names), "gui-schema-" + source["source_id"]))
        return source

    def choose_column(self, name, panel=None):
        panel = panel or self.panel
        item = next(item for item in panel.columns_tree.get_children()
                    if panel.columns_tree.item(item, "values")[0] == name)
        panel.columns_tree.selection_set(item)
        panel.columns_tree.focus(item)
        panel.columns_tree.see(item)
        self.pump()

    def assert_inside(self, widget, container):
        self.assertTrue(widget.winfo_ismapped())
        x = widget.winfo_rootx() - container.winfo_rootx()
        y = widget.winfo_rooty() - container.winfo_rooty()
        self.assertGreaterEqual(x, -1)
        self.assertGreaterEqual(y, -1)
        self.assertLessEqual(x + widget.winfo_width(), container.winfo_width() + 1)
        self.assertLessEqual(y + widget.winfo_height(), container.winfo_height() + 1)

    def test_thousand_columns_keep_bounded_geometry_and_constant_widget_count(self):
        source = self.fixture()
        baseline_widgets = len(descendants(self.panel))
        self.panel.load_config({"sources": [source]})
        self.assertEqual(len(self.panel.columns_tree.get_children()), 1000)
        self.assertIsNone(self.panel.column_menu.get_selection())
        self.assertEqual(len(descendants(self.panel)), baseline_widgets)
        for width, height in ((760, 520), (1366, 700), (760, 520)):
            with self.subTest(size=(width, height)):
                self.root.geometry("{}x{}+20+20".format(width, height))
                self.pump()
                self.assertEqual((self.root.winfo_width(), self.root.winfo_height()), (width, height))
                for widget in (self.panel.source_combo, self.panel.import_button,
                               self.panel.columns_tree, self.panel.alias_entry, self.panel.copy_button):
                    self.assert_inside(widget, self.panel)
                self.assertGreater(self.panel.columns_tree.winfo_height(), 80)
        self.choose_column(source["columns"][-1])
        self.assertEqual(self.panel.selected_column_var.get(), source["columns"][-1])
        self.assertLess(self.panel.winfo_reqwidth(), 1000)

    def test_visibility_alias_copy_and_many_sources_round_trip(self):
        first, second = self.fixture(), self.fixture(3, "Reference fields", r"C:\Datasets\reference.csv")
        self.panel.load_config({"sources": [first, second]})
        self.choose_column("record_id")
        self.panel.alias_var.set("record_key")
        self.panel.apply_alias()
        self.panel.copy_placeholder()
        self.assertEqual(self.root.clipboard_get(), "{{ds:record_key}}")
        self.panel.column_menu.clear_selection()
        self.panel.column_menu.toggle_column("category")
        self.assertEqual(len(self.panel.columns_tree.get_children()), 1)
        hidden = self.panel.get_config()["sources"][0]
        self.assertEqual(hidden["column_aliases"], {"record_id": "record_key"})
        self.assertEqual(hidden["visible_columns"], ["category"])
        self.panel.select_source(second["source_id"])
        self.assertIsNone(self.panel.column_menu.get_selection())
        self.panel.select_source(first["source_id"])
        self.assertEqual(self.panel.column_menu.get_selection(), ("category",))
        saved = json.loads(json.dumps(self.panel.get_config()))
        with patch("datasets_panel.open_source", side_effect=AssertionError("Configuration must not read source data")):
            self.panel.load_config(saved)
            self.panel.validate()
            self.assertEqual(self.panel.get_config(), saved)
        self.panel.column_menu.select_all()
        self.choose_column("record_id")
        self.assertEqual(self.panel.alias_var.get(), "record_key")
        for _ in range(30):
            self.panel.add_source()
        self.assertEqual(len(self.panel.get_config()["sources"]), 32)
        self.assertEqual(len(self.panel.source_combo.cget("values")), 32)
        self.assertIsNone(self.panel.get_config()["sources"][-1]["visible_columns"])

    def test_background_csv_import_returns_only_names_and_keeps_tk_responsive(self):
        path = self.directory / "wide.csv"
        names = ["field_{:04d}".format(index) for index in range(1000)]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(names)
            writer.writerow(["RECORD_VALUE_MUST_NOT_REACH_GUI"] * len(names))
        self.panel.add_source()
        self.panel.location_var.set(str(path))
        owner = threading.get_ident()
        calls = []
        closed = threading.Event()
        released = threading.Event()
        entered = threading.Event()

        class Reader:
            def __init__(self, spec):
                self.reader = open_source(spec)

            def schema(self):
                calls.append(threading.get_ident())
                entered.set()
                if not released.wait(3):
                    raise RuntimeError("test metadata reader was not released")
                return self.reader.schema()

            def iter_rows(self):
                raise AssertionError("The GUI must not request source rows.")

            def close(self):
                self.reader.close()
                closed.set()

        try:
            with patch("datasets_panel.open_source", Reader):
                self.panel.import_columns()
                self.wait_for(entered.is_set)
                tick = []
                self.root.after(0, lambda: tick.append(True))
                self.wait_for(lambda: bool(tick))
                self.assertEqual(self.panel.get_config()["sources"][0]["columns"], [])
                released.set()
                self.wait_for(lambda: len(self.panel.columns_tree.get_children()) == 1000)
        finally:
            released.set()
        self.assertTrue(closed.wait(1))
        self.assertTrue(calls and all(thread != owner for thread in calls))
        saved = json.dumps(self.panel.get_config())
        self.assertNotIn("RECORD_VALUE_MUST_NOT_REACH_GUI", saved)
        self.assertEqual(self.panel.get_config()["sources"][0]["columns"], names)

    def test_out_of_order_import_result_cannot_replace_changed_source(self):
        self.panel.add_source()
        self.panel.location_var.set("old.csv")
        entered, release, old_closed = threading.Event(), threading.Event(), threading.Event()

        class Reader:
            def __init__(self, spec):
                self.location = spec.location

            def schema(self):
                if self.location == "old.csv":
                    entered.set()
                    if not release.wait(3):
                        raise RuntimeError("old metadata reader was not released")
                return DatasetSchema((self.location,), "schema-" + self.location)

            def close(self):
                if self.location == "old.csv":
                    old_closed.set()

        try:
            with patch("datasets_panel.open_source", Reader):
                self.panel.import_columns()
                self.wait_for(entered.is_set)
                self.panel.location_var.set("new.csv")
                self.panel.import_columns()
                self.wait_for(lambda: self.panel.get_config()["sources"][0]["columns"] == ["new.csv"])
                release.set()
                self.wait_for(old_closed.is_set)
                self.pump()
        finally:
            release.set()
        self.assertEqual(self.panel.get_config()["sources"][0]["columns"], ["new.csv"])

    def test_pending_import_is_safe_after_switch_remove_reload_or_destroy(self):
        for action in ("switch", "remove", "reload", "destroy"):
            with self.subTest(action=action):
                if not self.panel.winfo_exists():
                    self.panel = DatasetsPanel(self.root)
                    self.panel.pack(fill="both", expand=True)
                first, second = self.fixture(2), self.fixture(3, "Other source")
                first["columns"] = []
                self.panel.load_config({"sources": [first, second]})
                entered, release, closed = threading.Event(), threading.Event(), threading.Event()

                class Reader:
                    def schema(self):
                        entered.set()
                        if not release.wait(3):
                            raise RuntimeError("reader was not released")
                        return DatasetSchema(("stale_name",), "stale-schema")

                    def close(self):
                        closed.set()

                try:
                    with patch("datasets_panel.open_source", return_value=Reader()):
                        self.panel.import_columns()
                        self.wait_for(entered.is_set)
                        if action == "switch":
                            self.panel.select_source(second["source_id"])
                        elif action == "remove":
                            self.panel.remove_source()
                        elif action == "reload":
                            self.panel.load_config({"sources": [second]})
                        else:
                            self.panel.destroy()
                        release.set()
                        self.wait_for(closed.is_set)
                        self.pump()
                finally:
                    release.set()
                if self.panel.winfo_exists():
                    self.assertNotIn("stale_name", json.dumps(self.panel.get_config()))
                self.assertEqual(self.errors, [])

    def test_schema_refresh_does_not_silently_remove_mapped_columns(self):
        source = self.fixture(2)
        source["column_aliases"] = {"record_id": "record_key"}
        self.panel.load_config({"sources": [source]})

        class Reader:
            def schema(self):
                return DatasetSchema(("new_column",), "replacement")

            def close(self):
                pass

        with patch("datasets_panel.open_source", return_value=Reader()):
            self.panel.import_columns()
            self.wait_for(lambda: "Import failed:" in self.panel.status_var.get())
            self.assertEqual(self.panel.get_config()["sources"][0]["columns"], source["columns"])
            self.assertEqual(self.panel.get_config()["sources"][0]["column_aliases"], source["column_aliases"])
            self.choose_column("record_id")
            self.panel.clear_alias()
            self.panel.import_columns()
            self.wait_for(lambda: self.panel.get_config()["sources"][0]["columns"] == ["new_column"])

    def test_editor_layout_validation_and_save_round_trip_with_wide_schema(self):
        original_cwd = Path.cwd()
        repository = Path(__file__).resolve().parent
        scratch = self.directory / "editor"
        scratch.mkdir()
        shutil.copytree(repository / "config", scratch / "config")
        import yaml
        data = yaml.safe_load((scratch / "config/config.yml").read_text(encoding="utf-8"))
        data["valkey"]["enabled"] = False
        data["database"]["enabled"] = False
        source = self.fixture()
        source["column_aliases"] = {"record_id": "record_key", "category": "record_category"}
        data["datasets"] = {"sources": [source]}
        (scratch / "config/config.yml").write_text(yaml.safe_dump(data), encoding="utf-8")
        editor = None
        try:
            os.chdir(scratch)
            from config_editor import ConfigEditor
            from config_loader import ConfigLoader
            loader = ConfigLoader(str(scratch / "config/config.yml"))
            with patch("tkinter.messagebox.showerror", side_effect=lambda title, message, **kwargs: self.errors.append((title, message))), \
                 patch("datasets_panel.open_source", side_effect=AssertionError("Editor load/save must not read source data")):
                editor = ConfigEditor(self.root, loader, tk.BooleanVar(self.root), tk.BooleanVar(self.root))
                editor.notebook.select(editor.datasets_panel)
                editor.minsize(1, 1)  # Diagnostic exact client sizes, independently of pixel rounding.
                panel = editor.datasets_panel
                self.choose_column("record_id", panel)
                for width, height in ((760, 520), (1366, 700), (760, 520)):
                    editor.geometry("{}x{}+20+20".format(width, height))
                    self.pump()
                    for widget in (panel.source_combo, panel.import_button, panel.columns_tree,
                                   panel.alias_entry, panel.copy_button, editor.save_button):
                        self.assert_inside(widget, editor)
                    self.assertGreater(panel.columns_tree.winfo_height(), 25)

                screenshot_dir = os.environ.get("READYART_GUI_SCREENSHOTS")
                if screenshot_dir:
                    from PIL import ImageGrab
                    destination = Path(screenshot_dir)
                    destination.mkdir(parents=True, exist_ok=True)
                    editor.geometry("1100x740+20+20")
                    editor.lift()
                    self.pump()
                    bounds = (editor.winfo_rootx(), editor.winfo_rooty(),
                              editor.winfo_rootx() + editor.winfo_width(),
                              editor.winfo_rooty() + editor.winfo_height())
                    ImageGrab.grab(bbox=bounds).save(destination / "datasets-tab.png")
                    panel.column_menu.open()
                    self.pump()
                    ImageGrab.grab(bbox=bounds).save(destination / "datasets-columns-menu.png")
                    panel.column_menu.close()
                    editor.geometry("760x520+20+20")
                    self.pump()
                    bounds = (editor.winfo_rootx(), editor.winfo_rooty(),
                              editor.winfo_rootx() + editor.winfo_width(),
                              editor.winfo_rooty() + editor.winfo_height())
                    ImageGrab.grab(bbox=bounds).save(destination / "datasets-small-window.png")

                panel.column_menu.clear_selection()
                panel.column_menu.toggle_column("category")
                expected = panel.get_config()
                collected = editor._get_current_editor_config_data()
                self.assertEqual(collected["datasets"], expected)
                self.assertTrue(editor.validate_config_handler(show_success_message=False))
                editor.new_profile_name_var.set("dataset-ui")
                with patch("tkinter.messagebox.showinfo"):
                    editor._save_profile_as_handler()
                profile_path = Path(loader.profiles_dir) / "dataset-ui.yml"
                self.assertTrue(profile_path.exists())
                self.assertEqual(yaml.safe_load(profile_path.read_text(encoding="utf-8"))["datasets"], expected)
                success, message = loader.load_profile_to_main_config("dataset-ui")
                self.assertTrue(success, message)
                editor.load_config_handler()
                self.assertEqual(panel.get_config(), expected)
                self.assertEqual(panel.column_menu.get_selection(), ("category",))
                self.assertEqual(expected["sources"][0]["column_aliases"]["record_id"], "record_key")

                # A stale active mapping must not be written or discarded by X.
                original_config = Path(loader.path).read_bytes()
                panel.location_var.set(r"C:\Datasets\changed.csv")
                self.assertFalse(editor.save_config_handler(silent=True))
                self.assertIn("import columns again", editor.status.cget("text"))
                editor.on_close_editor()
                self.assertTrue(editor.winfo_exists())
                self.assertEqual(Path(loader.path).read_bytes(), original_config)
                self.assertEqual(panel.get_config()["sources"][0]["location"], r"C:\Datasets\changed.csv")
                editor.new_profile_name_var.set("stale-dataset")
                with patch("tkinter.messagebox.showerror") as error_dialog, \
                     patch.object(loader, "save_profile", wraps=loader.save_profile) as save_profile:
                    editor._save_profile_as_handler()
                    save_profile.assert_not_called()
                    error_dialog.assert_called_once()
                self.assertFalse((Path(loader.profiles_dir) / "stale-dataset.yml").exists())

                # Disabling the unfinished source lets its draft be saved intact.
                panel.enabled_var.set(False)
                draft = panel.get_config()
                editor.new_profile_name_var.set("disabled-draft")
                with patch("tkinter.messagebox.showinfo"):
                    editor._save_profile_as_handler()
                draft_path = Path(loader.profiles_dir) / "disabled-draft.yml"
                self.assertEqual(yaml.safe_load(draft_path.read_text(encoding="utf-8"))["datasets"], draft)
                self.assertTrue(editor.save_config_handler(silent=True))
                editor.on_close_editor()
                self.assertFalse(editor.winfo_exists())
                self.assertEqual(yaml.safe_load(Path(loader.path).read_text(encoding="utf-8"))["datasets"], draft)
        finally:
            if editor is not None and editor.winfo_exists():
                editor.destroy()
            os.chdir(original_cwd)


if __name__ == "__main__":
    unittest.main()
