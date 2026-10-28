"""Real Tk checks for the column picker: python -B -m unittest test_dataset_column_menu -v.

One Tk root is reused for the suite. A missing Tk installation or unavailable
display produces an explicit unittest skip, not a passing GUI check. Validation
on a display should report all tests executed and zero skips. No source data,
application configuration, network, or model APIs are accessed.
"""

import unittest

try:
    import tkinter as tk
    from tkinter import ttk
except ImportError as exc:
    tk = ttk = None
    TK_UNAVAILABLE = str(exc)
else:
    TK_UNAVAILABLE = None
    from dataset_column_menu import ColumnVisibilityMenu


class ColumnVisibilityMenuTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if tk is None:
            raise unittest.SkipTest("Tk is unavailable: " + TK_UNAVAILABLE)
        try:
            cls.root = tk.Tk()
        except tk.TclError as exc:
            raise unittest.SkipTest("A working Tk display is required: {}".format(exc)) from exc
        cls.root.title("Dataset column menu checks")
        cls.root.withdraw()
        cls.global_bindings = {
            sequence: cls.root.bind_all(sequence)
            for sequence in ("<Button-1>", "<FocusOut>", "<Escape>", "<MouseWheel>")
        }

    @classmethod
    def tearDownClass(cls):
        cls.root.destroy()

    def setUp(self):
        self.callback_errors = []
        self.root.report_callback_exception = lambda *args: self.callback_errors.append(args)
        width = max(160, min(640, self.root.winfo_screenwidth() - 40))
        height = max(120, min(440, self.root.winfo_screenheight() - 80))
        self.root.geometry("{}x{}+20+20".format(width, height))
        self.root.deiconify()
        self.changes = []
        self.menu = ColumnVisibilityMenu(self.root, self.changes.append)
        self.menu.place(relx=1, rely=1, anchor="se")
        self.outside = ttk.Entry(self.root)
        self.outside.place(x=10, y=10)
        self.extra_widgets = []
        self.pump()
        self.root.focus_force()
        self.root.grab_set()  # Match the Configuration Editor's existing modal grab.
        self.pump()

    def tearDown(self):
        for widget in self.extra_widgets + [self.menu, self.outside]:
            if widget is not None and widget.winfo_exists():
                widget.destroy()
        self.root.grab_release()
        self.pump()
        self.root.withdraw()
        self.pump()
        self.assertEqual(self.callback_errors, [], "A Tk event callback raised an exception.")
        current_bindings = {sequence: self.root.bind_all(sequence) for sequence in self.global_bindings}
        self.assertEqual(current_bindings, self.global_bindings, "The picker changed application-wide bindings.")

    def pump(self):
        self.root.update_idletasks()
        self.root.update()

    def open_menu(self):
        self.menu.open()
        self.pump()
        self.assertIsNotNone(self.menu.popup, "The popup failed to open or immediately dismissed itself.")
        self.assertIs(self.root.grab_current(), self.menu.popup)
        # Bare Xvfb has no window manager to activate reopened windows. Establish
        # real focus before generating keys or testing a subsequent FocusOut.
        self.menu.search_entry.focus_force()
        self.pump()
        self.assertIs(self.root.focus_get(), self.menu.search_entry)
        return self.menu.popup

    def assert_closed_with_original_grab(self):
        self.assertIsNone(self.menu.popup)
        self.assertIs(self.root.grab_current(), self.root)

    @staticmethod
    def descendants(widget):
        children = widget.winfo_children()
        return children + [descendant for child in children
                           for descendant in ColumnVisibilityMenuTests.descendants(child)]

    def test_thousand_long_names_keep_fixed_viewport_and_native_scrolling(self):
        self.menu.set_columns(["short"])
        popup = self.open_menu()
        viewport = (popup.winfo_width(), popup.winfo_height())
        parent_size = (self.root.winfo_width(), self.root.winfo_height())
        self.menu.close()
        names = ["column_{:04d}_".format(index) + "long_name_" * 50 for index in range(1000)]
        self.menu.set_columns(names)
        popup = self.open_menu()
        self.assertEqual(len(self.menu.tree.get_children()), 1000)
        self.assertEqual((popup.winfo_width(), popup.winfo_height()), viewport)
        self.assertEqual((self.root.winfo_width(), self.root.winfo_height()), parent_size)
        widgets = self.descendants(popup)
        self.assertLess(len(widgets), 25, "The picker created widgets for individual columns.")
        self.assertEqual(sum(isinstance(widget, ttk.Treeview) for widget in widgets), 1)
        self.assertLess(self.menu.tree.xview()[1], 1, "Long names need horizontal scrolling.")
        self.assertLess(self.menu.tree.yview()[1], 1, "Long schemas need vertical scrolling.")
        self.assertEqual(self.changes, [], "Loading names must not report a user selection change.")

    def test_search_does_not_change_selection_and_all_applies_to_hidden_names(self):
        names = ["alpha", "beta", "alphabet", "hidden"]
        self.menu.set_columns(names)
        self.open_menu()
        self.menu.search_var.set("ALPHA")
        self.pump()
        displayed = [self.menu.tree.set(iid, "name") for iid in self.menu.tree.get_children()]
        self.assertEqual(displayed, ["alpha", "alphabet"])
        self.assertIsNone(self.menu.get_selection())
        self.assertEqual(self.changes, [])
        self.menu.clear_selection()
        self.menu.toggle_column("alphabet")
        self.menu.toggle_column("hidden")
        self.assertEqual(self.menu.get_selection(), ("alphabet", "hidden"))
        changes_before_search = list(self.changes)
        self.menu.search_var.set("no match")
        self.pump()
        self.assertEqual(self.menu.tree.get_children(), ())
        self.assertEqual(self.menu.get_selection(), ("alphabet", "hidden"))
        self.assertEqual(self.changes, changes_before_search)
        self.menu.select_all()
        self.assertIsNone(self.menu.get_selection())
        self.assertEqual(self.menu.tree.get_children(), ())
        self.menu.search_var.set("")
        self.pump()
        self.assertEqual(len(self.menu.tree.get_children()), 4)

    def test_programmatic_selection_is_silent_ordered_and_canonical(self):
        self.menu.set_columns(["a", "b", "c"], ["c", "a"])
        self.assertEqual(self.menu.get_selection(), ("a", "c"))
        self.assertIn("2 of 3", self.menu.cget("text"))
        self.menu.set_columns(["a", "b", "c"], ["c", "a", "b"])
        self.assertIsNone(self.menu.get_selection())
        self.assertIn("All", self.menu.cget("text"))
        self.menu.set_columns([], [])
        self.assertEqual(self.menu.get_selection(), ())
        self.menu.set_columns(["new"], self.menu.get_selection())
        self.assertEqual(self.menu.get_selection(), ())
        self.assertEqual(self.changes, [])
        self.menu.select_all()
        self.menu.set_columns(["new", "later"], self.menu.get_selection())
        self.assertIsNone(self.menu.get_selection())
        self.assertEqual(self.changes, [None])

    def test_actual_mouse_space_and_all_checkbox_controls(self):
        self.menu.set_columns(["alpha", "beta", "gamma"], [])
        self.open_menu()
        iid = self.menu.tree.get_children()[0]
        self.assertEqual(self.menu.tree.set(iid, "selected"), "[ ]")
        x, y, _, height = self.menu.tree.bbox(iid, "name")
        self.menu.tree.event_generate("<Button-1>", x=x + 4, y=y + height // 2)
        self.pump()
        self.assertEqual(self.menu.get_selection(), ("alpha",))
        self.assertEqual(self.menu.tree.set(iid, "selected"), "[x]")
        self.menu.tree.focus_set()
        self.pump()
        self.menu.tree.event_generate("<space>")
        self.pump()
        self.assertEqual(self.menu.get_selection(), ())
        self.assertEqual(self.menu.tree.set(iid, "selected"), "[ ]")
        self.menu.all_button.invoke()
        self.assertIsNone(self.menu.get_selection())
        self.assertEqual([self.menu.tree.set(row, "selected") for row in self.menu.tree.get_children()],
                         ["[x]", "[x]", "[x]"])
        self.menu.toggle_column("beta")
        self.assertTrue(self.menu.all_button.instate(["alternate"]))
        self.assertEqual([self.menu.tree.set(row, "selected") for row in self.menu.tree.get_children()],
                         ["[x]", "[ ]", "[x]"])
        self.menu.all_button.invoke()
        self.assertIsNone(self.menu.get_selection())
        self.assertFalse(self.menu.all_button.instate(["alternate"]))
        self.menu.all_button.invoke()
        self.assertEqual(self.menu.get_selection(), ())
        self.assertEqual(self.changes, [("alpha",), (), None, ("alpha", "gamma"), None, ()])

    def test_escape_outside_click_and_focusout_restore_previous_grab(self):
        self.menu.set_columns(["alpha", "beta"])
        self.open_menu()
        self.menu.search_entry.event_generate("<Escape>")
        self.pump()
        self.assert_closed_with_original_grab()

        popup = self.open_menu()
        popup.event_generate("<ButtonPress-1>", x=-10, y=-10,
                             rootx=popup.winfo_rootx() - 10, rooty=popup.winfo_rooty() - 10)
        self.pump()
        self.assert_closed_with_original_grab()

        self.open_menu()
        self.outside.focus_set()
        self.pump()
        self.assert_closed_with_original_grab()

    def test_screen_edge_bounds_and_disabled_state(self):
        screen_width, screen_height = self.root.winfo_screenwidth(), self.root.winfo_screenheight()
        self.root.geometry("300x200+{}+{}".format(max(0, screen_width - 300), max(0, screen_height - 200)))
        self.pump()
        self.menu.set_columns(["a long column name " * 100])
        popup = self.open_menu()
        self.assertGreaterEqual(popup.winfo_rootx(), 0)
        self.assertGreaterEqual(popup.winfo_rooty(), 0)
        self.assertLessEqual(popup.winfo_rootx() + popup.winfo_width(), screen_width)
        self.assertLessEqual(popup.winfo_rooty() + popup.winfo_height(), screen_height)
        self.menu.configure(state="disabled")
        self.pump()
        self.assert_closed_with_original_grab()
        self.menu.open()
        self.assertIsNone(self.menu.popup)
        self.menu.state(["!disabled"])
        self.open_menu()
        self.menu.state(["disabled"])
        self.pump()
        self.assert_closed_with_original_grab()

    def test_external_popup_and_ancestor_destruction_release_resources(self):
        self.menu.set_columns(["alpha"])
        popup = self.open_menu()
        popup.destroy()
        self.pump()
        self.assert_closed_with_original_grab()

        host = ttk.Frame(self.root)
        host.place(x=10, y=60, width=300, height=80)
        self.extra_widgets.append(host)
        child = ColumnVisibilityMenu(host, self.changes.append)
        child.pack(padx=5, pady=5)
        child.set_columns(["nested"])
        self.pump()
        child.open()
        self.pump()
        self.assertIsNotNone(child.popup)
        host.destroy()
        self.pump()
        self.assertIsNone(child.popup)
        self.assertIs(self.root.grab_current(), self.root)


if __name__ == "__main__":
    unittest.main()
