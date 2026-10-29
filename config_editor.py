"""Configuration Editor window for the Synthetic Dataset Generator.

Extracted from generate.py (refactor step 1). The ConfigEditor Toplevel is self-contained (no module
globals): its config instance, the two shared main-window tk.BooleanVars it syncs with
(master_duplication_enabled_var / no_user_impersonation_var), and the post-save dashboard refresh
(on_config_saved) are injected by the caller (see generate.py: open_config_editor()).
"""
import os
import json
import threading
import requests
import yaml
from urllib.parse import urlparse
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox

import api_profiles
from config_loader import sanitize_input
from generation import (DEFAULT_FIRST_LINE_PROMPT, DEFAULT_FIRST_LINE_STYLES, DEFAULT_GREETING_CONTEXT,
                        DEFAULT_GREETING_PROMPT, DEFAULT_NOTE_PLACEMENT, NOTE_PLACEMENTS, note_placement,
                        placement_cache_warning)
from logging_config import log_message
from api_handler import global_rate_limiter
from datasets_panel import DatasetsPanel
from dataset_settings import DatasetSettingsError
from ui_layout import (NotebookNavigator, ResponsiveGrid, ScrollableFrame,
                       bind_scroll_wheel, fit_window, scroll_to_widget, wrap_label)

SPACING = 8  # UI padding constant (mirrors generate.py)
QUESTIONS_FILE_PATH = os.path.join('input', 'questions.txt')  # mirrors generate.py (UI label only)


def validate_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme in ['http', 'https'], result.netloc])
    except:
        return False


def wrapped_checkbutton(parent, text, **kwargs):
    """Keep long option descriptions readable without shrinking the checkbox."""
    frame = ttk.Frame(parent)
    frame.columnconfigure(1, weight=1)
    button = ttk.Checkbutton(frame, **kwargs)
    button.grid(row=0, column=0, sticky="nw", padx=(0, 4))
    label = ttk.Label(frame, text=text, justify="left", wraplength=1)
    label.grid(row=0, column=1, sticky="ew")
    wrap_label(label, frame, padding=40)
    def toggle(event):
        button.focus_set()
        button.invoke()
    label.bind("<Button-1>", toggle)
    # The real checkbutton keeps keyboard traversal and the original command/variable.
    frame.checkbutton = button
    return frame


class ConfigEditor(tk.Toplevel):
    """A Toplevel window for editing all application configurations (config.yml and .env)."""
    def __init__(self, parent, global_config, master_duplication_enabled_var, no_user_impersonation_var, on_config_saved=None):
        super().__init__(parent)
        self.scrollbar_width = 20
        self.global_config = global_config
        self.master_duplication_enabled_var = master_duplication_enabled_var
        self.no_user_impersonation_var = no_user_impersonation_var
        self.on_config_saved = on_config_saved
        self.title("Configuration Editor")
        fit_window(self, preferred=(1280, 900), minimum=(760, 520))
        self.columnconfigure(0, weight=1)
        self.rowconfigure(2, weight=1)
        self.user_speaking_phrases_data = {"male": [], "female": [], "neutral": []} 
        self.user_speaking_fixes_data = {"male": [], "female": [], "neutral": []}
        self.active_display_gender = "female"
        self.max_character_cards_var = tk.StringVar(value=str(self.global_config.get('generation.max_character_cards', 10)))
        
        # Initialize num_threads_var_editor
        self.num_threads_var_editor = tk.StringVar(value=str(self.global_config.get('api.threads', 10)))

        self.notebook = ttk.Notebook(self)

        # --- NEW: Tab & Section Search Bar ---
        self.search_frame = ttk.Frame(self)
        self.search_frame.grid(row=0, column=0, sticky="ew", padx=SPACING, pady=(SPACING, 0))
        ttk.Label(self.search_frame, text="🔍 Search Tabs/Sections:").pack(side=tk.LEFT, padx=(0, 5))
        self.search_entry = ttk.Entry(self.search_frame)
        self.search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.search_entry.bind("<KeyRelease>", self.search_editor_tabs)
        self.search_entry.bind("<Return>", self.search_editor_tabs)
        # ------------------------------------

        self.notebook.grid(row=2, column=0, sticky="nsew", padx=SPACING, pady=SPACING)

        # --- API Tab ---
        self.api_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.api_tab, text="API")

        self.api_canvas = tk.Canvas(self.api_tab)
        self.api_scrollbar = tk.Scrollbar(self.api_tab, orient="vertical", command=self.api_canvas.yview, width=self.scrollbar_width, bg='#5a5a70', troughcolor='#2a2a35', activebackground='#8a8aa0', highlightthickness=0, bd=0)
        self.api_content_frame = ttk.Frame(self.api_canvas)
        self.api_content_frame.columnconfigure(1, weight=1)

        self.api_content_frame.bind(
            "<Configure>",
            lambda e: self.api_canvas.configure(scrollregion=self.api_canvas.bbox("all"))
        )

        self.api_canvas_window_id = self.api_canvas.create_window((0, 0), window=self.api_content_frame, anchor="nw")
        self.api_canvas.bind("<Configure>", lambda e: self.api_canvas.itemconfig(self.api_canvas_window_id, width=e.width) if e.width > 1 else None)

        self.api_canvas.pack(side="left", fill="both", expand=True)
        self.api_canvas.configure(yscrollcommand=self.api_scrollbar.set)
        self.api_scrollbar.pack(side="right", fill="y")

        bind_scroll_wheel(self.api_canvas)

        # Pricing Input Field (now inside api_content_frame)
        ttk.Label(self.api_content_frame, text="Cost per 1k Tokens ($):").grid(row=0, column=0, padx=SPACING, pady=SPACING, sticky="w")
        self.pricing_var = tk.StringVar()
        ttk.Entry(self.api_content_frame, width=10, textvariable=self.pricing_var).grid(row=0, column=1, padx=SPACING, pady=SPACING, sticky="w")
        ttk.Label(self.api_content_frame, text="(Enter 0 if unknown)").grid(row=1, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="w")

        ttk.Label(self.api_content_frame, text="API Budget Limit ($):").grid(row=2, column=0, padx=SPACING, pady=SPACING, sticky="w")
        self.budget_limit_var = tk.StringVar()
        ttk.Entry(self.api_content_frame, width=10, textvariable=self.budget_limit_var).grid(row=2, column=1, padx=SPACING, pady=SPACING, sticky="w")
        ttk.Label(self.api_content_frame, text="(Set to 0 to disable)").grid(row=3, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="w")

        # Valkey Configuration Section (now inside api_content_frame)
        valkey_frame = ttk.LabelFrame(self.api_content_frame, text="Valkey Cache Settings")
        valkey_frame.columnconfigure(1, weight=1)
        valkey_frame.grid(row=4, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="ew")

        db_frame = ttk.LabelFrame(self.api_content_frame, text="PostgreSQL Database Settings")
        db_frame.grid(row=5, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="ew")

        self.db_enabled_var = tk.BooleanVar()
        wrapped_checkbutton(db_frame, text="Enable PostgreSQL Database", variable=self.db_enabled_var).pack(fill=tk.X, padx=SPACING, pady=SPACING)

        ttk.Label(db_frame, text="Host:").pack(anchor="w", padx=(15,0), pady=SPACING)
        self.db_host_var = tk.StringVar()
        ttk.Entry(db_frame, width=40, textvariable=self.db_host_var).pack(fill=tk.X, padx=SPACING, pady=SPACING)

        ttk.Label(db_frame, text="Port:").pack(anchor="w", padx=(15,0), pady=SPACING)
        self.db_port_var = tk.StringVar()
        ttk.Entry(db_frame, width=15, textvariable=self.db_port_var).pack(fill=tk.X, padx=SPACING, pady=SPACING)

        ttk.Label(db_frame, text="Database Name:").pack(anchor="w", padx=(15,0), pady=SPACING)
        self.db_dbname_var = tk.StringVar()
        ttk.Entry(db_frame, width=40, textvariable=self.db_dbname_var).pack(fill=tk.X, padx=SPACING, pady=SPACING)

        ttk.Label(db_frame, text="User:").pack(anchor="w", padx=(15,0), pady=SPACING)
        self.db_user_var = tk.StringVar()
        ttk.Entry(db_frame, width=40, textvariable=self.db_user_var).pack(fill=tk.X, padx=SPACING, pady=SPACING)

        ttk.Label(db_frame, text="Password:").pack(anchor="w", padx=(15,0), pady=SPACING)
        self.db_password_var = tk.StringVar()
        ttk.Entry(db_frame, width=40, textvariable=self.db_password_var, show="*").pack(fill=tk.X, padx=SPACING, pady=SPACING)

        ttk.Label(db_frame, text="Connection Pool Size:").pack(anchor="w", padx=(15,0), pady=SPACING)
        self.db_pool_size_var = tk.StringVar()
        ttk.Entry(db_frame, width=15, textvariable=self.db_pool_size_var).pack(fill=tk.X, padx=SPACING, pady=SPACING)

        self.valkey_enabled_var = tk.BooleanVar()
        wrapped_checkbutton(valkey_frame, text="Enable Valkey Caching", variable=self.valkey_enabled_var).grid(row=0, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="ew")

        ttk.Label(valkey_frame, text="Host:").grid(row=1, column=0, padx=SPACING, pady=SPACING, sticky="e")
        self.valkey_host_var = tk.StringVar()
        ttk.Entry(valkey_frame, width=30, textvariable=self.valkey_host_var).grid(row=1, column=1, padx=SPACING, pady=SPACING, sticky="w")

        ttk.Label(valkey_frame, text="Port:").grid(row=2, column=0, padx=SPACING, pady=SPACING, sticky="e")
        self.valkey_port_var = tk.StringVar()
        ttk.Entry(valkey_frame, width=10, textvariable=self.valkey_port_var).grid(row=2, column=1, padx=SPACING, pady=SPACING, sticky="w")

        ttk.Label(valkey_frame, text="Database:").grid(row=3, column=0, padx=SPACING, pady=SPACING, sticky="e")
        self.valkey_db_var = tk.StringVar()
        ttk.Entry(valkey_frame, width=10, textvariable=self.valkey_db_var).grid(row=3, column=1, padx=SPACING, pady=SPACING, sticky="w")

        ttk.Label(valkey_frame, text="Password (optional):").grid(row=4, column=0, padx=SPACING, pady=SPACING, sticky="e")
        self.valkey_password_var = tk.StringVar()
        ttk.Entry(valkey_frame, width=30, textvariable=self.valkey_password_var, show="*").grid(row=4, column=1, padx=SPACING, pady=SPACING, sticky="w")

        self.master_duplication_mode_var_editor = tk.BooleanVar(self)
        self.master_duplication_mode_var_editor.set(self.master_duplication_enabled_var.get())
        master_duplication_check = wrapped_checkbutton(self.api_content_frame, text="Enable Master Duplication Mode (for enabled APIs 1-4)", variable=self.master_duplication_mode_var_editor, command=self._sync_global_duplication_var_from_editor)
        master_duplication_check.grid(row=6, column=0, columnspan=2, padx=SPACING, pady=(10,5), sticky="ew")

        num_api_slots = 6
        # (name, label) pairs for the per-slot "API Compatibility" dropdown; see api_profiles.py.
        self._api_profile_choices = api_profiles.list_profiles()
        for i in range(num_api_slots):
            frame_text = f"API Slot {i+1}"
            if i == 0: frame_text += " (Primary for Q/Continuation in Duplication)"
            if i == 4: frame_text += " (Slop Fixer LLM - Not part of Duplication)"
            if i == 5: frame_text += " (Anti-Slop Fixer LLM - Independent Misc Option Not Part of Duplication)"
            
            api_frame = ttk.LabelFrame(self.api_content_frame, text=f"API Slot {i+1}")
            api_frame._section_search_text = frame_text
            api_frame.grid(row=i + 7, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="ew")
            description = ttk.Label(api_frame, text=frame_text, justify="left", wraplength=1)
            description.grid(row=0, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew")
            wrap_label(description, api_frame, padding=2 * SPACING)
            
            ttk.Label(api_frame, text="API URL:").grid(row=1, column=0, padx=SPACING, pady=SPACING, sticky="e")
            url_var = tk.StringVar(); ttk.Entry(api_frame, width=1, textvariable=url_var).grid(row=1, column=1, padx=SPACING, pady=SPACING, sticky="ew")
            setattr(self, f'api_url_var_{i+1}', url_var) 
            
            ttk.Label(api_frame, text="Model Name:").grid(row=2, column=0, padx=SPACING, pady=SPACING, sticky="e")
            model_var = tk.StringVar(); ttk.Entry(api_frame, width=1, textvariable=model_var).grid(row=2, column=1, padx=SPACING, pady=SPACING, sticky="ew")
            setattr(self, f'api_model_var_{i+1}', model_var)
            
            ttk.Label(api_frame, text="API Key:").grid(row=3, column=0, padx=SPACING, pady=SPACING, sticky="e")
            key_var = tk.StringVar(); ttk.Entry(api_frame, width=1, textvariable=key_var, show="*").grid(row=3, column=1, padx=SPACING, pady=SPACING, sticky="ew")
            setattr(self, f'api_key_var_{i+1}', key_var)

            # Add status label and test button for API connection
            status_var = tk.StringVar(value="Not tested")
            status_label = ttk.Label(api_frame, textvariable=status_var, foreground="gray")
            status_label.grid(row=7, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew")
            wrap_label(status_label, api_frame, padding=2 * SPACING)
            setattr(self, f'api_status_var_{i+1}', status_var)
            setattr(self, f'api_status_label_{i+1}', status_label)
            test_btn = ttk.Button(api_frame, text="Test Connection", command=lambda idx=i: self.test_api_connection(idx))  # This should already work since it's a lambda
            test_btn.grid(row=8, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="w")
            setattr(self, f'api_test_btn_{i+1}', test_btn)

            # --- API Compatibility (payload param filtering per endpoint) ---
            compat_frame = ttk.LabelFrame(api_frame, text="API Compatibility")
            compat_frame.grid(row=9, column=0, columnspan=3, padx=SPACING, pady=(SPACING, 2), sticky="ew")
            compat_frame.grid_columnconfigure(0, weight=1)

            profile_var = tk.StringVar()
            setattr(self, f'api_profile_var_{i+1}', profile_var)
            profile_combo = ttk.Combobox(
                compat_frame, textvariable=profile_var, state="readonly", width=1,
                values=[lbl for _, lbl in self._api_profile_choices]
            )
            profile_combo.grid(row=0, column=0, padx=SPACING, pady=SPACING, sticky="ew")
            profile_combo.bind("<<ComboboxSelected>>", lambda e, idx=i: self._on_api_profile_change(idx))
            ttk.Button(compat_frame, text="Detect", command=lambda idx=i: self._detect_api_profile(idx)).grid(
                row=0, column=1, padx=SPACING, pady=SPACING)

            custom_params_var = tk.StringVar()
            setattr(self, f'api_custom_params_var_{i+1}', custom_params_var)
            custom_params_entry = ttk.Entry(compat_frame, textvariable=custom_params_var, width=1)
            custom_params_entry.grid(row=1, column=0, columnspan=2, padx=SPACING, pady=(0, 2), sticky="ew")
            setattr(self, f'api_custom_params_entry_{i+1}', custom_params_entry)
            ttk.Label(
                compat_frame,
                text="Trims the request body to what the chosen endpoint accepts, so requests don't "
                     "fail on params it rejects (for example, Mistral rejects top_k). The 'User "
                     "Defined' profile instead sends only the comma-separated params you list above "
                     "(e.g. temperature, top_p, max_tokens); model, messages and stream are always sent.",
                style='Small.TLabel', wraplength=520, justify="left"
            ).grid(row=2, column=0, columnspan=2, padx=SPACING, pady=(0, SPACING), sticky="w")


            if i < 4: # APIs 1-4 (indices 0-3) can be enabled/disabled for main generation
                enabled_var = tk.BooleanVar(self, value=(i==0)) # API 1 defaults to enabled
                setattr(self, f'api_enabled_var_{i+1}', enabled_var)
                wrapped_checkbutton(api_frame, text="Enabled for Generation/Duplication", variable=enabled_var).grid(row=4, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="ew")

                # Add threads setting for each API
                ttk.Label(api_frame, text="Number of Threads:").grid(row=5, column=0, padx=SPACING, pady=SPACING, sticky="e")
                threads_var = tk.StringVar(value="10")  # Default value
                setattr(self, f'api_threads_var_{i+1}', threads_var)
                ttk.Entry(api_frame, width=10, textvariable=threads_var).grid(row=5, column=1, padx=SPACING, pady=SPACING, sticky="w")

                # NEW: Add rate limit setting for each API
                ttk.Label(api_frame, text="Rate Limit (RPM):").grid(row=6, column=0, padx=SPACING, pady=SPACING, sticky="e")
                rate_limit_var = tk.StringVar(value="60")  # Default 60 requests per minute
                setattr(self, f'api_rate_limit_var_{i+1}', rate_limit_var)
                ttk.Entry(api_frame, width=10, textvariable=rate_limit_var).grid(row=6, column=1, padx=SPACING, pady=SPACING, sticky="w")
                ttk.Label(api_frame, text="(Requests/Min)").grid(row=6, column=2, padx=SPACING, pady=SPACING, sticky="w")
            else: # API Slot 5 (Slop Fixer) and Slot 6 (Anti-Slop Fixer)
                ttk.Label(api_frame, text="Number of Threads:").grid(row=4, column=0, padx=SPACING, pady=SPACING, sticky="e")
                threads_var = tk.StringVar(value="10")  # Default value
                setattr(self, f'api_threads_var_{i+1}', threads_var)
                ttk.Entry(api_frame, width=10, textvariable=threads_var).grid(row=4, column=1, padx=SPACING, pady=SPACING, sticky="w")

                # NEW: Add rate limit setting for Slop Fixer / Anti-Slop Fixer API
                ttk.Label(api_frame, text="Rate Limit (RPM):").grid(row=5, column=0, padx=SPACING, pady=SPACING, sticky="e")
                rate_limit_var = tk.StringVar(value="60")  # Default 60 requests per minute
                setattr(self, f'api_rate_limit_var_{i+1}', rate_limit_var)
                ttk.Entry(api_frame, width=10, textvariable=rate_limit_var).grid(row=5, column=1, padx=SPACING, pady=SPACING, sticky="w")
                ttk.Label(api_frame, text="(Requests/Min)").grid(row=5, column=2, padx=SPACING, pady=SPACING, sticky="w")

                # --- NEW: Independence note for Anti-Slop (Slot 6) ---
                if i == 5:
                    ttk.Label(
                        api_frame,
                        text="This API slot is fully independent from the Slop Fixer (Slot 5). "
                             "You can point it at a different model/provider, "
                             "or leave it unconfigured to disable anti-slop fixing entirely.",
                        style='Small.TLabel',
                        wraplength=500,
                        justify="left"
                    ).grid(row=6, column=0, columnspan=3, padx=SPACING, pady=(2, SPACING), sticky="w")
                # --- END independence note ---

            api_frame.grid_columnconfigure(1, weight=1) # Make entry fields expand

        # --- Generation Tab ---
        self.generation_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.generation_tab, text="Generation")

        # Setup Canvas and Scrollbar for Generation Tab (similar to Prompts tab)
        self.generation_canvas = tk.Canvas(self.generation_tab)
        self.generation_scrollbar = tk.Scrollbar(self.generation_tab, orient="vertical", command=self.generation_canvas.yview, width=self.scrollbar_width, bg='#5a5a70', troughcolor='#2a2a35', activebackground='#8a8aa0', highlightthickness=0, bd=0)
        gen_settings_frame = ttk.Frame(self.generation_canvas)

        gen_settings_frame.bind(
            "<Configure>",
            lambda e: self.generation_canvas.configure(scrollregion=self.generation_canvas.bbox("all"))
        )

        self.generation_canvas_window_id = self.generation_canvas.create_window((0, 0), window=gen_settings_frame, anchor="nw")
        self.generation_canvas.bind("<Configure>", lambda e: self.generation_canvas.itemconfig(self.generation_canvas_window_id, width=e.width) if e.width > 1 else None)
        self.generation_canvas.configure(yscrollcommand=self.generation_scrollbar.set)

        self.generation_canvas.pack(side="left", fill="both", expand=True)
        self.generation_scrollbar.pack(side="right", fill="y")

        # Bind mouse wheel to canvas for scrolling
        bind_scroll_wheel(self.generation_canvas)

        # Allow the inner frame to expand horizontally
        gen_settings_frame.grid_columnconfigure(1, weight=1)

        row_idx = 0
        def add_gen_setting(label_text, var_name, help_text=""): # Helper to add a setting row
            nonlocal row_idx
            field = ttk.Frame(gen_settings_frame)
            field.grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew")
            field.columnconfigure(0, weight=1)
            label = ttk.Label(field, text=label_text, justify="left", wraplength=1)
            label.grid(row=0, column=0, sticky="ew", padx=(0, SPACING))
            wrap_label(label, field, padding=180)
            var = tk.StringVar(); setattr(self, var_name, var)
            ttk.Entry(field, width=12, textvariable=var).grid(row=0, column=1, sticky="e")
            if help_text:
                help_label = ttk.Label(field, text=help_text, style='Small.TLabel', justify="left", wraplength=1)
                help_label.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(3, 0))
                wrap_label(help_label, field)
            row_idx += 1

        add_gen_setting("Number of Random Chunks:", 'num_random_chunks_var', "(Total tasks to generate per run)")
        add_gen_setting("Max Input Length:", 'sanitize_input_max_length_var', "(For sanitize_input function)")
        add_gen_setting("Subject Size (chars):", 'subject_size_var', "(Size of subject text for question gen if not using questions.txt)")
        add_gen_setting("Context Size (chars):", 'context_size_var', "(Total size of text (subject + surrounding) for Q/A gen if not using questions.txt)")
        add_gen_setting("Max Attempts (per Q/A turn):", 'max_attempts_var', "(Main retries for a valid answer per turn per API; also for Q/UserCont/SlopFixer API calls)") # MODIFIED HELP TEXT
        add_gen_setting("Number of Turns (per conversation):", 'num_turns_var', "(Total Q/A pairs, e.g., 1 for single Q/A)")
        add_gen_setting("Placeholder Share (0 to 1):", 'placeholder_share_var', "(Opening mode: share of rows that start with a [Start a new chat] user turn, as SillyTavern's Strict mode sends)")
        add_gen_setting("History Size (questions):", 'history_size_var', "(# recent initial questions to avoid repetition in question gen)")
        add_gen_setting("API Request Timeout (seconds):", 'api_request_timeout_var', "(For connect and read timeout, e.g., 300)")
        add_gen_setting("Max Newlines (Malformed):", 'max_newlines_malformed_var', "(Max newlines in a reply before it's considered malformed)")
        add_gen_setting("Max Text Length (Malformed):", 'max_text_length_malformed_var', "(Max length in chars before reply is considered malformed)")
        add_gen_setting("Max Character Cards:", 'max_character_cards_var', "(Maximum character profiles in Character Engine)")

        # --- Quality Scoring Section ---
        ttk.Separator(gen_settings_frame, orient="horizontal").grid(row=row_idx, column=0, columnspan=3, sticky="ew", pady=10)
        row_idx += 1
        ttk.Label(gen_settings_frame, text="Quality Scoring", style='Header.TLabel').grid(row=row_idx, column=0, columnspan=3, sticky="w", pady=(5, 10))
        row_idx += 1

        self.quality_enabled_var_editor = tk.BooleanVar(value=True)
        wrapped_checkbutton(gen_settings_frame, text="Enable Quality Scoring", variable=self.quality_enabled_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=2, sticky="ew"); row_idx += 1

        self.quality_use_llm_var_editor = tk.BooleanVar(value=False)
        wrapped_checkbutton(gen_settings_frame, text="Use LLM for Coherence/Naturalness/Engagement/Consistency (uses extra API calls)", variable=self.quality_use_llm_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=2, sticky="ew"); row_idx += 1

        self.quality_output_filter_var_editor = tk.BooleanVar(value=False)
        wrapped_checkbutton(gen_settings_frame, text="Flag conversations below threshold (for post-filtering)", variable=self.quality_output_filter_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=2, sticky="ew"); row_idx += 1

        self.quality_roleplay_dims_var_editor = tk.BooleanVar(value=False)
        wrapped_checkbutton(gen_settings_frame, text="Roleplay dimensions: voice_distinctiveness, grounding, persona_alignment (adds weighted scoring + metadata columns; heuristic base, LLM-judge refined)", variable=self.quality_roleplay_dims_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=2, sticky="ew"); row_idx += 1

        # --- Per-turn logic gate ---
        # When on, every freshly generated assistant reply is checked by the quality
        # LLM for objective logic errors (sexed anatomy vs. stated gender, profile-
        # contradicting body parts/clothing/injuries, impossible timeline/quantity
        # statements). A failing turn is sent back to the SAME main API slot with the
        # original system prompt, the full conversation context, and the judge's
        # specific findings, so the model knows exactly what to fix without guessing.
        # Requires a configured Quality Scoring API URL/Model above (or falls back to
        # the primary API). Off by default so composite scores stay comparable.
        self.quality_logic_check_var_editor = tk.BooleanVar(value=False)
        wrapped_checkbutton(
            gen_settings_frame,
            text="Per-turn logic check (rewrites failed turns via the same main API using the judge's findings as context)",
            variable=self.quality_logic_check_var_editor,
        ).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=2, sticky="ew"); row_idx += 1

        add_gen_setting("Logic Check Fail Threshold (0-100):", 'quality_logic_threshold_var',
                        "(Score below which a turn is rewritten; default 70)")
        add_gen_setting("Logic Max Rewrites per Turn:", 'quality_logic_max_rewrites_var',
                        "(How many regeneration rounds run per failing turn; default 1)")
        add_gen_setting("Max Chars for Logic Check:", 'quality_logic_max_chars_var',
                        "(Truncate conversation for the per-turn check)")

        add_gen_setting("Min Score Threshold (0-100):", 'quality_min_threshold_var', "(Conversations below this are flagged)")
        add_gen_setting("Max Chars for LLM Scoring:", 'quality_max_chars_var', "(Truncate conversation before sending to scoring LLM)")
        # Judge retry / drop policy (v11.4): the judge is the dataset gatekeeper.
        add_gen_setting("Judge Max Attempts:", 'quality_judge_max_attempts_var',
                        "(Transient judge failures are retried this many times, then the conversation is dropped; invalid JSON and a logic score of 0 drop immediately without a retry)")
        add_gen_setting("Judge Retry Delay (seconds):", 'quality_judge_retry_delay_var',
                        "(Wait before the next judge attempt; grows linearly per attempt)")
        add_gen_setting("Judge Result Timeout (seconds):", 'quality_result_timeout_var',
                        "(How long a worker waits for its queued verdict before dropping the row)")

        # Quality scoring API config
        ttk.Label(gen_settings_frame, text="Quality Scoring API URL:").grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=2, sticky="w"); row_idx += 1
        self.quality_api_url_var = tk.StringVar()
        ttk.Entry(gen_settings_frame, width=1, textvariable=self.quality_api_url_var).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=2, sticky="ew"); row_idx += 1

        ttk.Label(gen_settings_frame, text="Quality Scoring Model:").grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=2, sticky="w"); row_idx += 1
        self.quality_api_model_var = tk.StringVar()
        ttk.Entry(gen_settings_frame, width=1, textvariable=self.quality_api_model_var).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=2, sticky="ew"); row_idx += 1

        ttk.Label(gen_settings_frame, text="Quality Scoring API Key:").grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=2, sticky="w"); row_idx += 1
        self.quality_api_key_var = tk.StringVar()
        ttk.Entry(gen_settings_frame, width=1, textvariable=self.quality_api_key_var, show="*").grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=2, sticky="ew"); row_idx += 1
        
        self.remove_reasoning_var_editor = tk.BooleanVar() # Editor's local var for this setting
        wrapped_checkbutton(gen_settings_frame, text="Remove Reasoning (Strip ... tags from LLM output)", variable=self.remove_reasoning_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew"); row_idx+=1

        self.remove_em_dash_var_editor = tk.BooleanVar()
        wrapped_checkbutton(gen_settings_frame, text="Experimental: Remove Em Dash (—) from output", variable=self.remove_em_dash_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew"); row_idx+=1

        self.ensure_space_after_line_break_var_editor = tk.BooleanVar()
        wrapped_checkbutton(gen_settings_frame, text="Experimental: Ensure Space After Line Break (prevents words running together)", variable=self.ensure_space_after_line_break_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew"); row_idx+=1

        # NEW: Add checkbox for removing excessive asterisks
        self.remove_asterisks_var_editor = tk.BooleanVar()
        wrapped_checkbutton(gen_settings_frame, text="Experimental: Remove Excessive Asterisks (**, ****, etc.) from output", variable=self.remove_asterisks_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew"); row_idx+=1

        # NEW: Add checkbox for removing "* *" pattern
        self.remove_asterisk_space_asterisk_var_editor = tk.BooleanVar()
        wrapped_checkbutton(gen_settings_frame, text="Experimental: Remove '* *' Pattern (asterisk space asterisk) from output", variable=self.remove_asterisk_space_asterisk_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew"); row_idx+=1

        self.remove_all_asterisks_var_editor = tk.BooleanVar()
        wrapped_checkbutton(gen_settings_frame, text="Experimental: Remove ALL Asterisks (including single *) from output", variable=self.remove_all_asterisks_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew"); row_idx+=1

        self.remove_markdown_var_editor = tk.BooleanVar()
        wrapped_checkbutton(gen_settings_frame, text="Experimental: Remove Markdown Formatting (Convert to plain text)", variable=self.remove_markdown_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew"); row_idx+=1

        add_gen_setting("Max Slop Sentence Fix Iterations:", 'max_slop_sentence_fix_iterations_var', "(Iterations for sentence-level slop fixing by Slop Fixer LLM)")

        self.slop_to_anti_slop_fallback_var_editor = tk.BooleanVar()
        wrapped_checkbutton(gen_settings_frame, text="Slop → Anti-Slop Fallback (Use Anti-Slop API as final attempt if Slop Fixer fails)",
                        variable=self.slop_to_anti_slop_fallback_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew"); row_idx+=1

        self.discard_refused_conversations_var_editor = tk.BooleanVar()
        wrapped_checkbutton(gen_settings_frame, text="Discard Refused Conversations (Don't save a conversation if any reply needed a jailbreak)",
                        variable=self.discard_refused_conversations_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew"); row_idx+=1

        self.use_conversation_history_for_fixers_var_editor = tk.BooleanVar(value=True)
        wrapped_checkbutton(gen_settings_frame, text="Use Conversation History for Fixers (Pass conversation context to slop/anti-slop fixer LLMs for coherent rephrasing)",
                        variable=self.use_conversation_history_for_fixers_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew"); row_idx+=1

        self.include_metadata_var_editor = tk.BooleanVar(value=True)
        wrapped_checkbutton(gen_settings_frame, text="Include Metadata in Output (Flat, schema-stable provenance block per record; trainer-safe, never modifies conversations[])",
                        variable=self.include_metadata_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew"); row_idx+=1
        
        ttk.Label(gen_settings_frame, text="Output Format:").grid(row=row_idx, column=0, padx=SPACING, pady=SPACING, sticky="e")
        self.output_format_var = tk.StringVar(value="sharegpt")
        ttk.Label(gen_settings_frame, text="sharegpt").grid(row=row_idx, column=1, padx=SPACING, pady=SPACING, sticky="w")
        ttk.Label(gen_settings_frame, text="(Format for output.jsonl files)").grid(row=row_idx + 1, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="w"); row_idx+=1

        # --- Prompts Tab ---
        self.prompts_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.prompts_tab, text="Prompts")
        self.prompts_canvas = tk.Canvas(self.prompts_tab)
        self.prompts_scrollbar = tk.Scrollbar(self.prompts_tab, orient="vertical", command=self.prompts_canvas.yview, width=self.scrollbar_width, bg='#5a5a70', troughcolor='#2a2a35', activebackground='#8a8aa0', highlightthickness=0, bd=0)
        self.prompts_content_frame = ttk.Frame(self.prompts_canvas)
        self.prompts_content_frame.bind(
            "<Configure>",
            lambda e: self.prompts_canvas.configure(scrollregion=self.prompts_canvas.bbox("all"))
        )
        self.prompts_canvas_window_id = self.prompts_canvas.create_window((0, 0), window=self.prompts_content_frame, anchor="nw")
        self.prompts_canvas.bind("<Configure>", lambda e: self.prompts_canvas.itemconfig(self.prompts_canvas_window_id, width=e.width) if e.width > 1 else None)
        self.prompts_canvas.configure(yscrollcommand=self.prompts_scrollbar.set)

        # Pack canvas and scrollbar into the tab to fill available space
        self.prompts_canvas.pack(side="left", fill="both", expand=True)
        self.prompts_scrollbar.pack(side="right", fill="y")

        # Bind mouse wheel to canvas for scrolling
        bind_scroll_wheel(self.prompts_canvas)

        # Define Boolean variables BEFORE they are used in Checkbuttons
        self.use_questions_file_var_editor = tk.BooleanVar()
        self.use_variable_system_var_editor = tk.BooleanVar()
        self.opening_mode_var_editor = tk.BooleanVar()

        prompts_row_idx = 0

        def add_prompt_text_area(label_text, var_name, height=4): # Helper for text areas
            nonlocal prompts_row_idx
            label = ttk.Label(self.prompts_content_frame, text=label_text, justify="left", wraplength=1)
            label.grid(row=prompts_row_idx, column=0, padx=SPACING, pady=(SPACING, 3), sticky="ew")
            wrap_label(label, self.prompts_content_frame, padding=2 * SPACING)
            text_widget = scrolledtext.ScrolledText(self.prompts_content_frame, wrap=tk.WORD, height=height, width=1, undo=True)
            text_widget.grid(row=prompts_row_idx + 1, column=0, padx=SPACING, pady=(0, SPACING), sticky="ew")
            setattr(self, var_name, text_widget)
            prompts_row_idx += 2

        wrapped_checkbutton(self.prompts_content_frame, text=f"Use '{os.path.basename(QUESTIONS_FILE_PATH)}' for questions (disables subject/context chunking)", variable=self.use_questions_file_var_editor).grid(row=prompts_row_idx, column=0, columnspan=1, padx=SPACING, pady=SPACING, sticky="ew"); prompts_row_idx+=1
        wrapped_checkbutton(self.prompts_content_frame, text="Use Variable System Prompts (randomly chosen from list below)", variable=self.use_variable_system_var_editor).grid(row=prompts_row_idx, column=0, columnspan=1, padx=SPACING, pady=SPACING, sticky="ew"); prompts_row_idx+=1
        wrapped_checkbutton(self.prompts_content_frame, text="Opening Mode (each sample starts with the character's greeting, then a first line from the user's seat; runs with or without input files; not with the questions file or duplication mode)", variable=self.opening_mode_var_editor).grid(row=prompts_row_idx, column=0, columnspan=1, padx=SPACING, pady=SPACING, sticky="ew"); prompts_row_idx+=1

        add_prompt_text_area("Top Level System Prompt (Applied to ALL prompts):", 'top_level_system_prompt_text', height=12)
        add_prompt_text_area("Base System Prompt (used if not variable, or as one of variations):", 'system_base_prompt_text', height=12)
        add_prompt_text_area("System Prompt Variations (one per line, used if 'Variable System Prompts' is checked):", 'system_variations_text', height=12)
        add_prompt_text_area("Question Prompt (use {recent_questions}, {subject}, {context}):", 'question_prompt_text', height=12)
        add_prompt_text_area("Answer Prompt (instruction for the assistant's turn):", 'answer_prompt_text', height=12)
        add_prompt_text_area("User Continuation Prompt (use {last_assistant_message} for user's next turn):", 'user_continuation_prompt_text', height=12)
        add_prompt_text_area("Greeting Prompt (opening mode; use {speaker} and {context}, the book chunk when there is one; saved exactly as typed):", 'greeting_prompt_text', height=8)
        add_prompt_text_area("Greeting Context (opening mode; fills the greeting prompt's {context} when there is a book chunk, use {chunk} for the chunk; saved exactly as typed):", 'greeting_context_text', height=8)
        add_prompt_text_area("First Line Prompt (opening mode; system prompt for the user's first line, use {style}; saved exactly as typed):", 'first_line_prompt_text', height=8)
        add_prompt_text_area("First Line Styles (opening mode; one per line, one is picked at random for {style}):", 'first_line_styles_text', height=6)

        # --- NEW: Lore Tab ---
        self.lore_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.lore_tab, text="Lore")

        self.lore_canvas = tk.Canvas(self.lore_tab)
        self.lore_scrollbar = tk.Scrollbar(self.lore_tab, orient="vertical", command=self.lore_canvas.yview, width=self.scrollbar_width, bg='#5a5a70', troughcolor='#2a2a35', activebackground='#8a8aa0', highlightthickness=0, bd=0)
        self.lore_content_frame = ttk.Frame(self.lore_canvas)

        self.lore_content_frame.bind(
            "<Configure>",
            lambda e: self.lore_canvas.configure(scrollregion=self.lore_canvas.bbox("all"))
        )
        self.lore_canvas_window_id = self.lore_canvas.create_window((0, 0), window=self.lore_content_frame, anchor="nw")
        self.lore_canvas.bind("<Configure>", lambda e: self.lore_canvas.itemconfig(self.lore_canvas_window_id, width=e.width, height=max(e.height, self.lore_content_frame.winfo_reqheight())) if e.width > 1 else None)
        self.lore_canvas.configure(yscrollcommand=self.lore_scrollbar.set)

        self.lore_canvas.pack(side="left", fill="both", expand=True)
        self.lore_scrollbar.pack(side="right", fill="y")
        bind_scroll_wheel(self.lore_canvas)

        # Lore UI Content
        ttk.Label(self.lore_content_frame, text="World Lore & Background Information:").grid(row=0, column=0, padx=SPACING, pady=(SPACING, 0), sticky="w")
        self.lore_text = scrolledtext.ScrolledText(self.lore_content_frame, wrap=tk.WORD, width=1, height=16, undo=True)
        self.lore_text.grid(row=1, column=0, padx=SPACING, pady=(0, SPACING), sticky="nsew")
        self.lore_content_frame.grid_columnconfigure(0, weight=1)
        self.lore_content_frame.grid_rowconfigure(1, weight=1)

        self.prompts_content_frame.grid_columnconfigure(0, weight=1) # Make text areas expand

        # --- Character Engine Tab ---
        self.character_engine_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.character_engine_tab, text="Character Engine")
        self.character_engine_canvas = tk.Canvas(self.character_engine_tab)
        self.character_engine_scrollbar = tk.Scrollbar(self.character_engine_tab, orient="vertical", command=self.character_engine_canvas.yview, width=self.scrollbar_width, bg='#5a5a70', troughcolor='#2a2a35', activebackground='#8a8aa0', highlightthickness=0, bd=0)
        self.character_engine_content_frame = ttk.Frame(self.character_engine_canvas)
        self.character_engine_content_frame.bind(
            "<Configure>",
            lambda e: self.character_engine_canvas.configure(scrollregion=self.character_engine_canvas.bbox("all"))
        )
        self.character_engine_canvas_window_id = self.character_engine_canvas.create_window((0, 0), window=self.character_engine_content_frame, anchor="nw")
        self.character_engine_canvas.bind("<Configure>", lambda e: self.character_engine_canvas.itemconfig(self.character_engine_canvas_window_id, width=e.width) if e.width > 1 else None)
        self.character_engine_canvas.configure(yscrollcommand=self.character_engine_scrollbar.set)

        # Pack canvas and scrollbar into the tab to fill available space
        self.character_engine_canvas.pack(side="left", fill="both", expand=True)
        self.character_engine_scrollbar.pack(side="right", fill="y")

        # Bind mouse wheel to canvas for scrolling
        bind_scroll_wheel(self.character_engine_canvas)

        character_engine_row_idx = 0

        # Keep the count control and its explanation on separate rows.
        label = ttk.Label(self.character_engine_content_frame, text="Number of Characters per Conversation:", wraplength=1)
        label.grid(row=character_engine_row_idx, column=0, padx=SPACING, pady=SPACING, sticky="ew")
        wrap_label(label, self.character_engine_content_frame, padding=2 * SPACING)
        character_engine_row_idx += 1
        self.num_characters_var_editor = tk.StringVar(value="1")
        ttk.Entry(self.character_engine_content_frame, width=10, textvariable=self.num_characters_var_editor).grid(
            row=character_engine_row_idx, column=0, padx=SPACING, pady=(0, SPACING), sticky="w")
        character_engine_row_idx += 1
        count_help = ttk.Label(self.character_engine_content_frame, text="(1-10, requires enough character cards)", wraplength=1)
        count_help.grid(row=character_engine_row_idx, column=0, padx=SPACING, pady=(0, SPACING), sticky="ew")
        wrap_label(count_help, self.character_engine_content_frame, padding=2 * SPACING)
        character_engine_row_idx += 1

        # Persona breakthrough: which assistant slot (1 = first reply) the inner persona starts being
        # injected from. Slot 1 stays outward-behaviour-only; the persona 'breaks through' from here on.
        pb_label = ttk.Label(self.character_engine_content_frame, text="Persona Breakthrough Turn:", wraplength=1)
        pb_label.grid(row=character_engine_row_idx, column=0, padx=SPACING, pady=(SPACING, 0), sticky="ew")
        wrap_label(pb_label, self.character_engine_content_frame, padding=2 * SPACING)
        character_engine_row_idx += 1
        self.persona_breakthrough_turn_var_editor = tk.StringVar(value="2")
        # Explicit black-on-white: the 'superhero' theme gives ttk.Spinbox white text, but on
        # some platforms the field falls back to a native white background, making the number
        # unreadable (white on white). A dedicated style + direct widget options override the
        # theme on every platform; the map keeps the text legible when disabled.
        try:
            self.persona_breakthrough_style = 'PersonaBreakthrough.TSpinbox'
            ttk.Style(self).configure(self.persona_breakthrough_style,
                                      foreground="#000000", background="#ffffff")
            ttk.Style(self).map(self.persona_breakthrough_style,
                                foreground=[("disabled", "#808080")],
                                background=[("disabled", "#f0f0f0")])
        except tk.TclError:
            self.persona_breakthrough_style = None
        self.persona_breakthrough_spinbox = ttk.Spinbox(
            self.character_engine_content_frame, from_=1, to=99,
            textvariable=self.persona_breakthrough_turn_var_editor, width=6,
            style=self.persona_breakthrough_style or 'TSpinbox')
        self.persona_breakthrough_spinbox.grid(
            row=character_engine_row_idx, column=0, padx=SPACING, pady=(0, 0), sticky="w")
        character_engine_row_idx += 1
        pb_help = ttk.Label(
            self.character_engine_content_frame,
            text="(1 = expose inner persona from the very first reply; 2 = hold back until the second reply onward)",
            wraplength=1)
        pb_help.grid(row=character_engine_row_idx, column=0, padx=SPACING, pady=(0, SPACING), sticky="ew")
        wrap_label(pb_help, self.character_engine_content_frame, padding=2 * SPACING)
        character_engine_row_idx += 1
        self.persona_placement_var_editor = tk.StringVar(value=DEFAULT_NOTE_PLACEMENT)
        character_engine_row_idx = self._add_placement_combobox(
            character_engine_row_idx, "Persona Breakthrough Placement:", self.persona_placement_var_editor)

        # Add checkbox to enable character engine
        self.enable_character_engine_var_editor = tk.BooleanVar()
        self.include_names_in_prompt_var_editor = tk.BooleanVar(value=True)
        self.enable_character_checkbox = wrapped_checkbutton(
            self.character_engine_content_frame,
            text="Enable Character Engine (random character profiles in conversations)",
            variable=self.enable_character_engine_var_editor,
            command=self._toggle_character_engine_fields
        )
        self.enable_character_checkbox.grid(row=character_engine_row_idx, column=0, columnspan=1, padx=SPACING, pady=SPACING, sticky="ew")
        character_engine_row_idx += 1

        wrapped_checkbutton(
            self.character_engine_content_frame,
            text="Include Character Names in Prompt",
            variable=self.include_names_in_prompt_var_editor
        ).grid(row=character_engine_row_idx, column=0, columnspan=1, padx=SPACING, pady=SPACING, sticky="ew")
        character_engine_row_idx += 1

        # Add checkbox to enable emotional states
        self.enable_emotional_states_var_editor = tk.BooleanVar()
        wrapped_checkbutton(self.character_engine_content_frame, text="Enable Emotional States (randomly assign to conversations)",
                variable=self.enable_emotional_states_var_editor, command=self._toggle_emotional_states_fields).grid(row=character_engine_row_idx, column=0, columnspan=1, padx=SPACING, pady=SPACING, sticky="ew")
        character_engine_row_idx += 1

        # Add emotional states text area
        ttk.Label(self.character_engine_content_frame, text="Emotional States / Arcs (one per line; use '->' for a progression, e.g. 'neutral -> tense'):").grid(row=character_engine_row_idx, column=0, padx=SPACING, pady=SPACING, sticky="nw")
        character_engine_row_idx += 1
        emotional_states_text = scrolledtext.ScrolledText(self.character_engine_content_frame, wrap=tk.WORD, height=6, width=1, undo=True)
        emotional_states_text.grid(row=character_engine_row_idx, column=0, padx=SPACING, pady=SPACING, sticky="ew")
        setattr(self, 'emotional_states_text', emotional_states_text)
        character_engine_row_idx += 1
        self.emotion_placement_var_editor = tk.StringVar(value=DEFAULT_NOTE_PLACEMENT)
        character_engine_row_idx = self._add_placement_combobox(
            character_engine_row_idx, "Emotional State Placement:", self.emotion_placement_var_editor)
        # Shown when a 'system' placement would change the system prompt between replies
        self.placement_cache_warning_label = ttk.Label(
            self.character_engine_content_frame, text="", foreground="orange", wraplength=1)
        self.placement_cache_warning_label.grid(
            row=character_engine_row_idx, column=0, padx=SPACING, pady=SPACING, sticky="ew")
        wrap_label(self.placement_cache_warning_label, self.character_engine_content_frame, padding=2 * SPACING)
        character_engine_row_idx += 1

        # Per-character emotional states/arcs: in a multi-character scene (2+ selected characters) each
        # character gets its OWN randomly assigned state/arc instead of sharing one.
        self.per_char_random_emotions_var_editor = tk.BooleanVar()
        wrapped_checkbutton(
            self.character_engine_content_frame,
            text="Per-Character Emotions (multi-character scenes: give each character its own random state/arc)",
            variable=self.per_char_random_emotions_var_editor
        ).grid(row=character_engine_row_idx, column=0, columnspan=1, padx=SPACING, pady=SPACING, sticky="ew")
        per_char_emotion_help = ttk.Label(
            self.character_engine_content_frame, wraplength=1,
            text="(Applies only when 2 or more characters are selected. Off by default: every character "
                 "shares one emotional state/arc.)")
        per_char_emotion_help.grid(row=character_engine_row_idx, column=0, padx=SPACING, pady=(0, SPACING), sticky="ew")
        wrap_label(per_char_emotion_help, self.character_engine_content_frame, padding=2 * SPACING)
        character_engine_row_idx += 1

        # --- NEW (v10.6.0): User-side emotional states/arcs. Optional; steered onto the simulated
        # user's messages (opening question/first line + each continuation) rather than the character.
        self.enable_user_emotional_states_var_editor = tk.BooleanVar()
        wrapped_checkbutton(self.character_engine_content_frame,
                text="Enable User Emotional States (assign an emotional state/arc to the simulated user)",
                variable=self.enable_user_emotional_states_var_editor,
                command=self._toggle_user_emotional_states_fields).grid(
                row=character_engine_row_idx, column=0, columnspan=1, padx=SPACING, pady=SPACING, sticky="ew")
        character_engine_row_idx += 1
        ttk.Label(self.character_engine_content_frame,
            text="User Emotional States / Arcs (one per line; use '->' for a progression, e.g. 'curious -> defensive'):").grid(
            row=character_engine_row_idx, column=0, padx=SPACING, pady=SPACING, sticky="nw")
        character_engine_row_idx += 1
        user_emotional_states_text = scrolledtext.ScrolledText(self.character_engine_content_frame, wrap=tk.WORD, height=5, width=1, undo=True)
        # Off by default (optional feature); the load handler re-enables it when configured on
        user_emotional_states_text.config(state='disabled')
        user_emotional_states_text.grid(row=character_engine_row_idx, column=0, padx=SPACING, pady=SPACING, sticky="ew")
        setattr(self, 'user_emotional_states_text', user_emotional_states_text)
        character_engine_row_idx += 1
        user_emotion_help = ttk.Label(
            self.character_engine_content_frame, wraplength=1,
            text="(Optional. Applies to the user's side of the conversation only -- the opening message and user "
                 "continuations. Off by default.)")
        user_emotion_help.grid(row=character_engine_row_idx, column=0, padx=SPACING, pady=(0, SPACING), sticky="ew")
        wrap_label(user_emotion_help, self.character_engine_content_frame, padding=2 * SPACING)
        character_engine_row_idx += 1

        # Add checkbox to enable random names
        self.enable_random_names_var_editor = tk.BooleanVar()
        wrapped_checkbutton(self.character_engine_content_frame, text="Enable Random Names (overrides character Name field with random names from list below)",
                variable=self.enable_random_names_var_editor, command=self._toggle_random_names_fields).grid(row=character_engine_row_idx, column=0, columnspan=1, padx=SPACING, pady=SPACING, sticky="ew")
        character_engine_row_idx += 1

        # Add random names text area
        ttk.Label(self.character_engine_content_frame, text="Random Names (one per line, randomly selected when enabled):").grid(row=character_engine_row_idx, column=0, padx=SPACING, pady=SPACING, sticky="nw")
        character_engine_row_idx += 1
        random_names_text = scrolledtext.ScrolledText(self.character_engine_content_frame, wrap=tk.WORD, height=6, width=1, undo=True)
        random_names_text.grid(row=character_engine_row_idx, column=0, padx=SPACING, pady=SPACING, sticky="ew")
        setattr(self, 'random_names_text', random_names_text)
        character_engine_row_idx += 1

        self.enable_class_selection_var_editor = tk.BooleanVar()
        wrapped_checkbutton(self.character_engine_content_frame,
                        text="Enable Class Selection (fantasy classes like mage, warlock, rogue, etc.)",
                        variable=self.enable_class_selection_var_editor,
                        command=self._toggle_class_fields).grid(row=character_engine_row_idx, column=0, columnspan=1, padx=SPACING, pady=SPACING, sticky="ew")
        character_engine_row_idx += 1

        # NEW: Add checkbox for Setting field
        self.enable_setting_selection_var_editor = tk.BooleanVar()
        wrapped_checkbutton(self.character_engine_content_frame,
                        text="Enable Setting Selection (custom location/environment for each character)",
                        variable=self.enable_setting_selection_var_editor,
                        command=self._toggle_setting_fields).grid(row=character_engine_row_idx, column=0, columnspan=1, padx=SPACING, pady=SPACING, sticky="ew")
        character_engine_row_idx += 1

        # Add checkbox to enable random settings pool (fills empty character settings + unifies ensembles)
        self.enable_random_settings_var_editor = tk.BooleanVar()
        wrapped_checkbutton(self.character_engine_content_frame, text="Enable Random Settings Pool (fill empty character settings; unify multi-character scenes)",
                variable=self.enable_random_settings_var_editor, command=self._toggle_random_settings_fields).grid(row=character_engine_row_idx, column=0, columnspan=1, padx=SPACING, pady=SPACING, sticky="ew")
        character_engine_row_idx += 1

        # Add random settings text area
        ttk.Label(self.character_engine_content_frame, text="Random Settings Pool (one per line, sampled into empty character settings): ").grid(row=character_engine_row_idx, column=0, padx=SPACING, pady=SPACING, sticky="nw")
        character_engine_row_idx += 1
        random_settings_text = scrolledtext.ScrolledText(self.character_engine_content_frame, wrap=tk.WORD, height=6, width=1, undo=True)
        random_settings_text.grid(row=character_engine_row_idx, column=0, padx=SPACING, pady=SPACING, sticky="ew")
        setattr(self, 'random_settings_text', random_settings_text)
        character_engine_row_idx += 1

        # --- Character Table (Card-based layout) ---
        self.character_entries = []  # All cards retain their variables, including off-page cards.
        self.character_page_size = 3
        self.character_page = 0
        self._loading_character_cards = False
        self._character_scroll_job = None
        self.bind("<Destroy>", self._cancel_character_scroll, add="+")

        character_table_lf = ttk.LabelFrame(
            self.character_engine_content_frame,
            text="Character Profiles (Each card is one complete character)"
        )
        character_table_lf.grid(
            row=character_engine_row_idx, column=0, columnspan=1,
            padx=SPACING, pady=SPACING, sticky="nsew"
        )
        character_engine_row_idx += 1

        # Bound the native frame height, not just the canvas viewport. Ttk draws
        # full-widget backing pixmaps on X11, so only one page may be managed.
        page_controls = self.character_page_controls = ttk.Frame(character_table_lf)
        page_controls.pack(fill=tk.X, padx=SPACING, pady=SPACING)
        page_controls.columnconfigure(3, weight=1)
        self.character_previous_button = ttk.Button(
            page_controls, text="Previous", command=lambda: self._show_character_page(self.character_page - 1))
        self.character_previous_button.grid(row=0, column=0, padx=(0, SPACING), sticky="w")
        ttk.Label(page_controls, text="Page:").grid(row=0, column=1, padx=(0, 4), sticky="w")
        self.character_page_selector = ttk.Combobox(page_controls, state="readonly", width=10)
        self.character_page_selector.grid(row=0, column=2, sticky="w")
        self.character_page_selector.bind("<<ComboboxSelected>>", self._select_character_page)
        self.character_next_button = ttk.Button(
            page_controls, text="Next", command=lambda: self._show_character_page(self.character_page + 1))
        self.character_next_button.grid(row=0, column=4, padx=(SPACING, 0), sticky="e")
        self.character_page_summary = ttk.Label(page_controls, text="No characters", wraplength=1)
        self.character_page_summary.grid(row=1, column=0, columnspan=5, sticky="ew", pady=(5, 0))
        wrap_label(self.character_page_summary, page_controls)

        # Off-page cards stay alive but unmanaged, preserving edit/undo state.
        self.character_cards_frame = ttk.Frame(character_table_lf)
        self.character_cards_frame.pack(fill=tk.BOTH, expand=True, padx=SPACING, pady=SPACING)

        # Add Character button
        self.add_char_btn = ttk.Button(
            self.character_engine_content_frame,
            text="➕ Add Character",
            command=self._add_character_row
        )
        self.add_char_btn.grid(
            row=character_engine_row_idx, column=0, columnspan=1,
            padx=SPACING, pady=SPACING, sticky="w"
        )
        character_engine_row_idx += 1

        # load_config_handler creates the saved cards, or the default empty cards.

        self.character_engine_content_frame.grid_columnconfigure(0, weight=1)

        # --- Detection Tab ---
        self.detection_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.detection_tab, text="Detection")

        # Setup Canvas and Scrollbar for Detection Tab (similar to Prompts tab)
        self.detection_canvas = tk.Canvas(self.detection_tab)
        self.detection_scrollbar = tk.Scrollbar(self.detection_tab, orient="vertical", command=self.detection_canvas.yview, width=self.scrollbar_width, bg='#5a5a70', troughcolor='#2a2a35', activebackground='#8a8aa0', highlightthickness=0, bd=0)
        self.detection_content_frame = ttk.Frame(self.detection_canvas)

        self.detection_content_frame.bind(
            "<Configure>",
            lambda e: self.detection_canvas.configure(scrollregion=self.detection_canvas.bbox("all"))
        )

        self.detection_canvas_window_id = self.detection_canvas.create_window((0, 0), window=self.detection_content_frame, anchor="nw")
        self.detection_canvas.bind("<Configure>", lambda e: self.detection_canvas.itemconfig(self.detection_canvas_window_id, width=e.width) if e.width > 1 else None)
        self.detection_canvas.configure(yscrollcommand=self.detection_scrollbar.set)

        self.detection_canvas.pack(side="left", fill="both", expand=True)
        self.detection_scrollbar.pack(side="right", fill="y")

        # Bind mouse wheel to canvas for scrolling
        bind_scroll_wheel(self.detection_canvas)

        self.gender_var_editor = tk.StringVar() # Editor's local var for gender
        gender_frame = ttk.Frame(self.detection_content_frame)
        gender_frame.grid(row=0, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="ew")
        gender_frame.columnconfigure(0, weight=1)
        gender_label = ttk.Label(gender_frame, text="Assistant Persona Gender (for user speaking detection):", wraplength=1)
        gender_label.grid(row=0, column=0, sticky="ew")
        wrap_label(gender_label, gender_frame)
        gender_choices = ttk.Frame(gender_frame)
        gender_choices.grid(row=1, column=0, sticky="w", pady=(5, 0))
        for label, value in (("Female", "female"), ("Male", "male"), ("Neutral", "neutral")):
            ttk.Radiobutton(gender_choices, text=label, variable=self.gender_var_editor, value=value,
                            command=self.on_gender_change_editor_handler).pack(side=tk.LEFT, padx=(0, SPACING))
        
        wrapped_checkbutton(self.detection_content_frame, text="Disable User Impersonation Detection (Globally)", variable=self.no_user_impersonation_var).grid(row=1, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="ew")

        detection_panels = ttk.Frame(self.detection_content_frame)
        detection_panels.grid(row=2, column=0, columnspan=2, sticky="ew")
        col1_frame = ttk.Frame(detection_panels)
        col2_frame = ttk.Frame(detection_panels)
        self._detection_layout = ResponsiveGrid(detection_panels, [col1_frame, col2_frame],
                                                min_width=420, max_columns=2, padx=4, pady=4)
        self.detection_content_frame.grid_columnconfigure(0, weight=1)
        
        def add_detection_list_pair(parent_frame, lf_text, phrases_var_name, fixes_var_name): # Helper for detection list pairs
            lf = ttk.LabelFrame(parent_frame, text=lf_text.split(" (")[0])
            lf._section_search_text = lf_text
            lf.pack(padx=SPACING, pady=SPACING, fill="both", expand=True)
            if " (" in lf_text:
                note = ttk.Label(lf, text=lf_text, justify="left", wraplength=1)
                note.pack(fill="x", pady=(0, 5))
                wrap_label(note, lf)
            label = ttk.Label(lf, text="Detection Phrases (one per line):", wraplength=1)
            label.pack(fill="x")
            wrap_label(label, lf)
            phrases_text = scrolledtext.ScrolledText(lf, wrap=tk.WORD, height=12, width=1, undo=True)
            phrases_text.pack(fill="both", expand=True, pady=(0,5))
            setattr(self, phrases_var_name, phrases_text)
            label = ttk.Label(lf, text="Fixes (appended to system prompt or for fixer rotation):", wraplength=1)
            label.pack(fill="x")
            wrap_label(label, lf)
            fixes_text = scrolledtext.ScrolledText(lf, wrap=tk.WORD, height=12, width=1, undo=True)
            fixes_text.pack(fill="both", expand=True)
            setattr(self, fixes_var_name, fixes_text)

        add_detection_list_pair(col1_frame, "Refusal Detection", 'refusal_phrases_text', 'refusal_fixes_text')
        add_detection_list_pair(col1_frame, "User Speaking Detection (Phrases/Fixes are Gender Specific)", 'user_speaking_phrases_text', 'user_speaking_fixes_text')
        add_detection_list_pair(col2_frame, "Slop Detection", 'slop_phrases_text', 'slop_fixes_text')
        add_detection_list_pair(col2_frame, "Anti-Slop Detection", 'anti_slop_phrases_text', 'anti_slop_fixes_text')

        # --- OOC Drift Detection (phrase-driven, no dedicated API slot) ---
        ooc_lf = ttk.LabelFrame(col2_frame, text="Out-of-Character (OOC) Detection")
        ooc_lf._section_search_text = "Out-of-Character (OOC) Detection"
        ooc_lf.pack(padx=SPACING, pady=SPACING, fill="both", expand=True)
        self.ooc_enabled_var_editor = tk.BooleanVar()
        wrapped_checkbutton(ooc_lf, text="Enable OOC Detection (catches helpful-bot narration breaks, god-viewing, fourth-wall drops)",
                            variable=self.ooc_enabled_var_editor, command=self._toggle_ooc_fields).pack(fill="x", pady=(0, 5))
        ooc_phrases_label = ttk.Label(ooc_lf, text="Detection Phrases (one per line):", wraplength=1)
        ooc_phrases_label.pack(fill="x")
        wrap_label(ooc_phrases_label, ooc_lf)
        self.ooc_phrases_text = scrolledtext.ScrolledText(ooc_lf, wrap=tk.WORD, height=12, width=1, undo=True)
        self.ooc_phrases_text.pack(fill="both", expand=True, pady=(0,5))
        ooc_fixes_label = ttk.Label(ooc_lf, text="Fixes (appended to system prompt or for fixer rotation):", wraplength=1)
        ooc_fixes_label.pack(fill="x")
        wrap_label(ooc_fixes_label, ooc_lf)
        self.ooc_fixes_text = scrolledtext.ScrolledText(ooc_lf, wrap=tk.WORD, height=12, width=1, undo=True)
        self.ooc_fixes_text.pack(fill="both", expand=True)

        anti_slop_note_frame = ttk.Frame(col2_frame)
        anti_slop_note_frame.pack(padx=SPACING, pady=(0, SPACING), fill="x")
        ttk.Label(
            anti_slop_note_frame,
            text="ℹ️ Anti-Slop is an optional, independent detection layer. "
                 "It uses its own API (Slot 6) and its own sampler settings, "
                 "fully separate from Slop Detection (Slot 5). "
                 "Configure it only if you need an additional pass for "
                 "specific undesirable phrases that Slop Detection doesn't cover.",
            style='Small.TLabel',
            wraplength=500,
            justify="left"
        ).pack(anchor="w", padx=5, pady=(5, 0))


        # --- Samplers Tab ---
        self.samplers_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.samplers_tab, text="Samplers")

        # Setup Canvas and Scrollbar for Samplers Tab
        self.samplers_canvas = tk.Canvas(self.samplers_tab)
        self.samplers_scrollbar = tk.Scrollbar(self.samplers_tab, orient="vertical", command=self.samplers_canvas.yview, width=self.scrollbar_width, bg='#5a5a70', troughcolor='#2a2a35', activebackground='#8a8aa0', highlightthickness=0, bd=0)
        sampler_params_frame = ttk.Frame(self.samplers_canvas)
        sampler_params_frame.columnconfigure(0, weight=1)

        sampler_params_frame.bind(
            "<Configure>",
            lambda e: self.samplers_canvas.configure(scrollregion=self.samplers_canvas.bbox("all"))
        )

        self.samplers_canvas_window_id = self.samplers_canvas.create_window((0, 0), window=sampler_params_frame, anchor="nw")
        self.samplers_canvas.bind("<Configure>", lambda e: self.samplers_canvas.itemconfig(self.samplers_canvas_window_id, width=e.width) if e.width > 1 else None)
        self.samplers_canvas.configure(yscrollcommand=self.samplers_scrollbar.set)

        self.samplers_canvas.pack(side="left", fill="both", expand=True)
        self.samplers_scrollbar.pack(side="right", fill="y")

        # Bind mouse wheel to canvas for scrolling
        bind_scroll_wheel(self.samplers_canvas)

        ttk.Label(sampler_params_frame, text="Sampler Priority (Order for API payload, one per line, e.g., temperature, top_p):").grid(row=0, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="w")
        self.sampler_priority_text = scrolledtext.ScrolledText(sampler_params_frame, wrap=tk.WORD, height=5, width=1, undo=True); self.sampler_priority_text.grid(row=1, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew")
        
        sampler_row = 2 
        def add_sampler_param(label, var_name, example): # Helper for sampler parameters
            nonlocal sampler_row
            field = ttk.Frame(sampler_params_frame)
            field.grid(row=sampler_row, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew")
            field.columnconfigure(0, weight=1)
            caption = ttk.Label(field, text=label, justify="left", wraplength=1)
            caption.grid(row=0, column=0, sticky="ew", padx=(0, SPACING))
            wrap_label(caption, field, padding=180)
            var = tk.StringVar(); setattr(self, var_name, var)
            ttk.Entry(field, width=12, textvariable=var).grid(row=0, column=1, sticky="e")
            help_label = ttk.Label(field, text=example, style='Small.TLabel', justify="left", wraplength=1)
            help_label.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(3, 0))
            wrap_label(help_label, field)
            sampler_row += 1

        add_sampler_param("Temperature:", 'temperature_var', "(E.g., 0.7)")
        add_sampler_param("Top P:", 'top_p_var', "(E.g., 0.9)")
        add_sampler_param("Top K:", 'top_k_var', "(E.g., 40)")
        add_sampler_param("Min P:", 'min_p_var', "(E.g., 0.05)")
        add_sampler_param("Repetition Penalty:", 'repetition_penalty_var', "(E.g., 1.1)")
        add_sampler_param("Max Tokens (Initial Question):", 'max_tokens_question_var', "(E.g., 256)")
        add_sampler_param("Max Tokens (Assistant Answer):", 'max_tokens_answer_var', "(E.g., 1024)")
        add_sampler_param("Max Tokens (User Continuation):", 'max_tokens_user_reply_var', "(E.g., 256)")
        ttk.Label(sampler_params_frame, text="Logit Bias (JSON format, e.g., {\"15\": 100}):").grid(row=sampler_row, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="w")
        sampler_row += 1
        self.logit_bias_text = scrolledtext.ScrolledText(sampler_params_frame, wrap=tk.WORD, height=5, width=1, undo=True)
        self.logit_bias_text.grid(row=sampler_row, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew")
        sampler_row += 1

        # --- v11.1 Bad Words (vLLM-style): toggles the top-level `bad_words` list on every main-API
        # request AND on the Slop Fixer (slot 5) / Anti-Slop Fixer (slot 6) requests. Leading
        # whitespace on each line is preserved exactly as typed (so `sunny` and ` sunny` are
        # distinct forbidden tokens, matching vLLM semantics). Blank lines are skipped.
        self.use_bad_words_var_editor = tk.BooleanVar()
        wrapped_checkbutton(
            sampler_params_frame,
            text="Enable Bad Words (send vLLM-style `bad_words` to every main-API + slop/anti-slop request)",
            variable=self.use_bad_words_var_editor,
            command=self._toggle_bad_words_fields
        ).grid(row=sampler_row, column=0, columnspan=3, padx=SPACING, pady=(SPACING, 0), sticky="w")
        sampler_row += 1
        bad_words_caption = ttk.Label(sampler_params_frame, text="Bad Words (one per line; leading whitespace is significant):", justify="left", wraplength=1)
        bad_words_caption.grid(row=sampler_row, column=0, columnspan=3, sticky="ew", padx=SPACING, pady=(4, 0))
        wrap_label(bad_words_caption, sampler_params_frame)
        sampler_row += 1
        self.bad_words_text = scrolledtext.ScrolledText(sampler_params_frame, wrap=tk.NONE, height=5, width=1, undo=True)
        self.bad_words_text.grid(row=sampler_row, column=0, columnspan=3, padx=SPACING, pady=(0, SPACING), sticky="ew")
        sampler_row += 1
        bad_words_help = ttk.Label(
            sampler_params_frame,
            style='Small.TLabel', justify="left", wraplength=1,
            text="(E.g. a line ` For Example` forbids the word WITH its leading space; a line `For Example` forbids it anywhere. "
                 "Both may be listed independently. Applies to question / answer / user-continuation / opening-mode requests plus Slop Fixer (slot 5) and Anti-Slop Fixer (slot 6).)")
        bad_words_help.grid(row=sampler_row, column=0, columnspan=3, sticky="ew", padx=SPACING, pady=(0, SPACING))
        wrap_label(bad_words_help, sampler_params_frame)
        sampler_row += 1

        self.enable_thinking_mode_var = tk.StringVar(value="default")
        slop_fixer_sampler_lf = ttk.LabelFrame(sampler_params_frame, text="Slop Fixer LLM Sampler Overrides (API Slot 5 - Optional)")
        slop_fixer_sampler_lf.grid(row=sampler_row, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew"); sampler_row+=1
        slop_fixer_sampler_lf.columnconfigure(0, weight=1)
        sf_sampler_row = 0 
        def add_slop_fixer_param(label, var_name, example): # Helper for slop fixer sampler params
            nonlocal sf_sampler_row
            field = ttk.Frame(slop_fixer_sampler_lf)
            field.grid(row=sf_sampler_row, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew")
            field.columnconfigure(0, weight=1)
            caption = ttk.Label(field, text=label, justify="left", wraplength=1)
            caption.grid(row=0, column=0, sticky="ew", padx=(0, SPACING))
            wrap_label(caption, field, padding=180)
            var = tk.StringVar(); setattr(self, var_name, var)
            ttk.Entry(field, width=12, textvariable=var).grid(row=0, column=1, sticky="e")
            help_label = ttk.Label(field, text=example, style='Small.TLabel', justify="left", wraplength=1)
            help_label.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(3, 0))
            wrap_label(help_label, field)
            sf_sampler_row += 1
        add_slop_fixer_param("Temperature (Slop Fixer):", 'slop_fixer_temp_var', "(E.g., 0.5, uses main if blank)")
        add_slop_fixer_param("Top P (Slop Fixer):", 'slop_fixer_top_p_var', "(E.g., 0.95, uses main if blank)")
        add_slop_fixer_param("Min P (Slop Fixer):", 'slop_fixer_min_p_var', "(E.g., 0.05, uses main if blank)")
        add_slop_fixer_param("Max Tokens (Slop Fixer):", 'slop_fixer_max_tokens_var', "(Auto-calculated if blank, e.g. 200)")
        add_slop_fixer_param("Top K (Slop Fixer):", 'slop_fixer_top_k_var', "(E.g., 40, uses main if blank)")
        add_slop_fixer_param("Repetition Penalty (Slop Fixer):", 'slop_fixer_repetition_penalty_var', "(E.g., 1.1, uses main if blank)")
        # --- NEW: Anti-Slop Fixer Sampler Settings ---
        ttk.Label(
            sampler_params_frame,
            text="The Anti-Slop Fixer (Slot 6) uses its own sampler settings, independent of the Slop Fixer (Slot 5). "
                 "Leave fields blank to inherit from the main sampler defaults.",
            style='Small.TLabel',
            wraplength=600,
            justify="left"
        ).grid(row=sampler_row, column=0, columnspan=3, padx=SPACING, pady=(8, 0), sticky="w")
        sampler_row += 1

        anti_slop_fixer_sampler_lf = ttk.LabelFrame(sampler_params_frame, text="Anti-Slop Fixer LLM Sampler Overrides (API Slot 6 - Optional)")
        anti_slop_fixer_sampler_lf.grid(row=sampler_row, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew"); sampler_row+=1
        anti_slop_fixer_sampler_lf.columnconfigure(0, weight=1)
        asf_sampler_row = 0
        def add_anti_slop_fixer_param(label, var_name, example): # Helper for anti-slop fixer sampler params
            nonlocal asf_sampler_row
            field = ttk.Frame(anti_slop_fixer_sampler_lf)
            field.grid(row=asf_sampler_row, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew")
            field.columnconfigure(0, weight=1)
            caption = ttk.Label(field, text=label, justify="left", wraplength=1)
            caption.grid(row=0, column=0, sticky="ew", padx=(0, SPACING))
            wrap_label(caption, field, padding=180)
            var = tk.StringVar(); setattr(self, var_name, var)
            ttk.Entry(field, width=12, textvariable=var).grid(row=0, column=1, sticky="e")
            help_label = ttk.Label(field, text=example, style='Small.TLabel', justify="left", wraplength=1)
            help_label.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(3, 0))
            wrap_label(help_label, field)
            asf_sampler_row += 1
        add_anti_slop_fixer_param("Temperature (Anti-Slop):", 'anti_slop_fixer_temp_var', "(E.g., 0.5, uses main if blank)")
        add_anti_slop_fixer_param("Top P (Anti-Slop):", 'anti_slop_fixer_top_p_var', "(E.g., 0.95, uses main if blank)")
        add_anti_slop_fixer_param("Min P (Anti-Slop):", 'anti_slop_fixer_min_p_var', "(E.g., 0.05, uses main if blank)")
        add_anti_slop_fixer_param("Max Tokens (Anti-Slop):", 'anti_slop_fixer_max_tokens_var', "(Auto-calculated if blank, e.g. 200)")
        add_anti_slop_fixer_param("Top K (Anti-Slop):", 'anti_slop_fixer_top_k_var', "(E.g., 40, uses main if blank)")
        add_anti_slop_fixer_param("Repetition Penalty (Anti-Slop):", 'anti_slop_fixer_repetition_penalty_var', "(E.g., 1.1, uses main if blank)")
        ttk.Label(sampler_params_frame, text="Thinking Mode:").grid(row=sampler_row, column=0, padx=SPACING, pady=SPACING, sticky="e")
        enable_thinking_combo = ttk.Combobox(
            sampler_params_frame,
            textvariable=self.enable_thinking_mode_var,
            values=["default", "enable", "disable"],
            state="readonly",
            width=20
        )
        enable_thinking_combo.grid(row=sampler_row, column=1, padx=SPACING, pady=SPACING, sticky="w")
        ttk.Label(sampler_params_frame, text="(Controls chat_template_kwargs enable_thinking)").grid(row=sampler_row + 1, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="w")
        sampler_row += 1


        # Dataset configuration remains isolated from the generation worker.
        self.datasets_panel = DatasetsPanel(self.notebook)
        self.notebook.add(self.datasets_panel, text="Datasets")

        # --- Profiles Tab ---
        self.profiles_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.profiles_tab, text="Profiles")
        self.profiles_scroll = ScrollableFrame(self.profiles_tab)
        self.profiles_scroll.pack(padx=SPACING, pady=SPACING, fill="both", expand=True)
        profiles_main_frame = self.profiles_scroll.content

        load_profile_frame = ttk.LabelFrame(profiles_main_frame, text="Load Profile")
        load_profile_frame.pack(padx=SPACING, pady=SPACING, fill="x", expand=False)
        ttk.Label(load_profile_frame, text="Available Profiles:").grid(row=0, column=0, padx=SPACING, pady=SPACING, sticky="w")
        self.profile_list_var = tk.StringVar()
        self.profile_combobox = ttk.Combobox(load_profile_frame, textvariable=self.profile_list_var, width=1, state="readonly")
        self.profile_combobox.grid(row=1, column=0, padx=SPACING, pady=SPACING, sticky="ew")
        ttk.Button(load_profile_frame, text="Load Selected Profile to Editor & config.yml", command=self._load_selected_profile_handler).grid(row=2, column=0, padx=SPACING, pady=SPACING, sticky="ew")
        ttk.Button(load_profile_frame, text="Delete Selected Profile", command=self._delete_selected_profile_handler).grid(row=3, column=0, padx=SPACING, pady=SPACING, sticky="ew")
        load_profile_frame.grid_columnconfigure(0, weight=1)

        save_profile_frame = ttk.LabelFrame(profiles_main_frame, text="Save Current Editor Configuration As Profile")
        save_profile_frame.pack(padx=SPACING, fill="x", expand=False, pady=(10,5))
        ttk.Label(save_profile_frame, text="New Profile Name:").grid(row=0, column=0, padx=SPACING, pady=SPACING, sticky="w")
        self.new_profile_name_var = tk.StringVar()
        ttk.Entry(save_profile_frame, textvariable=self.new_profile_name_var, width=1).grid(row=1, column=0, padx=SPACING, pady=SPACING, sticky="ew")
        ttk.Button(save_profile_frame, text="Save Current Editor Config As Profile...", command=self._save_profile_as_handler).grid(row=2, column=0, padx=SPACING, pady=SPACING, sticky="ew")
        save_profile_frame.grid_columnconfigure(0, weight=1)
        
        # The shell reserves search, navigation and actions; only the notebook grows.
        self.tab_navigator = NotebookNavigator(self, self.notebook)
        self.tab_navigator.grid(row=1, column=0, sticky="ew", padx=SPACING, pady=(4, 0))
        self.tab_navigator.refresh()
        toolbar = ttk.Frame(self)
        toolbar.grid(row=3, column=0, sticky="ew", padx=SPACING, pady=(0, SPACING))
        for column, (name, text, command) in enumerate((
                ("save_button", "Save to config.yml", self.save_config_handler),
                ("reload_button", "Reload from files", self.load_config_handler),
                ("validate_button", "Validate values", self.validate_config_handler))):
            toolbar.columnconfigure(column, weight=1)
            button = ttk.Button(toolbar, text=text, command=command)
            button.grid(row=0, column=column, sticky="ew", padx=4)
            setattr(self, name, button)
        self.status = ttk.Label(self, text="Ready", foreground="lightgray", wraplength=1)
        self.status.grid(row=4, column=0, sticky="ew", padx=SPACING, pady=(0, 5))
        wrap_label(self.status, self, padding=2 * SPACING)
        self._wrap_existing_notes(self.notebook)

        self.load_config_handler() # Load initial config into editor UI
        self._populate_profile_list() # Populate profiles dropdown
        self.protocol("WM_DELETE_WINDOW", self.on_close_editor) # Handle editor close button

    # --- NEW: Tab & Section Search Methods ---
    def _wrap_existing_notes(self, parent):
        """Update explanatory labels after their containers receive real widths."""
        for child in parent.winfo_children():
            if isinstance(child, ttk.Label):
                width = int(child.cget("wraplength") or 0)
                if width >= 500 or (width == 0 and len(str(child.cget("text"))) > 60):
                    child.configure(justify="left")
                    wrap_label(child, parent, padding=2 * SPACING)
            self._wrap_existing_notes(child)

    def search_editor_tabs(self, event=None):
        query = self.search_entry.get().strip().lower()
        if not query:
            self.search_entry.config(foreground="")
            return

        # 1. Match against Notebook Tab names
        for tab_id in self.notebook.tabs():
            if query in self.notebook.tab(tab_id, "text").lower():
                self.notebook.select(tab_id)
                self.search_entry.config(foreground="green")
                return

        # 2. Match against LabelFrame names inside tabs
        for tab_id in self.notebook.tabs():
            tab_widget = self.notebook.nametowidget(tab_id)
            if self._find_and_raise_label_frame(tab_widget, query, tab_id):
                return

        self.search_entry.config(foreground="red")

    def _find_and_raise_label_frame(self, widget, query, tab_id):
        if isinstance(widget, ttk.LabelFrame):
            lf_text = getattr(widget, '_section_search_text', widget.cget("text")).lower()
            if query in lf_text:
                self.notebook.select(tab_id)
                self._reveal_character_page_for(widget)
                widget.tkraise()
                self._scroll_canvas_to(widget)
                self.search_entry.config(foreground="green")
                return True
        for child in widget.winfo_children():
            if self._find_and_raise_label_frame(child, query, tab_id):
                return True
        return False

    def _scroll_canvas_to(self, target_widget):
        parent = target_widget.master
        while parent:
            if isinstance(parent, tk.Canvas):
                scroll_to_widget(parent, target_widget)
                return
            parent = parent.master
    # --- END NEW METHODS ---

    def _sync_global_duplication_var_from_editor(self):
        """Updates the global self.master_duplication_enabled_var when the editor's checkbox changes."""
        self.master_duplication_enabled_var.set(self.master_duplication_mode_var_editor.get())
        log_message(f"ConfigEditor: Master Duplication Mode (UI checkbox) set to {self.master_duplication_mode_var_editor.get()}", "INFO")

    def test_api_connection(self, slot_index):
        """Tests the connection for a specific API slot in a separate thread."""
        btn = getattr(self, f'api_test_btn_{slot_index+1}')
        # Guard: check widget exists before disabling
        if not (hasattr(btn, 'winfo_exists') and btn.winfo_exists()):
            return
        btn.config(state=tk.DISABLED)
        status_var = getattr(self, f'api_status_var_{slot_index+1}')
        status_var.set("Testing...")

        def run_test(self):
            try:
                api_url = getattr(self, f'api_url_var_{slot_index+1}').get().strip()
                model_name = getattr(self, f'api_model_var_{slot_index+1}').get().strip()
                api_key = getattr(self, f'api_key_var_{slot_index+1}').get().strip()

                if not api_url:
                    raise ValueError("API URL is missing")
                if not validate_url(api_url):
                    raise ValueError("Invalid URL format")

                payload = {
                    "model": model_name,
                    "messages": [
                        {"role": "system", "content": "You are a helpful assistant."},
                        {"role": "user", "content": "Reply with 'OK'."}
                    ],
                    "temperature": 0.5,
                    "max_tokens": 5,
                    "stream": False
                }
                headers = {
                    'Content-Type': 'application/json',
                    'Authorization': f"Bearer {api_key}"
                }

                response = requests.post(api_url, headers=headers, json=payload, timeout=15)

                if response.status_code == 200:
                    result_text = "Success"
                    result_color = "green"
                else:
                    result_text = f"Failed (Status {response.status_code})"
                    result_color = "red"
            except Exception as e:
                result_text = f"Error: {str(e)[:30]}"
                result_color = "red"

            # Guard: only update if the editor window still exists
            if hasattr(self, 'winfo_exists') and self.winfo_exists():
                def update_status():
                    if hasattr(self, 'winfo_exists') and self.winfo_exists():
                        status_var.set(result_text)
                        status_label = getattr(self, f'api_status_label_{slot_index+1}', None)
                        if status_label and hasattr(status_label, 'winfo_exists') and status_label.winfo_exists():
                            status_label.config(foreground=result_color)
                        if hasattr(btn, 'winfo_exists') and btn.winfo_exists():
                            btn.config(state=tk.NORMAL)

                self.after(0, update_status)

        threading.Thread(target=run_test, args=(self,), daemon=True).start()

    def save_config_handler(self, silent=False):
        """Save validated settings, returning whether the save completed."""
        try:
            # Validate configuration before saving
            if not self.validate_config_handler(show_success_message=False):
                # Keep the specific validation message so the field can be corrected.
                if not silent:
                    messagebox.showerror("Validation Error", "Cannot save due to validation errors. Please correct them.")
                return False

            # Proceed with saving logic
            config_to_save_to_main_yml = self._get_current_editor_config_data()
            self.master_duplication_enabled_var.set(self.master_duplication_mode_var_editor.get())
            config_to_save_to_main_yml['api']['master_duplication_mode'] = self.master_duplication_mode_var_editor.get()

            # Save API keys to environment variables
            for i in range(5):
                api_url = getattr(self, f'api_url_var_{i+1}').get()
                api_model = getattr(self, f'api_model_var_{i+1}').get()
                api_key = getattr(self, f'api_key_var_{i+1}').get()
                os.environ[f'API_URL_{i+1}'] = api_url
                os.environ[f'MODEL_NAME_{i+1}'] = api_model
                os.environ[f'API_KEY_{i+1}'] = api_key

            # Write to config.yml
            with open(self.global_config.path, 'w', encoding='utf-8') as f:
                yaml.dump(config_to_save_to_main_yml, f, sort_keys=False, indent=2, default_flow_style=False)

            self.global_config.load()

            # Apply rate limits
            for i in range(5):
                rpm = int(getattr(self, f'api_rate_limit_var_{i+1}').get())
                global_rate_limiter.set_rate_limit(i, rpm)

            # Update dashboard safely
            if hasattr(self.master, 'update_dashboard_safe') and callable(self.master.update_dashboard_safe):
                self.master.update_dashboard_safe()
            elif self.on_config_saved:
                self.on_config_saved()

            # Update status only if widget exists
            if hasattr(self.status, 'winfo_exists') and self.status.winfo_exists():
                self.status.config(text="Configuration saved to config.yml!", foreground="green")
            if not silent:
                messagebox.showinfo("Success", "Configuration saved to config.yml!")
            log_message("Configuration saved successfully from editor to main config.", "INFO")
            return True

        except ValueError as e_val:
            # Handle validation errors safely
            if hasattr(self.status, 'winfo_exists') and self.status.winfo_exists():
                self.status.config(text=f"Save failed: Invalid data. {str(e_val)}", foreground="red")
            if not silent:
                messagebox.showerror("Error", f"Save failed: Invalid data. {str(e_val)}")
            log_message(f"Save failed (ValueError): {str(e_val)}", "ERROR")

        except Exception as e_save:
            # Handle other errors safely
            if hasattr(self.status, 'winfo_exists') and self.status.winfo_exists():
                self.status.config(text=f"Save failed: {str(e_save)}", foreground="red")
            if not silent:
                messagebox.showerror("Error", f"Save failed: {str(e_save)}")
            log_message(f"Save failed: {str(e_save)}", "ERROR")
            import traceback
            log_message(traceback.format_exc(), "ERROR")
        return False


    def on_close_editor(self):
        """Handles saving config when editor is closed via 'X' button."""
        log_message("ConfigEditor: Close button clicked. Saving configuration automatically.", "INFO")

        try:
            if not self.save_config_handler(silent=True):
                log_message("ConfigEditor: Save failed; keeping editor open to preserve unsaved changes.", "WARNING")
                return
        except Exception as e:
            log_message(f"ConfigEditor: Error during auto-save on close: {e}", "ERROR")
            return
        self.destroy()

    def _populate_profile_list(self):
        """Refreshes the list of available profiles in the combobox."""
        # Check if widget exists before accessing
        if not hasattr(self, 'profile_combobox') or not hasattr(self.profile_combobox, 'winfo_exists') or not self.profile_combobox.winfo_exists():
            log_message("ConfigEditor: profile_combobox not available, skipping populate.", "DEBUG")
            return

        try:
            profiles = self.global_config.list_profiles()
            self.profile_combobox['values'] = profiles
            if profiles:
                self.profile_list_var.set(profiles[0])  # Default to first profile if list is not empty
            else:
                self.profile_list_var.set("")  # Clear selection if no profiles
            log_message("ConfigEditor: Profile list updated.", "DEBUG")
        except Exception as e:
            log_message(f"ConfigEditor: Error populating profile list: {e}", "ERROR")

    # --- API Compatibility profile helpers ---
    def _profile_name_to_label(self, name):
        """Map a stored profile name (e.g. 'mistral') to its dropdown label, defaulting sanely."""
        for n, lbl in self._api_profile_choices:
            if n == name:
                return lbl
        for n, lbl in self._api_profile_choices:
            if n == api_profiles.DEFAULT_PROFILE:
                return lbl
        return name

    def _profile_label_to_name(self, label):
        """Inverse of _profile_name_to_label; falls back to the default profile."""
        for n, lbl in self._api_profile_choices:
            if lbl == label:
                return n
        return api_profiles.DEFAULT_PROFILE

    def _on_api_profile_change(self, slot_index):
        """Enable the custom-params entry only for the 'User Defined' profile."""
        name = self._profile_label_to_name(getattr(self, f'api_profile_var_{slot_index+1}').get())
        entry = getattr(self, f'api_custom_params_entry_{slot_index+1}', None)
        if entry is not None:
            entry.configure(state="normal" if name == "custom" else "disabled")

    def _detect_api_profile(self, slot_index):
        """Guess the compatibility profile for a slot from its API URL host."""
        url = getattr(self, f'api_url_var_{slot_index+1}').get().strip()
        detected = api_profiles.detect_profile(url)
        status_var = getattr(self, f'api_status_var_{slot_index+1}', None)
        if detected:
            getattr(self, f'api_profile_var_{slot_index+1}').set(self._profile_name_to_label(detected))
            self._on_api_profile_change(slot_index)
            if status_var is not None:
                status_var.set(f"Compatibility profile set to: {detected}")
        elif status_var is not None:
            status_var.set("No known profile matches that URL. Leave as-is, or pick 'User Defined'.")

    def _get_current_editor_config_data(self):
        """Gathers all configuration data currently entered in the editor fields into a dictionary suitable for YAML."""
        self._sync_character_text_values()
        self.on_gender_change_editor_handler(save_current=True)

        apis_list_to_save = []
        for i in range(6): # For all 6 API slots
            api_entry = {
                'url': sanitize_input(getattr(self, f'api_url_var_{i+1}').get()),
                'model': sanitize_input(getattr(self, f'api_model_var_{i+1}').get()),
                'key': getattr(self, f'api_key_var_{i+1}').get() # Store key directly in config
            }
            if i < 4: # APIs 1-4 have 'enabled' field in YML
                api_entry['enabled'] = getattr(self, f'api_enabled_var_{i+1}').get()

            # Add threads setting for all API slots (1-5)
            if hasattr(self, f'api_threads_var_{i+1}'):
                api_entry['threads'] = int(getattr(self, f'api_threads_var_{i+1}').get())

            # NEW: Add rate limit setting for all API slots (1-5)
            if hasattr(self, f'api_rate_limit_var_{i+1}'):
                api_entry['rate_limit_rpm'] = int(getattr(self, f'api_rate_limit_var_{i+1}').get())

            # API compatibility profile (payload param filtering) for this slot
            if hasattr(self, f'api_profile_var_{i+1}'):
                api_entry['api_profile'] = self._profile_label_to_name(getattr(self, f'api_profile_var_{i+1}').get())
                custom_raw = getattr(self, f'api_custom_params_var_{i+1}').get()
                custom_list = [p.strip() for p in custom_raw.replace(',', ' ').split() if p.strip()]
                if custom_list:
                    api_entry['custom_allowed_params'] = custom_list

            apis_list_to_save.append(api_entry)

        slop_fixer_params_to_save = {}
        if self.slop_fixer_temp_var.get(): slop_fixer_params_to_save['temperature'] = float(self.slop_fixer_temp_var.get())
        if self.slop_fixer_top_p_var.get(): slop_fixer_params_to_save['top_p'] = float(self.slop_fixer_top_p_var.get())
        if self.slop_fixer_min_p_var.get(): slop_fixer_params_to_save['min_p'] = float(self.slop_fixer_min_p_var.get())
        if self.slop_fixer_max_tokens_var.get(): slop_fixer_params_to_save['max_tokens'] = int(self.slop_fixer_max_tokens_var.get())
        if self.slop_fixer_top_k_var.get(): slop_fixer_params_to_save['top_k'] = int(self.slop_fixer_top_k_var.get())
        if self.slop_fixer_repetition_penalty_var.get(): slop_fixer_params_to_save['repetition_penalty'] = float(self.slop_fixer_repetition_penalty_var.get())
        # --- NEW: Anti-Slop Params Save Logic ---
        anti_slop_fixer_params_to_save = {}
        if self.anti_slop_fixer_temp_var.get(): anti_slop_fixer_params_to_save['temperature'] = float(self.anti_slop_fixer_temp_var.get())
        if self.anti_slop_fixer_top_p_var.get(): anti_slop_fixer_params_to_save['top_p'] = float(self.anti_slop_fixer_top_p_var.get())
        if self.anti_slop_fixer_min_p_var.get(): anti_slop_fixer_params_to_save['min_p'] = float(self.anti_slop_fixer_min_p_var.get())
        if self.anti_slop_fixer_max_tokens_var.get(): anti_slop_fixer_params_to_save['max_tokens'] = int(self.anti_slop_fixer_max_tokens_var.get())
        if self.anti_slop_fixer_top_k_var.get(): anti_slop_fixer_params_to_save['top_k'] = int(self.anti_slop_fixer_top_k_var.get())
        if self.anti_slop_fixer_repetition_penalty_var.get(): anti_slop_fixer_params_to_save['repetition_penalty'] = float(self.anti_slop_fixer_repetition_penalty_var.get())

        config_data = {
            'api': {
                'master_duplication_mode': self.master_duplication_mode_var_editor.get(), # Use editor's var
                'apis': apis_list_to_save,
                'threads': int(self.num_threads_var_editor.get()),
                # NEW: Add Pricing Section
                'pricing': {
                    'cost_per_1k_tokens': float(self.pricing_var.get()),
                    'budget_limit': float(self.budget_limit_var.get() or 0.0)
                }
            },
            'valkey': {
                'host': sanitize_input(self.valkey_host_var.get() or 'localhost'),
                'port': int(self.valkey_port_var.get() or 6379),
                'db': int(self.valkey_db_var.get() or 0),
                'password': self.valkey_password_var.get() if self.valkey_password_var.get() else None,
                'enabled': self.valkey_enabled_var.get()
            },
            'database': {
                'enabled': self.db_enabled_var.get(),
                'host': sanitize_input(self.db_host_var.get() or 'localhost'),
                'port': int(self.db_port_var.get() or 5432),
                'dbname': sanitize_input(self.db_dbname_var.get()),
                'user': sanitize_input(self.db_user_var.get()),
                'password': self.db_password_var.get(),
                'pool_size': int(self.db_pool_size_var.get() or 10)
            },
            'generation': {
                'max_character_cards': int(self.max_character_cards_var.get()),
                'subject_size': int(self.subject_size_var.get()), 'context_size': int(self.context_size_var.get()),
                'max_attempts': int(self.max_attempts_var.get()),
                'num_turns': int(self.num_turns_var.get()),
                'placeholder_share': float(self.placeholder_share_var.get() or 0),
                'history_size': int(self.history_size_var.get()),
                'num_random_chunks': int(self.num_random_chunks_var.get()),
                'sanitize_input_max_length': int(self.sanitize_input_max_length_var.get()),
                'remove_reasoning': self.remove_reasoning_var_editor.get(),
                'remove_em_dash': self.remove_em_dash_var_editor.get(),
                'remove_asterisks': self.remove_asterisks_var_editor.get(),
                'remove_asterisk_space_asterisk': self.remove_asterisk_space_asterisk_var_editor.get(),
                'remove_all_asterisks': self.remove_all_asterisks_var_editor.get(),
                'ensure_space_after_line_break': self.ensure_space_after_line_break_var_editor.get(),
                'remove_markdown': self.remove_markdown_var_editor.get(),
                'max_slop_sentence_fix_iterations': int(self.max_slop_sentence_fix_iterations_var.get()),
                'output_format': self.output_format_var.get(),
                'api_request_timeout': int(self.api_request_timeout_var.get()),
                'max_newlines_malformed': int(self.max_newlines_malformed_var.get()),
                'max_text_length_malformed': int(self.max_text_length_malformed_var.get()),
                'slop_to_anti_slop_fallback': self.slop_to_anti_slop_fallback_var_editor.get(),
                'discard_refused_conversations': self.discard_refused_conversations_var_editor.get(),
                'use_conversation_history_for_fixers': self.use_conversation_history_for_fixers_var_editor.get(),
                'include_metadata': self.include_metadata_var_editor.get()
            },
            'quality': {
                'enabled': self.quality_enabled_var_editor.get(),
                'use_llm_scoring': self.quality_use_llm_var_editor.get(),
                'output_filter': self.quality_output_filter_var_editor.get(),
                'roleplay_dimensions': self.quality_roleplay_dims_var_editor.get(),
                'per_turn_logic_check': self.quality_logic_check_var_editor.get(),
                'logic_threshold': int(self.quality_logic_threshold_var.get() or 70),
                'logic_max_rewrites': max(0, int(self.quality_logic_max_rewrites_var.get() or 1)),
                'logic_max_chars_for_check': max(100, int(self.quality_logic_max_chars_var.get() or 4000)),
                'min_score_threshold': int(self.quality_min_threshold_var.get() or 50),
                'max_chars_for_scoring': int(self.quality_max_chars_var.get() or 8000),
                'judge_max_attempts': max(1, min(10, int(self.quality_judge_max_attempts_var.get() or 3))),
                'judge_retry_delay': max(0.0, float(self.quality_judge_retry_delay_var.get() or 0)),
                'result_timeout': max(30, int(self.quality_result_timeout_var.get() or 600)),
                'scoring_api': {
                    'url': sanitize_input(self.quality_api_url_var.get()),
                    'model': sanitize_input(self.quality_api_model_var.get()),
                    'key': self.quality_api_key_var.get(),
                },
            },
            'prompts': {
                'system': {
                    'base': sanitize_input(self.system_base_prompt_text.get("1.0", tk.END).strip()),
                    'variable': self.use_variable_system_var_editor.get(),
                    'variations': [sanitize_input(line.strip()) for line in self.system_variations_text.get("1.0", tk.END).split('\n') if line.strip()],
                    'top_level_system_prompt': sanitize_input(self.top_level_system_prompt_text.get("1.0", tk.END).strip()),
                },
                'question': sanitize_input(self.question_prompt_text.get("1.0", tk.END).strip()),
                'answer': sanitize_input(self.answer_prompt_text.get("1.0", tk.END).strip()),
                'user_continuation_prompt': sanitize_input(self.user_continuation_prompt_text.get("1.0", tk.END).strip()),
                'use_questions_file': self.use_questions_file_var_editor.get(),
                # Opening-mode prompts are saved as typed: quotes and markup are legitimate in roleplay prompts
                'opening_mode': self.opening_mode_var_editor.get(),
                'greeting': self.greeting_prompt_text.get("1.0", "end-1c"),
                'greeting_context': self.greeting_context_text.get("1.0", "end-1c"),
                'first_line': self.first_line_prompt_text.get("1.0", "end-1c"),
                'first_line_styles': [line.strip() for line in self.first_line_styles_text.get("1.0", "end-1c").split('\n') if line.strip()],
                'lore': sanitize_input(self.lore_text.get("1.0", tk.END).strip()),
            'character': {
                'enabled': self.enable_character_engine_var_editor.get(),
                'include_names_in_prompt': self.include_names_in_prompt_var_editor.get(),
                'class_enabled': self.enable_class_selection_var_editor.get(),
                'setting_enabled': self.enable_setting_selection_var_editor.get(),
                'num_characters': int(self.num_characters_var_editor.get()),
                'persona_breakthrough_turn': int(self.persona_breakthrough_turn_var_editor.get()),
                'persona_breakthrough_placement': self.persona_placement_var_editor.get(),
                # Profile fields are saved as typed, like the opening-mode prompts: roleplay text needs its quotes and apostrophes
                'characters': [
                    {
                        'name': data['name'].get().strip(),
                        'age': data['age'].get().strip(),
                        'gender': data['gender'].get().strip(),
                        'race': data['race'].get().strip(),
                        'job': data['job'].get().strip(),
                        'eye_color': data['eye_color'].get().strip(),
                        'hair': data['hair'].get().strip(),
                        'hair_color': data['hair_color'].get().strip(),
                        'clothing': data['clothing'].get().strip(),
                        'jewelry': data['jewelry'].get().strip(),
                        'appearance': data['appearance'].get().strip(),
                        'traits': data['traits'].get().strip(),
                        'backstory': data['backstory'].get().strip(),
                        'persona': data['persona'].get().strip(),
                        'personality': data['personality'].get().strip(),
                        'setting': data['setting'].get().strip() if self.enable_setting_selection_var_editor.get() else '',
                        'class': data['class'].get().strip(),
                        'greeting': data['greeting'].get().strip(),
                    }
                        for data in self.character_entries
                        if any(data[k].get().strip() for k in ['name', 'race', 'job', 'eye_color', 'hair', 'hair_color', 'clothing', 'jewelry', 'appearance', 'backstory', 'persona', 'personality', 'traits', 'setting', 'class', 'age', 'gender', 'greeting'])
                    ]
                },
                'emotional_states': {
                    'enabled': self.enable_emotional_states_var_editor.get(),
                    # Saved as typed: an arc separates its stages with '->'
                    'states': [line.strip() for line in self.emotional_states_text.get("1.0", tk.END).split('\n') if line.strip()],
                    'placement': self.emotion_placement_var_editor.get(),
                    'per_character': self.per_char_random_emotions_var_editor.get(),
                },
                'user_emotional_states': {
                    'enabled': self.enable_user_emotional_states_var_editor.get(),
                    # Saved as typed: an arc separates its stages with '->'
                    'states': [line.strip() for line in self.user_emotional_states_text.get("1.0", tk.END).split('\n') if line.strip()],
                },
                'random_names': {
                    'enabled': self.enable_random_names_var_editor.get(),
                    'names': [sanitize_input(line.strip()) for line in self.random_names_text.get("1.0", tk.END).split('\n') if line.strip()]
                },
                'random_settings': {
                    'enabled': self.enable_random_settings_var_editor.get(),
                    'settings': [sanitize_input(line.strip()) for line in self.random_settings_text.get("1.0", tk.END).split('\n') if line.strip()]
                },
            },
            'detection': {
                'no_user_impersonation': self.no_user_impersonation_var.get(), # Get from global Tkinter var
                'out_of_character': {
                    'enabled': bool(self.ooc_enabled_var_editor.get()),
                    'phrases': [sanitize_input(line.strip()) for line in self.ooc_phrases_text.get("1.0", tk.END).split('\n') if line.strip()],
                    'fixes': [line.strip() for line in self.ooc_fixes_text.get("1.0", tk.END).split('\n') if line.strip()]
                },
                'refusal': {
                    'phrases': [sanitize_input(line.strip()) for line in self.refusal_phrases_text.get("1.0", tk.END).split('\n') if line.strip()],
                    'fixes': [line.strip() for line in self.refusal_fixes_text.get("1.0", tk.END).split('\n') if line.strip()]
                },
            'anti_slop': {
                'phrases': [sanitize_input(line.strip()) for line in self.anti_slop_phrases_text.get("1.0", tk.END).split('\n') if line.strip()],
                'fixes': [sanitize_input(line.strip()) for line in self.anti_slop_fixes_text.get("1.0", tk.END).split('\n') if line.strip()]
                },
                'user_speaking': { # Save all gender data
                    'male': {'phrases': [sanitize_input(line) for line in self.user_speaking_phrases_data.get('male', [])], 'fixes': [sanitize_input(line) for line in self.user_speaking_fixes_data.get('male', [])]},
                    'female': {'phrases': [sanitize_input(line) for line in self.user_speaking_phrases_data.get('female', [])], 'fixes': [sanitize_input(line) for line in self.user_speaking_fixes_data.get('female', [])]},
                    'neutral': {'phrases': [sanitize_input(line) for line in self.user_speaking_phrases_data.get('neutral', [])], 'fixes': [sanitize_input(line) for line in self.user_speaking_fixes_data.get('neutral', [])]}
                },
                'slop': {
                    'phrases': [sanitize_input(line.strip()) for line in self.slop_phrases_text.get("1.0", tk.END).split('\n') if line.strip()],
                    'fixes': [sanitize_input(line.strip()) for line in self.slop_fixes_text.get("1.0", tk.END).split('\n') if line.strip()]
                }
            },
            'samplers': {
                'priority': [line.strip() for line in self.sampler_priority_text.get("1.0", tk.END).split('\n') if line.strip()],
                'temperature': float(self.temperature_var.get()), 'top_p': float(self.top_p_var.get()),
                'min_p': float(self.min_p_var.get()), 'top_k': int(self.top_k_var.get()),
                'top_k': int(self.top_k_var.get()), 'repetition_penalty': float(self.repetition_penalty_var.get()),
                'max_tokens_question': int(self.max_tokens_question_var.get()),
                'max_tokens_answer': int(self.max_tokens_answer_var.get()),
                'max_tokens_user_reply': int(self.max_tokens_user_reply_var.get()),
                'enable_thinking': self.enable_thinking_mode_var.get(),
                'logit_bias': self.logit_bias_text.get("1.0", tk.END).strip(),
                # v11.1 bad_words: preserve leading whitespace on every line exactly as typed
                # (only skip truly-empty lines after stripping the trailing newline); the
                # toggle gates whether the list is actually sent, not whether it is stored.
                'use_bad_words': bool(self.use_bad_words_var_editor.get()),
                'bad_words': [
                    line.rstrip('\r\n')
                    for line in self.bad_words_text.get("1.0", tk.END).split('\n')
                    if line.strip()
                ],
                'slop_fixer_params': slop_fixer_params_to_save,
                'anti_slop_params': anti_slop_fixer_params_to_save
            },
            'gender': self.gender_var_editor.get() # Save the currently selected gender for persona
        }
        # Preserve pricing settings from the loaded config if they exist
        if 'pricing' in self.global_config.config:
            config_data['pricing'] = self.global_config.config['pricing']
        config_data['datasets'] = self.datasets_panel.get_config()
        return config_data

    def _save_profile_as_handler(self):
        """Handles saving the current editor configuration as a new named profile."""
        profile_name = self.new_profile_name_var.get().strip()
        if not profile_name:
            messagebox.showerror("Error", "Profile name cannot be empty.")
            self.status.config(text="Profile name empty.", foreground="red")
            return

        if not self.validate_config_handler(show_success_message=False):
            messagebox.showerror("Validation Error", "Cannot save profile due to validation errors. Please correct them.")
            return

        safe_profile_name_check = "".join(c for c in profile_name if c.isalnum() or c in (' ', '_', '-')).rstrip()
        profile_path_check = os.path.join(self.global_config.profiles_dir, f"{safe_profile_name_check}.yml")
        if os.path.exists(profile_path_check): 
            if not messagebox.askyesno("Overwrite Profile", f"Profile '{safe_profile_name_check}' already exists. Overwrite?"):
                self.status.config(text="Save profile cancelled.", foreground="orange")
                return
        
        try:
            config_to_save_for_profile = self._get_current_editor_config_data() 
            success, msg = self.global_config.save_profile(profile_name, config_to_save_for_profile) 
            if success:
                self._populate_profile_list() # Refresh profile list
                self.new_profile_name_var.set("") # Clear input field
                messagebox.showinfo("Success", msg)
                self.status.config(text=msg, foreground="green")
            else:
                messagebox.showerror("Error", msg)
                self.status.config(text=msg, foreground="red")
        except ValueError as e_val: # Catch type conversion errors from _get_current_editor_config_data
            messagebox.showerror("Error", f"Save profile failed: Invalid data. {str(e_val)}")
            self.status.config(text=f"Save profile failed: Invalid data. {str(e_val)}", foreground="red")
        except Exception as e_save: # Catch other errors during save
            messagebox.showerror("Error", f"Save profile failed: {str(e_save)}")
            self.status.config(text=f"Save profile failed: {str(e_save)}", foreground="red")

    def _load_selected_profile_handler(self):
        """Handles loading a selected profile into the main config.yml and then reloading the editor."""
        selected_profile = self.profile_list_var.get()
        if not selected_profile:
            messagebox.showwarning("No Profile Selected", "Please select a profile to load.")
            self.status.config(text="No profile selected to load.", foreground="orange")
            return

        if not messagebox.askyesno("Load Profile", f"Load profile '{selected_profile}'?\nThis will overwrite your current config.yml and update the editor.\nUnsaved changes in the editor will be lost."):
            self.status.config(text="Load profile cancelled.", foreground="orange")
            return

        success, msg = self.global_config.load_profile_to_main_config(selected_profile) 
        if success:
            self.load_config_handler() # Reload editor UI from the now-updated main config
            messagebox.showinfo("Success", msg)
            self.status.config(text=msg, foreground="green")
            self.master_duplication_enabled_var.set(self.global_config.get('api.master_duplication_mode', False))
            self.master_duplication_mode_var_editor.set(self.master_duplication_enabled_var.get()) # Sync editor's checkbox
            if hasattr(self.master, 'update_dashboard_safe') and callable(self.master.update_dashboard_safe):
                self.master.update_dashboard_safe()
        else:
            messagebox.showerror("Error", msg)
            self.status.config(text=msg, foreground="red")

    def _toggle_ooc_fields(self):
        """Enables/disables OOC detection text fields based on checkbox state."""
        is_enabled = self.ooc_enabled_var_editor.get()
        for field_name in ('ooc_phrases_text', 'ooc_fixes_text'):
            field_widget = getattr(self, field_name, None)
            if field_widget is not None and field_widget.winfo_exists():
                field_widget.configure(state='normal' if is_enabled else 'disabled')

    def _add_placement_combobox(self, row, label_text, variable):
        """A 'depth0' / 'system' choice for one per-reply block; returns the next free row."""
        frame = self.character_engine_content_frame
        ttk.Label(frame, text=label_text).grid(row=row, column=0, padx=SPACING, pady=(SPACING, 0), sticky="w")
        row += 1
        combo = ttk.Combobox(frame, textvariable=variable, values=list(NOTE_PLACEMENTS), state="readonly", width=10)
        combo.grid(row=row, column=0, padx=SPACING, pady=0, sticky="w")
        combo.bind("<<ComboboxSelected>>", lambda _event: self._refresh_placement_cache_warning())
        row += 1
        help_label = ttk.Label(
            frame, wraplength=1,
            text="(depth0 = end of the last user message, after the history: the server reuses its prefix cache; "
                 "system = end of the system prompt, as in v10: the history is computed again whenever the block changes)")
        help_label.grid(row=row, column=0, padx=SPACING, pady=(0, SPACING), sticky="ew")
        wrap_label(help_label, frame, padding=2 * SPACING)
        return row + 1

    def _refresh_placement_cache_warning(self):
        """Show placement_cache_warning for the values in the editor (cleared when they do not parse)."""
        try:
            _share, warning = placement_cache_warning(
                int(self.num_turns_var.get()), self.opening_mode_var_editor.get(),
                int(self.persona_breakthrough_turn_var_editor.get()),
                self.enable_character_engine_var_editor.get()
                and any(data['persona'].get().strip() for data in self.character_entries),
                self.enable_emotional_states_var_editor.get(),
                [line.strip() for line in self.emotional_states_text.get("1.0", tk.END).split('\n') if line.strip()],
                self.persona_placement_var_editor.get(), self.emotion_placement_var_editor.get())
        except (ValueError, tk.TclError):
            warning = ""
        self.placement_cache_warning_label.config(text=warning)

    def _toggle_bad_words_fields(self):
        """v11.1: enable/disable the bad-words text field based on the toggle's state."""
        is_enabled = self.use_bad_words_var_editor.get()
        field_widget = getattr(self, 'bad_words_text')
        field_widget.config(state='normal' if is_enabled else 'disabled')
        log_message(f"Bad Words fields {'enabled' if is_enabled else 'disabled'}", "DEBUG")

    def _toggle_emotional_states_fields(self):
        """Enables/disables emotional states text field based on checkbox state."""
        is_enabled = self.enable_emotional_states_var_editor.get()

        emotional_states_fields = [
            'emotional_states_text'
        ]

        for field_name in emotional_states_fields:
            field_widget = getattr(self, field_name)
            if is_enabled:
                field_widget.config(state='normal')
            else:
                field_widget.config(state='disabled')

        log_message(f"Emotional States fields {'enabled' if is_enabled else 'disabled'}", "DEBUG")
        self._refresh_placement_cache_warning()

    def _toggle_user_emotional_states_fields(self):
        """Enables/disables the user-side emotional states text field (v10.6.0)."""
        is_enabled = self.enable_user_emotional_states_var_editor.get()
        field_widget = getattr(self, 'user_emotional_states_text')
        field_widget.config(state='normal' if is_enabled else 'disabled')
        log_message(f"User Emotional States fields {'enabled' if is_enabled else 'disabled'}", "DEBUG")

    def _toggle_random_names_fields(self):
        """Enables/disables random names text field based on checkbox state."""
        is_enabled = self.enable_random_names_var_editor.get()

        random_names_fields = [
            'random_names_text'
        ]

        for field_name in random_names_fields:
            field_widget = getattr(self, field_name)
            if is_enabled:
                field_widget.config(state='normal')
            else:
                field_widget.config(state='disabled')

        log_message(f"Random Names fields {'enabled' if is_enabled else 'disabled'}", "DEBUG")

    def _toggle_random_settings_fields(self):
        """Enables/disables random settings text field based on checkbox state."""
        is_enabled = self.enable_random_settings_var_editor.get()

        random_settings_fields = [
            'random_settings_text'
        ]

        for field_name in random_settings_fields:
            field_widget = getattr(self, field_name)
            if is_enabled:
                field_widget.config(state='normal')
            else:
                field_widget.config(state='disabled')

        log_message(f"Random Settings fields {'enabled' if is_enabled else 'disabled'}", "DEBUG")

    def _toggle_class_fields(self):
        """Enables/disables class selection text field based on checkbox state."""
        is_enabled = self.enable_class_selection_var_editor.get()

        class_fields = [
            'character_class_text'
        ]

        for field_name in class_fields:
            field_widget = getattr(self, field_name)
            if is_enabled:
                field_widget.config(state='normal')
            else:
                field_widget.config(state='disabled')

        log_message(f"Class Selection fields {'enabled' if is_enabled else 'disabled'}", "DEBUG")

    def _toggle_setting_fields(self):
        """Enables/disables setting text field based on checkbox state."""
        # Simply toggle visibility, as the main engine toggle handles enable/disable state
        self._update_setting_column_visibility()
        log_message(f"Setting Selection fields visibility updated", "DEBUG")

    def _update_setting_column_visibility(self):
        """Shows or hides the 'Setting' fields based on the Enable Setting checkbox."""
        is_enabled = self.enable_setting_selection_var_editor.get()
        for data in self.character_entries:
            if is_enabled:
                if 'setting_label' in data:
                    data['setting_label'].grid()
                if 'setting_widget' in data:
                    data['setting_widget'].grid()
            else:
                if 'setting_label' in data:
                    data['setting_label'].grid_remove()
                if 'setting_widget' in data:
                    data['setting_widget'].grid_remove()

        log_message(f"Setting Selection fields visibility updated", "DEBUG")

    def _toggle_character_engine_fields(self):
        """Enables/disables character engine text fields based on checkbox state."""
        is_enabled = self.enable_character_engine_var_editor.get()

        character_engine_fields = [
            'character_name_text',
            'character_race_text',
            'character_job_text',
            'character_eye_color_text',
            'character_clothing_text',
            'character_appearance_text',
            'character_backstory_text',
            'character_personality_text',
            'character_setting_text'
        ]

        for field_name in character_engine_fields:
            field_widget = getattr(self, field_name)
            if is_enabled:
                field_widget.config(state='normal')
            else:
                field_widget.config(state='disabled')

        log_message(f"Character Engine fields {'enabled' if is_enabled else 'disabled'}", "DEBUG")

    def _delete_selected_profile_handler(self):
        """Handles deleting a selected profile file."""
        selected_profile = self.profile_list_var.get()
        if not selected_profile:
            messagebox.showwarning("No Profile Selected", "Please select a profile to delete.")
            self.status.config(text="No profile selected to delete.", foreground="orange")
            return

        if messagebox.askyesno("Delete Profile", f"Are you sure you want to delete profile '{selected_profile}'? This cannot be undone."):
            success, msg = self.global_config.delete_profile(selected_profile) 
            if success:
                self._populate_profile_list() # Refresh profile list
                messagebox.showinfo("Success", msg)
                self.status.config(text=msg, foreground="green")
            else:
                messagebox.showerror("Error", msg)
                self.status.config(text=msg, foreground="red")
        else:
            self.status.config(text="Delete profile cancelled.", foreground="orange")

    def _load_gender_specific_texts_into_ui(self, gender_to_display):
        """Loads the stored phrase/fix data for the given gender into the UI text boxes."""
        self.user_speaking_phrases_text.delete(1.0, tk.END)
        self.user_speaking_phrases_text.insert(tk.END, "\n".join(self.user_speaking_phrases_data.get(gender_to_display, [])))
        self.user_speaking_fixes_text.delete(1.0, tk.END)
        self.user_speaking_fixes_text.insert(tk.END, "\n".join(self.user_speaking_fixes_data.get(gender_to_display, [])))
        self.active_display_gender = gender_to_display # Update which gender's data is currently shown

    def on_gender_change_editor_handler(self, save_current=True):
        """
        Called when gender radio button changes or when needing to sync UI to internal store.
        """
        if save_current and hasattr(self, 'active_display_gender') and self.active_display_gender:
            self.user_speaking_phrases_data[self.active_display_gender] = \
                [line.strip() for line in self.user_speaking_phrases_text.get("1.0", tk.END).split('\n') if line.strip()]
            self.user_speaking_fixes_data[self.active_display_gender] = \
                [line.strip() for line in self.user_speaking_fixes_text.get("1.0", tk.END).split('\n') if line.strip()]

        newly_selected_gender = self.gender_var_editor.get() # Get the newly selected gender from radio button
        self._load_gender_specific_texts_into_ui(newly_selected_gender) # Load its data into UI
        if save_current: # Log only if it was an interactive change
            log_message(f"ConfigEditor: Switched display to '{newly_selected_gender}' user speaking data.", "DEBUG")

    def load_config_handler(self):
        """Loads data from self.global_config (config.yml) into the editor's UI fields."""
        try:
            config = self.global_config.config # Get current config data
            self.datasets_panel.load_config(config.get('datasets', {}))

            api_config_main = config.get('api', {})
            # NEW: Load pricing value
            pricing_config = api_config_main.get('pricing', {})
            self.pricing_var.set(str(pricing_config.get('cost_per_1k_tokens', 0.0)))
            self.budget_limit_var.set(str(pricing_config.get('budget_limit', 0.0)))

            # NEW: Load valkey configuration
            valkey_config = config.get('valkey', {})
            self.valkey_enabled_var.set(valkey_config.get('enabled', True))
            self.valkey_host_var.set(valkey_config.get('host', 'localhost'))
            self.valkey_port_var.set(str(valkey_config.get('port', 6379)))
            self.valkey_db_var.set(str(valkey_config.get('db', 0)))
            self.valkey_password_var.set(valkey_config.get('password') or '')

            db_config = config.get('database', {})
            self.db_enabled_var.set(db_config.get('enabled', False))
            self.db_host_var.set(db_config.get('host', 'localhost'))
            self.db_port_var.set(str(db_config.get('port', 5432)))
            self.db_dbname_var.set(db_config.get('dbname', ''))
            self.db_user_var.set(db_config.get('user', ''))
            self.db_password_var.set(db_config.get('password', ''))
            self.db_pool_size_var.set(str(db_config.get('pool_size', 10)))

            self.master_duplication_mode_var_editor.set(self.master_duplication_enabled_var.get())

            apis_conf_from_yml = api_config_main.get('apis', [])
            for i in range(6): # Load data for all 6 API slots
                api_details_yml = apis_conf_from_yml[i] if i < len(apis_conf_from_yml) and isinstance(apis_conf_from_yml[i], dict) else {}
                getattr(self, f'api_url_var_{i+1}').set(os.getenv(f'API_URL_{i+1}', api_details_yml.get('url', '')))
                getattr(self, f'api_model_var_{i+1}').set(os.getenv(f'MODEL_NAME_{i+1}', api_details_yml.get('model', '')))
                getattr(self, f'api_key_var_{i+1}').set(os.getenv(f'API_KEY_{i+1}', api_details_yml.get('key', '')))
                
                if i < 4: # APIs 1-4 have 'enabled' field
                    default_enabled = (i == 0) # API 1 defaults to enabled if not specified
                    getattr(self, f'api_enabled_var_{i+1}').set(api_details_yml.get('enabled', default_enabled))
                
                # Load threads setting for all API slots (1-5)
                if hasattr(self, f'api_threads_var_{i+1}'):
                    getattr(self, f'api_threads_var_{i+1}').set(str(api_details_yml.get('threads', 10)))

                # NEW: Load rate limit setting for all API slots (1-5)
                if hasattr(self, f'api_rate_limit_var_{i+1}'):
                    getattr(self, f'api_rate_limit_var_{i+1}').set(str(api_details_yml.get('rate_limit_rpm', 60)))

                # Load API compatibility profile for this slot
                if hasattr(self, f'api_profile_var_{i+1}'):
                    prof_name = api_details_yml.get('api_profile', api_profiles.DEFAULT_PROFILE)
                    getattr(self, f'api_profile_var_{i+1}').set(self._profile_name_to_label(prof_name))
                    custom_list = api_details_yml.get('custom_allowed_params', []) or []
                    getattr(self, f'api_custom_params_var_{i+1}').set(', '.join(str(p) for p in custom_list))
                    self._on_api_profile_change(i)

            gen_config = config.get('generation', {})
            if hasattr(self, 'max_character_cards_var'):
                self.max_character_cards_var.set(str(gen_config.get('max_character_cards', 10)))
            self.subject_size_var.set(str(gen_config.get('subject_size', 1000)))
            self.context_size_var.set(str(gen_config.get('context_size', 3000)))
            self.num_random_chunks_var.set(str(gen_config.get('num_random_chunks', 12000)))
            self.sanitize_input_max_length_var.set(str(gen_config.get('sanitize_input_max_length', 100000000)))
            samplers_config_load = config.get('samplers', {})
            # Handle legacy boolean vs new string format
            legacy_val = samplers_config_load.get('enable_thinking', False)
            if isinstance(legacy_val, bool):
                # Legacy True meant "Send Disable Command", Legacy False meant "Don't Send"
                new_val = "disable" if legacy_val else "default"
            else:
                new_val = legacy_val if legacy_val in ["default", "enable", "disable"] else "default"
            self.enable_thinking_mode_var.set(new_val)
            self.max_attempts_var.set(str(gen_config.get('max_attempts', samplers_config_load.get('max_attempts',5))))
            self.num_turns_var.set(str(gen_config.get('num_turns', 1)))
            self.placeholder_share_var.set(str(gen_config.get('placeholder_share', 0.0)))
            self.history_size_var.set(str(gen_config.get('history_size', samplers_config_load.get('history_size',10))))
            self.api_request_timeout_var.set(str(gen_config.get('api_request_timeout', 300)))
            self.remove_reasoning_var_editor.set(gen_config.get('remove_reasoning', False))
            self.max_newlines_malformed_var.set(str(gen_config.get('max_newlines_malformed', 16)))
            self.max_text_length_malformed_var.set(str(gen_config.get('max_text_length_malformed', 5000)))
            self.remove_em_dash_var_editor.set(gen_config.get('remove_em_dash', False))
            self.remove_asterisks_var_editor.set(gen_config.get('remove_asterisks', False))
            self.remove_asterisk_space_asterisk_var_editor.set(gen_config.get('remove_asterisk_space_asterisk', False))
            self.remove_all_asterisks_var_editor.set(gen_config.get('remove_all_asterisks', False))
            self.remove_markdown_var_editor.set(gen_config.get('remove_markdown', False))
            self.ensure_space_after_line_break_var_editor.set(gen_config.get('ensure_space_after_line_break', False))
            self.max_slop_sentence_fix_iterations_var.set(str(gen_config.get('max_slop_sentence_fix_iterations', 5)))
            self.slop_to_anti_slop_fallback_var_editor.set(gen_config.get('slop_to_anti_slop_fallback', False))
            self.discard_refused_conversations_var_editor.set(gen_config.get('discard_refused_conversations', False))
            self.use_conversation_history_for_fixers_var_editor.set(gen_config.get('use_conversation_history_for_fixers', True))
            self.include_metadata_var_editor.set(gen_config.get('include_metadata', True))
            # Load quality scoring config
            quality_conf = config.get('quality', {})
            self.quality_enabled_var_editor.set(quality_conf.get('enabled', True))
            self.quality_use_llm_var_editor.set(quality_conf.get('use_llm_scoring', False))
            self.quality_output_filter_var_editor.set(quality_conf.get('output_filter', False))
            self.quality_roleplay_dims_var_editor.set(quality_conf.get('roleplay_dimensions', False))
            self.quality_logic_check_var_editor.set(quality_conf.get('per_turn_logic_check', False))
            self.quality_logic_threshold_var.set(str(quality_conf.get('logic_threshold', 70)))
            self.quality_logic_max_rewrites_var.set(str(quality_conf.get('logic_max_rewrites', 1)))
            self.quality_logic_max_chars_var.set(str(quality_conf.get('logic_max_chars_for_check', 4000)))
            self.quality_min_threshold_var.set(str(quality_conf.get('min_score_threshold', 50)))
            self.quality_max_chars_var.set(str(quality_conf.get('max_chars_for_scoring', 8000)))
            self.quality_judge_max_attempts_var.set(str(quality_conf.get('judge_max_attempts', 3)))
            self.quality_judge_retry_delay_var.set(str(quality_conf.get('judge_retry_delay', 1.0)))
            self.quality_result_timeout_var.set(str(quality_conf.get('result_timeout', 600)))
            self.quality_api_url_var.set(quality_conf.get('scoring_api', {}).get('url', ''))
            self.quality_api_model_var.set(quality_conf.get('scoring_api', {}).get('model', ''))
            self.quality_api_key_var.set(quality_conf.get('scoring_api', {}).get('key', ''))
            self.output_format_var.set(gen_config.get('output_format', 'sharegpt'))

            prompts_config = config.get('prompts', {})
            # --- NEW: Load Top Level System Prompt ---
            self.top_level_system_prompt_text.delete(1.0, tk.END)
            self.top_level_system_prompt_text.insert(tk.END, prompts_config.get('system', {}).get('top_level_system_prompt', ''))
            self.use_questions_file_var_editor.set(prompts_config.get('use_questions_file', False))
            system_conf = prompts_config.get('system', {})
            self.use_variable_system_var_editor.set(system_conf.get('variable', False))
            self.system_base_prompt_text.delete(1.0, tk.END); self.system_base_prompt_text.insert(tk.END, system_conf.get('base', 'You are a helpful AI assistant.'))
            self.system_variations_text.delete(1.0, tk.END); self.system_variations_text.insert(tk.END, "\n".join(system_conf.get('variations', [])))
            self.question_prompt_text.delete(1.0, tk.END); self.question_prompt_text.insert(tk.END, prompts_config.get('question', 'Generate a question... {subject} ... {context} ... {recent_questions}'))
            self.answer_prompt_text.delete(1.0, tk.END); self.answer_prompt_text.insert(tk.END, prompts_config.get('answer', 'Answer the question.'))
            self.user_continuation_prompt_text.delete(1.0, tk.END); self.user_continuation_prompt_text.insert(tk.END, prompts_config.get('user_continuation_prompt', 'Continue based on: {last_assistant_message}'))
            self.opening_mode_var_editor.set(prompts_config.get('opening_mode', False))
            self.greeting_prompt_text.delete(1.0, tk.END); self.greeting_prompt_text.insert(tk.END, prompts_config.get('greeting', DEFAULT_GREETING_PROMPT))
            self.greeting_context_text.delete(1.0, tk.END); self.greeting_context_text.insert(tk.END, prompts_config.get('greeting_context', DEFAULT_GREETING_CONTEXT))
            self.first_line_prompt_text.delete(1.0, tk.END); self.first_line_prompt_text.insert(tk.END, prompts_config.get('first_line', DEFAULT_FIRST_LINE_PROMPT))
            styles_conf = prompts_config.get('first_line_styles', list(DEFAULT_FIRST_LINE_STYLES))
            self.first_line_styles_text.delete(1.0, tk.END); self.first_line_styles_text.insert(tk.END, "\n".join(styles_conf if isinstance(styles_conf, list) else [str(styles_conf)]))

            self.lore_text.delete(1.0, tk.END)
            self.lore_text.insert(tk.END, config.get('prompts', {}).get('lore', ''))

            if hasattr(self, 'max_character_cards_var'):
                self.max_character_cards_var.set(str(gen_config.get('max_character_cards', 10)))
            else:
                self.max_character_cards_var = tk.StringVar(value=str(gen_config.get('max_character_cards', 10)))
            character_conf = prompts_config.get('character', {})
            self.enable_character_engine_var_editor.set(character_conf.get('enabled', True))
            self.include_names_in_prompt_var_editor.set(character_conf.get('include_names_in_prompt', True))
            self.enable_class_selection_var_editor.set(character_conf.get('class_enabled', False))
            self.enable_setting_selection_var_editor.set(character_conf.get('setting_enabled', False))
            self.num_characters_var_editor.set(str(character_conf.get('num_characters', 1)))
            try:
                pb_load = max(1, int(character_conf.get('persona_breakthrough_turn', 2)))
            except (TypeError, ValueError):
                pb_load = 2
            self.persona_breakthrough_turn_var_editor.set(str(pb_load))
            self.persona_placement_var_editor.set(
                note_placement(character_conf.get('persona_breakthrough_placement')) or DEFAULT_NOTE_PLACEMENT)

            # Clear existing entries first
            for data in self.character_entries[:]:
                try:
                    for widget in data['frame'].winfo_children():
                        widget.destroy()
                    data['frame'].destroy()
                except:
                    pass
            self.character_entries.clear()

            # Load characters (new format)
            characters_list = character_conf.get('characters', [])

            # Fallback for old config format (separate lists)
            if not characters_list:
                old_names = character_conf.get('name', [])
                old_ages = character_conf.get('age', [])
                old_genders = character_conf.get('gender', [])
                old_races = character_conf.get('race', [])
                old_jobs = character_conf.get('job', [])
                old_eye_colors = character_conf.get('eye_color', [])
                old_hair = character_conf.get('hair', [])
                old_hair_color = character_conf.get('hair_color', [])
                old_clothing = character_conf.get('clothing', [])
                old_jewelry = character_conf.get('jewelry', [])
                old_appearance = character_conf.get('appearance', [])
                old_backstory = character_conf.get('backstory', [])
                old_persona = character_conf.get('persona', [])
                old_personality = character_conf.get('personality', [])
                old_traits = character_conf.get('traits', [])
                old_setting = character_conf.get('setting', [])
                old_class = character_conf.get('class', [])
                old_greeting = character_conf.get('greeting', [])

                if any([old_names, old_races, old_jobs, old_eye_colors, old_hair, old_hair_color, old_clothing, old_jewelry, old_appearance, old_backstory, old_persona, old_personality, old_traits, old_setting, old_class, old_genders]):
                    max_len = max(
                        len(old_names), len(old_races), len(old_jobs), len(old_eye_colors), len(old_hair), len(old_hair_color),
                        len(old_clothing), len(old_jewelry), len(old_appearance), len(old_backstory),
                        len(old_persona), len(old_personality), len(old_traits), len(old_setting), len(old_class), len(old_genders)
                    )
                    for i in range(max_len):
                        characters_list.append({
                            'name': old_names[i] if i < len(old_names) else '',
                            'age': old_ages[i] if i < len(old_ages) else '25',
                            'gender': old_genders[i] if i < len(old_genders) else '',
                            'race': old_races[i] if i < len(old_races) else '',
                            'job': old_jobs[i] if i < len(old_jobs) else '',
                            'eye_color': old_eye_colors[i] if i < len(old_eye_colors) else '',
                            'hair': old_hair[i] if i < len(old_hair) else '',
                            'hair_color': old_hair_color[i] if i < len(old_hair_color) else '',
                            'clothing': old_clothing[i] if i < len(old_clothing) else '',
                            'jewelry': old_jewelry[i] if i < len(old_jewelry) else '',
                            'appearance': old_appearance[i] if i < len(old_appearance) else '',
                            'backstory': old_backstory[i] if i < len(old_backstory) else '',
                            'persona': old_persona[i] if i < len(old_persona) else '',
                            'personality': old_personality[i] if i < len(old_personality) else '',
                            'traits': old_traits[i] if i < len(old_traits) else '',
                            'setting': old_setting[i] if i < len(old_setting) else '',
                            'class': old_class[i] if i < len(old_class) else '',
                            'greeting': old_greeting[i] if i < len(old_greeting) else ''
                        })

            # Construct loaded cards without ever managing the entire list.
            self._cancel_character_scroll()
            self._loading_character_cards = True
            try:
                if characters_list:
                    for i, char_data in enumerate(characters_list):
                        if i >= int(self.max_character_cards_var.get()):
                            log_message(f"Skipping character {i+1} - limit reached", "WARNING")
                            break
                        self._add_character_row(character_data=char_data)
                else:
                    for _ in range(min(3, int(self.max_character_cards_var.get()))):
                        self._add_character_row()
            finally:
                self._loading_character_cards = False
                self._update_character_card_labels()
                self._show_character_page(0, scroll_to_top=False)
                self.character_engine_canvas.yview_moveto(0)

            # Apply the enabled/disabled state and class visibility
            self._toggle_character_engine_fields()
            self._update_class_column_visibility()
            self._update_setting_column_visibility()

            # NEW: Load emotional states
            emotional_states_conf = prompts_config.get('emotional_states', {})
            self.enable_emotional_states_var_editor.set(emotional_states_conf.get('enabled', False))
            self.emotional_states_text.delete(1.0, tk.END)
            self.emotional_states_text.insert(tk.END, "\n".join(emotional_states_conf.get('states', [])))
            self.emotion_placement_var_editor.set(
                note_placement(emotional_states_conf.get('placement')) or DEFAULT_NOTE_PLACEMENT)
            self.per_char_random_emotions_var_editor.set(bool(emotional_states_conf.get('per_character', False)))
            self._toggle_emotional_states_fields()

            # NEW (v10.6.0): Load user-side emotional states/arcs. The widget starts 'disabled'
            # (optional feature, off by default), and this Tk build silently drops text edits on a
            # disabled widget, so the enabled var must be applied and the field re-enabled BEFORE
            # the delete/insert -- same order as the character-side emotional_states load above.
            user_emotional_states_conf = prompts_config.get('user_emotional_states', {})
            self.enable_user_emotional_states_var_editor.set(user_emotional_states_conf.get('enabled', False))
            self._toggle_user_emotional_states_fields()
            self.user_emotional_states_text.delete(1.0, tk.END)
            self.user_emotional_states_text.insert(tk.END, "\n".join(user_emotional_states_conf.get('states', [])))

            # NEW: Load random names
            random_names_conf = prompts_config.get('random_names', {})
            self.enable_random_names_var_editor.set(random_names_conf.get('enabled', False))
            self.random_names_text.delete(1.0, tk.END)
            self.random_names_text.insert(tk.END, "\n".join(random_names_conf.get('names', [])))
            self._toggle_random_names_fields()

            # NEW: Load random settings pool
            random_settings_conf = prompts_config.get('random_settings', {})
            self.enable_random_settings_var_editor.set(random_settings_conf.get('enabled', False))
            self.random_settings_text.delete(1.0, tk.END)
            self.random_settings_text.insert(tk.END, "\n".join(random_settings_conf.get('settings', [])))
            self._toggle_random_settings_fields()

            detection_conf = config.get('detection', {})
            self.no_user_impersonation_var.set(detection_conf.get('no_user_impersonation', False)) # Set global var
            refusal_conf = detection_conf.get('refusal', {})
            self.refusal_phrases_text.delete(1.0, tk.END); self.refusal_phrases_text.insert(tk.END, "\n".join(refusal_conf.get('phrases', [])))
            self.refusal_fixes_text.delete(1.0, tk.END); self.refusal_fixes_text.insert(tk.END, "\n".join(refusal_conf.get('fixes', [])))

            # OOC drift detection loading (default enabled: True, matching generate.py)
            ooc_conf = detection_conf.get('out_of_character', {})
            self.ooc_enabled_var_editor.set(bool(ooc_conf.get('enabled', True)))
            self.ooc_phrases_text.delete(1.0, tk.END)
            self.ooc_phrases_text.insert(tk.END, "\n".join(ooc_conf.get('phrases', [])))
            self.ooc_fixes_text.delete(1.0, tk.END)
            self.ooc_fixes_text.insert(tk.END, "\n".join(ooc_conf.get('fixes', [])))
            self._toggle_ooc_fields()
            
            user_speaking_conf = detection_conf.get('user_speaking', {}) 
            for gender_val in ["male", "female", "neutral"]: 
                gender_specific_data = user_speaking_conf.get(gender_val, {}) 
                self.user_speaking_phrases_data[gender_val] = gender_specific_data.get('phrases', [])
                self.user_speaking_fixes_data[gender_val] = gender_specific_data.get('fixes', [])
            
            loaded_gender_from_config = config.get('gender', 'female') 
            self.gender_var_editor.set(loaded_gender_from_config) # Set radio button
            self._load_gender_specific_texts_into_ui(loaded_gender_from_config) # Load selected gender's data into UI

            slop_conf = detection_conf.get('slop', {})
            self.slop_phrases_text.delete(1.0, tk.END); self.slop_phrases_text.insert(tk.END, "\n".join(slop_conf.get('phrases', [])))
            self.slop_fixes_text.delete(1.0, tk.END); self.slop_fixes_text.insert(tk.END, "\n".join(slop_conf.get('fixes', [])))

            # Anti-slop configuration loading
            anti_slop_conf = detection_conf.get('anti_slop', {})
            self.anti_slop_phrases_text.delete(1.0, tk.END)
            self.anti_slop_phrases_text.insert(tk.END, "\n".join(anti_slop_conf.get('phrases', [])))
            self.anti_slop_fixes_text.delete(1.0, tk.END)
            self.anti_slop_fixes_text.insert(tk.END, "\n".join(anti_slop_conf.get('fixes', [])))

            self.sampler_priority_text.delete(1.0, tk.END); self.sampler_priority_text.insert(tk.END, "\n".join(samplers_config_load.get('priority', ["repetition_penalty", "top_p", "top_k", "temperature", "max_tokens_answer"])))
            self.temperature_var.set(str(samplers_config_load.get('temperature', 0.5))) 
            self.top_p_var.set(str(samplers_config_load.get('top_p', 0.9)))
            self.min_p_var.set(str(samplers_config_load.get('min_p', 0.0)))
            self.top_k_var.set(str(samplers_config_load.get('top_k', 50))) 
            self.repetition_penalty_var.set(str(samplers_config_load.get('repetition_penalty', 1.1)))
            self.max_tokens_question_var.set(str(samplers_config_load.get('max_tokens_question', 256))) 
            self.max_tokens_answer_var.set(str(samplers_config_load.get('max_tokens_answer', 1024))) 
            self.max_tokens_user_reply_var.set(str(samplers_config_load.get('max_tokens_user_reply', 256)))
            self.logit_bias_text.delete("1.0", tk.END)
            self.logit_bias_text.insert("1.0", samplers_config_load.get('logit_bias', ''))

            # v11.1 bad_words: set toggle then re-sync the field state before touching it
            # (disabled-Text drops edits on this Tk build -- see user_emotional_states note).
            self.use_bad_words_var_editor.set(bool(samplers_config_load.get('use_bad_words', False)))
            self._toggle_bad_words_fields()
            self.bad_words_text.config(state='normal')
            try:
                self.bad_words_text.delete("1.0", tk.END)
                self.bad_words_text.insert("1.0", "\n".join(samplers_config_load.get('bad_words', [])))
            finally:
                self._toggle_bad_words_fields()

            slop_fixer_sampler_conf = samplers_config_load.get('slop_fixer_params', {})
            self.slop_fixer_temp_var.set(str(slop_fixer_sampler_conf.get('temperature', '')))
            self.slop_fixer_top_p_var.set(str(slop_fixer_sampler_conf.get('top_p', '')))
            self.slop_fixer_min_p_var.set(str(slop_fixer_sampler_conf.get('min_p', '')))
            self.slop_fixer_max_tokens_var.set(str(slop_fixer_sampler_conf.get('max_tokens', '')))
            self.slop_fixer_top_k_var.set(str(slop_fixer_sampler_conf.get('top_k', '')))
            self.slop_fixer_repetition_penalty_var.set(str(slop_fixer_sampler_conf.get('repetition_penalty', '')))

            # --- NEW: Load Anti-Slop Sampler Config ---
            anti_slop_fixer_sampler_conf = samplers_config_load.get('anti_slop_params', {})
            self.anti_slop_fixer_temp_var.set(str(anti_slop_fixer_sampler_conf.get('temperature', '')))
            self.anti_slop_fixer_top_p_var.set(str(anti_slop_fixer_sampler_conf.get('top_p', '')))
            self.anti_slop_fixer_min_p_var.set(str(anti_slop_fixer_sampler_conf.get('min_p', '')))
            self.anti_slop_fixer_max_tokens_var.set(str(anti_slop_fixer_sampler_conf.get('max_tokens', '')))
            self.anti_slop_fixer_top_k_var.set(str(anti_slop_fixer_sampler_conf.get('top_k', '')))
            self.anti_slop_fixer_repetition_penalty_var.set(str(anti_slop_fixer_sampler_conf.get('repetition_penalty', '')))

            self.status.config(text="Config loaded from self.global_config.", foreground="blue")
            log_message("Configuration loaded into editor.", "INFO")
            self.validate_config_handler(show_success_message=False) # Validate silently on load
        except Exception as e:
            messagebox.showerror("Error", f"Failed to load config into editor: {str(e)}")
            log_message(f"Failed to load config into editor: {str(e)}", "ERROR")
            import traceback; log_message(traceback.format_exc(), "ERROR")
            if hasattr(self.status, 'winfo_exists') and self.status.winfo_exists():
                self.status.config(text=f"Failed to load config: {str(e)}", foreground="red")

    def validate_config_handler(self, event=None, show_success_message=True): 
        """Performs basic validation of numeric and list fields in the editor."""
        try:
            self.datasets_panel.validate()
            int(self.subject_size_var.get()); int(self.context_size_var.get()); int(self.max_attempts_var.get())
            int(self.db_port_var.get())
            int(self.db_pool_size_var.get())
            num_turns_val = int(self.num_turns_var.get()); assert num_turns_val > 0, "Number of turns must be > 0"
            placeholder_share_val = float(self.placeholder_share_var.get() or 0); assert 0 <= placeholder_share_val <= 1, "Placeholder share must be between 0 and 1"
            int(self.history_size_var.get()); int(self.max_slop_sentence_fix_iterations_var.get())
            float(self.temperature_var.get()); float(self.top_p_var.get()); float(self.min_p_var.get()); int(self.top_k_var.get())
            float(self.repetition_penalty_var.get());
            int(self.max_tokens_question_var.get()); int(self.max_tokens_answer_var.get()); int(self.max_tokens_user_reply_var.get())
            num_chars_val = int(self.num_characters_var_editor.get())
            for idx, char_data in enumerate(self.character_entries):
                age_value = char_data.get('age', '').get().strip()
                if age_value:
                    try:
                        age_int = int(age_value)
                        # Edit the numbers 18 and 60 below to your desired range
                        if age_int < 18 or age_int > 60:
                            raise AssertionError(f"Character {idx+1} age must be between 18 and 60 (got {age_int})")
                    except ValueError:
                        raise AssertionError(f"Character {idx+1} age must be a valid number (got '{age_value}')")
            assert 1 <= num_chars_val <= 10, "Number of characters must be between 1 and 10"
            pb_val = int(self.persona_breakthrough_turn_var_editor.get())
            assert 1 <= pb_val <= 99, "Persona Breakthrough Turn must be between 1 and 99"
            if self.slop_fixer_temp_var.get(): float(self.slop_fixer_temp_var.get())
            if self.slop_fixer_top_p_var.get(): float(self.slop_fixer_top_p_var.get())
            if self.slop_fixer_min_p_var.get(): float(self.slop_fixer_min_p_var.get())
            if self.slop_fixer_max_tokens_var.get(): int(self.slop_fixer_max_tokens_var.get())
            if self.slop_fixer_top_k_var.get(): int(self.slop_fixer_top_k_var.get())
            if self.slop_fixer_repetition_penalty_var.get(): float(self.slop_fixer_repetition_penalty_var.get())

            if self.valkey_port_var.get():
                port_val = int(self.valkey_port_var.get())
                assert 1 <= port_val <= 65535, "Port must be between 1 and 65535"

            if self.valkey_db_var.get():
                db_val = int(self.valkey_db_var.get())
                assert db_val >= 0, "Database number must be non-negative"

            def get_text_as_list(text_widget): return [line.strip() for line in text_widget.get("1.0", tk.END).split('\n') if line.strip()]
            get_text_as_list(self.system_variations_text)
            get_text_as_list(self.refusal_phrases_text); get_text_as_list(self.refusal_fixes_text)
            get_text_as_list(self.slop_phrases_text); get_text_as_list(self.slop_fixes_text)
            get_text_as_list(self.ooc_phrases_text); get_text_as_list(self.ooc_fixes_text)
            get_text_as_list(self.sampler_priority_text)
            # v11.1 bad_words: do NOT strip -- leading whitespace on each entry is intentional
            # (matches vLLM semantics where ` For Example` and `For Example` are different tokens).
            if self.use_bad_words_var_editor.get():
                raw_bad_words = self.bad_words_text.get("1.0", tk.END).split('\n')
                assert any(line.strip() for line in raw_bad_words), "Enable Bad Words requires at least one non-empty line"

            logit_bias_str = self.logit_bias_text.get("1.0", tk.END).strip()
            if logit_bias_str:
                try:
                    json.loads(logit_bias_str)
                except json.JSONDecodeError:
                    raise ValueError("Logit bias must be valid JSON format")

            if self.enable_emotional_states_var_editor.get():
                get_text_as_list(self.emotional_states_text)
            
            if self.enable_user_emotional_states_var_editor.get():
                get_text_as_list(self.user_emotional_states_text)
            
            if self.enable_random_names_var_editor.get():
                get_text_as_list(self.random_names_text)
            
            if self.enable_random_settings_var_editor.get():
                get_text_as_list(self.random_settings_text)
            
            max_cards_val = int(self.max_character_cards_var.get())
            assert max_cards_val > 0, "Max character cards must be > 0"
            assert max_cards_val <= 100, "Max character cards should not exceed 100"
            quality_threshold = int(self.quality_min_threshold_var.get() or 50)
            assert 0 <= quality_threshold <= 100, "Quality threshold must be 0-100"
            quality_max_chars = int(self.quality_max_chars_var.get() or 8000)
            assert quality_max_chars > 100, "Max chars for scoring must be > 100"
            judge_attempts_val = int(self.quality_judge_max_attempts_var.get() or 3)
            assert 1 <= judge_attempts_val <= 10, "Judge max attempts must be 1-10"
            judge_delay_val = float(self.quality_judge_retry_delay_var.get() or 0)
            assert 0 <= judge_delay_val <= 30, "Judge retry delay must be 0-30 seconds"
            result_timeout_val = int(self.quality_result_timeout_var.get() or 600)
            assert result_timeout_val >= 30, "Judge result timeout must be at least 30 seconds"
            if self.quality_logic_check_var_editor.get():
                logic_threshold_val = int(self.quality_logic_threshold_var.get() or 70)
                assert 0 <= logic_threshold_val <= 100, "Logic check threshold must be 0-100"
                logic_max_rewrites_val = int(self.quality_logic_max_rewrites_var.get() or 1)
                assert logic_max_rewrites_val >= 0, "Logic max rewrites must be >= 0"
                logic_max_chars_val = int(self.quality_logic_max_chars_var.get() or 4000)
                assert logic_max_chars_val > 100, "Max chars for logic check must be > 100"

            self._refresh_placement_cache_warning()
            if show_success_message:
                self.status.config(text="Validation successful (basic checks).", foreground="green")
            log_message("ConfigEditor: Validation successful.", "DEBUG")
            return True 
        except DatasetSettingsError as e_dataset:
            self.status.config(text=f"Dataset configuration: {str(e_dataset)}", foreground="red")
            log_message(f"ConfigEditor dataset validation: {str(e_dataset)}", "WARNING")
        except ValueError as e_val: # Error during type conversion
            self.status.config(text=f"Validation Error: Invalid number. {str(e_val)}", foreground="red")
            log_message(f"ConfigEditor Validation (ValueError): {str(e_val)}", "WARNING")
        except AssertionError as e_assert: # Error from assert statement (e.g., num_turns <= 0)
            self.status.config(text=f"Validation Error: {str(e_assert)}", foreground="red")
            log_message(f"ConfigEditor Validation (AssertionError): {str(e_assert)}", "WARNING")
        except Exception as e_other: # Other unexpected errors
            self.status.config(text=f"Validation Error: {str(e_other)}", foreground="red")
            log_message(f"ConfigEditor Validation (OtherError): {str(e_other)}", "WARNING")
        return False

    def _sync_character_text_values(self, visible_only=False):
        """Capture paste/programmatic edits even if no key/focus event fired."""
        for data in self.character_entries:
            if visible_only and data['frame'].winfo_manager() != 'pack':
                continue
            for key in ('clothing', 'jewelry', 'appearance', 'backstory', 'persona', 'personality', 'traits', 'setting', 'greeting'): 
                widget = data[f'{key}_widget']
                if widget.winfo_exists():
                    data[key].set(widget.get('1.0', tk.END).strip())

    def _cancel_character_scroll(self, event=None):
        if event is not None and event.widget != self:
            return
        if self._character_scroll_job is not None:
            self.after_cancel(self._character_scroll_job)
            self._character_scroll_job = None

    def _queue_character_scroll(self, widget):
        self._cancel_character_scroll()
        def reveal():
            self._character_scroll_job = None
            if widget.winfo_exists() and widget.winfo_manager() == 'pack':
                self._scroll_canvas_to(widget)
        self._character_scroll_job = self.after_idle(reveal)

    def _select_character_page(self, event=None):
        page = self.character_page_selector.current()
        if page >= 0:
            self._show_character_page(page)

    def _show_character_page(self, page, scroll_to_top=True):
        """Keep at most three card windows managed; retain every card's state."""
        self._cancel_character_scroll()
        self._sync_character_text_values(visible_only=True)
        count = len(self.character_entries)
        pages = max(1, (count + self.character_page_size - 1) // self.character_page_size)
        self.character_page = max(0, min(int(page), pages - 1))
        first = self.character_page * self.character_page_size
        last = min(first + self.character_page_size, count)
        for data in self.character_entries:
            data['frame'].pack_forget()
        for data in self.character_entries[first:last]:
            data['frame'].pack(fill=tk.X, pady=8, padx=8)
        if count:
            self.character_cards_frame.pack(fill=tk.BOTH, expand=True, padx=SPACING, pady=SPACING)
        else:
            # An empty pack container retains its previous requested height.
            self.character_cards_frame.pack_forget()
        self.character_page_selector.configure(
            values=tuple(f"{index + 1} / {pages}" for index in range(pages)),
            state='readonly' if count else 'disabled')
        self.character_page_selector.current(self.character_page)
        self.character_page_summary.configure(
            text=f"Characters {first + 1} to {last} of {count}" if count else "No characters")
        self.character_previous_button.configure(state='normal' if self.character_page else 'disabled')
        self.character_next_button.configure(state='normal' if self.character_page + 1 < pages else 'disabled')
        if scroll_to_top:
            # Keep the pager visible instead of jumping above the engine settings.
            self._queue_character_scroll(self.character_page_controls)
        # The existing content <Configure> binding updates the scrollregion.
        # Do not call update_idletasks here: bulk loading must remain bounded
        # before Tk can draw any intermediate layout.

    def _reveal_character_page_for(self, widget):
        """Section search can find an unmanaged card on a different page."""
        ancestor = widget
        while ancestor is not None and ancestor != self.character_cards_frame:
            for index, data in enumerate(self.character_entries):
                if ancestor == data['frame']:
                    page = index // self.character_page_size
                    if page != self.character_page:
                        self._show_character_page(page, scroll_to_top=False)
                    return
            ancestor = ancestor.master

    def _add_character_row(self, character_data=None):
        """Create a card whose short fields reflow and long fields use its full width."""
        max_cards_str = self.max_character_cards_var.get()
        max_cards = int(max_cards_str) if max_cards_str and max_cards_str.strip() else 10
        if len(self.character_entries) >= max_cards:
            messagebox.showwarning("Limit Reached", f"Maximum {max_cards} character cards allowed.")
            return
        character_data = character_data or {}
        row_index = len(self.character_entries)
        card_frame = ttk.LabelFrame(self.character_cards_frame,
                                    text=f"  ✦ Character {row_index + 1}  ", padding=8)
        # _show_character_page is the only place that manages card frames.
        card_frame.columnconfigure(0, weight=1)
        entry_vars = {}
        label_font = ('Segoe UI', 9, 'bold')
        entry_font = ('Segoe UI', 9)

        short_fields = ttk.Frame(card_frame)
        short_fields.grid(row=0, column=0, sticky="ew")
        groups = []
        for key, caption in (("name", "Name:"), ("age", "Age:"), ("race", "Race:"),
                             ("job", "Job:"), ("eye_color", "Eye Color:"), ("gender", "Gender:"), ("hair", "Hair:"), ("hair_color", "Hair Color:")):
            group = ttk.Frame(short_fields)
            group.columnconfigure(0, weight=1)
            label = ttk.Label(group, text=caption, font=label_font, foreground='#e0e0e0')
            label.grid(row=0, column=0, sticky="w", pady=(0, 3))
            default = '25' if key == 'age' else ''
            var = tk.StringVar(value=character_data.get(key, default))
            widget = ttk.Entry(group, textvariable=var, width=1, font=entry_font)
            widget.grid(row=1, column=0, sticky="ew")
            entry_vars[key] = var
            entry_vars[f'{key}_label'] = label
            entry_vars[f'{key}_widget'] = widget
            groups.append(group)
        entry_vars['short_layout'] = ResponsiveGrid(short_fields, groups, min_width=180,
                                                    max_columns=3, padx=4, pady=5)

        def validate_age(value):
            if not value:
                return True
            try:
                return 0 <= int(value) <= 999
            except ValueError:
                return False
        entry_vars['age_widget'].configure(
            validate='key', validatecommand=(card_frame.register(validate_age), '%P'))

        next_row = 1
        for key, caption, height in (("clothing", "Clothing:", 2),
                                     ("jewelry", "Jewelry:", 2),
                                     ("appearance", "Appearance:", 2),
                                     ("backstory", "Backstory:", 3),
                                     ("persona", "Persona (inner personality):", 3),
                                     ("personality", "Personality (outward behavior):", 3),
                                     ("traits", "Traits:", 2),
                                     ("setting", "Setting:", 2),
                                     ("greeting", "Greeting (optional, opening mode):", 3)):
            label = ttk.Label(card_frame, text=caption, font=label_font, foreground='#e0e0e0')
            label.grid(row=next_row, column=0, sticky="w", padx=4, pady=(8, 3))
            var = tk.StringVar(value=character_data.get(key, ''))
            widget = tk.Text(card_frame, width=1, height=height, font=entry_font,
                             bg='#2a2a35', fg='#ffffff', wrap=tk.WORD,
                             relief=tk.SOLID, borderwidth=1)
            widget.insert('1.0', character_data.get(key, ''))
            widget.grid(row=next_row + 1, column=0, sticky="ew", padx=4, pady=(0, 4))
            def on_change(event=None, text_widget=widget, value_var=var):
                if text_widget.winfo_exists():
                    value_var.set(text_widget.get('1.0', tk.END).strip())
            widget.bind('<KeyRelease>', on_change)
            widget.bind('<FocusOut>', on_change)
            entry_vars[key] = var
            entry_vars[f'{key}_label'] = label
            entry_vars[f'{key}_widget'] = widget
            next_row += 2

        action_row_frame = ttk.Frame(card_frame)
        action_row_frame.grid(row=next_row, column=0, sticky="ew", padx=4, pady=(8, 4))
        action_row_frame.columnconfigure(0, weight=1)
        class_label = ttk.Label(action_row_frame, text="⚔ Class:", font=label_font, foreground='#e0e0e0')
        class_label.grid(row=0, column=0, sticky="w", pady=(0, 3))
        class_var = tk.StringVar(value=character_data.get('class', ''))
        class_entry = ttk.Entry(action_row_frame, textvariable=class_var, width=1, font=entry_font)
        class_entry.grid(row=1, column=0, sticky="ew")
        entry_vars.update({'class': class_var, 'class_label': class_label,
                           'class_entry': class_entry, 'class_frame': action_row_frame})
        delete_button = ttk.Button(card_frame, text="✕ Remove Character",
                                   command=lambda idx=row_index: self._remove_character_row(idx),
                                   style='Danger.TButton')
        delete_button.grid(row=next_row + 1, column=0, sticky="e", padx=4, pady=(8, 4))
        entry_vars['delete_btn'] = delete_button
        entry_vars['frame'] = card_frame
        self.character_entries.append(entry_vars)
        if not self._loading_character_cards:
            self._update_character_card_labels()
            self._toggle_character_engine_fields()
            self._show_character_page(row_index // self.character_page_size)
            self._queue_character_scroll(card_frame)

    def _validate_character_ages(self):
        """Highlights age fields that are outside the 18-60 range."""
        for idx, data in enumerate(self.character_entries):
            age_widget = data.get('age_widget')
            if age_widget and hasattr(age_widget, 'winfo_exists') and age_widget.winfo_exists():
                try:
                    age_value = int(data['age'].get().strip())
                    # Edit the numbers 18 and 60 below to match your new range
                    if age_value < 18 or age_value > 60:
                        age_widget.config(foreground='#ff6b6b')  # Red for invalid
                    else:
                        age_widget.config(foreground='#ffffff')  # White for valid
                except (ValueError, TypeError):
                    age_widget.config(foreground='#ff6b6b')  # Red for non-numeric

    def _remove_character_row(self, row_index):
        """Removes a character card from the table."""
        if 0 <= row_index < len(self.character_entries):
            self._cancel_character_scroll()
            self._sync_character_text_values(visible_only=True)
            entry_data = self.character_entries[row_index]
            entry_data['frame'].destroy()
            self.character_entries.pop(row_index)

            # Update delete button commands for remaining rows
            for i, data in enumerate(self.character_entries):
                data['delete_btn'].config(
                    command=lambda idx=i: self._remove_character_row(idx)
                )

            self._update_character_card_labels()
            self._show_character_page(self.character_page)

    def _update_character_card_labels(self):
        """Updates the text on each character card frame to match its index."""
        for i, data in enumerate(self.character_entries):
            data['frame'].config(text=f"Character {i + 1}")

    def _toggle_character_engine_fields(self):
        """Enables/disables character engine fields based on checkbox state."""
        is_enabled = self.enable_character_engine_var_editor.get()

        # Enable/disable the Add Character button
        if hasattr(self, 'add_char_btn') and self.add_char_btn.winfo_exists():
            self.add_char_btn.config(state='normal' if is_enabled else 'disabled')

        # Enable/disable the persona breakthrough spinbox
        if hasattr(self, 'persona_breakthrough_spinbox') and self.persona_breakthrough_spinbox.winfo_exists():
            self.persona_breakthrough_spinbox.config(state='normal' if is_enabled else 'disabled')

        # Enable/disable all entry fields inside the character cards
        for data in self.character_entries:
            state = 'normal' if is_enabled else 'disabled'
            for key, widget in data.items():
                if key in ('frame', 'delete_btn', 'class_frame'):
                    continue
                if isinstance(widget, (ttk.Entry, tk.Entry)):
                    widget.config(state=state)
                elif isinstance(widget, tk.Text):
                    widget.config(state=state if is_enabled else 'disabled')

        # Also toggle class and setting visibility
        self._update_class_column_visibility()
        self._update_setting_column_visibility()

        log_message(f"Character Engine fields {'enabled' if is_enabled else 'disabled'}", "DEBUG")

    def _toggle_class_fields(self):
        """Enables/disables class selection text field based on checkbox state."""
        # Simply toggle visibility, as the main engine toggle handles enable/disable state
        self._update_class_column_visibility()
        log_message(f"Class Selection fields visibility updated", "DEBUG")

    def _update_class_column_visibility(self):
        """Shows or hides the 'Class' fields based on the Enable Class checkbox."""
        is_enabled = self.enable_class_selection_var_editor.get()
        for data in self.character_entries:
            if is_enabled:
                # Show class label, entry, and action row
                if 'class_label' in data:
                    data['class_label'].grid()
                if 'class_entry' in data:
                    data['class_entry'].grid()
                if 'class_frame' in data:
                    data['class_frame'].grid()
            else:
                # Hide class label, entry, and action row
                if 'class_label' in data:
                    data['class_label'].grid_remove()
                if 'class_entry' in data:
                    data['class_entry'].grid_remove()
                if 'class_frame' in data:
                    data['class_frame'].grid_remove()

        log_message(f"Class Selection fields visibility updated", "DEBUG")
# --- End of ConfigEditor Class ---


# --- Main UI Setup ---

