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
from logging_config import log_message
from api_handler import global_rate_limiter

SPACING = 8  # UI padding constant (mirrors generate.py)
QUESTIONS_FILE_PATH = os.path.join('input', 'questions.txt')  # mirrors generate.py (UI label only)


def validate_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme in ['http', 'https'], result.netloc])
    except:
        return False


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
        self.geometry("1600x1000") # Adjusted for potentially more content
        self.minsize(1600, 1000)
        self.user_speaking_phrases_data = {"male": [], "female": [], "neutral": []} 
        self.user_speaking_fixes_data = {"male": [], "female": [], "neutral": []}
        self.active_display_gender = "female"
        self.max_character_cards_var = tk.StringVar(value=str(self.global_config.get('generation.max_character_cards', 10)))
        
        # Initialize num_threads_var_editor
        self.num_threads_var_editor = tk.StringVar(value=str(self.global_config.get('api.threads', 10)))

        self.notebook = ttk.Notebook(self)

        # --- NEW: Tab & Section Search Bar ---
        self.search_frame = ttk.Frame(self)
        self.search_frame.pack(fill=tk.X, padx=SPACING, pady=(SPACING, 0))
        ttk.Label(self.search_frame, text="🔍 Search Tabs/Sections:").pack(side=tk.LEFT, padx=(0, 5))
        self.search_entry = ttk.Entry(self.search_frame)
        self.search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.search_entry.bind("<KeyRelease>", self.search_editor_tabs)
        self.search_entry.bind("<Return>", self.search_editor_tabs)
        # ------------------------------------

        self.notebook.pack(fill=tk.BOTH, expand=True, padx=SPACING, pady=SPACING)

        # --- API Tab ---
        self.api_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.api_tab, text="API")

        self.api_canvas = tk.Canvas(self.api_tab)
        self.api_scrollbar = tk.Scrollbar(self.api_tab, orient="vertical", command=self.api_canvas.yview, width=self.scrollbar_width, bg='#5a5a70', troughcolor='#2a2a35', activebackground='#8a8aa0', highlightthickness=0, bd=0)
        self.api_content_frame = ttk.Frame(self.api_canvas)

        self.api_content_frame.bind(
            "<Configure>",
            lambda e: self.api_canvas.configure(scrollregion=self.api_canvas.bbox("all"))
        )

        self.api_canvas_window_id = self.api_canvas.create_window((0, 0), window=self.api_content_frame, anchor="nw")
        self.api_canvas.bind("<Configure>", lambda e: self.api_canvas.itemconfig(self.api_canvas_window_id, width=e.width) if e.width > 1 else None)

        self.api_canvas.pack(side="left", fill="both", expand=True)
        self.api_canvas.configure(yscrollcommand=self.api_scrollbar.set)
        self.api_scrollbar.pack(side="right", fill="y")

        self.api_canvas.bind("<MouseWheel>", lambda e: self.api_canvas.yview_scroll(int(-1*(e.delta/120)), "units"))

        # Pricing Input Field (now inside api_content_frame)
        ttk.Label(self.api_content_frame, text="Cost per 1k Tokens ($):").grid(row=0, column=0, padx=SPACING, pady=SPACING, sticky="w")
        self.pricing_var = tk.StringVar()
        ttk.Entry(self.api_content_frame, width=10, textvariable=self.pricing_var).grid(row=0, column=1, padx=SPACING, pady=SPACING, sticky="w")
        ttk.Label(self.api_content_frame, text="(Enter 0 if unknown)").grid(row=0, column=2, padx=SPACING, pady=SPACING, sticky="w")

        ttk.Label(self.api_content_frame, text="API Budget Limit ($):").grid(row=1, column=0, padx=SPACING, pady=SPACING, sticky="w")
        self.budget_limit_var = tk.StringVar()
        ttk.Entry(self.api_content_frame, width=10, textvariable=self.budget_limit_var).grid(row=1, column=1, padx=SPACING, pady=SPACING, sticky="w")
        ttk.Label(self.api_content_frame, text="(Set to 0 to disable)").grid(row=1, column=2, padx=SPACING, pady=SPACING, sticky="w")

        # Valkey Configuration Section (now inside api_content_frame)
        valkey_frame = ttk.LabelFrame(self.api_content_frame, text="Valkey Cache Settings")
        valkey_frame.grid(row=2, column=0, padx=SPACING, pady=SPACING, sticky="ew")

        db_frame = ttk.LabelFrame(self.api_content_frame, text="PostgreSQL Database Settings")
        db_frame.grid(row=3, column=0, padx=SPACING, pady=SPACING, sticky="ew")

        self.db_enabled_var = tk.BooleanVar()
        ttk.Checkbutton(db_frame, text="Enable PostgreSQL Database", variable=self.db_enabled_var).pack(anchor="w", padx=SPACING, pady=SPACING)

        ttk.Label(db_frame, text="Host:").pack(anchor="w", padx=(15,0), pady=SPACING)
        self.db_host_var = tk.StringVar()
        ttk.Entry(db_frame, width=40, textvariable=self.db_host_var).pack(anchor="w", padx=SPACING, pady=SPACING)

        ttk.Label(db_frame, text="Port:").pack(anchor="w", padx=(15,0), pady=SPACING)
        self.db_port_var = tk.StringVar()
        ttk.Entry(db_frame, width=15, textvariable=self.db_port_var).pack(anchor="w", padx=SPACING, pady=SPACING)

        ttk.Label(db_frame, text="Database Name:").pack(anchor="w", padx=(15,0), pady=SPACING)
        self.db_dbname_var = tk.StringVar()
        ttk.Entry(db_frame, width=40, textvariable=self.db_dbname_var).pack(anchor="w", padx=SPACING, pady=SPACING)

        ttk.Label(db_frame, text="User:").pack(anchor="w", padx=(15,0), pady=SPACING)
        self.db_user_var = tk.StringVar()
        ttk.Entry(db_frame, width=40, textvariable=self.db_user_var).pack(anchor="w", padx=SPACING, pady=SPACING)

        ttk.Label(db_frame, text="Password:").pack(anchor="w", padx=(15,0), pady=SPACING)
        self.db_password_var = tk.StringVar()
        ttk.Entry(db_frame, width=40, textvariable=self.db_password_var, show="*").pack(anchor="w", padx=SPACING, pady=SPACING)

        ttk.Label(db_frame, text="Connection Pool Size:").pack(anchor="w", padx=(15,0), pady=SPACING)
        self.db_pool_size_var = tk.StringVar()
        ttk.Entry(db_frame, width=15, textvariable=self.db_pool_size_var).pack(anchor="w", padx=SPACING, pady=SPACING)

        self.valkey_enabled_var = tk.BooleanVar()
        ttk.Checkbutton(valkey_frame, text="Enable Valkey Caching", variable=self.valkey_enabled_var).grid(row=0, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="w")

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
        master_duplication_check = ttk.Checkbutton(self.api_content_frame, text="Enable Master Duplication Mode (for enabled APIs 1-4)", variable=self.master_duplication_mode_var_editor, command=self._sync_global_duplication_var_from_editor)
        master_duplication_check.grid(row=4, column=0, columnspan=2, padx=SPACING, pady=(10,5), sticky="w")

        num_api_slots = 6
        # (name, label) pairs for the per-slot "API Compatibility" dropdown; see api_profiles.py.
        self._api_profile_choices = api_profiles.list_profiles()
        for i in range(num_api_slots):
            frame_text = f"API Slot {i+1}"
            if i == 0: frame_text += " (Primary for Q/Continuation in Duplication)"
            if i == 4: frame_text += " (Slop Fixer LLM - Not part of Duplication)"
            if i == 5: frame_text += " (Anti-Slop Fixer LLM - Independent Misc Option Not Part of Duplication)"
            
            api_frame = ttk.LabelFrame(self.api_content_frame, text=frame_text) # Changed parent to self.api_content_frame
            api_frame.grid(row=i + 5, column=0, padx=SPACING, pady=SPACING, sticky="ew")
            self.api_tab.grid_columnconfigure(0, weight=1) 
            
            ttk.Label(api_frame, text="API URL:").grid(row=0, column=0, padx=SPACING, pady=SPACING, sticky="e")
            url_var = tk.StringVar(); ttk.Entry(api_frame, width=60, textvariable=url_var).grid(row=0, column=1, padx=SPACING, pady=SPACING, sticky="ew")
            setattr(self, f'api_url_var_{i+1}', url_var) 
            
            ttk.Label(api_frame, text="Model Name:").grid(row=1, column=0, padx=SPACING, pady=SPACING, sticky="e")
            model_var = tk.StringVar(); ttk.Entry(api_frame, width=60, textvariable=model_var).grid(row=1, column=1, padx=SPACING, pady=SPACING, sticky="ew")
            setattr(self, f'api_model_var_{i+1}', model_var)
            
            ttk.Label(api_frame, text="API Key:").grid(row=2, column=0, padx=SPACING, pady=SPACING, sticky="e")
            key_var = tk.StringVar(); ttk.Entry(api_frame, width=60, textvariable=key_var, show="*").grid(row=2, column=1, padx=SPACING, pady=SPACING, sticky="ew")
            setattr(self, f'api_key_var_{i+1}', key_var)

            # Add status label and test button for API connection
            status_var = tk.StringVar(value="Not tested")
            status_label = ttk.Label(api_frame, textvariable=status_var, foreground="gray")
            status_label.grid(row=6, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="w")
            setattr(self, f'api_status_var_{i+1}', status_var)
            setattr(self, f'api_status_label_{i+1}', status_label)
            test_btn = ttk.Button(api_frame, text="Test Connection", command=lambda idx=i: self.test_api_connection(idx))  # This should already work since it's a lambda
            test_btn.grid(row=7, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="w")
            setattr(self, f'api_test_btn_{i+1}', test_btn)

            # --- API Compatibility (payload param filtering per endpoint) ---
            compat_frame = ttk.LabelFrame(api_frame, text="API Compatibility")
            compat_frame.grid(row=8, column=0, columnspan=3, padx=SPACING, pady=(SPACING, 2), sticky="ew")
            compat_frame.grid_columnconfigure(0, weight=1)

            profile_var = tk.StringVar()
            setattr(self, f'api_profile_var_{i+1}', profile_var)
            profile_combo = ttk.Combobox(
                compat_frame, textvariable=profile_var, state="readonly", width=52,
                values=[lbl for _, lbl in self._api_profile_choices]
            )
            profile_combo.grid(row=0, column=0, padx=SPACING, pady=SPACING, sticky="ew")
            profile_combo.bind("<<ComboboxSelected>>", lambda e, idx=i: self._on_api_profile_change(idx))
            ttk.Button(compat_frame, text="Detect", command=lambda idx=i: self._detect_api_profile(idx)).grid(
                row=0, column=1, padx=SPACING, pady=SPACING)

            custom_params_var = tk.StringVar()
            setattr(self, f'api_custom_params_var_{i+1}', custom_params_var)
            custom_params_entry = ttk.Entry(compat_frame, textvariable=custom_params_var, width=60)
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
                ttk.Checkbutton(api_frame, text="Enabled for Generation/Duplication", variable=enabled_var).grid(row=3, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="w")

                # Add threads setting for each API
                ttk.Label(api_frame, text="Number of Threads:").grid(row=4, column=0, padx=SPACING, pady=SPACING, sticky="e")
                threads_var = tk.StringVar(value="10")  # Default value
                setattr(self, f'api_threads_var_{i+1}', threads_var)
                ttk.Entry(api_frame, width=10, textvariable=threads_var).grid(row=4, column=1, padx=SPACING, pady=SPACING, sticky="w")

                # NEW: Add rate limit setting for each API
                ttk.Label(api_frame, text="Rate Limit (RPM):").grid(row=5, column=0, padx=SPACING, pady=SPACING, sticky="e")
                rate_limit_var = tk.StringVar(value="60")  # Default 60 requests per minute
                setattr(self, f'api_rate_limit_var_{i+1}', rate_limit_var)
                ttk.Entry(api_frame, width=10, textvariable=rate_limit_var).grid(row=5, column=1, padx=SPACING, pady=SPACING, sticky="w")
                ttk.Label(api_frame, text="(Requests/Min)").grid(row=5, column=2, padx=SPACING, pady=SPACING, sticky="w")
            else: # API Slot 5 (Slop Fixer) and Slot 6 (Anti-Slop Fixer)
                ttk.Label(api_frame, text="Number of Threads:").grid(row=3, column=0, padx=SPACING, pady=SPACING, sticky="e")
                threads_var = tk.StringVar(value="10")  # Default value
                setattr(self, f'api_threads_var_{i+1}', threads_var)
                ttk.Entry(api_frame, width=10, textvariable=threads_var).grid(row=3, column=1, padx=SPACING, pady=SPACING, sticky="w")

                # NEW: Add rate limit setting for Slop Fixer / Anti-Slop Fixer API
                ttk.Label(api_frame, text="Rate Limit (RPM):").grid(row=4, column=0, padx=SPACING, pady=SPACING, sticky="e")
                rate_limit_var = tk.StringVar(value="60")  # Default 60 requests per minute
                setattr(self, f'api_rate_limit_var_{i+1}', rate_limit_var)
                ttk.Entry(api_frame, width=10, textvariable=rate_limit_var).grid(row=4, column=1, padx=SPACING, pady=SPACING, sticky="w")
                ttk.Label(api_frame, text="(Requests/Min)").grid(row=4, column=2, padx=SPACING, pady=SPACING, sticky="w")

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
                    ).grid(row=5, column=0, columnspan=3, padx=SPACING, pady=(2, SPACING), sticky="w")
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
        self.generation_canvas.bind("<MouseWheel>", lambda e: self.generation_canvas.yview_scroll(int(-1*(e.delta/120)), "units"))

        # Allow the inner frame to expand horizontally
        gen_settings_frame.grid_columnconfigure(1, weight=1)

        row_idx = 0
        def add_gen_setting(label_text, var_name, help_text=""): # Helper to add a setting row
            nonlocal row_idx
            ttk.Label(gen_settings_frame, text=label_text).grid(row=row_idx, column=0, padx=SPACING, pady=SPACING, sticky="e")
            var = tk.StringVar(); setattr(self, var_name, var)
            ttk.Entry(gen_settings_frame, width=10, textvariable=var).grid(row=row_idx, column=1, padx=SPACING, pady=SPACING, sticky="w")
            if help_text: ttk.Label(gen_settings_frame, text=help_text).grid(row=row_idx, column=2, padx=SPACING, pady=SPACING, sticky="w")
            row_idx += 1

        add_gen_setting("Number of Random Chunks:", 'num_random_chunks_var', "(Total tasks to generate per run)")
        add_gen_setting("Max Input Length:", 'sanitize_input_max_length_var', "(For sanitize_input function)")
        add_gen_setting("Subject Size (chars):", 'subject_size_var', "(Size of subject text for question gen if not using questions.txt)")
        add_gen_setting("Context Size (chars):", 'context_size_var', "(Total size of text (subject + surrounding) for Q/A gen if not using questions.txt)")
        add_gen_setting("Max Attempts (per Q/A turn):", 'max_attempts_var', "(Main retries for a valid answer per turn per API; also for Q/UserCont/SlopFixer API calls)") # MODIFIED HELP TEXT
        add_gen_setting("Number of Turns (per conversation):", 'num_turns_var', "(Total Q/A pairs, e.g., 1 for single Q/A)")
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
        ttk.Checkbutton(gen_settings_frame, text="Enable Quality Scoring", variable=self.quality_enabled_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=2, sticky="w"); row_idx += 1

        self.quality_use_llm_var_editor = tk.BooleanVar(value=False)
        ttk.Checkbutton(gen_settings_frame, text="Use LLM for Coherence/Naturalness/Engagement/Consistency (uses extra API calls)", variable=self.quality_use_llm_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=2, sticky="w"); row_idx += 1

        self.quality_output_filter_var_editor = tk.BooleanVar(value=False)
        ttk.Checkbutton(gen_settings_frame, text="Flag conversations below threshold (for post-filtering)", variable=self.quality_output_filter_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=2, sticky="w"); row_idx += 1

        add_gen_setting("Min Score Threshold (0-100):", 'quality_min_threshold_var', "(Conversations below this are flagged)")
        add_gen_setting("Max Chars for LLM Scoring:", 'quality_max_chars_var', "(Truncate conversation before sending to scoring LLM)")

        # Quality scoring API config
        ttk.Label(gen_settings_frame, text="Quality Scoring API URL:").grid(row=row_idx, column=0, padx=SPACING, pady=2, sticky="e")
        self.quality_api_url_var = tk.StringVar()
        ttk.Entry(gen_settings_frame, width=40, textvariable=self.quality_api_url_var).grid(row=row_idx, column=1, columnspan=2, padx=SPACING, pady=2, sticky="w"); row_idx += 1

        ttk.Label(gen_settings_frame, text="Quality Scoring Model:").grid(row=row_idx, column=0, padx=SPACING, pady=2, sticky="e")
        self.quality_api_model_var = tk.StringVar()
        ttk.Entry(gen_settings_frame, width=40, textvariable=self.quality_api_model_var).grid(row=row_idx, column=1, columnspan=2, padx=SPACING, pady=2, sticky="w"); row_idx += 1

        ttk.Label(gen_settings_frame, text="Quality Scoring API Key:").grid(row=row_idx, column=0, padx=SPACING, pady=2, sticky="e")
        self.quality_api_key_var = tk.StringVar()
        ttk.Entry(gen_settings_frame, width=40, textvariable=self.quality_api_key_var, show="*").grid(row=row_idx, column=1, columnspan=2, padx=SPACING, pady=2, sticky="w"); row_idx += 1
        
        self.remove_reasoning_var_editor = tk.BooleanVar() # Editor's local var for this setting
        ttk.Checkbutton(gen_settings_frame, text="Remove Reasoning (Strip ... tags from LLM output)", variable=self.remove_reasoning_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="w"); row_idx+=1

        self.remove_em_dash_var_editor = tk.BooleanVar()
        ttk.Checkbutton(gen_settings_frame, text="Experimental: Remove Em Dash (—) from output", variable=self.remove_em_dash_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="w"); row_idx+=1

        self.ensure_space_after_line_break_var_editor = tk.BooleanVar()
        ttk.Checkbutton(gen_settings_frame, text="Experimental: Ensure Space After Line Break (prevents words running together)", variable=self.ensure_space_after_line_break_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="w"); row_idx+=1

        # NEW: Add checkbox for removing excessive asterisks
        self.remove_asterisks_var_editor = tk.BooleanVar()
        ttk.Checkbutton(gen_settings_frame, text="Experimental: Remove Excessive Asterisks (**, ****, etc.) from output", variable=self.remove_asterisks_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="w"); row_idx+=1

        # NEW: Add checkbox for removing "* *" pattern
        self.remove_asterisk_space_asterisk_var_editor = tk.BooleanVar()
        ttk.Checkbutton(gen_settings_frame, text="Experimental: Remove '* *' Pattern (asterisk space asterisk) from output", variable=self.remove_asterisk_space_asterisk_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="w"); row_idx+=1

        self.remove_all_asterisks_var_editor = tk.BooleanVar()
        ttk.Checkbutton(gen_settings_frame, text="Experimental: Remove ALL Asterisks (including single *) from output", variable=self.remove_all_asterisks_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="w"); row_idx+=1

        self.remove_markdown_var_editor = tk.BooleanVar()
        ttk.Checkbutton(gen_settings_frame, text="Experimental: Remove Markdown Formatting (Convert to plain text)", variable=self.remove_markdown_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="w"); row_idx+=1

        add_gen_setting("Max Slop Sentence Fix Iterations:", 'max_slop_sentence_fix_iterations_var', "(Iterations for sentence-level slop fixing by Slop Fixer LLM)")

        self.slop_to_anti_slop_fallback_var_editor = tk.BooleanVar()
        ttk.Checkbutton(gen_settings_frame, text="Slop → Anti-Slop Fallback (Use Anti-Slop API as final attempt if Slop Fixer fails)",
                        variable=self.slop_to_anti_slop_fallback_var_editor).grid(row=row_idx, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="w"); row_idx+=1
        
        ttk.Label(gen_settings_frame, text="Output Format:").grid(row=row_idx, column=0, padx=SPACING, pady=SPACING, sticky="e")
        self.output_format_var = tk.StringVar(value="sharegpt")
        ttk.Label(gen_settings_frame, text="sharegpt").grid(row=row_idx, column=1, padx=SPACING, pady=SPACING, sticky="w")
        ttk.Label(gen_settings_frame, text="(Format for output.jsonl files)").grid(row=row_idx, column=2, padx=SPACING, pady=SPACING, sticky="w"); row_idx+=1

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
        self.prompts_canvas.bind("<MouseWheel>", lambda e: self.prompts_canvas.yview_scroll(int(-1*(e.delta/120)), "units"))

        # Define Boolean variables BEFORE they are used in Checkbuttons
        self.use_questions_file_var_editor = tk.BooleanVar()
        self.use_variable_system_var_editor = tk.BooleanVar()

        prompts_row_idx = 0

        def add_prompt_text_area(label_text, var_name, height=4): # Helper for text areas
            nonlocal prompts_row_idx
            ttk.Label(self.prompts_content_frame, text=label_text).grid(row=prompts_row_idx, column=0, padx=SPACING, pady=SPACING, sticky="nw")
            text_widget = scrolledtext.ScrolledText(self.prompts_content_frame, wrap=tk.WORD, height=height, width=130, undo=True)
            text_widget.grid(row=prompts_row_idx, column=1, padx=SPACING, pady=SPACING, sticky="ew")
            setattr(self, var_name, text_widget)
            prompts_row_idx += 1

        ttk.Checkbutton(self.prompts_content_frame, text=f"Use '{os.path.basename(QUESTIONS_FILE_PATH)}' for questions (disables subject/context chunking)", variable=self.use_questions_file_var_editor).grid(row=prompts_row_idx, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="w"); prompts_row_idx+=1
        ttk.Checkbutton(self.prompts_content_frame, text="Use Variable System Prompts (randomly chosen from list below)", variable=self.use_variable_system_var_editor).grid(row=prompts_row_idx, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="w"); prompts_row_idx+=1

        add_prompt_text_area("Top Level System Prompt (Applied to ALL prompts):", 'top_level_system_prompt_text', height=12)
        add_prompt_text_area("Base System Prompt (used if not variable, or as one of variations):", 'system_base_prompt_text', height=12)
        add_prompt_text_area("System Prompt Variations (one per line, used if 'Variable System Prompts' is checked):", 'system_variations_text', height=12)
        add_prompt_text_area("Question Prompt (use {recent_questions}, {subject}, {context}):", 'question_prompt_text', height=12)
        add_prompt_text_area("Answer Prompt (instruction for the assistant's turn):", 'answer_prompt_text', height=12)
        add_prompt_text_area("User Continuation Prompt (use {last_assistant_message} for user's next turn):", 'user_continuation_prompt_text', height=12)

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
        self.lore_canvas.bind("<MouseWheel>", lambda e: self.lore_canvas.yview_scroll(int(-1*(e.delta/120)), "units"))

        # Lore UI Content
        ttk.Label(self.lore_content_frame, text="World Lore & Background Information:").grid(row=0, column=0, padx=SPACING, pady=(SPACING, 0), sticky="w")
        self.lore_text = scrolledtext.ScrolledText(self.lore_content_frame, wrap=tk.WORD, undo=True)
        self.lore_text.grid(row=1, column=0, padx=SPACING, pady=(0, SPACING), sticky="nsew")
        self.lore_content_frame.grid_columnconfigure(0, weight=1)
        self.lore_content_frame.grid_rowconfigure(1, weight=1)

        self.prompts_content_frame.grid_columnconfigure(1, weight=1) # Make text areas expand

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
        self.character_engine_canvas.bind("<MouseWheel>", lambda e: self.character_engine_canvas.yview_scroll(int(-1*(e.delta/120)), "units"))

        character_engine_row_idx = 0

        # Add number of characters setting
        ttk.Label(self.character_engine_content_frame, text="Number of Characters per Conversation:").grid(
            row=character_engine_row_idx, column=0, padx=SPACING, pady=SPACING, sticky="e"
        )
        self.num_characters_var_editor = tk.StringVar(value="1")
        num_chars_entry = ttk.Entry(self.character_engine_content_frame, width=10, textvariable=self.num_characters_var_editor)
        num_chars_entry.grid(row=character_engine_row_idx, column=1, padx=SPACING, pady=SPACING, sticky="w")
        ttk.Label(self.character_engine_content_frame, text="(1-10, requires enough character cards)").grid(
            row=character_engine_row_idx, column=2, padx=SPACING, pady=SPACING, sticky="w"
        )
        character_engine_row_idx += 1

        def add_character_engine_text_area(label_text, var_name, height=4):
            nonlocal character_engine_row_idx
            ttk.Label(self.character_engine_content_frame, text=label_text).grid(row=character_engine_row_idx, column=0, padx=SPACING, pady=SPACING, sticky="nw")
            text_widget = scrolledtext.ScrolledText(self.character_engine_content_frame, wrap=tk.WORD, height=height, width=130, undo=True)
            text_widget.grid(row=character_engine_row_idx, column=1, padx=SPACING, pady=SPACING, sticky="ew")
            setattr(self, var_name, text_widget)
            character_engine_row_idx += 1

        # Add checkbox to enable character engine
        self.enable_character_engine_var_editor = tk.BooleanVar()
        self.include_names_in_prompt_var_editor = tk.BooleanVar(value=True)
        self.enable_character_checkbox = ttk.Checkbutton(
            self.character_engine_content_frame,
            text="Enable Character Engine (random character profiles in conversations)",
            variable=self.enable_character_engine_var_editor,
            command=self._toggle_character_engine_fields
        )
        self.enable_character_checkbox.grid(row=character_engine_row_idx, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="w")
        character_engine_row_idx += 1

        ttk.Checkbutton(
            self.character_engine_content_frame,
            text="Include Character Names in Prompt",
            variable=self.include_names_in_prompt_var_editor
        ).grid(row=character_engine_row_idx, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="w")
        character_engine_row_idx += 1

        # Add checkbox to enable emotional states
        self.enable_emotional_states_var_editor = tk.BooleanVar()
        ttk.Checkbutton(self.character_engine_content_frame, text="Enable Emotional States (randomly assign to conversations)",
                variable=self.enable_emotional_states_var_editor, command=self._toggle_emotional_states_fields).grid(row=character_engine_row_idx, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="w")
        character_engine_row_idx += 1

        # Add emotional states text area
        ttk.Label(self.character_engine_content_frame, text="Emotional States (one per line):").grid(row=character_engine_row_idx, column=0, padx=SPACING, pady=SPACING, sticky="nw")
        emotional_states_text = scrolledtext.ScrolledText(self.character_engine_content_frame, wrap=tk.WORD, height=6, width=130, undo=True)
        emotional_states_text.grid(row=character_engine_row_idx, column=1, padx=SPACING, pady=SPACING, sticky="ew")
        setattr(self, 'emotional_states_text', emotional_states_text)
        character_engine_row_idx += 1

        self.enable_class_selection_var_editor = tk.BooleanVar()
        ttk.Checkbutton(self.character_engine_content_frame,
                        text="Enable Class Selection (fantasy classes like mage, warlock, rogue, etc.)",
                        variable=self.enable_class_selection_var_editor,
                        command=self._toggle_class_fields).grid(row=character_engine_row_idx, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="w")
        character_engine_row_idx += 1

        # NEW: Add checkbox for Setting field
        self.enable_setting_selection_var_editor = tk.BooleanVar()
        ttk.Checkbutton(self.character_engine_content_frame,
                        text="Enable Setting Selection (custom location/environment for each character)",
                        variable=self.enable_setting_selection_var_editor,
                        command=self._toggle_setting_fields).grid(row=character_engine_row_idx, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="w")
        character_engine_row_idx += 1

        # --- Character Table (Card-based layout) ---
        self.character_entries = []  # List of dicts with StringVar per field per character

        character_table_lf = ttk.LabelFrame(
            self.character_engine_content_frame,
            text="Character Profiles (Each card is one complete character)"
        )
        character_table_lf.grid(
            row=character_engine_row_idx, column=0, columnspan=2,
            padx=SPACING, pady=SPACING, sticky="nsew"
        )
        character_engine_row_idx += 1

        # Container for character cards (NO nested canvas, outer tab canvas handles scrolling)
        self.character_cards_frame = ttk.Frame(character_table_lf)
        self.character_cards_frame.pack(fill=tk.BOTH, expand=True, padx=SPACING, pady=SPACING)

        # Add Character button
        self.add_char_btn = ttk.Button(
            self.character_engine_content_frame,
            text="➕ Add Character",
            command=self._add_character_row
        )
        self.add_char_btn.grid(
            row=character_engine_row_idx, column=0, columnspan=2,
            padx=SPACING, pady=SPACING, sticky="w"
        )
        character_engine_row_idx += 1

        # Add 3 default empty character slots
        for _ in range(3):
            self._add_character_row()

        self.character_engine_content_frame.grid_columnconfigure(1, weight=1)

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
        self.detection_canvas.bind("<MouseWheel>", lambda e: self.detection_canvas.yview_scroll(int(-1*(e.delta/120)), "units"))

        self.gender_var_editor = tk.StringVar() # Editor's local var for gender
        gender_frame = ttk.Frame(self.detection_content_frame)
        gender_frame.grid(row=0, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="w")
        ttk.Label(gender_frame, text="Assistant Persona Gender (for user speaking detection):").pack(side=tk.LEFT, padx=(0,10))
        ttk.Radiobutton(gender_frame, text="Female", variable=self.gender_var_editor, value="female", command=lambda: self.on_gender_change_editor_handler()).pack(side=tk.LEFT)
        ttk.Radiobutton(gender_frame, text="Male", variable=self.gender_var_editor, value="male", command=lambda: self.on_gender_change_editor_handler).pack(side=tk.LEFT)
        ttk.Radiobutton(gender_frame, text="Neutral", variable=self.gender_var_editor, value="neutral", command=lambda: self.on_gender_change_editor_handler).pack(side=tk.LEFT)
        
        ttk.Checkbutton(self.detection_content_frame, text="Disable User Impersonation Detection (Globally)", variable=self.no_user_impersonation_var).grid(row=1, column=0, columnspan=2, padx=SPACING, pady=SPACING, sticky="w")

        col1_frame = ttk.Frame(self.detection_content_frame); col1_frame.grid(row=2, column=0, padx=SPACING, pady=SPACING, sticky="nsew")
        col2_frame = ttk.Frame(self.detection_content_frame); col2_frame.grid(row=2, column=1, padx=SPACING, pady=SPACING, sticky="nsew")
        self.detection_content_frame.grid_columnconfigure(0, weight=1); self.detection_content_frame.grid_columnconfigure(1, weight=1)
        self.detection_content_frame.grid_rowconfigure(2, weight=1)
        
        def add_detection_list_pair(parent_frame, lf_text, phrases_var_name, fixes_var_name): # Helper for detection list pairs
            lf = ttk.LabelFrame(parent_frame, text=lf_text)
            lf.pack(padx=SPACING, pady=SPACING, fill="both", expand=True)
            ttk.Label(lf, text="Detection Phrases (one per line):").pack(anchor="w")
            phrases_text = scrolledtext.ScrolledText(lf, wrap=tk.WORD, height=12, undo=True)
            phrases_text.pack(fill="both", expand=True, pady=(0,5))
            setattr(self, phrases_var_name, phrases_text)
            ttk.Label(lf, text="Fixes (appended to system prompt or for fixer rotation):").pack(anchor="w")
            fixes_text = scrolledtext.ScrolledText(lf, wrap=tk.WORD, height=12, undo=True)
            fixes_text.pack(fill="both", expand=True)
            setattr(self, fixes_var_name, fixes_text)

        add_detection_list_pair(col1_frame, "Refusal Detection", 'refusal_phrases_text', 'refusal_fixes_text')
        add_detection_list_pair(col1_frame, "User Speaking Detection (Phrases/Fixes are Gender Specific)", 'user_speaking_phrases_text', 'user_speaking_fixes_text')
        add_detection_list_pair(col2_frame, "Slop Detection", 'slop_phrases_text', 'slop_fixes_text')
        add_detection_list_pair(col2_frame, "Anti-Slop Detection", 'anti_slop_phrases_text', 'anti_slop_fixes_text')

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
        self.notebook.add(self.samplers_tab, text="Samplers (Model Params)")

        # Setup Canvas and Scrollbar for Samplers Tab
        self.samplers_canvas = tk.Canvas(self.samplers_tab)
        self.samplers_scrollbar = tk.Scrollbar(self.samplers_tab, orient="vertical", command=self.samplers_canvas.yview, width=self.scrollbar_width, bg='#5a5a70', troughcolor='#2a2a35', activebackground='#8a8aa0', highlightthickness=0, bd=0)
        sampler_params_frame = ttk.Frame(self.samplers_canvas)

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
        self.samplers_canvas.bind("<MouseWheel>", lambda e: self.samplers_canvas.yview_scroll(int(-1*(e.delta/120)), "units"))

        ttk.Label(sampler_params_frame, text="Sampler Priority (Order for API payload, one per line, e.g., temperature, top_p):").grid(row=0, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="w")
        self.sampler_priority_text = scrolledtext.ScrolledText(sampler_params_frame, wrap=tk.WORD, height=5, width=30, undo=True); self.sampler_priority_text.grid(row=1, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew")
        
        sampler_row = 2 
        def add_sampler_param(label, var_name, example): # Helper for sampler parameters
            nonlocal sampler_row
            ttk.Label(sampler_params_frame, text=label).grid(row=sampler_row, column=0, padx=SPACING, pady=SPACING, sticky="e")
            var = tk.StringVar(); setattr(self, var_name, var)
            ttk.Entry(sampler_params_frame, width=10, textvariable=var).grid(row=sampler_row, column=1, padx=SPACING, pady=SPACING, sticky="w")
            ttk.Label(sampler_params_frame, text=example).grid(row=sampler_row, column=2, padx=SPACING, pady=SPACING, sticky="w")
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
        self.logit_bias_text = scrolledtext.ScrolledText(sampler_params_frame, wrap=tk.WORD, height=5, width=50, undo=True)
        self.logit_bias_text.grid(row=sampler_row, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew")
        sampler_row += 1
        self.enable_thinking_mode_var = tk.StringVar(value="default")
        slop_fixer_sampler_lf = ttk.LabelFrame(sampler_params_frame, text="Slop Fixer LLM Sampler Overrides (API Slot 5 - Optional)")
        slop_fixer_sampler_lf.grid(row=sampler_row, column=0, columnspan=3, padx=SPACING, pady=SPACING, sticky="ew"); sampler_row+=1
        sf_sampler_row = 0 
        def add_slop_fixer_param(label, var_name, example): # Helper for slop fixer sampler params
            nonlocal sf_sampler_row
            ttk.Label(slop_fixer_sampler_lf, text=label).grid(row=sf_sampler_row, column=0, padx=SPACING, pady=SPACING, sticky="e")
            var = tk.StringVar(); setattr(self, var_name, var)
            ttk.Entry(slop_fixer_sampler_lf, width=10, textvariable=var).grid(row=sf_sampler_row, column=1, padx=SPACING, pady=SPACING, sticky="w")
            ttk.Label(slop_fixer_sampler_lf, text=example).grid(row=sf_sampler_row, column=2, padx=SPACING, pady=SPACING, sticky="w")
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
        asf_sampler_row = 0
        def add_anti_slop_fixer_param(label, var_name, example): # Helper for anti-slop fixer sampler params
            nonlocal asf_sampler_row
            ttk.Label(anti_slop_fixer_sampler_lf, text=label).grid(row=asf_sampler_row, column=0, padx=SPACING, pady=SPACING, sticky="e")
            var = tk.StringVar(); setattr(self, var_name, var)
            ttk.Entry(anti_slop_fixer_sampler_lf, width=10, textvariable=var).grid(row=asf_sampler_row, column=1, padx=SPACING, pady=SPACING, sticky="w")
            ttk.Label(anti_slop_fixer_sampler_lf, text=example).grid(row=asf_sampler_row, column=2, padx=SPACING, pady=SPACING, sticky="w")
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
        ttk.Label(sampler_params_frame, text="(Controls chat_template_kwargs enable_thinking)").grid(row=sampler_row, column=2, padx=SPACING, pady=SPACING, sticky="w")
        sampler_row += 1


        # --- Profiles Tab ---
        self.profiles_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.profiles_tab, text="Profiles")
        profiles_main_frame = ttk.Frame(self.profiles_tab)
        profiles_main_frame.pack(padx=SPACING, pady=SPACING, fill="both", expand=True)

        load_profile_frame = ttk.LabelFrame(profiles_main_frame, text="Load Profile")
        load_profile_frame.pack(padx=SPACING, pady=SPACING, fill="x", expand=False)
        ttk.Label(load_profile_frame, text="Available Profiles:").grid(row=0, column=0, padx=SPACING, pady=SPACING, sticky="e")
        self.profile_list_var = tk.StringVar()
        self.profile_combobox = ttk.Combobox(load_profile_frame, textvariable=self.profile_list_var, width=40, state="readonly")
        self.profile_combobox.grid(row=0, column=1, padx=SPACING, pady=SPACING, sticky="ew")
        ttk.Button(load_profile_frame, text="Load Selected Profile to Editor & config.yml", command=self._load_selected_profile_handler).grid(row=0, column=2, padx=SPACING, pady=SPACING)
        ttk.Button(load_profile_frame, text="Delete Selected Profile", command=self._delete_selected_profile_handler).grid(row=0, column=3, padx=SPACING, pady=SPACING)
        load_profile_frame.grid_columnconfigure(1, weight=1) 

        save_profile_frame = ttk.LabelFrame(profiles_main_frame, text="Save Current Editor Configuration As Profile")
        save_profile_frame.pack(padx=SPACING, fill="x", expand=False, pady=(10,5))
        ttk.Label(save_profile_frame, text="New Profile Name:").grid(row=0, column=0, padx=SPACING, pady=SPACING, sticky="e")
        self.new_profile_name_var = tk.StringVar()
        ttk.Entry(save_profile_frame, textvariable=self.new_profile_name_var, width=43).grid(row=0, column=1, padx=SPACING, pady=SPACING, sticky="ew")
        ttk.Button(save_profile_frame, text="Save Current Editor Config As Profile...", command=self._save_profile_as_handler).grid(row=0, column=2, padx=SPACING, pady=SPACING)
        save_profile_frame.grid_columnconfigure(1, weight=1) 
        
        # --- Editor Toolbar and Status Bar ---
        self.status = ttk.Label(self, text="Ready", foreground="lightgray")  # Status bar at the bottom of editor
        self.status.pack(side=tk.BOTTOM, fill=tk.X, padx=SPACING, pady=(0,5))
        toolbar = ttk.Frame(self) # Toolbar for main editor actions
        ttk.Button(toolbar, text="Save to config.yml", command=self.save_config_handler).pack(side=tk.LEFT, padx=SPACING)
        ttk.Button(toolbar, text="Revert Changes (Reload from Files)", command=self.load_config_handler).pack(side=tk.LEFT, padx=SPACING)
        ttk.Button(toolbar, text="Validate Current Editor Values", command=self.validate_config_handler).pack(side=tk.LEFT, padx=SPACING)
        toolbar.pack(fill=tk.X, pady=SPACING, padx=SPACING)
        
        self.load_config_handler() # Load initial config into editor UI
        self._populate_profile_list() # Populate profiles dropdown
        self.protocol("WM_DELETE_WINDOW", self.on_close_editor) # Handle editor close button

    # --- NEW: Tab & Section Search Methods ---
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
            lf_text = widget.cget("text").lower()
            if query in lf_text:
                self.notebook.select(tab_id)
                widget.tkraise()
                self._scroll_canvas_to(widget)
                self.search_entry.config(foreground="green")
                return True
        for child in widget.winfo_children():
            if self._find_and_raise_label_frame(child, query, tab_id):
                return True
        return False

    def _scroll_canvas_to(self, target_widget):
        target_widget.update_idletasks()
        parent = target_widget.master
        while parent:
            if isinstance(parent, tk.Canvas):
                # Calculate scroll fraction to bring widget near the top
                y_pos = max(0, (target_widget.winfo_y() - 20) / parent.winfo_height())
                parent.yview_moveto(min(1.0, y_pos))
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
        try:
            # Validate configuration before saving
            if not self.validate_config_handler(show_success_message=False):
                # Check if status widget exists before updating
                if hasattr(self.status, 'winfo_exists') and self.status.winfo_exists():
                    self.status.config(text="Save failed: Validation errors found.", foreground="red")
                if not silent:
                    messagebox.showerror("Validation Error", "Cannot save due to validation errors. Please correct them.")
                return

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


    def on_close_editor(self):
        """Handles saving config when editor is closed via 'X' button."""
        log_message("ConfigEditor: Close button clicked. Saving configuration automatically.", "INFO")

        # Unbind all mouse wheel bindings to prevent stale callbacks
        self.samplers_canvas.unbind("<MouseWheel>")
        self.api_canvas.unbind("<MouseWheel>")
        self.generation_canvas.unbind("<MouseWheel>")
        self.prompts_canvas.unbind("<MouseWheel>")
        self.lore_canvas.unbind("<MouseWheel>")
        self.character_engine_canvas.unbind("<MouseWheel>")
        self.detection_canvas.unbind("<MouseWheel>")

        try:
            self.save_config_handler(silent=True)
        except Exception as e:
            log_message(f"ConfigEditor: Error during auto-save on close: {e}", "ERROR")
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
                'slop_to_anti_slop_fallback': self.slop_to_anti_slop_fallback_var_editor.get()
            },
            'quality': {
                'enabled': self.quality_enabled_var_editor.get(),
                'use_llm_scoring': self.quality_use_llm_var_editor.get(),
                'output_filter': self.quality_output_filter_var_editor.get(),
                'min_score_threshold': int(self.quality_min_threshold_var.get() or 50),
                'max_chars_for_scoring': int(self.quality_max_chars_var.get() or 8000),
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
                'lore': sanitize_input(self.lore_text.get("1.0", tk.END).strip()),
            'character': {
                'enabled': self.enable_character_engine_var_editor.get(),
                'include_names_in_prompt': self.include_names_in_prompt_var_editor.get(),
                'class_enabled': self.enable_class_selection_var_editor.get(),
                'setting_enabled': self.enable_setting_selection_var_editor.get(),
                'num_characters': int(self.num_characters_var_editor.get()),
                'characters': [
                    {
                        'name': sanitize_input(data['name'].get().strip()),
                        'age': sanitize_input(data['age'].get().strip()),
                        'gender': sanitize_input(data['gender'].get().strip()),
                        'race': sanitize_input(data['race'].get().strip()),
                        'job': sanitize_input(data['job'].get().strip()),
                        'clothing': sanitize_input(data['clothing'].get().strip()),
                        'appearance': sanitize_input(data['appearance'].get().strip()),
                        'traits': sanitize_input(data['traits'].get().strip()),
                        'backstory': sanitize_input(data['backstory'].get().strip()),
                        'personality': sanitize_input(data['personality'].get().strip()),
                        'setting': sanitize_input(data['setting'].get().strip()) if self.enable_setting_selection_var_editor.get() else '',
                        'class': sanitize_input(data['class'].get().strip())
                    }
                        for data in self.character_entries
                        if any(data[k].get().strip() for k in ['name', 'race', 'job', 'clothing', 'appearance', 'backstory', 'personality', 'traits', 'setting', 'class', 'age', 'gender'])
                    ]
                },
                'emotional_states': {
                    'enabled': self.enable_emotional_states_var_editor.get(),
                    'states': [sanitize_input(line.strip()) for line in self.emotional_states_text.get("1.0", tk.END).split('\n') if line.strip()]
                },
            },
            'detection': {
                'no_user_impersonation': self.no_user_impersonation_var.get(), # Get from global Tkinter var
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
                'slop_fixer_params': slop_fixer_params_to_save,
                'anti_slop_params': anti_slop_fixer_params_to_save
            },
            'gender': self.gender_var_editor.get() # Save the currently selected gender for persona
        }
        # Preserve pricing settings from the loaded config if they exist
        if 'pricing' in self.global_config.config:
            config_data['pricing'] = self.global_config.config['pricing']
        return config_data

    def _save_profile_as_handler(self):
        """Handles saving the current editor configuration as a new named profile."""
        profile_name = self.new_profile_name_var.get().strip()
        if not profile_name:
            messagebox.showerror("Error", "Profile name cannot be empty.")
            self.status.config(text="Profile name empty.", foreground="red")
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
            # Load quality scoring config
            quality_conf = config.get('quality', {})
            self.quality_enabled_var_editor.set(quality_conf.get('enabled', True))
            self.quality_use_llm_var_editor.set(quality_conf.get('use_llm_scoring', False))
            self.quality_output_filter_var_editor.set(quality_conf.get('output_filter', False))
            self.quality_min_threshold_var.set(str(quality_conf.get('min_score_threshold', 50)))
            self.quality_max_chars_var.set(str(quality_conf.get('max_chars_for_scoring', 8000)))
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
                old_clothing = character_conf.get('clothing', [])
                old_appearance = character_conf.get('appearance', [])
                old_backstory = character_conf.get('backstory', [])
                old_personality = character_conf.get('personality', [])
                old_traits = character_conf.get('traits', [])
                old_setting = character_conf.get('setting', [])
                old_class = character_conf.get('class', [])

                if any([old_names, old_races, old_jobs, old_clothing, old_appearance, old_backstory, old_personality, old_traits, old_setting, old_class, old_genders]):
                    max_len = max(
                        len(old_names), len(old_races), len(old_jobs), len(old_clothing),
                        len(old_appearance), len(old_backstory), len(old_personality), len(old_traits),
                        len(old_setting), len(old_class), len(old_genders)
                    )
                    for i in range(max_len):
                        characters_list.append({
                            'name': old_names[i] if i < len(old_names) else '',
                            'age': old_ages[i] if i < len(old_ages) else '25',
                            'gender': old_genders[i] if i < len(old_genders) else '',
                            'race': old_races[i] if i < len(old_races) else '',
                            'job': old_jobs[i] if i < len(old_jobs) else '',
                            'clothing': old_clothing[i] if i < len(old_clothing) else '',
                            'appearance': old_appearance[i] if i < len(old_appearance) else '',
                            'backstory': old_backstory[i] if i < len(old_backstory) else '',
                            'personality': old_personality[i] if i < len(old_personality) else '',
                            'traits': old_traits[i] if i < len(old_traits) else '',
                            'setting': old_setting[i] if i < len(old_setting) else '',
                            'class': old_class[i] if i < len(old_class) else ''
                        })

            # Populate UI with loaded data (limit to MAX_CHARACTER_CARDS)
            if characters_list:
                for i, char_data in enumerate(characters_list):
                    if i >= int(self.max_character_cards_var.get()):
                        log_message(f"Skipping character {i+1} - limit reached", "WARNING")
                        break
                    self._add_character_row(character_data=char_data)
            else:
                # Add 3 empty rows if config is empty
               default_empty_rows = min(3, int(self.max_character_cards_var.get()))
               for _ in range(default_empty_rows):
                   self._add_character_row()

            # Apply the enabled/disabled state and class visibility
            self._toggle_character_engine_fields()
            self._update_class_column_visibility()
            self._update_setting_column_visibility()

            # NEW: Load emotional states
            emotional_states_conf = prompts_config.get('emotional_states', {})
            self.enable_emotional_states_var_editor.set(emotional_states_conf.get('enabled', False))
            self.emotional_states_text.delete(1.0, tk.END)
            self.emotional_states_text.insert(tk.END, "\n".join(emotional_states_conf.get('states', [])))
            self._toggle_emotional_states_fields()

            detection_conf = config.get('detection', {})
            self.no_user_impersonation_var.set(detection_conf.get('no_user_impersonation', False)) # Set global var
            refusal_conf = detection_conf.get('refusal', {})
            self.refusal_phrases_text.delete(1.0, tk.END); self.refusal_phrases_text.insert(tk.END, "\n".join(refusal_conf.get('phrases', [])))
            self.refusal_fixes_text.delete(1.0, tk.END); self.refusal_fixes_text.insert(tk.END, "\n".join(refusal_conf.get('fixes', [])))
            
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
            int(self.subject_size_var.get()); int(self.context_size_var.get()); int(self.max_attempts_var.get())
            int(self.db_port_var.get())
            int(self.db_pool_size_var.get())
            num_turns_val = int(self.num_turns_var.get()); assert num_turns_val > 0, "Number of turns must be > 0"
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
            get_text_as_list(self.sampler_priority_text)

            logit_bias_str = self.logit_bias_text.get("1.0", tk.END).strip()
            if logit_bias_str:
                try:
                    json.loads(logit_bias_str)
                except json.JSONDecodeError:
                    raise ValueError("Logit bias must be valid JSON format")

            if self.enable_emotional_states_var_editor.get():
                get_text_as_list(self.emotional_states_text)
            
            max_cards_val = int(self.max_character_cards_var.get())
            assert max_cards_val > 0, "Max character cards must be > 0"
            assert max_cards_val <= 100, "Max character cards should not exceed 100"
            quality_threshold = int(self.quality_min_threshold_var.get() or 50)
            assert 0 <= quality_threshold <= 100, "Quality threshold must be 0-100"
            quality_max_chars = int(self.quality_max_chars_var.get() or 8000)
            assert quality_max_chars > 100, "Max chars for scoring must be > 100"

            if show_success_message:
                self.status.config(text="Validation successful (basic checks).", foreground="green")
            log_message("ConfigEditor: Validation successful.", "DEBUG")
            return True 
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

    def _update_canvas_scrollregion(self, canvas_widget):
        """Safely update canvas scrollregion with bounds checking."""
        try:
            canvas_widget.update_idletasks()
            bbox = canvas_widget.bbox("all")

            if bbox and len(bbox) == 4:
                x1, y1, x2, y2 = bbox

                # Sanity check - reject unreasonably large values
                MAX_CANVAS_SIZE = 50000  # pixels
                if all(0 <= val <= MAX_CANVAS_SIZE for val in bbox):
                    # Add small padding
                    padding = 10
                    canvas_widget.configure(scrollregion=(x1 - padding, y1 - padding,
                                                           x2 + padding, y2 + padding))
                else:
                    log_message(f"Canvas bbox out of bounds: {bbox}", "WARNING")
        except Exception as e:
            log_message(f"Error updating canvas scrollregion: {e}", "ERROR")

    def _add_character_row(self, character_data=None):
        """Adds a new character card with a clean, organized layout."""
        max_cards_str = self.max_character_cards_var.get()
        max_cards = int(max_cards_str) if max_cards_str and max_cards_str.strip() else 10

        if len(self.character_entries) >= max_cards:
            messagebox.showwarning("Limit Reached", f"Maximum {max_cards} character cards allowed.")
            return

        if character_data is None:
            character_data = {}

        row_index = len(self.character_entries)

        # === Card Container ===
        card_frame = ttk.LabelFrame(
            self.character_cards_frame,
            text=f"  ✦ Character {row_index + 1}  ",
            padding=15
        )
        card_frame.pack(fill=tk.X, pady=15, padx=15)

        entry_vars = {}

        # === Styling Constants ===
        LABEL_FONT = ('Segoe UI', 9, 'bold')
        ENTRY_FONT = ('Segoe UI', 9)
        LABEL_FG = '#e0e0e0'
        ENTRY_BG = '#2a2a35'
        ENTRY_FG = '#ffffff'

        # === Helper: Create a labeled field ===
        def create_field(parent, label_text, var_key, row, col,
                        width=25, is_multiline=False, height=3, colspan=1):
            # Label
            lbl = ttk.Label(parent, text=label_text, font=LABEL_FONT, foreground=LABEL_FG)
            lbl.grid(row=row, column=col*2, padx=(10, 5), pady=8, sticky="ne")
            entry_vars[f'{var_key}_label'] = lbl  # Store label reference in entry_vars

            var = tk.StringVar(value=character_data.get(var_key, ''))

            if is_multiline:
                widget = tk.Text(parent, width=width, height=height,
                                font=ENTRY_FONT, bg=ENTRY_BG, fg=ENTRY_FG,
                                wrap=tk.WORD, relief=tk.SOLID, borderwidth=1)
                widget.insert("1.0", character_data.get(var_key, ''))
                widget.grid(row=row, column=col*2+1, padx=5, pady=8, sticky="ew", columnspan=colspan)

                def on_change(e=None):
                    var.set(widget.get("1.0", tk.END).strip())
                widget.bind("<KeyRelease>", on_change)
                widget.bind("<FocusOut>", on_change)
            else:
                widget = ttk.Entry(parent, textvariable=var, width=width,
                                font=ENTRY_FONT)
                widget.grid(row=row, column=col*2+1, padx=5, pady=8, sticky="ew", columnspan=colspan)

            entry_vars[var_key] = var
            entry_vars[f'{var_key}_widget'] = widget
            return widget

        # Configure grid columns (even = labels, odd = fields)
        for i in range(10):
            card_frame.columnconfigure(i, weight=1 if i % 2 == 1 else 0)

        # === Row 0: Name and Age ===
        create_field(card_frame, "Name:", 'name', row=0, col=0, width=20)

        # === FIX: Age field with proper label reference ===
        age_var = tk.StringVar(value=character_data.get('age', '25'))
        age_label = ttk.Label(card_frame, text="Age:", font=LABEL_FONT, foreground=LABEL_FG)
        age_label.grid(row=0, column=5, padx=(10, 5), pady=8, sticky="ne")

        age_widget = ttk.Entry(card_frame, textvariable=age_var, width=5, font=ENTRY_FONT)
        age_widget.grid(row=0, column=6, padx=5, pady=8, sticky="w")

        def validate_age(new_value):
            if not new_value:  # Allow empty for typing
                return True
            try:
                age = int(new_value)
                # Currently allows 0-999 for typing flexibility
                return 0 <= age <= 999
            except ValueError:
                return False

        validation_cmd = card_frame.register(validate_age)
        age_widget.config(validate="key", validatecommand=(validation_cmd, "%P"))

        # Store references in entry_vars
        entry_vars['age'] = age_var
        entry_vars['age_widget'] = age_widget
        entry_vars['age_label'] = age_label  # FIX: Use age_label, not lbl

        # === Row 1: Race and Job ===
        create_field(card_frame, "Race:", 'race', row=1, col=0, width=18)
        create_field(card_frame, "Job:", 'job', row=1, col=2, width=18)
        create_field(card_frame, "Gender:", 'gender', row=1, col=4, width=12)

        # === Row 2: Clothing (full width) ===
        create_field(card_frame, "Clothing:", 'clothing', row=2, col=0,
                    width=50, is_multiline=True, height=2, colspan=4)

        # === Row 3: Appearance (full width) ===
        create_field(card_frame, "Appearance:", 'appearance', row=3, col=0,
                    width=50, is_multiline=True, height=2, colspan=4)

        # === Row 4: Backstory (full width) ===
        create_field(card_frame, "Backstory:", 'backstory', row=4, col=0,
                    width=50, is_multiline=True, height=3, colspan=4)

        # === Row 5: Personality (full width) ===
        create_field(card_frame, "Personality:", 'personality', row=5, col=0,
                    width=50, is_multiline=True, height=3, colspan=4)

        # === Row 6: Traits (full width) ===
        create_field(card_frame, "Traits:", 'traits', row=6, col=0,
                    width=50, is_multiline=True, height=2, colspan=4)

        # === Row 7: Setting (full width) - Hidden by default ===
        create_field(card_frame, "Setting:", 'setting', row=7, col=0,
                    width=50, is_multiline=True, height=2, colspan=4)
        entry_vars['setting_label'].grid_remove()
        entry_vars['setting_widget'].grid_remove()

        # === Row 8: Class & Actions (separate row at bottom) ===
        action_row_frame = ttk.Frame(card_frame)
        action_row_frame.grid(row=8, column=0, columnspan=10, sticky="ew", pady=(15, 5))
        action_row_frame.columnconfigure(0, weight=0)
        action_row_frame.columnconfigure(1, weight=0)
        action_row_frame.columnconfigure(2, weight=1)

        class_lbl = ttk.Label(action_row_frame, text="⚔ Class:", font=LABEL_FONT, foreground=LABEL_FG)
        class_lbl.grid(row=0, column=0, padx=(10, 5), sticky="w")
        entry_vars['class_label'] = class_lbl

        class_var = tk.StringVar(value=character_data.get('class', ''))
        class_entry = ttk.Entry(action_row_frame, textvariable=class_var, width=20, font=ENTRY_FONT)
        class_entry.grid(row=0, column=1, padx=5, sticky="w")
        entry_vars['class'] = class_var
        entry_vars['class_entry'] = class_entry

        # Spacer
        ttk.Label(action_row_frame, text="").grid(row=0, column=2)

        # Delete button - far right
        delete_btn = ttk.Button(
            card_frame, text="✕ Remove Character",
            command=lambda idx=row_index: self._remove_character_row(idx),
            style='Danger.TButton'
        )
        delete_btn.grid(row=9, column=0, columnspan=10, padx=(20, 10), pady=(5, 15), sticky="e")
        entry_vars['delete_btn'] = delete_btn
        entry_vars['class_frame'] = action_row_frame

        entry_vars['frame'] = card_frame
        self.character_entries.append(entry_vars)

        self._update_character_card_labels()
        self._update_canvas_scrollregion(self.character_engine_canvas)

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
            entry_data = self.character_entries[row_index]
            entry_data['frame'].destroy()
            self.character_entries.pop(row_index)

            # Update delete button commands for remaining rows
            for i, data in enumerate(self.character_entries):
                data['delete_btn'].config(
                    command=lambda idx=i: self._remove_character_row(idx)
                )

            self._update_character_card_labels()
            # Update scroll region (with safety check)
            self._update_canvas_scrollregion(self.character_engine_canvas)

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

