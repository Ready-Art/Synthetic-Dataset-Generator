# 📚 ReadyArt Synthetic Dataset Generator v7.8.5

A highly advanced, multi-threaded Python application designed for generating high-quality, multi-turn synthetic conversation datasets using LLM APIs. Built with robust error handling, real-time monitoring, crash recovery, and a comprehensive Tkinter GUI, this tool is ideal for researchers, developers, and AI engineers creating training/fine-tuning datasets at scale.

---

## 📖 Overview

This generator automates the creation of structured conversational data by:
1. Extracting or generating subjects & contexts from large text files.
2. Prompting LLMs to generate initial questions.
3. Conducting multi-turn Q/A dialogues with configurable turn counts.
4. Automatically detecting and fixing common LLM output issues (refusals, slop, user impersonation, incomplete quotes).
5. Outputting clean, formatted JSONL datasets ready for fine-tuning.

The system is built for **reliability, scalability, and transparency**, featuring per-API rate limiting, response caching, live dashboard analytics, and seamless crash recovery.

---

## ✨ Key Features

### 🔹 Multi-API Engine & Execution Modes
- **6 Configurable API Slots**: Slots 1–4 for primary generation, Slot 5 for Slop Fixing, Slot 6 for Anti-Slop Fixing.
- **Master Duplication Mode**: Runs each task simultaneously across multiple enabled APIs for parallel generation, comparison, or dataset diversification.
- **Collaborative/Non-Duplication Mode**: Distributes tasks across enabled APIs to maximize throughput.
- **Per-Thread Configuration**: Each API can have independent thread counts, models, keys, and sampler parameters.

### 🔹 Intelligent Question & Context Generation
- **Predefined Questions**: Use a `questions.txt` file to drive generation.
- **Auto-Chunking**: Randomly extracts `subject` and `context` windows from large `.txt` files in the `input/` directory.
- **Repetition Avoidance**: Maintains a configurable history of recent questions to prevent duplicate prompts.

### 🔹 Advanced Issue Detection & Auto-Correction
- **Real-Time Detection**: Identifies refusals, user impersonation, "slop" (undesirable filler/phrasing), anti-slop patterns, and unbalanced quotation marks.
- **Multi-Layer Fix Pipeline**:
  1. **Jailbreaks & System Prompt Fixes**: Appends corrective instructions dynamically.
  2. **Sentence-Level Rewriting**: Delegates problematic sentences to dedicated Slop/Anti-Slop Fixer LLMs for precise rewriting without regenerating the entire response.
  3. **Fallback Rotations**: Cycles through pre-configured fix instructions if initial attempts fail.
- **Issue Timestamping**: Tracks all detections over a rolling 60-minute window with live graph visualization.

### 🔹 Character & Emotional State Engine
- **Dynamic Persona Injection**: Randomly assigns names, ages, jobs, clothing, appearances, and backstories to conversations.
- **Emotional State Modulation**: Applies randomized emotional tones (e.g., `happy`, `angry`, `neutral`) to influence response style and vocabulary.

### 🔹 Crash Recovery & State Management
- **Auto-Save Progress**: Writes `generation_state.json` periodically with completed task IDs, counters, system prompt states, and config snapshots.
- **Smart Resume**: Detects configuration changes on resume. Prompts for compatibility confirmation to prevent data corruption or duplicate processing.
- **Clean State Reset**: Safely backs up old output files before starting fresh runs.

### 🔹 Real-Time GUI & Dashboard
- **Live Progress Tracking**: Per-API or global progress bars with estimated time remaining.
- **API Performance Monitoring**: Average/min/max response times per slot.
- **Rate Limit & Cost Tracking**: Live RPM status, token consumption, and estimated API cost.
- **Issue Logging**: Scrollable, syntax-highlighted logs for refusals, user-speaking, slop, anti-slop, and errors.
- **Time-Series Graph**: Visualizes issue frequency over the last 60 minutes.

### 🔹 Configuration & Profile Management
- **Built-In Config Editor**: Full GUI for editing `config.yml` without manual file manipulation.
- **Profile System**: Save, load, and delete named configuration profiles for quick switching between generation strategies.
- **Validation**: Automatic type-checking and range validation before saving.

---

## 🏗️ Architecture & How It Works

```
[Input Files / questions.txt] 
        ↓
[Task Queue (Thread-Safe)]
        ↓
[Worker Threads (Configurable per API)]
        ├── Fetch Task → Generate Question → Multi-Turn Loop
        ├── Call Primary API(s) → Detect Issues → Auto-Fix Pipeline
        ├── Post-Process Text → Format Output
        └── Write to JSONL → Update State → Queue Next Task
        ↓
[GUI Dashboard / Logger / Cache / Rate Limiter]
```

- **Thread Pool**: Dynamically sized based on API config. Workers pull from a shared `Queue`.
- **Rate Limiter**: Sliding-window RPM tracker per API slot. Sleeps safely outside locks to prevent thread starvation.
- **Caching**: MD5-hashed prompt caching via Valkey/Redis to skip redundant API calls.
- **Output Writer**: Thread-safe JSONL appending with per-API file routing in duplication mode.

---

## 🚀 Installation & Setup

### Prerequisites
- Python 3.8+
- pip

### 1. Clone & Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Project Structure
```
├── config/
│   └── config.yml          # Main configuration file
├── input/                  # Place .txt files for chunking here
├── output/                 # Generated JSONL, logs, and state files
├── generate.py             # Main application entry point
├── detection.py            # Issue detection logic
├── text_utils.py           # Post-processing utilities
├── config_loader.py        # YAML & profile management
├── api_handler.py          # Rate limiter & Valkey caching
├── logging_config.py       # Console & file logging
└── requirements.txt
```

### 3. Initial Configuration
1. Open the app: `python generate.py`
2. Click **Edit Config** to set up:
   - API URLs, Models, Keys (Slots 1–6)
   - Sampler parameters (temperature, top_p, max_tokens, etc.)
   - Detection phrases & fix instructions
   - Generation settings (turns, chunk sizes, output format)
3. Save configuration. The app will auto-create `config/config.yml`.

---

## ⚙️ Configuration Guide

All settings are managed via the built-in GUI editor or directly in `config/config.yml`. Key sections include:

| Section | Purpose |
|---------|---------|
| `api.apis` | Define up to 6 API endpoints, keys, models, threads, and RPM limits |
| `generation` | Chunk sizes, max attempts, turns, history size, timeout, output format |
| `prompts` | System prompts, question/answer templates, character & emotional state lists |
| `detection` | Refusal/slop/anti-slop/user-speaking phrases, jailbreaks, fix instructions |
| `samplers` | Global & per-task sampler overrides (question, answer, user reply, fixer APIs) |
| `valkey` | Optional Redis/Valkey caching host, port, DB, password |

> 💡 **Tip**: Use the **Profiles** tab in the config editor to save different setups (e.g., `Creative_Writing`, `Technical_QA`, `High_Temp_Exploration`).

---

## 🖥️ GUI & Dashboard

| Component | Function |
|-----------|----------|
| **Metrics Bar** | Live refusal %, user-speaking %, slop %, error %, token count, estimated cost |
| **Rate Limit Status** | Real-time RPM usage per API slot (color-coded: green → orange → red) |
| **Response Times** | Avg/min/max latency per API slot |
| **Progress Frame** | Per-API (duplication) or global progress bars with ETA |
| **Dashboard Notebook** | Tabs for Totals & API 1–6. Each shows recent issues with highlighted phrases |
| **Issue Graph** | 60-minute rolling histogram of detected issues |
| **Controls** | Start, Pause/Resume, Stop & Clear Job, Edit Config, Quit |

---

## 🔧 Advanced Features

### 🔄 Crash Recovery Workflow
1. App crashes or is force-closed.
2. On next launch, detects `output/generation_state.json`.
3. Prompts: `"Previous generation state found. Resume?"`
4. Validates config compatibility. If mismatched, warns of potential overlaps.
5. Restores completed task IDs, counters, and prompt states. Continues seamlessly.

### 🛡️ Auto-Fix Pipeline
```text
LLM Output → Detection Scan
   ├─ Refusal? → Append Jailbreak → Retry
   ├─ User Speaking? → Append Fix Instruction → Retry
   ├─ Slop? → Slop Fixer LLM rewrites sentence → Check again
   ├─ Anti-Slop? → Anti-Slop Fixer LLM rewrites → Check again
   └─ Incomplete Quote? → Append quote-pairing instruction → Retry
```
Max iterations and fallbacks are fully configurable per issue type.

### 📦 Output Formats
- **ShareGPT**: `[{"from": "human", "value": "..."}, {"from": "gpt", "value": "..."}]`
- **OpenAI/Jinja2**: `[{"role": "user", "content": "..."}, {"role": "assistant", "content": "..."}]`
- Files are saved as `output/output.jsonl` (or `output/output_api_slot_X.jsonl` in duplication mode).

---

## 🆘 Troubleshooting

| Issue | Solution |
|-------|----------|
| `API URL missing` or `Invalid URL format` | Verify endpoint in Config Editor. Ensure it starts with `http://` or `https://` |
| `Rate limit reached. Waiting...` | Increase RPM in config or reduce thread count for that API |
| `No tasks to process` | Ensure `input/` contains `.txt` files or enable `questions.txt` in config |
| `Config validation failed` | Check numeric fields (temp, top_p, tokens) for valid ranges. Editor highlights errors |
| `State file incompatible` | Config changed significantly since last run. Choose `No` to backup & start fresh, or `Yes` to force resume |
| `Valkey connection failed` | Verify host/port in config. App will disable caching but continue generation |

Logs are saved to `output/log.txt` with color-coded severity levels. Debug prompt payloads are written to `output/debug_prompt_api_slot_X.jsonl`.

---

## 📜 License & Contributing

AGPLv3 License

This project is licensed under AGPLv3. However, portions of this software were originally written by Sleep Deprived & WestFox35 (FrenzyBiscuit) and are licensed under the MIT license.

**Disclaimer**: Ensure compliance with all API provider Terms of Service, rate limits, and usage policies when running this generator at scale.

---

> 🌟 **Built for scale. Designed for quality.** Generate cleaner datasets, faster.
