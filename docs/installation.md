<!--
SPDX-License-Identifier: AGPL-3.0-or-later

ReadyArt Synthetic Dataset Generator
Copyright (C) 2026 Sleep Deprived, FrenzyBiscuit (WestFox35) and other contributors

All code here is AGPLv3, and has been for some time. Very early versions were MIT by
Sleep Deprived and FrenzyBiscuit (WestFox35), but the generator has changed so much that
it is unlikely any of that code survives. Unless you have the original MIT-era code to
compare against, treat this file as AGPLv3-only. See docs/licensing.md.
-->

# 📦 Requirements & Installation

- **Python 3.11+**
- **PostgreSQL** (optional, for database storage)
- **Valkey or Redis** (optional, for response caching)

An older interpreter is rejected at startup: `generate.py` checks `sys.version_info` and exits with a message telling you to install Python 3.11+.

## Python Dependencies

Install via `pip install -r requirements.txt`:

```
requests>=2.32.4,<3
python-dotenv==1.0.0
PyYAML==6.0.1
tk==0.1.0
matplotlib
psutil
colorama
redis
ttkbootstrap
psycopg2-binary
```

**Note:** `tkinter` is usually included with Python installations but may need to be installed separately on some Linux distributions (e.g., `sudo apt-get install python3-tk`).

## 🚀 Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/Ready-Art/Synthetic-Dataset-Generator.git
   cd readyart-dataset-generator
   ```

2. **Create and activate a virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install requests redis psycopg2-binary PyYAML colorama ttkbootstrap matplotlib psutil
   ```

4. **Run the application:**
   ```bash
   python generate.py
   ```

5. **Set up directories:**
   The application automatically creates `input/` and `output/` directories on first run.

6. **Prepare input files:**
   Place `.txt` files in the `input/` directory. These will be used as source material for generating questions and context. See [The `input/` folder](input-folder.md) for the accepted format, the minimum file size, and what the loader strips out.

7. **Configure the application:**
   Launch the app and click **Edit Config**, or manually edit `config/config.yml`.
