"""Load repo-root ``.env`` into ``os.environ`` when keys are missing or blank.

No ``python-dotenv`` dependency. Used by CLIs so FMP keys work when the shell
never sourced ``.env`` (Windows / IDE subprocesses).
"""

from __future__ import annotations

import os
from pathlib import Path


def merge_dotenv_into_environ(repo_root: Path) -> None:
    p = repo_root / ".env"
    if not p.is_file():
        return
    try:
        text = p.read_text(encoding="utf-8-sig")
    except OSError:
        return
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if not key:
            continue
        cur = os.environ.get(key, "")
        if cur is None or str(cur).strip() == "":
            os.environ[key] = val
        # Backward-compatible alias: many local .env files use TUSHARE_API_KEY.
        if key.upper() == "TUSHARE_API_KEY":
            cur_tok = os.environ.get("TUSHARE_TOKEN", "")
            if cur_tok is None or str(cur_tok).strip() == "":
                os.environ["TUSHARE_TOKEN"] = val
