"""Read-replica auto-detect + UI freshness indicator.

Two responsibilities:
1. ``bootstrap_read_replica`` — at Streamlit startup, if ``STOCK_ANALYZER_READ_DB``
   isn't set AND a snapshot file exists next to ``stock.db``, point the env at
   it. Lets the app default to the snapshot so bulk + UI can run concurrently.
2. ``compute_db_status`` — returns ``{source, is_replica, age_seconds, label,
   color}`` for the status banner so users can see how stale the snapshot is.

Both live in dashboards/ (not db/) to keep the data layer free of UI concerns.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

from db.schema import DB_PATH, READ_DB_ENV

# Default snapshot filename, sibling of stock.db. Bulk's `--snapshot-path`
# default writes here, so the auto-detect lines up by convention.
DEFAULT_SNAPSHOT_NAME = "stock_read.db"

# Age thresholds (seconds) — color bands for the freshness indicator.
_AGE_FRESH_S = 3600          # < 1 h: green
_AGE_OK_S = 24 * 3600        # < 24 h: yellow
# >= 24 h: red

_COLOR_FRESH = "#22c55e"
_COLOR_STALE = "#f59e0b"
_COLOR_OLD = "#ef4444"
_COLOR_MAIN = "#94a3b8"
_COLOR_ERR = "#ef4444"


def bootstrap_read_replica(
    snapshot_path: Path | None = None,
    db_path: Path | None = None,
) -> str | None:
    """If env var unset and a snapshot file exists, set env to point at it.

    Returns the path string the env was set to (or already had), or None if
    the env stays unset (no snapshot found). Idempotent: existing env values
    are preserved verbatim, even if invalid — caller policy decides errors.

    Call this BEFORE any `get_conn(readonly=True)` so that the very first
    repository read picks up the snapshot.
    """
    existing = os.environ.get(READ_DB_ENV, "").strip()
    if existing:
        return existing

    snap = snapshot_path or (Path(db_path or DB_PATH).parent / DEFAULT_SNAPSHOT_NAME)
    if snap.is_file():
        resolved = str(snap.resolve())
        os.environ[READ_DB_ENV] = resolved
        return resolved
    return None


def _format_age(seconds: float) -> str:
    if seconds < 60:
        return f"{int(seconds)}s ago"
    if seconds < 3600:
        return f"{int(seconds // 60)}m ago"
    if seconds < 86400:
        return f"{int(seconds // 3600)}h ago"
    return f"{int(seconds // 86400)}d ago"


def compute_db_status(now: float | None = None) -> dict:
    """Return status dict for the UI banner.

    Keys:
      ``source``       — absolute path string (or "" if neither file exists)
      ``is_replica``   — True if env var points at a non-DB_PATH file
      ``age_seconds``  — float, mtime-derived; 0 if file missing
      ``label``        — short user-facing string ("副本 · 12m ago" / "主库")
      ``color``        — hex color for the indicator dot
    """
    now = now if now is not None else time.time()
    env_val = os.environ.get(READ_DB_ENV, "").strip()
    main_path = Path(DB_PATH)

    if env_val:
        p = Path(env_val).expanduser()
        is_replica = p.resolve() != main_path.resolve() if p.exists() else True
        if not p.is_file():
            return {
                "source": str(p),
                "is_replica": True,
                "age_seconds": 0.0,
                "label": f"副本缺失：{p.name}",
                "color": _COLOR_ERR,
            }
        age = max(0.0, now - p.stat().st_mtime)
        if not is_replica:
            return {
                "source": str(p),
                "is_replica": False,
                "age_seconds": age,
                "label": "主库（与 bulk 共用）",
                "color": _COLOR_MAIN,
            }
        if age < _AGE_FRESH_S:
            color = _COLOR_FRESH
        elif age < _AGE_OK_S:
            color = _COLOR_STALE
        else:
            color = _COLOR_OLD
        return {
            "source": str(p),
            "is_replica": True,
            "age_seconds": age,
            "label": f"副本 · {_format_age(age)}",
            "color": color,
        }

    # No env var — reading from main stock.db directly.
    if not main_path.is_file():
        return {
            "source": str(main_path),
            "is_replica": False,
            "age_seconds": 0.0,
            "label": "主库缺失",
            "color": _COLOR_ERR,
        }
    age = max(0.0, now - main_path.stat().st_mtime)
    return {
        "source": str(main_path),
        "is_replica": False,
        "age_seconds": age,
        "label": "主库（与 bulk 共用，bulk 跑时可能不可用）",
        "color": _COLOR_MAIN,
    }


def render_status_caption(st_module) -> None:
    """Render a small inline status indicator (caller passes streamlit module).

    Kept dependency-injected so the helper itself is testable without importing
    streamlit at module load time.
    """
    info = compute_db_status()
    dot = (
        f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;'
        f'background:{info["color"]};margin-right:6px;vertical-align:middle;"></span>'
    )
    st_module.markdown(
        f'<div style="color:#64748b;font-size:.78rem;margin:0 0 4px;">'
        f'{dot}<span style="color:#94a3b8;">数据源：</span>'
        f'<span style="color:{info["color"]};font-weight:600;">{info["label"]}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )
