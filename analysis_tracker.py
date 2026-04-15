"""Persistent tracker for stock analysis progress."""

import json
import os
from datetime import datetime

BASE_DIR = os.path.dirname(__file__)
TRACKER_FILE = os.path.join(BASE_DIR, "saved_tables", "us_tracker.json")


def _load_tracker() -> dict:
    """Load the tracker state from disk."""
    try:
        with open(TRACKER_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {"analyzed": {}}


def _save_tracker(data: dict):
    """Persist the tracker state to disk."""
    os.makedirs(os.path.dirname(TRACKER_FILE), exist_ok=True)
    with open(TRACKER_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_analyzed_tickers() -> dict:
    """Return {ticker: {"timestamp": ..., "status": ...}} for all analyzed tickers."""
    return _load_tracker().get("analyzed", {})


def mark_analyzed(ticker: str, status: str = "complete", metadata: dict | None = None):
    """Mark a ticker as analyzed with current timestamp.

    Optional `metadata` is stored alongside the tracker entry (e.g. market_cap).
    """
    data = _load_tracker()
    data.setdefault("analyzed", {})
    entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": status,
    }
    if metadata:
        entry.update(metadata)
    data["analyzed"][ticker.upper()] = entry
    _save_tracker(data)


def remove_analyzed(ticker: str):
    """Remove a ticker from the analyzed set."""
    data = _load_tracker()
    data.get("analyzed", {}).pop(ticker.upper(), None)
    _save_tracker(data)


def patch_metadata(ticker: str, metadata: dict):
    """Update specific metadata fields for an existing tracker entry.

    Unlike mark_analyzed(), this does NOT change the timestamp or status —
    it only merges the given fields into the existing entry. If the ticker
    is not in the tracker, this is a no-op.
    """
    data = _load_tracker()
    analyzed = data.get("analyzed", {})
    tk = ticker.upper()
    if tk in analyzed:
        analyzed[tk].update(metadata)
        _save_tracker(data)


def get_next_unanalyzed(universe: dict, analyzed: dict) -> str | None:
    """Return the next ticker in the universe that hasn't been analyzed yet.

    Iterates in universe insertion order (market-cap descending).
    Returns None if all are analyzed.
    """
    analyzed_upper = {t.upper() for t in analyzed}
    for ticker in universe:
        if ticker.upper() not in analyzed_upper:
            return ticker
    return None
