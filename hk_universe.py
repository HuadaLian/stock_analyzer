"""HK universe provider (ordinary-stock oriented, cache-backed)."""

from __future__ import annotations

import json
import os
import time
from collections import OrderedDict
from datetime import datetime

from db.repository import get_all_tickers

BASE_DIR = os.path.dirname(__file__)
CACHE_DIR = os.path.join(BASE_DIR, "saved_tables")
UNIVERSE_FILE = os.path.join(CACHE_DIR, "hk_universe.json")
_CACHE_MAX_AGE_DAYS = 7


def fetch_hk_universe(force_refresh: bool = False, progress_callback=None) -> OrderedDict:
    os.makedirs(CACHE_DIR, exist_ok=True)

    if not force_refresh and os.path.exists(UNIVERSE_FILE):
        try:
            mtime = os.path.getmtime(UNIVERSE_FILE)
            age_days = (time.time() - mtime) / 86400
            if age_days < _CACHE_MAX_AGE_DAYS:
                with open(UNIVERSE_FILE, "r", encoding="utf-8") as f:
                    cached = json.load(f)
                stocks = cached.get("stocks")
                if stocks:
                    return OrderedDict((k, v) for k, v in stocks.items())
        except Exception:
            pass

    df = get_all_tickers(market="HK")
    out: OrderedDict[str, dict] = OrderedDict()
    if df is not None and not df.empty:
        for _, row in df.iterrows():
            tk = str(row.get("ticker") or "").strip().upper()
            if not tk:
                continue
            out[tk] = {
                "name": str(row.get("name") or "").strip(),
                "market": "HK",
                "source": "db_companies_hk",
            }

    if progress_callback:
        progress_callback(f"✅ 已获取 {len(out):,} 只港股普通股候选")

    payload = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "count": len(out),
        "source": "db_companies_hk",
        "stocks": dict(out),
    }
    try:
        with open(UNIVERSE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
    except OSError:
        pass
    return out
