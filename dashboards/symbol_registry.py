"""Unified symbol registry for global single-stock search."""

from __future__ import annotations

import json
from pathlib import Path
import streamlit as st

from dashboards.cache import get_all_tickers_cached


def _normalize_market(market: str) -> str:
    m = (market or "").strip().upper()
    return m if m in {"US", "CN", "HK"} else "US"


def _infer_market_from_ticker(ticker: str) -> str:
    t = (ticker or "").strip().upper()
    if t.endswith((".SS", ".SZ", ".BJ")):
        return "CN"
    if t.endswith(".HK"):
        return "HK"
    return "US"


def _load_fmp_cached_universe() -> list[dict]:
    p = Path(__file__).resolve().parents[1] / "saved_tables" / "global_active_universe.json"
    if not p.is_file():
        return []
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []
    tickers = raw.get("tickers") or {}
    out: list[dict] = []
    for tk, meta in tickers.items():
        t = str(tk or "").strip().upper()
        if not t:
            continue
        market = _infer_market_from_ticker(t)
        name = str((meta or {}).get("name") or "").strip()
        label = f"{t} [{market}]"
        if name:
            label = f"{t} [{market}] - {name}"
        out.append({"ticker": t, "market": market, "name": name, "label": label})
    return out


@st.cache_data(ttl=600, show_spinner=False)
def build_symbol_registry() -> list[dict]:
    df = get_all_tickers_cached(market=None)
    rows: list[dict] = []
    seen: set[str] = set()

    if df is not None and not df.empty:
        for _, r in df.iterrows():
            ticker = str(r.get("ticker") or "").strip().upper()
            if not ticker:
                continue
            seen.add(ticker)
            raw_market = str(r.get("market") or "").strip().upper()
            market = _normalize_market(raw_market) if raw_market in {"US", "CN", "HK"} else _infer_market_from_ticker(ticker)
            name = str(r.get("name") or "").strip()
            label = f"{ticker} [{market}]"
            if name:
                label = f"{ticker} [{market}] - {name}"
            rows.append({"ticker": ticker, "market": market, "name": name, "label": label})

    # Merge FMP active global universe so UI can search symbols not yet filled in DB.
    for item in _load_fmp_cached_universe():
        t = item["ticker"]
        if t in seen:
            continue
        rows.append(item)
    return rows


def search_registry_options(query: str, limit: int = 50) -> list[dict]:
    q = (query or "").strip().upper()
    rows = build_symbol_registry()
    if not q:
        return rows[:limit]
    starts = [r for r in rows if r["ticker"].startswith(q)]
    contains = [r for r in rows if q in r["ticker"] and not r["ticker"].startswith(q)]
    name_hit = [r for r in rows if q in r["name"].upper() and q not in r["ticker"]]
    out = starts + contains + name_hit
    return out[:limit]
