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


def _dedupe_rows_by_ticker_market(rows: list[dict]) -> list[dict]:
    seen: set[tuple[str, str]] = set()
    out: list[dict] = []
    for r in rows:
        key = (r["ticker"], r["market"])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _market_bucket(m: str | None) -> str:
    x = (m or "US").strip().upper()
    return x if x in {"US", "CN", "HK"} else "US"


def _interleave_by_market(rows: list[dict], limit: int) -> list[dict]:
    """Prefer a mix of US/CN/HK when many rows match (avoids one market filling the cap)."""
    if len(rows) <= limit:
        return rows
    buckets: dict[str, list[dict]] = {"US": [], "CN": [], "HK": []}
    for r in rows:
        buckets[_market_bucket(r.get("market"))].append(r)
    order = ["US", "CN", "HK"]
    out: list[dict] = []
    i = 0
    while len(out) < limit and any(buckets[m] for m in order):
        m = order[i % 3]
        i += 1
        if buckets[m]:
            out.append(buckets[m].pop(0))
    for m in order:
        for r in buckets[m]:
            if len(out) >= limit:
                return out
            out.append(r)
    return out


def _stratified_head(rows: list[dict], limit: int) -> list[dict]:
    """When query is empty, avoid returning only the first alphabetical slice (often one market)."""
    if len(rows) <= limit:
        return rows
    return _interleave_by_market(list(rows), limit)


def _ticker_search_hits(ticker: str, q: str) -> bool:
    t = (ticker or "").upper()
    if not t or not q:
        return False
    if t.startswith(q) or q in t:
        return True
    base = t.split(".", 1)[0]
    if base.isdigit() and q.isdigit() and (base.startswith(q) or q in base):
        return True
    return False


@st.cache_data(ttl=600, show_spinner=False)
def build_symbol_registry() -> list[dict]:
    df = get_all_tickers_cached(market=None)
    rows: list[dict] = []
    seen: set[tuple[str, str]] = set()

    if df is not None and not df.empty:
        for _, r in df.iterrows():
            ticker = str(r.get("ticker") or "").strip().upper()
            if not ticker:
                continue
            raw_market = str(r.get("market") or "").strip().upper()
            market = _normalize_market(raw_market) if raw_market in {"US", "CN", "HK"} else _infer_market_from_ticker(ticker)
            key = (ticker, market)
            if key in seen:
                continue
            seen.add(key)
            name = str(r.get("name") or "").strip()
            label = f"{ticker} [{market}]"
            if name:
                label = f"{ticker} [{market}] - {name}"
            rows.append({"ticker": ticker, "market": market, "name": name, "label": label})

    # Merge FMP active global universe so UI can search symbols not yet filled in DB.
    for item in _load_fmp_cached_universe():
        t = item["ticker"]
        m = item["market"]
        if (t, m) in seen:
            continue
        seen.add((t, m))
        rows.append(item)
    return rows


def find_registry_row(rows: list[dict], market: str, ticker: str) -> dict | None:
    m = (market or "US").strip().upper()
    t = (ticker or "").strip().upper()
    for r in rows:
        if r["ticker"] == t and r["market"] == m:
            return r
    for r in rows:
        if r["ticker"] == t:
            return r
    return None


def search_registry_options(query: str, limit: int = 50) -> list[dict]:
    q = (query or "").strip().upper()
    rows = build_symbol_registry()
    if not q:
        return _stratified_head(rows, limit)
    starts = [r for r in rows if r["ticker"].startswith(q)]
    contains = [
        r
        for r in rows
        if (not r["ticker"].startswith(q)) and _ticker_search_hits(r["ticker"], q)
    ]
    name_u = q
    name_hit = [r for r in rows if name_u in (r.get("name") or "").upper() and not _ticker_search_hits(r["ticker"], q)]
    merged = _dedupe_rows_by_ticker_market(starts + contains + name_hit)
    return _interleave_by_market(merged, limit)
