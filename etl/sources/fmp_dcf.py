"""
FMP DCF history fetch/load helpers.

Endpoints (tried in order):
    1. /api/v3/historical-discounted-cash-flow-statement/{ticker}  (historical data if available)
    2. /stable/discounted-cash-flow?symbol={ticker}  (realtime DCF, fallback)
"""

from __future__ import annotations

import requests
import duckdb

from etl.loader import upsert_fmp_dcf_history
from etl.sources.fmp import load_api_key


_FMP_V3_BASE = "https://financialmodelingprep.com/api/v3"
_FMP_STABLE_BASE = "https://financialmodelingprep.com/stable"


def _parse_fmp_dcf_payload(ticker: str, payload: list | dict) -> list[dict]:
    """Normalize FMP payload into DB-ready row dicts."""
    data = payload
    if isinstance(payload, dict):
        data = payload.get("historicalStockList") or payload.get("historical") or []

    rows: list[dict] = []
    for entry in data if isinstance(data, list) else []:
        date = entry.get("date") or entry.get("Date")
        dcf = entry.get("dcf")
        if dcf is None:
            dcf = entry.get("DCF")
        stock_price = entry.get("Stock Price")
        if stock_price is None:
            stock_price = entry.get("stockPrice")

        if not date or dcf is None:
            continue

        rows.append({
            "ticker": ticker.upper(),
            "date": str(date)[:10],
            "dcf_value": float(dcf),
            "stock_price": float(stock_price) if stock_price is not None else None,
        })

    rows.sort(key=lambda r: r["date"])
    return rows


def fetch_fmp_dcf_history(ticker: str, api_key: str) -> list[dict]:
    """Fetch and parse FMP historical DCF rows for one ticker.
    
    Tries endpoints in order:
      1. /api/v3/historical-discounted-cash-flow-statement/{ticker}
      2. /stable/discounted-cash-flow?symbol={ticker}
    
    Returns list of row dicts, empty list if both fail.
    """
    # 1. Try v3 historical endpoint
    try:
        resp = requests.get(
            f"{_FMP_V3_BASE}/historical-discounted-cash-flow-statement/{ticker}",
            params={"apikey": api_key},
            timeout=30,
        )
        resp.raise_for_status()
        rows = _parse_fmp_dcf_payload(ticker, resp.json())
        if rows:
            return rows
    except Exception as e:
        pass  # Fall through to stable endpoint
    
    # 2. Fall back to stable realtime endpoint
    try:
        resp = requests.get(
            f"{_FMP_STABLE_BASE}/discounted-cash-flow",
            params={"symbol": ticker, "apikey": api_key},
            timeout=30,
        )
        resp.raise_for_status()
        rows = _parse_fmp_dcf_payload(ticker, resp.json())
        return rows
    except Exception as e:
        pass  # Return empty list
    
    return []


def load_fmp_dcf_history(
    ticker: str,
    conn: duckdb.DuckDBPyConnection,
    fetch_fn=fetch_fmp_dcf_history,
    api_key: str | None = None,
) -> int:
    """Fetch FMP DCF history and upsert into fmp_dcf_history.

    Returns number of fetched rows.
    """
    key = api_key or load_api_key()
    rows = fetch_fn(ticker, key)
    upsert_fmp_dcf_history(conn, rows)
    return len(rows)
