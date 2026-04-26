"""
FMP (Financial Modeling Prep) data fetchers.
Base URL: /stable  (legacy /api/v3 was sunset 2025-08-31)

Three functions used by Phase 2:
    fetch_profile(ticker)              → dict for companies table
    fetch_ohlcv(ticker)                → list[dict] for ohlcv_daily
    fetch_fcf_annual(ticker, shares)   → list[dict] for fundamentals_annual
"""

from __future__ import annotations

import os
import requests
from pathlib import Path

_FMP_BASE = "https://financialmodelingprep.com/stable"


# ---------------------------------------------------------------------------
# API key
# ---------------------------------------------------------------------------

def load_api_key() -> str:
    key = os.environ.get("FMP_API_KEY", "")
    if not key:
        env_path = Path(__file__).parent.parent.parent / ".env"
        try:
            for line in env_path.read_text().splitlines():
                if line.startswith("FMP_API_KEY="):
                    key = line.split("=", 1)[1].strip()
                    break
        except Exception:
            pass
    if not key:
        raise RuntimeError("FMP_API_KEY not set in environment or .env")
    return key


def _get(endpoint: str, **params) -> list | dict:
    params["apikey"] = load_api_key()
    r = requests.get(f"{_FMP_BASE}/{endpoint}", params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and "Error Message" in data:
        raise RuntimeError(f"FMP error: {data['Error Message']}")
    return data


# ---------------------------------------------------------------------------
# fetch_profile  →  companies table
# ---------------------------------------------------------------------------

def fetch_profile(ticker: str) -> dict:
    """
    Returns dict matching companies table columns.

    Notes:
    - sharesOutstanding is ADR-adjusted since FMP Feb 2025 changelog.
    - shares_out stored in millions.
    """
    data = _get("profile", symbol=ticker)
    p = data[0] if isinstance(data, list) and data else data

    shares_raw = p.get("sharesOutstanding") or p.get("outstandingShares") or 0
    mkt_cap    = p.get("mktCap") or p.get("marketCap")

    # /stable/profile no longer returns sharesOutstanding directly — derive it
    # from marketCap / price when both are present.
    if not shares_raw and mkt_cap and p.get("price"):
        try:
            shares_raw = float(mkt_cap) / float(p["price"])
        except (TypeError, ZeroDivisionError):
            shares_raw = 0

    return {
        "ticker":      ticker.upper(),
        "market":      "US",
        "name":        p.get("companyName") or p.get("name"),
        "exchange":    p.get("exchange") or p.get("exchangeShortName"),
        "sector":      p.get("sector"),
        "industry":    p.get("industry"),
        "currency":    (p.get("currency") or "USD").upper(),
        "description": p.get("description"),
        "shares_out":  float(shares_raw) / 1_000_000 if shares_raw else None,
        # market_cap not in companies table; used for ohlcv_daily.market_cap
        "_shares_out_raw": float(shares_raw) if shares_raw else None,
        "_market_cap":     float(mkt_cap) if mkt_cap else None,
    }


# ---------------------------------------------------------------------------
# fetch_ohlcv  →  ohlcv_daily table
# ---------------------------------------------------------------------------

def fetch_ohlcv(
    ticker: str,
    shares_out_raw: float | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict]:
    """
    Returns list of dicts matching ohlcv_daily columns.

    adjClose from FMP is forward-adjusted (前复权): historical prices adjusted
    downward to match today's price level.  open/high/low are raw, so we scale
    them by the same ratio (adjClose / close) to put all prices on the same basis.

    market_cap (millions) = adj_close × (shares_out_raw / 1_000_000)
    """
    params = {"symbol": ticker}
    if date_from:
        params["from"] = date_from
    if date_to:
        params["to"] = date_to

    data = _get("historical-price-eod/full", **params)
    historical = data if isinstance(data, list) else data.get("historical", [])
    if not historical:
        raise ValueError(f"FMP returned no price history for {ticker}")

    rows = []
    for row in historical:
        close_raw = float(row.get("close") or 0)
        adj_close = float(row.get("adjClose") or close_raw)
        ratio     = (adj_close / close_raw) if close_raw else 1.0

        mkt_cap = None
        if shares_out_raw and adj_close:
            mkt_cap = adj_close * shares_out_raw / 1_000_000

        rows.append({
            "ticker":     ticker.upper(),
            "date":       row["date"],
            "open":       float(row.get("open") or 0) * ratio,
            "high":       float(row.get("high") or 0) * ratio,
            "low":        float(row.get("low")  or 0) * ratio,
            "close":      float(row.get("close") or 0),
            "volume":     int(row.get("volume") or 0),
            "adj_close":  adj_close,
            "market_cap": mkt_cap,
        })

    return rows


# ---------------------------------------------------------------------------
# FX rates  →  used by fetch_fcf_annual when reportedCurrency != USD
# ---------------------------------------------------------------------------

def fetch_fx_to_usd(currency: str, date_from: str, date_to: str) -> dict[str, float]:
    """
    Returns {date_str: rate}, where rate means "1 {currency} = N USD".

    Tries direct quote {CCY}USD first; falls back to USD{CCY} (inverted).
    Uses the same /historical-price-eod/full endpoint as price data.
    """
    if currency.upper() == "USD":
        return {}

    fx_dict: dict[str, float] = {}
    inverted = False
    last_err = ""

    for symbol, inv in [(f"{currency}USD", False), (f"USD{currency}", True)]:
        try:
            data = _get("historical-price-eod/full",
                        symbol=symbol, **{"from": date_from, "to": date_to})
            historical = data if isinstance(data, list) else data.get("historical", [])
            if not historical:
                last_err = f"{symbol} returned empty"
                continue
            for row in historical:
                d = row.get("date")
                rate = row.get("close") or row.get("adjClose")
                if d and rate:
                    fx_dict[d] = (1.0 / float(rate)) if inv else float(rate)
            if fx_dict:
                inverted = inv
                break
        except Exception as e:
            last_err = f"{symbol}: {e}"
            continue

    if not fx_dict:
        raise RuntimeError(
            f"FMP could not fetch {currency}/USD FX history "
            f"(tried {currency}USD and USD{currency}): {last_err}"
        )
    _ = inverted  # already baked into fx_dict values
    return fx_dict


def _rate_on_or_before(fx_dict: dict[str, float], target_date: str) -> float:
    """Return the FX rate effective on `target_date`, walking back to the closest prior date."""
    if not fx_dict:
        return 1.0
    sorted_dates = sorted(fx_dict.keys(), reverse=True)
    s = str(target_date)[:10]
    for d in sorted_dates:
        if d <= s:
            return fx_dict[d]
    return fx_dict[sorted_dates[-1]]   # before any data — use the oldest available


# ---------------------------------------------------------------------------
# fetch_fcf_annual  →  fundamentals_annual table (FCF columns only)
# ---------------------------------------------------------------------------

def fetch_fcf_annual(ticker: str, shares_out_raw: float | None = None) -> list[dict]:
    """
    Returns list of dicts for fundamentals_annual, FCF columns only.
    Other columns (revenue, roic, etc.) are left as None for later phases.

    Currency normalization: stored values are always in USD.
    - reporting_currency: the original FMP reportedCurrency (e.g. 'CNY' for BABA)
    - fx_to_usd: rate applied on each fiscal_end_date (1.0 when already USD)
    - fcf / fcf_per_share: USD-converted

    Per-share FCF uses the LATEST shares_out_raw (not historical), so per-share
    values share the same forward-adjusted price basis as adj_close.

    Values stored:
        fcf          in millions USD
        fcf_per_share in USD
        shares_out   in millions  (weighted avg basic, from FMP field)
    """
    data = _get("cash-flow-statement", symbol=ticker, period="annual", limit=30)
    if not data or not isinstance(data, list):
        raise ValueError(f"FMP returned no cash flow data for {ticker}")

    reporting = (data[0].get("reportedCurrency") or "USD").upper()

    # Fetch FX rates if reporting currency isn't USD
    fx_by_date: dict[str, float] = {}
    if reporting != "USD":
        report_dates = [e.get("date", "")[:10] for e in data if e.get("date")]
        if report_dates:
            fx_by_date = fetch_fx_to_usd(
                reporting, min(report_dates), max(report_dates)
            )

    rows = []
    for entry in data:
        date_str = entry.get("date", "")
        if not date_str:
            continue

        # FCF = freeCashFlow if available, else operatingCashFlow - |capitalExpenditure|
        fcf_raw = entry.get("freeCashFlow")
        if fcf_raw is None:
            ocf   = entry.get("operatingCashFlow")
            capex = entry.get("capitalExpenditure")
            if ocf is not None and capex is not None:
                fcf_raw = float(ocf) - abs(float(capex))
        fcf_native = float(fcf_raw) if fcf_raw is not None else None

        # Currency conversion: rate is 1 reporting = N USD on fiscal_end_date
        rate = _rate_on_or_before(fx_by_date, date_str[:10]) if reporting != "USD" else 1.0
        fcf_usd = fcf_native * rate if fcf_native is not None else None

        # Per-share FCF in USD using LATEST shares (same basis as adj_close)
        fcf_per_share = None
        if fcf_usd is not None and shares_out_raw and shares_out_raw > 0:
            fcf_per_share = fcf_usd / shares_out_raw

        # Weighted avg shares from FMP (stored in millions)
        wa_shares_raw = entry.get("weightedAverageShsOut")
        shares_out_hist = float(wa_shares_raw) / 1_000_000 if wa_shares_raw else None

        rows.append({
            "ticker":             ticker.upper(),
            "fiscal_year":        int(date_str[:4]),
            "fiscal_end_date":    date_str[:10],
            "filing_date":        (entry.get("filingDate") or entry.get("fillingDate") or "")[:10] or None,
            "currency":           "USD",
            "reporting_currency": reporting,
            "fx_to_usd":          rate,
            # FCF fields (Phase 2) — stored in USD millions
            "fcf":                fcf_usd / 1_000_000 if fcf_usd is not None else None,
            "fcf_per_share":      fcf_per_share,
            "shares_out":         shares_out_hist,
            "source":             "fmp",
            # All other fundamentals_annual columns → None (filled in later phases)
            "revenue":                   None,
            "revenue_per_share":         None,
            "gross_profit":              None,
            "gross_margin":              None,
            "operating_income":          None,
            "operating_margin":          None,
            "net_income":                None,
            "profit_margin":             None,
            "eps":                       None,
            "depreciation":              None,
            "effective_tax_rate":        None,
            "dividend_per_share":        None,
            "total_equity":              None,
            "long_term_debt":            None,
            "working_capital":           None,
            "book_value_per_share":      None,
            "tangible_bv_per_share":     None,
            "roic":                      None,
            "return_on_capital":         None,
            "return_on_equity":          None,
        })

    return rows
