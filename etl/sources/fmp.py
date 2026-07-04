"""
FMP (Financial Modeling Prep) data fetchers.
Base URL: /stable  (legacy /api/v3 was sunset 2025-08-31)

Three functions used by Phase 2:
    fetch_profile(ticker)              → dict for companies table
    fetch_ohlcv(ticker)                → list[dict] for ohlcv_daily
    fetch_fcf_annual(ticker, shares)   → list[dict] for fundamentals_annual
"""

from __future__ import annotations

import logging
import os
import requests
from datetime import date, timedelta
from pathlib import Path

from db.fcf_quality import (
    apply_fcf_native_adjustments,
    build_fcf_work_row,
    suppressed_flagged_fcf_notes,
)

_FMP_BASE = "https://financialmodelingprep.com/stable"
ANNUAL_HISTORY_LIMIT = 15

_log = logging.getLogger(__name__)


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


def _normalize_text(value: object) -> str:
    return str(value or "").strip().upper()


def infer_market(symbol: str, exchange: str | None, country: str | None) -> str:
    """Infer market bucket: US / CN / HK / GLOBAL.

    Rules are intentionally conservative:
    - CN/HK first by ticker suffix and exchange keywords
    - US by country/exchange evidence
    - everything else falls back to GLOBAL
    """
    sym = _normalize_text(symbol)
    ex = _normalize_text(exchange)
    ctry = _normalize_text(country)

    if sym.endswith((".SS", ".SZ", ".BJ")):
        return "CN"
    if sym.endswith(".HK"):
        return "HK"

    if "HONG KONG" in ex or ex in {"HKEX", "SEHK"} or ctry in {"HK", "HKG", "HONG KONG"}:
        return "HK"

    if (
        "SHANGHAI" in ex
        or "SHENZHEN" in ex
        or ex in {"SSE", "SZSE", "BSE"}
        or ctry in {"CN", "CHN", "CHINA"}
    ):
        return "CN"

    us_exchanges = {
        "NASDAQ", "NYSE", "NYSEARCA", "NYSE AMERICAN", "AMEX",
        "BATS", "IEX", "OTC", "OTCQB", "OTCQX", "PINK",
    }
    if ctry in {"US", "USA", "UNITED STATES"} or ex in us_exchanges:
        return "US"

    return "GLOBAL"


def search_symbols(query: str, limit: int = 500) -> list[dict]:
    """Search active symbols in FMP stable API.

    FMP legacy universe endpoints (available-traded/list, stock/list) are
    deprecated for non-legacy plans. ``stable/search-symbol`` is available and
    returns active symbols for a query prefix.
    """
    q = (query or "").strip()
    if not q:
        return []
    data = _get("search-symbol", query=q, limit=int(limit))
    if isinstance(data, list):
        return [d for d in data if isinstance(d, dict)]
    return []


# ---------------------------------------------------------------------------
# fetch_profile  →  companies table
# ---------------------------------------------------------------------------

def _as_profile_dict(raw, ticker: str) -> dict | None:
    """Normalize FMP /profile responses to a single dict (or None).

    FMP occasionally returns shapes other than the documented [dict] — empty
    list, list of multiple entries, or even None — and the previous code did
    `data[0]` blindly, which crashed callers with a bare AttributeError on the
    next `.get`. This helper folds every shape into either a dict or None so
    `fetch_profile` can raise a clean ValueError that bulk records as `failed`.
    """
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, list):
        if not raw:
            return None
        sym = ticker.upper()
        for entry in raw:
            if isinstance(entry, dict) and str(entry.get("symbol", "")).upper() == sym:
                return entry
        first = raw[0]
        if isinstance(first, dict):
            if len(raw) > 1:
                _log.warning(
                    "FMP profile returned %d entries for %s; using first (no symbol match)",
                    len(raw), sym,
                )
            return first
        return None
    return None


def fetch_profile(ticker: str) -> dict:
    """
    Returns dict matching companies table columns.

    Notes:
    - sharesOutstanding is ADR-adjusted since FMP Feb 2025 changelog.
    - shares_out stored in millions.
    """
    data = _get("profile", symbol=ticker)
    p = _as_profile_dict(data, ticker)
    if p is None:
        shape = type(data).__name__ + (f"[{len(data)}]" if isinstance(data, list) else "")
        raise ValueError(f"FMP profile returned no usable dict for {ticker.upper()} (shape={shape})")

    shares_raw = p.get("sharesOutstanding") or p.get("outstandingShares") or 0
    mkt_cap    = p.get("mktCap") or p.get("marketCap")

    # /stable/profile no longer returns sharesOutstanding directly — derive it
    # from marketCap / price when both are present.
    if not shares_raw and mkt_cap and p.get("price"):
        try:
            shares_raw = float(mkt_cap) / float(p["price"])
        except (TypeError, ZeroDivisionError):
            shares_raw = 0

    ex_short = p.get("exchangeShortName") or p.get("exchange")
    ex_full = (p.get("exchangeFullName") or p.get("fullExchangeName") or "").strip() or None
    country = (p.get("country") or "").strip() or None
    market = infer_market(ticker, ex_short, country)

    raw_cik = p.get("cik")
    cik_str: str | None = None
    if raw_cik is not None and str(raw_cik).strip():
        digits = "".join(ch for ch in str(raw_cik).strip() if ch.isdigit())
        if digits:
            cik_str = digits.zfill(10)

    is_actively_trading: bool | None
    if "isActivelyTrading" in p and p.get("isActivelyTrading") is not None:
        is_actively_trading = bool(p.get("isActivelyTrading"))
    else:
        is_actively_trading = None

    return {
        "ticker":      ticker.upper(),
        "market":      market,
        "name":        p.get("companyName") or p.get("name"),
        "exchange":    ex_short,
        "exchange_full_name": ex_full,
        "country":     country,
        "cik":         cik_str,
        "is_actively_trading": is_actively_trading,
        "sector":      p.get("sector"),
        "industry":    p.get("industry"),
        "currency":    (p.get("currency") or "USD").upper(),
        "description": p.get("description"),
        "shares_out":  float(shares_raw) / 1_000_000 if shares_raw else None,
        # market_cap not in companies table; used for ohlcv_daily.market_cap
        "_shares_out_raw": float(shares_raw) if shares_raw else None,
        "_market_cap":     float(mkt_cap) if mkt_cap else None,
        "_is_etf":       bool(p.get("isEtf")),
        "_is_fund":      bool(p.get("isFund")),
    }


# ---------------------------------------------------------------------------
# fetch_ohlcv  →  ohlcv_daily table
# ---------------------------------------------------------------------------

def fetch_ohlcv(
    ticker: str,
    shares_out_raw: float | None = None,
    market_cap_fx_to_usd: float | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict]:
    """
    Returns list of dicts matching ohlcv_daily columns.

    adjClose from FMP is forward-adjusted (前复权): historical prices adjusted
    downward to match today's price level.  open/high/low are raw, so we scale
    them by the same ratio (adjClose / close) to put all prices on the same basis.

    market_cap (millions) = adj_close × (shares_out_raw / 1_000_000)
    market_cap_usd (millions) = market_cap × market_cap_fx_to_usd
    """
    params = {"symbol": ticker}
    if date_from:
        params["from"] = date_from
    if date_to:
        params["to"] = date_to

    data = _get("historical-price-eod/full", **params)
    historical = data if isinstance(data, list) else data.get("historical", [])
    if not historical:
        # Incremental OHLCV: requesting only the last few calendar days often yields
        # [] when no new daily bar exists yet (same-day, weekend, holiday). A full
        # backfill with a wide [from, to] should still error so bad keys / dead
        # symbols are not silently "success".
        if date_from and date_to:
            try:
                d0 = date.fromisoformat(str(date_from)[:10])
                d1 = date.fromisoformat(str(date_to)[:10])
                span_days = (d1 - d0).days
                if 0 <= span_days <= 7:
                    _log.warning(
                        "FMP empty OHLCV for %s (from=%s to=%s, span=%d d) — no new bars; continuing",
                        ticker,
                        date_from,
                        date_to,
                        span_days,
                    )
                    return []
            except ValueError:
                pass
        raise ValueError(f"FMP returned no price history for {ticker}")

    rows = []
    for row in historical:
        close_raw = float(row.get("close") or 0)
        adj_close = float(row.get("adjClose") or close_raw)
        ratio     = (adj_close / close_raw) if close_raw else 1.0

        mkt_cap = None
        if shares_out_raw and adj_close:
            mkt_cap = adj_close * shares_out_raw / 1_000_000
        mkt_cap_usd = None
        if mkt_cap is not None and market_cap_fx_to_usd and market_cap_fx_to_usd > 0:
            mkt_cap_usd = mkt_cap * float(market_cap_fx_to_usd)

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
            "market_cap_usd": mkt_cap_usd,
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


def fetch_latest_fx_to_usd(currency: str) -> float:
    """Return latest available FX where 1 {currency} = N USD."""
    ccy = (currency or "USD").upper()
    if ccy == "USD":
        return 1.0
    end = date.today().isoformat()
    start = (date.today() - timedelta(days=14)).isoformat()
    fx = fetch_fx_to_usd(ccy, start, end)
    if not fx:
        raise RuntimeError(f"no FX history for {ccy}/USD in {start}..{end}")
    return float(fx[max(fx.keys())])


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

def fetch_fcf_annual(
    ticker: str,
    shares_out_raw: float | None = None,
    date_from: str | None = None,
) -> list[dict]:
    """
    Returns list of dicts for fundamentals_annual, FCF columns only.
    Other columns (revenue, roic, etc.) are left as None for later phases.

    Currency normalization: stored values are always in USD.
    - reporting_currency: the original FMP reportedCurrency (e.g. 'CNY' for BABA)
    - fx_to_usd: rate applied on each fiscal_end_date (1.0 when already USD)
    - fcf / fcf_per_share: USD-converted

    Per-share FCF: 默认仍用 profile 的 ``shares_out_raw``（与前复权 adj_close 同基准、拆股年对齐）；
    当报表加权股本与 profile 严重不一致且符合「错误 profile / ADR 次要上市」启发式时，
    改用 ``fcf_millions / weightedAverageShsOut``（与 ``shares_out`` 列同源）。

    Values stored:
        fcf          in millions USD
        fcf_per_share in USD
        shares_out   in millions  (weighted avg basic, from FMP field)
    """
    params: dict[str, str | int] = {
        "symbol": ticker,
        "period": "annual",
        "limit": ANNUAL_HISTORY_LIMIT,
    }
    if date_from:
        params["from"] = date_from

    data = _get(
        "cash-flow-statement",
        **params,
    )
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

    parsed: list[dict] = []
    for entry in data:
        date_str = entry.get("date", "")
        if not date_str:
            continue

        fcf_raw = entry.get("freeCashFlow")
        if fcf_raw is None:
            ocf = entry.get("operatingCashFlow")
            capex = entry.get("capitalExpenditure")
            if ocf is not None and capex is not None:
                fcf_raw = float(ocf) - abs(float(capex))
        fcf_native = float(fcf_raw) if fcf_raw is not None else None

        ocf_native = None
        if entry.get("operatingCashFlow") is not None:
            try:
                ocf_native = float(entry.get("operatingCashFlow"))
            except (TypeError, ValueError):
                ocf_native = None

        fy = int(date_str[:4])
        row_rep = (entry.get("reportedCurrency") or reporting or "USD").upper()
        rate = _rate_on_or_before(fx_by_date, date_str[:10]) if reporting != "USD" else 1.0

        wa_shares_raw = entry.get("weightedAverageShsOut")
        shares_out_hist = float(wa_shares_raw) / 1_000_000 if wa_shares_raw else None

        parsed.append({
            "_entry": entry,
            "fiscal_year": fy,
            "fiscal_end_date": date_str[:10],
            "filing_date": (entry.get("filingDate") or entry.get("fillingDate") or "")[:10] or None,
            "row_reporting": row_rep,
            "fx_to_usd": rate,
            "fcf_native": fcf_native,
            "ocf_native": ocf_native,
            "shares_out_hist": shares_out_hist,
        })

    parsed.sort(key=lambda r: r["fiscal_year"])

    work = [
        build_fcf_work_row(
            fiscal_year=int(p["fiscal_year"]),
            fcf_native=p["fcf_native"],
            ocf_native=p["ocf_native"],
            reporting_currency=str(p["row_reporting"]),
            fx_to_usd=float(p["fx_to_usd"]),
            entry=p["_entry"],
        )
        for p in parsed
    ]
    adj_meta = apply_fcf_native_adjustments(work)

    rows: list[dict] = []
    for p, meta in zip(parsed, adj_meta):
        fcf_native_adj = meta["fcf_native_adjusted"]
        fcf_native_adj_f = float(fcf_native_adj) if fcf_native_adj is not None else None
        rate = float(p["fx_to_usd"])
        fcf_usd = fcf_native_adj_f * rate if fcf_native_adj_f is not None else None

        fcf_millions = fcf_usd / 1_000_000 if fcf_usd is not None else None
        sh_hist = p.get("shares_out_hist")
        fcf_ps_stmt: float | None = None
        if fcf_millions is not None and sh_hist is not None:
            try:
                shf = float(sh_hist)
            except (TypeError, ValueError):
                shf = 0.0
            if shf > 0:
                fcf_ps_stmt = float(fcf_millions) / shf
                if not (1e-9 < fcf_ps_stmt < 500.0):
                    fcf_ps_stmt = None

        fcf_ps_profile: float | None = None
        if fcf_usd is not None and shares_out_raw and float(shares_out_raw) > 0:
            fcf_ps_profile = float(fcf_usd) / float(shares_out_raw)

        fcf_per_share: float | None = None
        if fcf_ps_stmt is not None and fcf_ps_profile is not None:
            rh = None
            if shares_out_raw and float(shares_out_raw) > 0 and sh_hist is not None:
                try:
                    sh_prof_m = float(shares_out_raw) / 1_000_000.0
                    if sh_prof_m > 0:
                        rh = float(sh_hist) / sh_prof_m
                except (TypeError, ValueError):
                    rh = None
            adr_broken = (
                fcf_ps_profile < 0.25
                and fcf_ps_stmt > 2.0
                and fcf_millions is not None
                and float(fcf_millions) > 1_500.0
                and (fcf_ps_stmt / max(fcf_ps_profile, 1e-12)) > 8.0
            )
            if rh is not None and rh < 0.5 and adr_broken:
                fcf_per_share = fcf_ps_stmt
            elif rh is not None and rh < 0.5:
                fcf_per_share = fcf_ps_profile
            elif 0.5 <= (fcf_ps_stmt / max(fcf_ps_profile, 1e-12)) <= 2.0:
                fcf_per_share = fcf_ps_profile
            else:
                fcf_per_share = fcf_ps_stmt
        elif fcf_ps_stmt is not None:
            fcf_per_share = fcf_ps_stmt
        else:
            fcf_per_share = fcf_ps_profile

        quality = str(meta["quality"])
        unit_notes = meta.get("fcf_fmp_unit_notes")
        if quality == "flagged_magnitude":
            unit_notes = suppressed_flagged_fcf_notes(
                unit_notes,
                fcf_millions=fcf_millions,
                fcf_per_share=fcf_per_share,
            )
            fcf_millions = None
            fcf_per_share = None

        rows.append({
            "ticker":             ticker.upper(),
            "fiscal_year":        int(p["fiscal_year"]),
            "fiscal_end_date":    p["fiscal_end_date"],
            "filing_date":        p["filing_date"],
            "currency":           "USD",
            "reporting_currency": str(p["row_reporting"]),
            "fx_to_usd":          rate,
            "fcf":                fcf_millions,
            "fcf_per_share":      fcf_per_share,
            "shares_out":         p["shares_out_hist"],
            "source":             "fmp",
            "fcf_quality":        quality,
            "fcf_scale_divisor":  int(meta["scale_divisor"]),
            "fcf_fmp_unit_notes": unit_notes,
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

    rows.sort(key=lambda r: r["fiscal_year"], reverse=True)
    return rows


def _extract_numeric_breakdown(entry: dict, exclude_keys: set[str]) -> dict[str, float]:
    """Extract non-meta numeric fields from a segmentation payload entry."""
    out: dict[str, float] = {}
    for key, value in entry.items():
        if key in exclude_keys:
            continue
        if isinstance(value, (int, float)):
            out[str(key)] = float(value)
    return out


def _parse_revenue_breakdown_rows(
    ticker: str,
    data: list | dict,
    dim_field: str,
) -> list[dict]:
    """Parse FMP segment/geography payloads into normalized rows.

    Handles common FMP shapes:
    1) [{"date": "2024-12-31", "SegmentA": 1.0, "SegmentB": 2.0}, ...]
    2) [{"2024-12-31": {"SegmentA": 1.0, "SegmentB": 2.0}}, ...]
    """
    rows: list[dict] = []
    payload = data if isinstance(data, list) else [data]
    exclude = {"date", "calendarYear", "fiscalYear", "symbol", "reportedCurrency", "fillingDate", "filingDate", "period", "finalLink", "link", "data"}

    def _append_for_year(fiscal_year: int, values: dict[str, float]) -> None:
        if not values:
            return
        total = sum(v for v in values.values() if v is not None)
        if total == 0:
            return
        for label, raw_value in values.items():
            if raw_value is None:
                continue
            revenue_m = raw_value / 1_000_000
            pct = (raw_value / total) if total else None
            rows.append({
                "ticker": ticker.upper(),
                "fiscal_year": fiscal_year,
                dim_field: label,
                "revenue": revenue_m,
                "pct": pct,
            })

    for item in payload:
        if not isinstance(item, dict):
            continue

        # Shape: {fiscalYear/date, ..., data: {SegmentA: value, SegmentB: value}}
        if isinstance(item.get("data"), dict):
            year_text = str(item.get("fiscalYear") or item.get("calendarYear") or str(item.get("date", ""))[:4])
            if year_text.isdigit():
                fiscal_year = int(year_text)
                values = _extract_numeric_breakdown(item["data"], set())
                _append_for_year(fiscal_year, values)
                continue

        if "date" in item or "calendarYear" in item:
            year_text = str(item.get("calendarYear") or str(item.get("date", ""))[:4])
            if not year_text.isdigit():
                continue
            fiscal_year = int(year_text)
            values = _extract_numeric_breakdown(item, exclude)
            _append_for_year(fiscal_year, values)
            continue

        for key, val in item.items():
            key_s = str(key)
            if len(key_s) >= 4 and key_s[:4].isdigit() and isinstance(val, dict):
                fiscal_year = int(key_s[:4])
                values = _extract_numeric_breakdown(val, set())
                _append_for_year(fiscal_year, values)

    return rows


def fetch_revenue_by_segment(ticker: str) -> list[dict]:
    """Fetch annual revenue by business segment and normalize for DB."""
    data = _get("revenue-product-segmentation", symbol=ticker)
    rows = _parse_revenue_breakdown_rows(ticker, data, dim_field="segment")
    return rows


def fetch_revenue_by_geography(ticker: str) -> list[dict]:
    """Fetch annual revenue by geography and normalize for DB."""
    data = _get("revenue-geographic-segmentation", symbol=ticker)
    rows = _parse_revenue_breakdown_rows(ticker, data, dim_field="region")
    return rows


def fetch_management(ticker: str) -> list[dict]:
    """Fetch key executives and normalize for management table."""
    data = _get("key-executives", symbol=ticker)
    payload = data if isinstance(data, list) else [data]

    rows: list[dict] = []
    title_seen: dict[str, int] = {}
    today = date.today().isoformat()

    for item in payload:
        if not isinstance(item, dict):
            continue
        name = (item.get("name") or "").strip()
        title = (item.get("title") or item.get("position") or "").strip()
        if not name:
            continue
        if not title:
            title = f"Executive {len(rows) + 1}"

        count = title_seen.get(title, 0) + 1
        title_seen[title] = count
        title_key = title if count == 1 else f"{title} ({count})"

        rows.append({
            "ticker": ticker.upper(),
            "name": name,
            "title": title_key,
            "updated_at": today,
        })

    return rows


def fetch_interest_expense_annual(ticker: str, date_from: str | None = None) -> list[dict]:
    """Fetch annual interest expense from income statement endpoint, FX-converted to USD millions."""
    params: dict[str, str | int] = {
        "symbol": ticker,
        "period": "annual",
        "limit": ANNUAL_HISTORY_LIMIT,
    }
    if date_from:
        params["from"] = date_from

    data = _get(
        "income-statement",
        **params,
    )
    if not data or not isinstance(data, list):
        return []

    reporting = (data[0].get("reportedCurrency") or "USD").upper()
    fx_by_date: dict[str, float] = {}
    if reporting != "USD":
        report_dates = [str(e.get("date") or "")[:10] for e in data if e.get("date")]
        if report_dates:
            try:
                fx_by_date = fetch_fx_to_usd(reporting, min(report_dates), max(report_dates))
            except RuntimeError:
                fx_by_date = {}

    rows: list[dict] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue
        date_str = str(entry.get("date") or "")
        if len(date_str) < 4 or not date_str[:4].isdigit():
            continue

        value = entry.get("interestExpense")
        if value is None:
            value = entry.get("interestExpenseNonOperating")
        if value is None:
            continue

        rate = _rate_on_or_before(fx_by_date, date_str[:10]) if reporting != "USD" else 1.0
        rows.append({
            "ticker": ticker.upper(),
            "fiscal_year": int(date_str[:4]),
            "interest_expense": float(value) * rate / 1_000_000,
        })

    return rows


def fetch_income_statement_annual(ticker: str, date_from: str | None = None) -> list[dict]:
    """Fetch annual income statement fields required by D2/D3, FX-converted to USD millions.

    For non-USD reporters (e.g. TSM in TWD) the raw values are converted using the
    historical FX rate effective on each fiscal_end_date — same convention as
    fetch_fcf_annual — so all figures stored in fundamentals_annual share the USD basis."""
    params: dict[str, str | int] = {
        "symbol": ticker,
        "period": "annual",
        "limit": ANNUAL_HISTORY_LIMIT,
    }
    if date_from:
        params["from"] = date_from

    data = _get(
        "income-statement",
        **params,
    )
    if not data or not isinstance(data, list):
        return []

    reporting = (data[0].get("reportedCurrency") or "USD").upper()
    fx_by_date: dict[str, float] = {}
    if reporting != "USD":
        report_dates = [str(e.get("date") or "")[:10] for e in data if e.get("date")]
        if report_dates:
            try:
                fx_by_date = fetch_fx_to_usd(reporting, min(report_dates), max(report_dates))
            except RuntimeError:
                fx_by_date = {}

    rows: list[dict] = []
    for entry in data:
        if not isinstance(entry, dict):
            continue

        date_str = str(entry.get("date") or "")
        if len(date_str) < 4 or not date_str[:4].isdigit():
            continue

        rate = _rate_on_or_before(fx_by_date, date_str[:10]) if reporting != "USD" else 1.0

        def _to_usd_m(value):
            if value is None:
                return None
            return float(value) * rate / 1_000_000

        interest = entry.get("interestExpense")
        if interest is None:
            interest = entry.get("interestExpenseNonOperating")

        rows.append({
            "ticker": ticker.upper(),
            "fiscal_year": int(date_str[:4]),
            "fiscal_end_date": date_str[:10],
            "revenue": _to_usd_m(entry.get("revenue")),
            "operating_income": _to_usd_m(entry.get("operatingIncome")),
            "depreciation": _to_usd_m(entry.get("depreciationAndAmortization")),
            "interest_expense": _to_usd_m(interest),
        })

    return rows
