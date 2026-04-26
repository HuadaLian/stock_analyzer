# data_provider.py
import os as _os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# ═══════════════════════════════════════════════════════════════════════
#  FMP (Financial Modeling Prep) helpers
#  Base URL: /stable  (legacy /api/v3 endpoints were sunset 2025-08-31)
# ═══════════════════════════════════════════════════════════════════════

_FMP_BASE = "https://financialmodelingprep.com/stable"

def _load_fmp_api_key() -> str:
    """Load FMP API key from environment or .env file."""
    key = _os.environ.get("FMP_API_KEY", "")
    if not key:
        try:
            env_path = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), ".env")
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("FMP_API_KEY="):
                        key = line.split("=", 1)[1].strip()
                        break
        except Exception:
            pass
    return key


def _fmp_ohlcv(ticker: str) -> pd.DataFrame:
    """Fetch US OHLCV from FMP.

    Uses adjClose for close and scales open/high/low by the same adjustment
    ratio so all prices are on a forward-adjusted (前复权) basis.
    Returns DataFrame with columns: Date, Open, High, Low, Close, Volume.
    """
    import requests

    api_key = _load_fmp_api_key()
    if not api_key:
        raise RuntimeError("FMP_API_KEY 未设置，无法使用 FMP 数据源")

    url = f"{_FMP_BASE}/historical-price-eod/full"
    r = requests.get(url, params={"symbol": ticker, "apikey": api_key}, timeout=30)
    r.raise_for_status()
    raw = r.json()

    # /stable returns list directly; /api/v3 wrapped in {"historical": [...]}
    if isinstance(raw, list):
        historical = raw
    else:
        historical = raw.get("historical", [])
    if not historical:
        raise ValueError(f"FMP 未返回 {ticker} 的历史价格数据")

    records = []
    for row in historical:
        close_raw = float(row.get("close") or 0)
        adj_close = float(row.get("adjClose") or close_raw)
        ratio = adj_close / close_raw if close_raw else 1.0
        records.append({
            "Date":   row["date"],
            "Open":   float(row.get("open") or 0) * ratio,
            "High":   float(row.get("high") or 0) * ratio,
            "Low":    float(row.get("low") or 0) * ratio,
            "Close":  adj_close,
            "Volume": float(row.get("volume") or 0),
        })

    df = pd.DataFrame(records)
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date").reset_index(drop=True)
    return df


def _fmp_fcf_data(ticker: str, shares_outstanding: float = None) -> dict:
    """Fetch annual FCF from FMP (up to 30 years).

    Checks reportedCurrency: stores it as 'fmp_currency'.
    Returns actual (full-value) OCF/CapEx/FCF in the reported currency.
    CapEx is stored as a positive number (absolute value of cash outflow).
    """
    import requests

    result = {
        "fmp_fcf_table": pd.DataFrame(),
        "fmp_currency": "USD",
    }

    api_key = _load_fmp_api_key()
    if not api_key:
        return result

    url = f"{_FMP_BASE}/cash-flow-statement"
    try:
        r = requests.get(
            url,
            params={"symbol": ticker, "period": "annual", "limit": 30, "apikey": api_key},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as _e:
        _sc = getattr(getattr(_e, "response", None), "status_code", "?")
        raise RuntimeError(f"FMP cash-flow HTTP {_sc}: {_e}") from _e

    if isinstance(data, dict) and "Error Message" in data:
        raise RuntimeError(f"FMP API error: {data['Error Message']}")
    if not data or not isinstance(data, list):
        raise RuntimeError(f"FMP cash-flow 返回非预期格式: {str(data)[:200]}")

    # Detect currency (consistent across entries for a given company)
    fmp_currency = (data[0].get("reportedCurrency") or "USD").upper()
    result["fmp_currency"] = fmp_currency

    rows = []
    for entry in data:
        date_str = entry.get("date", "")
        if not date_str:
            continue
        ts = pd.Timestamp(date_str)

        ocf = entry.get("operatingCashFlow")
        capex_raw = entry.get("capitalExpenditure")   # negative in FMP
        fcf_direct = entry.get("freeCashFlow")

        ocf_val = float(ocf) if ocf is not None else None
        capex_val = abs(float(capex_raw)) if capex_raw is not None else None
        if fcf_direct is not None:
            fcf_val = float(fcf_direct)
        elif ocf_val is not None and capex_val is not None:
            fcf_val = ocf_val - capex_val
        else:
            fcf_val = None

        fcf_ps = None
        if fcf_val is not None and shares_outstanding and shares_outstanding > 0:
            fcf_ps = fcf_val / shares_outstanding

        rows.append({
            "年份":  ts,
            "OCF":   ocf_val,
            "CapEx": capex_val,
            "FCF":   fcf_val,
            "每股FCF": fcf_ps,
        })

    if not rows:
        return result

    sorted_rows = sorted(rows, key=lambda x: x["年份"], reverse=True)
    for i, row in enumerate(sorted_rows):
        w3 = sorted_rows[i: i + 3]
        w5 = sorted_rows[i: i + 5]
        ps3 = [r["每股FCF"] for r in w3 if r["每股FCF"] is not None]
        ps5 = [r["每股FCF"] for r in w5 if r["每股FCF"] is not None]
        row["3年均每股FCF"] = np.mean(ps3) if ps3 else None
        row["5年均每股FCF"] = np.mean(ps5) if ps5 else None

    tbl = pd.DataFrame(sorted_rows)
    tbl["年份"] = tbl["年份"].dt.strftime("%Y-%m-%d")
    result["fmp_fcf_table"] = tbl
    return result


def _fmp_profile(ticker: str) -> dict:
    """Fetch company profile from FMP, including ADR-adjusted shares outstanding.

    Since FMP's Feb 2025 changelog, Quote/Profile endpoints return
    sharesOutstanding already scaled to the ADR share count (not ordinary
    shares) for ADR tickers.  We also query /api/v4/shares_float for an
    explicit adrRatio field when available.

    Returns:
        shares_outstanding  – ADR-adjusted share count (float or None)
        market_cap          – in USD (float or None)
        adr_ratio           – e.g. 0.4 for CYATY (float or None if not an ADR)
        is_adr              – True if adrRatio found and != 1
    """
    import requests

    result = {
        "shares_outstanding": None,
        "market_cap": None,
        "adr_ratio": None,
        "is_adr": False,
    }

    api_key = _load_fmp_api_key()
    if not api_key:
        return result

    # ── 1. Stable profile endpoint ──────────────────────────────────────
    try:
        r = requests.get(
            f"{_FMP_BASE}/profile",
            params={"symbol": ticker, "apikey": api_key},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list) and data:
            p = data[0]
        elif isinstance(data, dict):
            p = data
        else:
            p = {}

        so = p.get("sharesOutstanding") or p.get("outstandingShares")
        if so:
            result["shares_outstanding"] = float(so)
        mc = p.get("mktCap") or p.get("marketCap")
        if mc:
            result["market_cap"] = float(mc)
    except Exception:
        pass

    # ── 2. v4/shares_float – explicit adrRatio when available ───────────
    try:
        r2 = requests.get(
            "https://financialmodelingprep.com/api/v4/shares_float",
            params={"symbol": ticker, "apikey": api_key},
            timeout=15,
        )
        r2.raise_for_status()
        sf = r2.json()
        if isinstance(sf, list) and sf:
            sf0 = sf[0]
        elif isinstance(sf, dict):
            sf0 = sf
        else:
            sf0 = {}

        adr_ratio = sf0.get("adrRatio")
        if adr_ratio is not None:
            adr_ratio = float(adr_ratio)
            result["adr_ratio"] = adr_ratio
            result["is_adr"] = (adr_ratio != 1.0)

        # If shares_float has a better sharesOutstanding, prefer it
        so2 = sf0.get("outstandingShares") or sf0.get("sharesOutstanding")
        if so2 and result["shares_outstanding"] is None:
            result["shares_outstanding"] = float(so2)
    except Exception:
        pass

    return result


def _fmp_dcf_history(ticker: str) -> dict:
    """Fetch FMP DCF valuation history for *ticker*.

    Tries endpoints in order:
      1. Annual history  – v3/historical-discounted-cash-flow/{symbol}
      2. Stable realtime – /stable/discounted-cash-flow?symbol={symbol}  (single point)

    Returns:
        dcf_df        – DataFrame(date, dcf, stock_price) sorted ascending,
                        or empty DataFrame if nothing was fetched.
        dcf_current   – float or None  (most-recent dcf value)
        dcf_source    – "annual" | "realtime" | None
    """
    import requests

    result = {
        "dcf_df": pd.DataFrame(),
        "dcf_current": None,
        "dcf_source": None,
    }

    api_key = _load_fmp_api_key()
    if not api_key:
        return result

    _v3 = "https://financialmodelingprep.com/api/v3"

    def _parse_rows(raw) -> list[dict]:
        if isinstance(raw, dict):
            raw = raw.get("historicalStockList") or raw.get("historical") or []
        rows = []
        for entry in (raw if isinstance(raw, list) else []):
            d = entry.get("date") or entry.get("Date")
            v = entry.get("dcf") or entry.get("DCF")
            sp = entry.get("Stock Price") or entry.get("stockPrice")
            if d and v is not None:
                rows.append({
                    "date": pd.Timestamp(d),
                    "dcf": float(v),
                    "stock_price": float(sp) if sp is not None else None,
                })
        return rows

    # ── 1. Annual historical DCF ────────────────────────────────────────
    try:
        r = requests.get(
            f"{_v3}/historical-discounted-cash-flow/{ticker}",
            params={"apikey": api_key},
            timeout=20,
        )
        r.raise_for_status()
        rows = _parse_rows(r.json())
        if rows:
            df = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
            result["dcf_df"] = df
            result["dcf_current"] = df["dcf"].iloc[-1]
            result["dcf_source"] = "annual"
            return result
    except Exception:
        pass

    # ── 2. Stable realtime fallback (single point) ──────────────────────
    try:
        r = requests.get(
            f"{_FMP_BASE}/discounted-cash-flow",
            params={"symbol": ticker, "apikey": api_key},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list) and data:
            entry = data[0]
        elif isinstance(data, dict):
            entry = data
        else:
            entry = {}
        v = entry.get("dcf")
        if v is not None:
            result["dcf_current"] = float(v)
            result["dcf_source"] = "realtime"
    except Exception:
        pass

    return result


def _fmp_analyst_data(ticker: str) -> dict:
    """Fetch analyst data from FMP /stable endpoints.

    Endpoints (all confirmed working in fmp_api_demo.ipynb):
      price-target-consensus  → targetHigh/Low/Consensus/Median
      price-target-summary    → lastMonth/Quarter/Year count + avg target
      grades                  → individual analyst actions (gradingCompany, newGrade, action)
      grades-consensus        → strongBuy/buy/hold/sell/strongSell counts

    Returns keys: price_target, price_target_summary, grades,
                  grades_consensus, analyst_count, fmp_analyst_status.
    """
    import requests

    result = {
        "price_target": None,
        "price_target_summary": None,
        "grades": [],
        "grades_consensus": None,
        "analyst_count": 0,
        "fmp_analyst_status": "",
    }

    api_key = _load_fmp_api_key()
    if not api_key:
        result["fmp_analyst_status"] = "FMP_API_KEY 未设置，无分析师数据"
        return result

    errors = []

    def _get(endpoint, **params):
        params["apikey"] = api_key
        r = requests.get(f"{_FMP_BASE}/{endpoint}", params=params, timeout=15)
        r.raise_for_status()
        return r.json()

    # ── 1. Price target consensus ───────────────────────────────────────
    try:
        data = _get("price-target-consensus", symbol=ticker)
        if isinstance(data, list) and data:
            result["price_target"] = data[0]
        elif isinstance(data, dict) and "targetConsensus" in data:
            result["price_target"] = data
        elif isinstance(data, dict) and "Error Message" in data:
            errors.append(f"目标价共识: {data['Error Message']}")
    except Exception as e:
        _sc = getattr(getattr(e, "response", None), "status_code", "?")
        errors.append(f"目标价共识 HTTP {_sc}: {e}")

    # ── 2. Price target summary (lastMonth/Quarter/Year averages) ───────
    try:
        data = _get("price-target-summary", symbol=ticker)
        if isinstance(data, list) and data:
            result["price_target_summary"] = data[0]
        elif isinstance(data, dict) and "lastMonthCount" in data:
            result["price_target_summary"] = data
        elif isinstance(data, dict) and "Error Message" in data:
            errors.append(f"目标价汇总: {data['Error Message']}")
    except Exception as e:
        _sc = getattr(getattr(e, "response", None), "status_code", "?")
        errors.append(f"目标价汇总 HTTP {_sc}: {e}")

    # ── 3. Individual analyst grades (actions) ──────────────────────────
    try:
        data = _get("grades", symbol=ticker, limit=30)
        if isinstance(data, list):
            result["grades"] = data
            from datetime import datetime, timedelta as _td
            cutoff = (datetime.now() - _td(days=90)).strftime("%Y-%m-%d")
            recent = [x for x in data if (x.get("date") or "") >= cutoff]
            result["analyst_count"] = len(
                {x.get("gradingCompany") or "" for x in recent if x.get("gradingCompany")}
            )
        elif isinstance(data, dict) and "Error Message" in data:
            errors.append(f"评级动作: {data['Error Message']}")
    except Exception as e:
        _sc = getattr(getattr(e, "response", None), "status_code", "?")
        errors.append(f"评级动作 HTTP {_sc}: {e}")

    # ── 4. Grades consensus (strongBuy/buy/hold/sell/strongSell counts) ─
    try:
        data = _get("grades-consensus", symbol=ticker)
        if isinstance(data, list) and data:
            result["grades_consensus"] = data[0]
        elif isinstance(data, dict) and ("strongBuy" in data or "buy" in data):
            result["grades_consensus"] = data
        elif isinstance(data, dict) and "Error Message" in data:
            errors.append(f"评级共识: {data['Error Message']}")
    except Exception as e:
        _sc = getattr(getattr(e, "response", None), "status_code", "?")
        errors.append(f"评级共识 HTTP {_sc}: {e}")

    has_data = any([
        result["price_target"], result["price_target_summary"],
        result["grades"], result["grades_consensus"],
    ])
    if errors and not has_data:
        result["fmp_analyst_status"] = " | ".join(errors)
    elif has_data:
        n = result["analyst_count"]
        result["fmp_analyst_status"] = f"近90日 {n} 家机构" if n else "已加载"
        if errors:
            result["fmp_analyst_status"] += f" (部分失败: {'; '.join(errors)})"
    else:
        result["fmp_analyst_status"] = "无分析师数据"

    return result


def _convert_fcf_to_usd(tbl: pd.DataFrame, from_currency: str) -> tuple:
    """Convert FCF table monetary columns from *from_currency* to USD.

    Correct endpoint (confirmed from fmp_api_demo.ipynb section 16):
        GET /stable/historical-price-eod/full?symbol=EURUSD&from=...&to=...

    The function:
    - Derives the date range from the earliest/latest row in *tbl* so it fetches
      only as much history as needed
    - Tries {from}USD (direct quote) first, then USD{from} (inverted) as fallback
    - Attaches a "兑USD汇率" column showing the per-year rate used (1 {from} = ? USD)
    - Applies year-end rates (closest date on-or-before each report date)
    - Recomputes rolling averages from the converted per-share values

    Returns (converted_tbl, fx_note_str).
    Raises RuntimeError with a descriptive message if no FX data is available.
    """
    import requests

    api_key = _load_fmp_api_key()
    if not api_key:
        raise RuntimeError("FMP_API_KEY 未设置，无法获取汇率数据")

    # Derive date range from the FCF table (oldest → today)
    raw_dates = [str(d)[:10] for d in tbl["年份"].dropna() if str(d)[:4].isdigit()]
    if not raw_dates:
        raise RuntimeError("FCF 表格无有效日期，无法确定汇率查询范围")
    date_from = min(raw_dates)          # oldest report year-end
    date_to   = datetime.now().strftime("%Y-%m-%d")

    fx_dict: dict = {}
    inverted = False   # True → stored rate is "USD per 1 {from}", must invert
    _last_err = ""

    for symbol, inv in [
        (f"{from_currency}USD", False),   # direct:   1 {from} = ? USD
        (f"USD{from_currency}", True),    # inverted: 1 USD    = ? {from}
    ]:
        try:
            url = f"{_FMP_BASE}/historical-price-eod/full"
            r = requests.get(
                url,
                params={"symbol": symbol, "from": date_from, "to": date_to,
                        "apikey": api_key},
                timeout=30,
            )
            r.raise_for_status()
            raw = r.json()
            if isinstance(raw, dict) and "Error Message" in raw:
                _last_err = f"{symbol}: {raw['Error Message']}"
                continue
            # /stable/historical-price-eod/full returns a list directly
            historical = raw if isinstance(raw, list) else raw.get("historical", [])
            if not historical:
                _last_err = f"{symbol} 返回空历史"
                continue
            for row in historical:
                d    = row.get("date")
                rate = row.get("close") or row.get("adjClose")
                if d and rate:
                    fx_dict[d] = float(rate)
            if fx_dict:
                inverted = inv
                break
        except Exception as _e:
            _last_err = str(_e)
            continue

    if not fx_dict:
        raise RuntimeError(
            f"FMP 无法获取 {from_currency}/USD 历史汇率 "
            f"(尝试了 {from_currency}USD 和 USD{from_currency}): {_last_err}"
        )

    sorted_fx_dates = sorted(fx_dict.keys(), reverse=True)  # newest first

    def _to_usd_rate(date_str: str) -> float:
        """1 {from_currency} = ? USD — closest available date on-or-before date_str."""
        s = str(date_str)[:10]
        for d in sorted_fx_dates:
            if d <= s:
                r = fx_dict[d]
                return (1.0 / r) if inverted else r
        # Fallback: oldest available rate
        r = fx_dict[sorted_fx_dates[-1]]
        return (1.0 / r) if inverted else r

    tbl = tbl.copy()

    # ── Per-row FX rate column (1 {from} = ? USD, 4 d.p.) ───────────────
    tbl["兑USD汇率"] = tbl["年份"].apply(
        lambda d: round(_to_usd_rate(str(d)[:10]), 4)
    )

    # ── Convert total-value columns (OCF / CapEx / FCF) ──────────────────
    for col in ["OCF", "CapEx", "FCF"]:
        if col in tbl.columns:
            tbl[col] = tbl.apply(
                lambda row, c=col: row[c] * _to_usd_rate(str(row["年份"])[:10])
                if pd.notna(row[c]) else None,
                axis=1,
            )

    # ── Convert per-share FCF ────────────────────────────────────────────
    if "每股FCF" in tbl.columns:
        tbl["每股FCF"] = tbl.apply(
            lambda row: row["每股FCF"] * _to_usd_rate(str(row["年份"])[:10])
            if pd.notna(row["每股FCF"]) else None,
            axis=1,
        )

    # ── Recompute rolling averages from the now-USD per-share values ─────
    sorted_rows = tbl.to_dict("records")  # already sorted descending
    for i, row in enumerate(sorted_rows):
        w3 = sorted_rows[i: i + 3]
        w5 = sorted_rows[i: i + 5]
        ps3 = [r["每股FCF"] for r in w3 if r.get("每股FCF") is not None and pd.notna(r["每股FCF"])]
        ps5 = [r["每股FCF"] for r in w5 if r.get("每股FCF") is not None and pd.notna(r["每股FCF"])]
        row["3年均每股FCF"] = np.mean(ps3) if ps3 else None
        row["5年均每股FCF"] = np.mean(ps5) if ps5 else None
    tbl = pd.DataFrame(sorted_rows)

    # ── Summary note ─────────────────────────────────────────────────────
    latest_date = str(tbl["年份"].iloc[0])[:10] if len(tbl) > 0 else ""
    latest_rate = _to_usd_rate(latest_date) if latest_date else 1.0
    pair_label  = f"USD{from_currency}倒算" if inverted else f"{from_currency}USD"
    fx_note = f"1 {from_currency} ≈ {latest_rate:.4f} USD ({pair_label})"
    return tbl, fx_note


def _extend_with_fmp_fcf(fin: dict, fmp_fin: dict) -> None:
    """Extend the primary FCF table with older rows from FMP (in-place).

    If the primary table is empty, promotes the entire FMP table as the main source.
    Otherwise only appends years not already present in the primary table.
    Rolling averages will be recomputed by _apply_adjusted_fcf downstream.
    """
    fmp_tbl = fmp_fin.get("fmp_fcf_table")
    if fmp_tbl is None or fmp_tbl.empty:
        return

    main_tbl = fin.get("fcf_table")
    if main_tbl is None or main_tbl.empty:
        # No primary data — use FMP as sole source
        fin["fcf_table"] = fmp_tbl.copy()
        ps = {}
        for _, row in fmp_tbl.iterrows():
            if pd.notna(row.get("每股FCF")):
                yr = str(row["年份"])[:4]
                ps[pd.Timestamp(f"{yr}-12-31")] = row["每股FCF"]
        if ps:
            fin["fcf_per_share_by_year"] = ps
        return

    existing_years = set()
    for _, row in main_tbl.iterrows():
        try:
            existing_years.add(int(str(row["年份"])[:4]))
        except (ValueError, TypeError):
            pass

    older_rows = []
    for _, row in fmp_tbl.iterrows():
        try:
            yr = int(str(row["年份"])[:4])
        except (ValueError, TypeError):
            continue
        if yr not in existing_years:
            older_rows.append(row.to_dict())

    if not older_rows:
        return

    extra = pd.DataFrame(older_rows)
    # Align columns: add NaN for columns present in main but absent in FMP rows
    for col in main_tbl.columns:
        if col not in extra.columns:
            extra[col] = np.nan
    extra = extra[main_tbl.columns]

    combined = pd.concat([main_tbl, extra], ignore_index=True)
    combined = combined.sort_values("年份", ascending=False).reset_index(drop=True)
    fin["fcf_table"] = combined

    # Rebuild fcf_per_share_by_year to include older FMP entries
    ps = {}
    for _, row in combined.iterrows():
        if pd.notna(row.get("每股FCF")):
            yr = str(row["年份"])[:4]
            ps[pd.Timestamp(f"{yr}-12-31")] = row["每股FCF"]
    if ps:
        fin["fcf_per_share_by_year"] = ps


# ═══════════════════════════════════════════════════════════════════════
#  FCF helpers – per-market
# ═══════════════════════════════════════════════════════════════════════

def _yf_fcf_and_shares(yf_ticker_str):
    """Get annual FCF history + shares from yfinance (US & HK).

    For US tickers: if yfinance reports a non-USD financialCurrency (e.g. CNY
    for Chinese ADRs like BABA), the entire cashflow table is discarded and an
    empty result is returned.  The caller must then rely on LLM-sourced data
    that performs proper USD conversion from the filing itself.
    """
    import yfinance as yf

    tk = yf.Ticker(yf_ticker_str)
    result = {
        "market_cap": None,
        "currency": "USD",
        "shares_outstanding": None,
        "fcf_table": pd.DataFrame(),          # year, OCF, CapEx, FCF, FCF/share
        "fcf_per_share_by_year": {},
        "latest_fcf": None,
        "avg_fcf_5y": None,
    }

    # --- info ----------------------------------------------------------
    try:
        info = tk.info
        result["market_cap"] = info.get("marketCap")
        result["currency"] = info.get("currency", "USD")
        result["shares_outstanding"] = info.get("sharesOutstanding")

        # financialCurrency is the currency of the financial statements,
        # which differs from `currency` (trading currency) for foreign-listed
        # companies (e.g. BABA: currency=USD but financialCurrency=CNY).
        # If the financials are not in USD, discard the cashflow table entirely
        # to prevent RMB values being treated as USD.
        fin_currency = info.get("financialCurrency", "USD") or "USD"
        if fin_currency.upper() != "USD":
            result["_non_usd_financials"] = fin_currency
            return result
    except Exception:
        pass

    # --- cashflow ------------------------------------------------------
    try:
        cf = tk.cashflow
        if cf is None or cf.empty:
            return result

        ocf_row = capex_row = fcf_row = None
        for idx in cf.index:
            il = str(idx).lower()
            if "free cash flow" in il:
                fcf_row = idx
            if "operating" in il and "cash" in il:
                ocf_row = idx
            if "capital expend" in il:
                capex_row = idx

        rows = []
        total_fcf = {}
        for col in cf.columns:
            ts = pd.Timestamp(col)
            ocf_val = float(cf.loc[ocf_row, col]) if ocf_row and pd.notna(cf.loc[ocf_row, col]) else None
            capex_val = float(cf.loc[capex_row, col]) if capex_row and pd.notna(cf.loc[capex_row, col]) else None

            if fcf_row is not None and pd.notna(cf.loc[fcf_row, col]):
                fcf_val = float(cf.loc[fcf_row, col])
            elif ocf_val is not None and capex_val is not None:
                fcf_val = ocf_val + capex_val   # capex is negative
            else:
                fcf_val = None

            if fcf_val is not None:
                total_fcf[ts] = fcf_val
            rows.append({"年份": ts, "OCF": ocf_val, "CapEx": capex_val, "FCF": fcf_val})

        if not total_fcf:
            return result

        # --- shares by year from balance sheet -------------------------
        shares_by_year = {}
        try:
            bs = tk.balance_sheet
            if bs is not None and not bs.empty:
                for idx in bs.index:
                    il = str(idx).lower()
                    if "ordinary shares" in il or "share issued" in il:
                        for col in bs.columns:
                            val = bs.loc[idx, col]
                            if pd.notna(val) and val > 0:
                                shares_by_year[pd.Timestamp(col)] = float(val)
                        if shares_by_year:
                            break
        except Exception:
            pass

        current_shares = result["shares_outstanding"]

        # --- build table & per-share dict ------------------------------
        for row in rows:
            ts = row["年份"]
            shares = shares_by_year.get(ts) or current_shares
            if shares and shares > 0 and row["FCF"] is not None:
                row["每股FCF"] = row["FCF"] / shares
                result["fcf_per_share_by_year"][ts] = row["每股FCF"]
            else:
                row["每股FCF"] = None

        # Sort descending and compute rolling averages
        sorted_rows = sorted(rows, key=lambda x: x["年份"], reverse=True)
        for i, row in enumerate(sorted_rows):
            w3 = sorted_rows[i: i + 3]
            w5 = sorted_rows[i: i + 5]
            ps3 = [r["每股FCF"] for r in w3 if r["每股FCF"] is not None]
            ps5 = [r["每股FCF"] for r in w5 if r["每股FCF"] is not None]
            row["3年均每股FCF"] = np.mean(ps3) if ps3 else None
            row["5年均每股FCF"] = np.mean(ps5) if ps5 else None

        tbl = pd.DataFrame(sorted_rows).head(10)
        tbl["年份"] = tbl["年份"].dt.strftime("%Y-%m-%d")
        result["fcf_table"] = tbl

        sorted_dates = sorted(total_fcf.keys(), reverse=True)
        result["latest_fcf"] = total_fcf[sorted_dates[0]]
        n = min(5, len(sorted_dates))
        result["avg_fcf_5y"] = sum(total_fcf[d] for d in sorted_dates[:n]) / n

    except Exception:
        pass

    return result


def _ak_fcf_data(symbol):
    """Get annual FCF from akshare (A-share stocks) via 新浪现金流量表."""
    import akshare as ak

    full_symbol = f"sh{symbol}" if symbol.startswith(("6", "9")) else f"sz{symbol}"
    result = {
        "fcf_table": pd.DataFrame(),
        "fcf_per_share_by_year": {},
        "latest_fcf": None,
        "avg_fcf_5y": None,
        "shares_outstanding": None,
    }

    try:
        df = ak.stock_financial_report_sina(stock=full_symbol, symbol="现金流量表")
    except Exception:
        return result

    columns = df.columns.tolist()
    date_col = next((c for c in columns if "期" in c or "日" in c), None)
    ocf_col = next((c for c in columns if "经营活动" in c and "净额" in c), None)
    capex_col = next((c for c in columns if "固定资产" in c and "支付" in c), None)

    if not all([date_col, ocf_col, capex_col]):
        return result

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df_annual = df[df[date_col].dt.month == 12].copy()
    df_annual[ocf_col] = pd.to_numeric(df_annual[ocf_col], errors="coerce")
    df_annual[capex_col] = pd.to_numeric(df_annual[capex_col], errors="coerce")
    df_annual = df_annual.dropna(subset=[ocf_col, capex_col])

    df_annual["FCF"] = df_annual[ocf_col] - df_annual[capex_col]

    # --- 获取总股本 (用 akshare) ----------------------------------------
    shares_by_year = {}
    try:
        bs = ak.stock_financial_report_sina(stock=full_symbol, symbol="资产负债表")
        bs_cols = bs.columns.tolist()
        bs_date_col = next((c for c in bs_cols if "期" in c or "日" in c), None)
        shares_col = next((c for c in bs_cols if "股本" in c and "实收" not in c), None)
        if not shares_col:
            shares_col = next((c for c in bs_cols if "实收资本" in c or "股本" in c), None)
        if bs_date_col and shares_col:
            bs[bs_date_col] = pd.to_datetime(bs[bs_date_col], errors="coerce")
            bs_annual = bs[bs[bs_date_col].dt.month == 12].copy()
            bs_annual[shares_col] = pd.to_numeric(bs_annual[shares_col], errors="coerce")
            for _, row in bs_annual.iterrows():
                if pd.notna(row[bs_date_col]) and pd.notna(row[shares_col]) and row[shares_col] > 0:
                    shares_by_year[pd.Timestamp(row[bs_date_col])] = float(row[shares_col])
    except Exception:
        pass

    # Always try to get latest shares from individual info
    current_shares = None
    try:
        info_df = ak.stock_individual_info_em(symbol=symbol)
        for _, row in info_df.iterrows():
            if "总股本" in str(row.iloc[0]):
                current_shares = float(row.iloc[1])
                break
    except Exception:
        pass

    # Fallback: most recent from balance sheet
    if not current_shares and shares_by_year:
        latest_date = max(shares_by_year.keys())
        current_shares = shares_by_year[latest_date]

    result["shares_outstanding"] = current_shares

    # --- build output --------------------------------------------------
    rows = []
    for _, r in df_annual.iterrows():
        ts = pd.Timestamp(r[date_col])
        shares = shares_by_year.get(ts) or current_shares
        fcf_ps = (r["FCF"] / shares) if (shares and shares > 0) else None
        rows.append({
            "年份": ts,
            "OCF": float(r[ocf_col]),
            "CapEx": float(r[capex_col]),
            "FCF": float(r["FCF"]),
            "每股FCF": fcf_ps,
        })
        if fcf_ps is not None:
            result["fcf_per_share_by_year"][ts] = fcf_ps

    # Sort descending and compute rolling averages
    sorted_rows = sorted(rows, key=lambda x: x["年份"], reverse=True)
    for i, row in enumerate(sorted_rows):
        w3 = sorted_rows[i: i + 3]
        w5 = sorted_rows[i: i + 5]
        ps3 = [r["每股FCF"] for r in w3 if r["每股FCF"] is not None]
        ps5 = [r["每股FCF"] for r in w5 if r["每股FCF"] is not None]
        row["3年均每股FCF"] = np.mean(ps3) if ps3 else None
        row["5年均每股FCF"] = np.mean(ps5) if ps5 else None

    tbl = pd.DataFrame(sorted_rows).head(10)
    tbl["年份"] = tbl["年份"].dt.strftime("%Y-%m-%d")
    result["fcf_table"] = tbl

    if sorted_rows:
        result["latest_fcf"] = sorted_rows[0]["FCF"]
        n = min(5, len(sorted_rows))
        result["avg_fcf_5y"] = sum(r["FCF"] for r in sorted_rows[:n]) / n

    return result


# ═══════════════════════════════════════════════════════════════════════
#  DCF Valuation Lines  (uses 3-year rolling avg per-share FCF)
# ═══════════════════════════════════════════════════════════════════════
def _add_yf_cross_check(primary_fin, yf_fin):
    """Add yfinance cross-check columns to the primary (XBRL) FCF table."""
    tbl = primary_fin.get("fcf_table")
    if tbl is None or tbl.empty:
        return
    yf_ps_by_year = {}
    for ts, val in yf_fin.get("fcf_per_share_by_year", {}).items():
        yf_ps_by_year[pd.Timestamp(ts).year] = val
    if not yf_ps_by_year:
        return  # No yfinance data to cross-check

    yf_vals = []
    for _, row in tbl.iterrows():
        yr = int(str(row["年份"])[:4])
        yf_val = yf_ps_by_year.get(yr)
        yf_vals.append(yf_val)
    tbl["yf每股FCF"] = yf_vals


def compute_dcf_lines(fcf_per_share_by_year):
    """Compute DCF valuation lines at 14x, 24x, 34x multiples.

        For each annual report year, FCF_0 = mean of up to 3 years' per-share FCF
        ending at that year. Returns a DataFrame for marker+dashed line plotting.
        """
    if not fcf_per_share_by_year:
        return pd.DataFrame()

    sorted_dates = sorted(fcf_per_share_by_year.keys())
    dates, v14, v24, v34 = [], [], [], []

    for i, date in enumerate(sorted_dates):
        start_idx = max(0, i - 2)  # up to 3 years (current + 2 prior)
        window = sorted_dates[start_idx : i + 1]
        avg_fcf = np.mean([fcf_per_share_by_year[d] for d in window])

        # If 3-year avg is ≤ 0, fall back to 5-year window
        if avg_fcf <= 0:
            start_idx5 = max(0, i - 4)
            window5 = sorted_dates[start_idx5 : i + 1]
            avg_fcf5 = np.mean([fcf_per_share_by_year[d] for d in window5])
            if avg_fcf5 > avg_fcf:
                avg_fcf = avg_fcf5

        dates.append(pd.Timestamp(date))
        v14.append(14 * avg_fcf)
        v24.append(24 * avg_fcf)
        v34.append(34 * avg_fcf)

    # Extend last value to today so the step-line reaches the right edge
    dates.append(pd.Timestamp(datetime.now().date()))
    v14.append(v14[-1])
    v24.append(v24[-1])
    v34.append(v34[-1])

    return pd.DataFrame(
        {"date": dates, "dcf_14x": v14, "dcf_24x": v24, "dcf_34x": v34}
    )


# ═══════════════════════════════════════════════════════════════════════
#  US stocks – FMP-only path (OHLCV + FCF)
# ═══════════════════════════════════════════════════════════════════════
def get_us_data(ticker, years=15):
    """Fetch US OHLCV + FCF + shares via FMP.

    Data source priority for FCF:
        FMP (primary, up to 30 years) → Gemini AI reads filings to fill gaps

        Shares outstanding and market cap come from FMP (ADR-adjusted since Feb 2025):
      FMP /stable/profile → ADR-adjusted sharesOutstanding, mktCap
      FMP /api/v4/shares_float → explicit adrRatio if available
    """
    from downloader import SmartSECDownloader

    ticker = ticker.upper()

    # --- OHLCV: FMP ----------------------------------------------------
    hist = _fmp_ohlcv(ticker)
    if hist.empty:
        raise ValueError(f"未找到 {ticker} 的历史数据 (FMP 无数据)")

    # Latest price
    last_price = float(hist["Close"].iloc[-1]) if not hist.empty else None

    # --- Shares + market cap: FMP primary (ADR-adjusted) ---------------
    # FMP Feb 2025 changelog: sharesOutstanding in Quote/Profile is now
    # ADR-ratio-adjusted, so dividing total FCF by this gives FCF per ADR.
    profile = _fmp_profile(ticker)
    shares_outstanding = profile["shares_outstanding"]
    market_cap = profile["market_cap"]
    adr_ratio = profile["adr_ratio"]
    is_adr = profile["is_adr"]

    # US path uses USD display currency.
    currency = "USD"

    # --- Get CIK (still needed for filing downloads / AI fill) ---------
    dl = SmartSECDownloader(email="lianhdff@gmail.com")
    cik = None
    try:
        cik = dl.get_cik(ticker)
    except Exception:
        pass

    # --- FCF from FMP (primary source, up to 30 years) -----------------
    fin = {
        "fcf_table": pd.DataFrame(),
        "fcf_per_share_by_year": {},
    }
    fmp_currency = "USD"
    fmp_status = ""
    try:
        fmp_fin = _fmp_fcf_data(ticker, shares_outstanding)
        fmp_currency = fmp_fin.get("fmp_currency", "USD")
        fmp_tbl = fmp_fin.get("fmp_fcf_table")
        fmp_rows = len(fmp_tbl) if fmp_tbl is not None and not fmp_tbl.empty else 0
        if fmp_rows == 0:
            fmp_status = "FMP: 未返回 FCF 数据"
        else:
            fmp_status = f"FMP: 获取到 {fmp_rows} 年数据 ({fmp_currency})"
            if is_adr and adr_ratio is not None:
                fmp_status += f" | ADR 比例: 1 ADR = {adr_ratio} 股 (已按 ADR 股本折算每股 FCF)"
            if fmp_currency != "USD":
                fmp_status += " ⚠️ 非 USD 报告货币，将在分析时换算"

            fin["fcf_table"] = fmp_tbl.copy()
            # Build fcf_per_share_by_year from FMP table
            ps = {}
            for _, row in fmp_tbl.iterrows():
                if pd.notna(row.get("每股FCF")):
                    yr = str(row["年份"])[:4]
                    ps[pd.Timestamp(f"{yr}-12-31")] = row["每股FCF"]
            if ps:
                fin["fcf_per_share_by_year"] = ps
    except Exception as _fmp_err:
        fmp_status = f"FMP: 获取失败 — {_fmp_err}"

    # --- Analyst ratings + price target from FMP -----------------------
    analyst_data = None
    try:
        analyst_data = _fmp_analyst_data(ticker)
        a_status = analyst_data.get("fmp_analyst_status", "")
        if a_status:
            fmp_status = (fmp_status + " | 分析师: " + a_status) if fmp_status else ("分析师: " + a_status)
    except Exception as _ae:
        fmp_status = (fmp_status + f" | 分析师获取失败: {_ae}") if fmp_status else f"分析师获取失败: {_ae}"

    # --- FMP DCF: annual history first, realtime fallback ---------------
    fmp_dcf_df = pd.DataFrame()
    fmp_dcf_value = None
    try:
        dcf_hist = _fmp_dcf_history(ticker)
        fmp_dcf_df = dcf_hist["dcf_df"]
        fmp_dcf_value = dcf_hist["dcf_current"]
    except Exception:
        pass

    return {
        "ohlcv": hist,
        "last_price": last_price,
        "market_cap": market_cap,
        "currency": currency,
        "cik": cik,
        "shares_outstanding": shares_outstanding,
        "adr_ratio": adr_ratio,
        "is_adr": is_adr,
        "fmp_currency": fmp_currency,
        "fmp_status": fmp_status,
        "analyst_data": analyst_data,
        "fmp_dcf_value": fmp_dcf_value,
        "fmp_dcf_df": fmp_dcf_df,
        **{k: v for k, v in fin.items() if k not in ("market_cap", "currency", "shares_outstanding")},
    }


# ═══════════════════════════════════════════════════════════════════════
#  A-shares (akshare OHLCV + akshare FCF)
# ═══════════════════════════════════════════════════════════════════════

def _sina_cn_ohlcv(code: str, start: str, end: str) -> pd.DataFrame:
    """Fetch A-share OHLCV from Sina Finance (qfq, fast one-shot download).

    Sina qfq.js row format: date,open,close,high,low,volume,amount
    Returns standard DataFrame: Date, Open, High, Low, Close, Volume
    """
    import requests, re, json

    prefix = "sh" if code.startswith(("6", "9")) else "sz"
    symbol = f"{prefix}{code}"
    url = f"https://finance.sina.com.cn/realstock/company/{symbol}/qfq.js"

    r = requests.get(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://finance.sina.com.cn/",
    }, timeout=20)
    r.raise_for_status()

    m = re.search(r'=(\{.*\})', r.text, re.DOTALL)
    if not m:
        raise ValueError(f"无法解析新浪 qfq.js 响应 (symbol={symbol})")

    raw_rows = json.loads(m.group(1)).get("d", [])
    if not raw_rows:
        raise ValueError(f"新浪财经未返回 {symbol} 的历史数据")

    records = []
    for line in raw_rows:
        p = line.split(",")
        if len(p) < 5:
            continue
        records.append({
            "Date":   p[0],
            "Open":   p[1],
            "Close":  p[2],   # 新浪格式: date,open,close,high,low,...
            "High":   p[3],
            "Low":    p[4],
            "Volume": p[5] if len(p) > 5 else None,
        })

    df = pd.DataFrame(records)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)

    start_dt = pd.to_datetime(start)
    end_dt   = pd.to_datetime(end)
    df = df[(df["Date"] >= start_dt) & (df["Date"] <= end_dt)].reset_index(drop=True)

    if df.empty:
        raise ValueError(f"新浪财经无 {symbol} 在 {start}~{end} 的数据")
    return df[["Date", "Open", "High", "Low", "Close", "Volume"]]


def _tx_cn_ohlcv(code: str, start: str, end: str) -> pd.DataFrame:
    """Fetch A-share OHLCV from Tencent Finance via akshare (qfq fallback).

    akshare stock_zh_a_hist_tx row format: date,open,close,high,low,amount
    Returns standard DataFrame: Date, Open, High, Low, Close, Volume
    """
    import akshare as ak

    prefix = "sh" if code.startswith(("6", "9")) else "sz"
    symbol = f"{prefix}{code}"

    # Tencent needs YYYYMMDD dates
    start_8 = pd.to_datetime(start).strftime("%Y%m%d")
    end_8   = pd.to_datetime(end).strftime("%Y%m%d")

    raw = ak.stock_zh_a_hist_tx(symbol=symbol, start_date=start_8,
                                  end_date=end_8, adjust="qfq")
    if raw is None or raw.empty:
        raise ValueError(f"腾讯财经无 {symbol} 的数据")

    # Tencent columns: date, open, close, high, low, amount
    df = raw.rename(columns={
        "date": "Date", "open": "Open", "close": "Close",
        "high": "High", "low": "Low", "amount": "Volume",
    })
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
    return df[["Date", "Open", "High", "Low", "Close", "Volume"]]


def _ts_cn_ohlcv(code: str, start: str, end: str) -> pd.DataFrame:
    """Fetch A-share OHLCV from Tushare (forward-adjusted, qfq)."""
    import os, tushare as ts

    token = os.environ.get("TUSHARE_TOKEN", "")
    if not token:
        try:
            env_path = os.path.join(os.path.dirname(__file__), ".env")
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("TUSHARE_TOKEN="):
                        token = line.split("=", 1)[1].strip()
                        break
        except Exception:
            pass
    if not token:
        raise RuntimeError("TUSHARE_TOKEN 未设置，无法使用 Tushare 备用数据源")

    ts.set_token(token)
    ts_code = f"{code}.SH" if code.startswith(("6", "9")) else f"{code}.SZ"
    df = ts.pro_bar(ts_code=ts_code, start_date=start, end_date=end, adj="qfq")
    if df is None or df.empty:
        raise ValueError(f"Tushare 未返回 {code} 的数据")

    df = df.rename(columns={
        "trade_date": "Date", "open": "Open", "high": "High",
        "low": "Low", "close": "Close", "vol": "Volume",
    })
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date").reset_index(drop=True)
    return df[["Date", "Open", "High", "Low", "Close", "Volume"]]


def get_cn_data(code, years=15):
    """Fetch A-share OHLCV via 新浪财经 (primary) / 腾讯财经 / Tushare (fallback); FCF via akshare 新浪报表."""
    import akshare as ak

    end   = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=365 * years)).strftime("%Y-%m-%d")

    # ── Primary: 新浪财经 (fast one-shot, all history) ─────────────────
    df = None
    _sina_err = _tx_err = _ts_err = None
    try:
        df = _sina_cn_ohlcv(code, start, end)
    except Exception as e:
        _sina_err = e

    # ── Fallback 1: 腾讯财经 (via akshare, slower per-year loop) ──────
    if df is None or df.empty:
        try:
            df = _tx_cn_ohlcv(code, start, end)
        except Exception as e:
            _tx_err = e

    # ── Fallback 2: Tushare ────────────────────────────────────────────
    if df is None or df.empty:
        try:
            start_8 = start.replace("-", "")
            end_8   = end.replace("-", "")
            df = _ts_cn_ohlcv(code, start_8, end_8)
        except Exception as e:
            _ts_err = e
            raise ConnectionError(
                f"获取 {code} K线失败 — 新浪: {_sina_err} | 腾讯: {_tx_err} | Tushare: {_ts_err}"
            )

    if df is None or df.empty:
        raise ValueError(f"未找到 {code} 的历史数据")

    # Financial data via akshare 新浪现金流量表
    fin = _ak_fcf_data(code)
    fin["currency"] = "CNY"

    # Market cap from akshare
    try:
        info_df = ak.stock_individual_info_em(symbol=code)
        for _, row in info_df.iterrows():
            if "总市值" in str(row.iloc[0]):
                fin["market_cap"] = float(row.iloc[1])
                break
    except Exception:
        pass

    return {"ohlcv": df, **fin}


# ═══════════════════════════════════════════════════════════════════════
#  HK stocks (Futu OpenD kline + yfinance financials)
# ═══════════════════════════════════════════════════════════════════════
def get_hk_data(code, years=15):
    """Fetch HK kline via Futu OpenD; financials via yfinance."""
    from futu_client import FutuClient

    futu_code = FutuClient.build_code(code, "HK")

    with FutuClient() as fc:
        kline = fc.get_history_kline(futu_code, years)
        snap = fc.get_snapshot(futu_code)

    ohlcv = kline.rename(columns={
        "time_key": "Date", "open": "Open", "high": "High",
        "low": "Low", "close": "Close", "volume": "Volume",
    })

    yf_code = f"{code.lstrip('0') or '0'}.HK"
    fin = _yf_fcf_and_shares(yf_code)
    fin["currency"] = "HKD"
    fin["market_cap"] = snap.get("total_market_val")

    return {"ohlcv": ohlcv, **fin}
