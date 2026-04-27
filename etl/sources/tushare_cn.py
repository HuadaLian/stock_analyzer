"""Tushare helpers for A-share single-ticker ETL."""

from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import tushare as ts


def load_tushare_token(repo_root: Path | None = None) -> str:
    """Load Tushare token from env/.env with backward-compatible key names."""
    token = (
        os.environ.get("TUSHARE_TOKEN", "").strip()
        or os.environ.get("TUSHARE_API_KEY", "").strip()
    )
    if token:
        return token

    root = repo_root or Path(__file__).resolve().parents[2]
    env_path = root / ".env"
    if not env_path.is_file():
        return ""
    try:
        for raw in env_path.read_text(encoding="utf-8-sig").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            k = key.strip().upper()
            v = val.strip().strip('"').strip("'")
            if k in ("TUSHARE_TOKEN", "TUSHARE_API_KEY") and v:
                return v
    except OSError:
        return ""
    return ""


def _to_ts_code(code: str) -> str:
    c = str(code).strip()
    if c.endswith(".SH") or c.endswith(".SZ"):
        return c.upper()
    return f"{c}.SH" if c.startswith(("6", "9")) else f"{c}.SZ"


def fetch_stock_basic_row(code: str) -> dict | None:
    token = load_tushare_token()
    if not token:
        raise RuntimeError("Tushare token missing (set TUSHARE_TOKEN or TUSHARE_API_KEY)")
    ts.set_token(token)
    pro = ts.pro_api()
    ts_code = _to_ts_code(code)
    df = pro.stock_basic(
        ts_code=ts_code,
        list_status="L",
        fields="ts_code,symbol,name,area,industry,market,list_date",
    )
    if df is None or df.empty:
        return None
    return df.iloc[0].to_dict()


def fetch_ohlcv_qfq(code: str, years: int = 15) -> pd.DataFrame:
    token = load_tushare_token()
    if not token:
        raise RuntimeError("Tushare token missing (set TUSHARE_TOKEN or TUSHARE_API_KEY)")
    ts.set_token(token)
    ts_code = _to_ts_code(code)
    start = (datetime.now() - timedelta(days=365 * years)).strftime("%Y%m%d")
    end = datetime.now().strftime("%Y%m%d")
    df = ts.pro_bar(ts_code=ts_code, start_date=start, end_date=end, adj="qfq")
    if df is None or df.empty:
        raise ValueError(f"Tushare pro_bar empty for {ts_code}")
    out = df.rename(
        columns={
            "trade_date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "vol": "volume",
        }
    )[["date", "open", "high", "low", "close", "volume"]].copy()
    out["date"] = pd.to_datetime(out["date"], format="%Y%m%d", errors="coerce")
    out = out.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    out["adj_close"] = pd.to_numeric(out["close"], errors="coerce")
    out["market_cap"] = None
    return out


def fetch_fundamentals_annual_rows(code: str) -> list[dict]:
    """Build fundamentals_annual rows (USD-normalized columns + reporting currency)."""
    token = load_tushare_token()
    if not token:
        raise RuntimeError("Tushare token missing (set TUSHARE_TOKEN or TUSHARE_API_KEY)")
    ts.set_token(token)
    pro = ts.pro_api()
    ts_code = _to_ts_code(code)

    cf = pro.cashflow(
        ts_code=ts_code,
        fields="ts_code,ann_date,end_date,n_cashflow_act,c_pay_acq_const_fiolta,free_cashflow",
    )
    if cf is None or cf.empty:
        return []
    cf = cf.copy()
    cf["end_date"] = pd.to_datetime(cf["end_date"], format="%Y%m%d", errors="coerce")
    cf = cf[cf["end_date"].dt.month == 12].copy()
    if cf.empty:
        return []
    cf = cf.sort_values("end_date", ascending=False).drop_duplicates(subset=["end_date"])

    bs = pro.balancesheet(
        ts_code=ts_code,
        fields="ts_code,end_date,total_share",
    )
    share_map: dict[str, float] = {}
    if bs is not None and not bs.empty:
        b = bs.copy()
        b["end_date"] = pd.to_datetime(b["end_date"], format="%Y%m%d", errors="coerce")
        b = b.dropna(subset=["end_date"]).sort_values("end_date", ascending=False).drop_duplicates(subset=["end_date"])
        for _, r in b.iterrows():
            ed = r["end_date"].strftime("%Y-%m-%d")
            v = pd.to_numeric(r.get("total_share"), errors="coerce")
            if pd.notna(v) and float(v) > 0:
                share_map[ed] = float(v)

    out: list[dict] = []
    for _, r in cf.iterrows():
        ed = r["end_date"]
        if pd.isna(ed):
            continue
        eds = ed.strftime("%Y-%m-%d")
        fy = int(eds[:4])

        fcf_raw = pd.to_numeric(r.get("free_cashflow"), errors="coerce")
        if pd.isna(fcf_raw):
            ocf = pd.to_numeric(r.get("n_cashflow_act"), errors="coerce")
            capex = pd.to_numeric(r.get("c_pay_acq_const_fiolta"), errors="coerce")
            if pd.notna(ocf) and pd.notna(capex):
                fcf_raw = float(ocf) - abs(float(capex))
            else:
                fcf_raw = pd.NA

        shares_raw = share_map.get(eds)
        fcf_ps = None
        if shares_raw and pd.notna(fcf_raw) and float(shares_raw) > 0:
            fcf_ps = float(fcf_raw) / float(shares_raw)

        out.append(
            {
                "ticker": f"{code}.SS" if str(code).startswith(("6", "9")) else f"{code}.SZ",
                "fiscal_year": fy,
                "fiscal_end_date": eds,
                "filing_date": None,
                "currency": "USD",
                "reporting_currency": "CNY",
                "fx_to_usd": 1.0,
                "fcf": (float(fcf_raw) / 1_000_000) if pd.notna(fcf_raw) else None,
                "fcf_per_share": fcf_ps,
                "shares_out": (float(shares_raw) / 1_000_000) if shares_raw else None,
                "source": "tushare",
                "revenue": None,
                "revenue_per_share": None,
                "gross_profit": None,
                "gross_margin": None,
                "operating_income": None,
                "operating_margin": None,
                "net_income": None,
                "profit_margin": None,
                "eps": None,
                "depreciation": None,
                "effective_tax_rate": None,
                "dividend_per_share": None,
                "total_equity": None,
                "long_term_debt": None,
                "working_capital": None,
                "book_value_per_share": None,
                "tangible_bv_per_share": None,
                "roic": None,
                "return_on_capital": None,
                "return_on_equity": None,
            }
        )
    return out
