"""Single-ticker CN ETL via Tushare, keeping DB schema unchanged."""

from __future__ import annotations

import argparse
from pathlib import Path

from db.schema import get_conn
from etl.dotenv_local import merge_dotenv_into_environ
from etl.loader import upsert_company, upsert_ohlcv_daily, upsert_fundamentals_annual
from etl.compute import compute_ema, compute_dcf_history, compute_dcf_lines
from etl.sources.tushare_cn import fetch_stock_basic_row, fetch_ohlcv_qfq, fetch_fundamentals_annual_rows


def run_cn_ticker(code: str) -> dict:
    c = str(code).strip().zfill(6)
    ts_basic = fetch_stock_basic_row(c) or {}
    ticker_db = f"{c}.SS" if c.startswith(("6", "9")) else f"{c}.SZ"
    ohlcv = fetch_ohlcv_qfq(c)
    fund_rows = fetch_fundamentals_annual_rows(c)

    ohlcv_rows = []
    for _, r in ohlcv.iterrows():
        ohlcv_rows.append(
            {
                "ticker": ticker_db,
                "date": str(r["date"])[:10],
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
                "volume": int(float(r["volume"])) if r["volume"] is not None else 0,
                "adj_close": float(r["adj_close"]),
                "market_cap": None,
            }
        )

    company = {
        "ticker": ticker_db,
        "market": "CN",
        "name": str(ts_basic.get("name") or c),
        "exchange": "SSE" if ticker_db.endswith(".SS") else "SZSE",
        "exchange_full_name": "Shanghai Stock Exchange" if ticker_db.endswith(".SS") else "Shenzhen Stock Exchange",
        "country": "CN",
        "sector": None,
        "industry": str(ts_basic.get("industry") or "") or None,
        "currency": "CNY",
        "description": "A-share common stock (Tushare)",
        "shares_out": None,
    }

    with get_conn() as conn:
        upsert_company(conn, company)
        upsert_ohlcv_daily(conn, ohlcv_rows)
        if fund_rows:
            upsert_fundamentals_annual(conn, fund_rows)
        ema_rows = compute_ema(ticker_db, conn)
        dcf_hist_rows = compute_dcf_history(ticker_db, conn)
        dcf_lines = compute_dcf_lines(ticker_db, conn)

    return {
        "ticker": ticker_db,
        "ohlcv_rows": len(ohlcv_rows),
        "fund_rows": len(fund_rows),
        "ema_rows": int(ema_rows or 0),
        "dcf_history_rows": int(dcf_hist_rows or 0),
        "has_dcf_metrics": bool(dcf_lines),
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Run CN single ticker ETL via Tushare")
    p.add_argument("--code", required=True, help="A-share code, e.g. 600519")
    args = p.parse_args()

    merge_dotenv_into_environ(Path(__file__).resolve().parents[1])
    out = run_cn_ticker(args.code)
    print(out)


if __name__ == "__main__":
    main()
