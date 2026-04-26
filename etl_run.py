"""
ETL CLI entry point.

Usage:
    python etl_run.py --tickers NVDA
    python etl_run.py --tickers NVDA AAPL MSFT
"""

import argparse
import sys
from datetime import date
from db.schema import get_conn, init_db
from etl.sources.fmp import fetch_profile, fetch_ohlcv, fetch_fcf_annual
from etl.sources.fmp_dcf import load_fmp_dcf_history
from etl.loader import upsert_company, upsert_ohlcv_daily, upsert_fundamentals_annual
from etl.compute import compute_ema, compute_dcf_history, compute_dcf_lines


def run_ticker(ticker: str, conn) -> None:
    ticker = ticker.upper()
    print(f"\n{'='*50}")
    print(f"  {ticker}")
    print(f"{'='*50}")

    # 1. Profile → companies
    print("  [1/7] Fetching profile...")
    profile = fetch_profile(ticker)
    upsert_company(conn, profile)
    shares_out_raw = profile.get("_shares_out_raw")
    print(f"        {profile['name']} | {profile['sector']} | shares: {shares_out_raw:,.0f}" if shares_out_raw else f"        {profile['name']}")

    # 2. Cash flow → fundamentals_annual (FCF columns)
    print("  [2/7] Fetching annual FCF...")
    fcf_rows = fetch_fcf_annual(ticker, shares_out_raw)
    upsert_fundamentals_annual(conn, fcf_rows)
    print(f"        {len(fcf_rows)} annual FCF rows written")

    # 3. OHLCV → ohlcv_daily (aligned to annual fundamentals span)
    date_span = conn.execute(
        """
        SELECT MIN(fiscal_end_date), MAX(fiscal_end_date)
        FROM fundamentals_annual
        WHERE ticker = ?
        """,
        [ticker],
    ).fetchone()
    ohlcv_from = str(date_span[0]) if date_span and date_span[0] is not None else None
    ohlcv_to = str(date.today())

    print("  [3/7] Fetching daily OHLCV...")
    ohlcv_rows = fetch_ohlcv(ticker, shares_out_raw, date_from=ohlcv_from, date_to=ohlcv_to)
    upsert_ohlcv_daily(conn, ohlcv_rows)
    print(f"        {len(ohlcv_rows):,} daily bars written ({ohlcv_from} -> {ohlcv_to})")

    # 4. FMP DCF history → fmp_dcf_history (non-blocking)
    print("  [4/7] Fetching FMP DCF history...")
    try:
        fmp_dcf_rows = load_fmp_dcf_history(ticker, conn)
        print(f"        {fmp_dcf_rows:,} FMP DCF rows written")
    except Exception as e:
        print(f"        skipped: {e}")

    # 5. Compute EMA → ohlcv_daily.ema10/ema250
    print("  [5/7] Computing EMA10/EMA250...")
    ema_rows = compute_ema(ticker, conn)
    print(f"        {ema_rows:,} EMA rows updated")

    # 6. Compute DCF history step-lines → dcf_history
    print("  [6/7] Computing DCF history...")
    dcf_hist_rows = compute_dcf_history(ticker, conn)
    print(f"        {dcf_hist_rows:,} DCF history rows written")

    # 7. Compute latest DCF metrics → dcf_metrics
    print("  [7/7] Computing latest DCF metrics...")
    result = compute_dcf_lines(ticker, conn)
    if result:
        avg = result["fcf_per_share_avg3yr"]
        print(f"        3yr avg FCF/share: ${avg:.2f} | "
              f"14x=${14*avg:.2f}  24x=${24*avg:.2f}  34x=${34*avg:.2f}")
    else:
        print("        No FCF data available for DCF computation")

    print(f"  Done: {ticker}")


def main():
    parser = argparse.ArgumentParser(description="Stock Analyzer ETL")
    parser.add_argument("--tickers", nargs="+", required=True,
                        help="Ticker symbols to fetch, e.g. NVDA AAPL")
    parser.add_argument("--init", action="store_true",
                        help="Re-initialize DB schema before running")
    args = parser.parse_args()

    if args.init:
        print("Initialising database schema...")
        init_db()

    failed = []
    with get_conn() as conn:
        for ticker in args.tickers:
            try:
                run_ticker(ticker, conn)
            except Exception as e:
                print(f"  ERROR {ticker}: {e}", file=sys.stderr)
                failed.append(ticker)

    print(f"\nCompleted: {len(args.tickers) - len(failed)}/{len(args.tickers)} tickers")
    if failed:
        print(f"Failed: {', '.join(failed)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
