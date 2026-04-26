"""
ETL CLI entry point.

Usage:
    python etl_run.py --tickers NVDA
    python etl_run.py --tickers NVDA AAPL MSFT
"""

import argparse
import sys

from db.schema import get_conn, init_db
from etl.pipeline import USRunOptions, run_us_ticker


def main():
    parser = argparse.ArgumentParser(description="Stock Analyzer ETL")
    parser.add_argument("--tickers", nargs="+", required=True,
                        help="Ticker symbols to fetch, e.g. NVDA AAPL")
    parser.add_argument("--init", action="store_true",
                        help="Re-initialize DB schema before running")
    parser.add_argument(
        "--skip-optional",
        action="store_true",
        help="Skip management, segment/geo revenue, interest expense (faster)",
    )
    args = parser.parse_args()

    if args.init:
        print("Initialising database schema...")
        init_db()

    opts = USRunOptions(skip_optional=args.skip_optional, verbose=True)
    failed = []
    with get_conn() as conn:
        for ticker in args.tickers:
            try:
                run_us_ticker(conn, ticker, opts)
            except Exception as e:
                print(f"  ERROR {ticker}: {e}", file=sys.stderr)
                failed.append(ticker)

    print(f"\nCompleted: {len(args.tickers) - len(failed)}/{len(args.tickers)} tickers")
    if failed:
        print(f"Failed: {', '.join(failed)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
