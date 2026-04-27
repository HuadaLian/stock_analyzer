"""Per-ticker completeness / D1 readiness (see ``db.data_quality_spec``).

Usage:
    python -m db.checks --ticker NVDA
    python -m db.checks --all
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, asdict

from db.schema import get_conn


@dataclass
class Coverage:
    total: int
    non_null: int

    @property
    def ratio(self) -> float:
        if self.total <= 0:
            return 0.0
        return self.non_null / self.total


@dataclass
class TickerCompleteness:
    ticker: str
    company_exists: bool
    ohlcv_rows: int
    ohlcv_start: str | None
    ohlcv_end: str | None
    ema10: Coverage
    ema250: Coverage
    fundamentals_rows: int
    fiscal_year_min: int | None
    fiscal_year_max: int | None
    fcf_per_share: Coverage
    filing_date: Coverage
    dcf_history_rows: int
    dcf_history_expected_rows: int
    dcf_history_anchor_date: Coverage
    fmp_dcf_history_rows: int
    fmp_dcf_value: Coverage
    dcf_metrics_exists: bool
    dcf_metrics_computed_at: str | None
    dcf_metrics_latest_price_date: str | None

    @property
    def d1_ema_ready(self) -> bool:
        return self.ema10.non_null > 0 and self.ema250.non_null > 0

    @property
    def d1_dcf_ready(self) -> bool:
        return self.dcf_history_rows > 0

    @property
    def d1_fmp_dcf_ready(self) -> bool:
        return self.fmp_dcf_history_rows > 0


def _coverage(total: int, non_null: int) -> Coverage:
    return Coverage(total=int(total or 0), non_null=int(non_null or 0))


def check_ticker(conn, ticker: str) -> TickerCompleteness:
    t = ticker.upper()

    company_exists = bool(conn.execute(
        "SELECT COUNT(*) FROM companies WHERE ticker = ?", [t]
    ).fetchone()[0])

    ohlcv = conn.execute(
        """
        SELECT COUNT(*), MIN(date), MAX(date),
               SUM(CASE WHEN ema10 IS NOT NULL THEN 1 ELSE 0 END),
               SUM(CASE WHEN ema250 IS NOT NULL THEN 1 ELSE 0 END)
        FROM ohlcv_daily
        WHERE ticker = ?
        """,
        [t],
    ).fetchone()

    fund = conn.execute(
        """
        SELECT COUNT(*), MIN(fiscal_year), MAX(fiscal_year),
               SUM(CASE WHEN fcf_per_share IS NOT NULL THEN 1 ELSE 0 END),
               SUM(CASE WHEN filing_date IS NOT NULL THEN 1 ELSE 0 END)
        FROM fundamentals_annual
        WHERE ticker = ?
        """,
        [t],
    ).fetchone()

    dcf_hist = conn.execute(
        """
        SELECT COUNT(*),
               SUM(CASE WHEN anchor_date IS NOT NULL THEN 1 ELSE 0 END)
        FROM dcf_history
        WHERE ticker = ?
        """,
        [t],
    ).fetchone()

    fmp_dcf = conn.execute(
        """
        SELECT COUNT(*),
               SUM(CASE WHEN dcf_value IS NOT NULL THEN 1 ELSE 0 END)
        FROM fmp_dcf_history
        WHERE ticker = ?
        """,
        [t],
    ).fetchone()

    dcf_metrics = conn.execute(
        """
        SELECT COUNT(*), MAX(computed_at), MAX(latest_price_date)
        FROM dcf_metrics
        WHERE ticker = ?
        """,
        [t],
    ).fetchone()

    return TickerCompleteness(
        ticker=t,
        company_exists=company_exists,
        ohlcv_rows=int(ohlcv[0] or 0),
        ohlcv_start=str(ohlcv[1]) if ohlcv[1] is not None else None,
        ohlcv_end=str(ohlcv[2]) if ohlcv[2] is not None else None,
        ema10=_coverage(ohlcv[0], ohlcv[3]),
        ema250=_coverage(ohlcv[0], ohlcv[4]),
        fundamentals_rows=int(fund[0] or 0),
        fiscal_year_min=int(fund[1]) if fund[1] is not None else None,
        fiscal_year_max=int(fund[2]) if fund[2] is not None else None,
        fcf_per_share=_coverage(fund[0], fund[3]),
        filing_date=_coverage(fund[0], fund[4]),
        dcf_history_rows=int(dcf_hist[0] or 0),
        dcf_history_expected_rows=int(fund[3] or 0),
        dcf_history_anchor_date=_coverage(dcf_hist[0], dcf_hist[1]),
        fmp_dcf_history_rows=int(fmp_dcf[0] or 0),
        fmp_dcf_value=_coverage(fmp_dcf[0], fmp_dcf[1]),
        dcf_metrics_exists=bool(dcf_metrics[0]),
        dcf_metrics_computed_at=str(dcf_metrics[1]) if dcf_metrics[1] is not None else None,
        dcf_metrics_latest_price_date=str(dcf_metrics[2]) if dcf_metrics[2] is not None else None,
    )


def _fmt_cov(c: Coverage) -> str:
    return f"{c.non_null}/{c.total} ({c.ratio:.1%})"


def print_report(r: TickerCompleteness) -> None:
    print(f"Ticker: {r.ticker}")
    print("-" * 72)
    print(f"company_exists:                {r.company_exists}")
    print(f"ohlcv_rows:                    {r.ohlcv_rows}")
    print(f"ohlcv_date_range:              {r.ohlcv_start} -> {r.ohlcv_end}")
    print(f"ema10_coverage:                {_fmt_cov(r.ema10)}")
    print(f"ema250_coverage:               {_fmt_cov(r.ema250)}")
    print(f"fundamentals_rows:             {r.fundamentals_rows}")
    print(f"fiscal_year_range:             {r.fiscal_year_min} -> {r.fiscal_year_max}")
    print(f"fcf_per_share_coverage:        {_fmt_cov(r.fcf_per_share)}")
    print(f"filing_date_coverage:          {_fmt_cov(r.filing_date)}")
    print(f"dcf_history_rows:              {r.dcf_history_rows}/{r.dcf_history_expected_rows}")
    print(f"dcf_history_anchor_coverage:   {_fmt_cov(r.dcf_history_anchor_date)}")
    print(f"fmp_dcf_history_rows:          {r.fmp_dcf_history_rows}")
    print(f"fmp_dcf_value_coverage:        {_fmt_cov(r.fmp_dcf_value)}")
    print(f"dcf_metrics_exists:            {r.dcf_metrics_exists}")
    print(f"dcf_metrics_computed_at:       {r.dcf_metrics_computed_at}")
    print(f"dcf_metrics_latest_price_date: {r.dcf_metrics_latest_price_date}")
    print("-" * 72)
    print(f"D1 readiness -> EMA:{r.d1_ema_ready} DCF:{r.d1_dcf_ready} FMP_DCF:{r.d1_fmp_dcf_ready}")


def list_distinct_data_tickers(conn) -> list[str]:
    """Tickers that appear in any D1-related table (same universe as ``--all``)."""
    rows = conn.execute(
        """
        SELECT DISTINCT ticker
        FROM (
            SELECT ticker FROM ohlcv_daily
            UNION ALL
            SELECT ticker FROM fundamentals_annual
            UNION ALL
            SELECT ticker FROM dcf_history
            UNION ALL
            SELECT ticker FROM fmp_dcf_history
        )
        ORDER BY ticker
        """
    ).fetchall()
    return [str(r[0]) for r in rows]


def main() -> None:
    parser = argparse.ArgumentParser(description="Check DB completeness for D1/data-readiness")
    parser.add_argument("--ticker", help="Ticker to inspect, e.g. NVDA")
    parser.add_argument("--all", action="store_true", help="Inspect all tickers with any data")
    parser.add_argument("--json", action="store_true", help="Print JSON output")
    args = parser.parse_args()

    if not args.ticker and not args.all:
        parser.error("Provide --ticker <TICKER> or --all")

    with get_conn(readonly=True) as conn:
        if args.all:
            reports = [check_ticker(conn, t) for t in list_distinct_data_tickers(conn)]
            if args.json:
                print(json.dumps([asdict(r) for r in reports], ensure_ascii=False, indent=2))
            else:
                for r in reports:
                    print_report(r)
                    print()
        else:
            report = check_ticker(conn, args.ticker)
            if args.json:
                print(json.dumps(asdict(report), ensure_ascii=False, indent=2))
            else:
                print_report(report)


if __name__ == "__main__":
    main()
