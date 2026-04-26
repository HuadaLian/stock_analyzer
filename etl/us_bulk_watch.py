"""
Append-only health snapshot for US bulk ETL (safe to run hourly via Task Scheduler).

Usage:
    python -m etl.us_bulk_watch

Writes one block per run to reports/us_etl_watch.log
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from db.schema import get_conn

_REPO = Path(__file__).resolve().parents[1]
_WATCH_LOG = _REPO / "reports" / "us_etl_watch.log"


def main() -> None:
    (_REPO / "reports").mkdir(parents=True, exist_ok=True)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [f"\n=== {now}  us_bulk_watch ===\n"]

    try:
        with get_conn(readonly=True) as conn:
            def cnt(sql: str) -> int:
                r = conn.execute(sql).fetchone()
                return int(r[0]) if r and r[0] is not None else 0

            d = cnt("SELECT COUNT(*) FROM etl_us_bulk_state WHERE status = 'done'")
            f = cnt("SELECT COUNT(*) FROM etl_us_bulk_state WHERE status = 'failed'")
            s = cnt("SELECT COUNT(*) FROM etl_us_bulk_state WHERE status = 'skipped'")
            r = cnt("SELECT COUNT(*) FROM etl_us_bulk_state WHERE status = 'running'")
            t = cnt("SELECT COUNT(*) FROM etl_us_bulk_state")
            cu = cnt("SELECT COUNT(*) FROM companies WHERE market = 'US'")
            ohlc = cnt(
                "SELECT COUNT(DISTINCT o.ticker) FROM ohlcv_daily o "
                "JOIN companies c ON c.ticker = o.ticker WHERE c.market = 'US'"
            )
            fa = cnt(
                "SELECT COUNT(DISTINCT f.ticker) FROM fundamentals_annual f "
                "JOIN companies c ON c.ticker = f.ticker WHERE c.market = 'US'"
            )

            lines.append(f"etl_us_bulk_state.done:     {d}\n")
            lines.append(f"etl_us_bulk_state.failed:   {f}\n")
            lines.append(f"etl_us_bulk_state.skipped:  {s}\n")
            lines.append(f"etl_us_bulk_state.running:  {r}\n")
            lines.append(f"etl_us_bulk_state.total:    {t}\n")
            lines.append(f"companies (US):             {cu}\n")
            lines.append(f"ohlcv_daily distinct US:   {ohlc}\n")
            lines.append(f"fundamentals_annual US tk:  {fa}\n")
    except Exception as e:
        lines.append(f"(query error: {e})\n")

    with open(_WATCH_LOG, "a", encoding="utf-8") as f:
        f.writelines(lines)

    print("".join(lines))


if __name__ == "__main__":
    main()
