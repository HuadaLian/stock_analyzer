"""Backfill latest ohlcv_daily.market_cap_usd with latest FX rates."""

from __future__ import annotations

import duckdb

from db.schema import DB_PATH
from etl.sources.fmp import fetch_latest_fx_to_usd


def main() -> None:
    conn = duckdb.connect(str(DB_PATH))
    rows = conn.execute(
        "SELECT upper(coalesce(currency, 'USD')) AS ccy, COUNT(*) FROM companies GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall()

    fx_map: dict[str, float] = {"USD": 1.0}
    for ccy, _ in rows:
        c = str(ccy or "USD").upper()
        if c == "USD":
            continue
        try:
            fx_map[c] = fetch_latest_fx_to_usd(c)
        except Exception:
            continue

    conn.execute("CREATE TEMP TABLE fx_map(ccy VARCHAR, fx_to_usd DOUBLE)")
    for ccy, rate in fx_map.items():
        conn.execute("INSERT INTO fx_map VALUES (?, ?)", [ccy, float(rate)])

    conn.execute(
        """
        UPDATE ohlcv_daily o
        SET market_cap_usd = o.market_cap * f.fx_to_usd
        FROM (
            SELECT o2.ticker, o2.date
            FROM ohlcv_daily o2
            JOIN (
                SELECT ticker, MAX(date) AS mx
                FROM ohlcv_daily
                GROUP BY ticker
            ) m ON o2.ticker = m.ticker AND o2.date = m.mx
        ) latest
        JOIN companies c ON c.ticker = latest.ticker
        JOIN fx_map f ON f.ccy = upper(coalesce(c.currency, 'USD'))
        WHERE o.ticker = latest.ticker
          AND o.date = latest.date
          AND o.market_cap IS NOT NULL
        """
    )

    updated = conn.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT o.ticker, o.market_cap_usd
            FROM ohlcv_daily o
            JOIN (
                SELECT ticker, MAX(date) AS mx
                FROM ohlcv_daily
                GROUP BY ticker
            ) m ON o.ticker = m.ticker AND o.date = m.mx
            WHERE o.market_cap_usd IS NOT NULL
        ) t
        """
    ).fetchone()[0]
    print("fx currencies:", len(fx_map))
    print("latest rows with market_cap_usd:", int(updated))
    conn.close()


if __name__ == "__main__":
    main()
