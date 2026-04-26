"""
Derived metric computation.

Reads raw data from the DB, computes derived values, and writes results back.
Safe to re-run at any time — all writes use upsert.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import duckdb
from datetime import datetime

from etl.loader import (
    upsert_dcf_history,
    upsert_dcf_metrics,
    upsert_ohlcv_ema,
)


def compute_short_potential(latest_price: float | None,
                            dcf_34x: float | None) -> float | None:
    """做空潜力 = max(0, (latest_price - dcf_34x) / dcf_34x).

    衡量当前价格相对 34x DCF 的超额溢价；价格 ≤ 34x 时返回 0 (无做空空间)。
    inputs 缺失或 dcf_34x ≤ 0 时返回 None (估值不可比)。
    """
    if latest_price is None or dcf_34x is None or dcf_34x <= 0:
        return None
    return max(0.0, (latest_price - dcf_34x) / dcf_34x)


def compute_invest_potential(latest_price: float | None,
                             dcf_14x: float | None,
                             dcf_24x: float | None) -> float | None:
    """投资潜力 = (dcf_14x - latest_price) / dcf_24x.

    分子衡量价格相对保守估值线 (14x) 的折价；用 24x 而非 14x 做归一是为了
    给一个有上界的相对值，便于跨股票比较。价格 > 14x 时为负 (无折价)。
    inputs 缺失或 dcf_24x ≤ 0 时返回 None。
    """
    if latest_price is None or dcf_14x is None or dcf_24x is None or dcf_24x <= 0:
        return None
    return (dcf_14x - latest_price) / dcf_24x


def _latest_close(ticker: str,
                  conn: duckdb.DuckDBPyConnection) -> tuple[float | None, object]:
    """从 ohlcv_daily 取最新一行的 (adj_close, date)；缺数据返回 (None, None)。"""
    row = conn.execute("""
        SELECT adj_close, date FROM ohlcv_daily
        WHERE ticker = ? AND adj_close IS NOT NULL
        ORDER BY date DESC LIMIT 1
    """, [ticker]).fetchone()
    if not row or row[0] is None:
        return None, None
    return float(row[0]), row[1]


def compute_dcf_lines(ticker: str, conn: duckdb.DuckDBPyConnection) -> dict:
    """
    Compute DCF 14x/24x/34x lines from historical per-share FCF.

    Logic preserved from data_provider.compute_dcf_lines():
    - For each year: avg = mean of up to 3 years' fcf_per_share ending at that year
    - If 3-year avg <= 0: fall back to 5-year window
    - dcf_Nx = avg × N

    Writes the latest values to dcf_metrics table.
    Returns the full per-year series as a DataFrame for chart rendering.
    """
    rows = conn.execute("""
        SELECT fiscal_year, fiscal_end_date, fcf_per_share
        FROM fundamentals_annual
        WHERE ticker = ? AND fcf_per_share IS NOT NULL
        ORDER BY fiscal_year ASC
    """, [ticker]).fetchall()

    if not rows:
        return {}

    # Build {date: fcf_per_share} dict
    fcf_by_year: dict[pd.Timestamp, float] = {}
    for fiscal_year, fiscal_end_date, fcf_ps in rows:
        if fcf_ps is not None:
            date = pd.Timestamp(fiscal_end_date) if fiscal_end_date else pd.Timestamp(f"{fiscal_year}-12-31")
            fcf_by_year[date] = float(fcf_ps)

    sorted_dates = sorted(fcf_by_year.keys())
    dates, v14, v24, v34 = [], [], [], []
    avg = 0.0   # last-iteration value used as latest_avg below

    for i, date in enumerate(sorted_dates):
        # 3-year forward rolling window: year-N's value is mean of years N-2..N
        window = sorted_dates[max(0, i - 2): i + 1]
        avg = float(np.mean([fcf_by_year[d] for d in window]))

        # Fallback to 5-year window if 3-year avg <= 0 (loss-making years)
        if avg <= 0:
            window5 = sorted_dates[max(0, i - 4): i + 1]
            avg5 = float(np.mean([fcf_by_year[d] for d in window5]))
            if avg5 > avg:
                avg = avg5

        dates.append(date)
        v14.append(14 * avg)
        v24.append(24 * avg)
        v34.append(34 * avg)

    # `avg` here = the last fiscal year's value with 5yr fallback applied;
    # this is the same number the chart's last step uses, so screener and chart agree.
    latest_avg = avg

    # Extend last value to today so the step-line reaches the right edge of the chart
    dates.append(pd.Timestamp(datetime.now().date()))
    v14.append(v14[-1])
    v24.append(v24[-1])
    v34.append(v34[-1])

    dcf_df = pd.DataFrame({"date": dates, "dcf_14x": v14, "dcf_24x": v24, "dcf_34x": v34})

    d14 = 14 * latest_avg
    d24 = 24 * latest_avg
    d34 = 34 * latest_avg

    latest_price, latest_date = _latest_close(ticker, conn)
    short_pot = compute_short_potential(latest_price, d34)
    invest_pot = compute_invest_potential(latest_price, d14, d24)

    upsert_dcf_metrics(conn, {
        "ticker":               ticker,
        "fcf_per_share_avg3yr": latest_avg,
        "dcf_14x":              d14,
        "dcf_24x":              d24,
        "dcf_34x":              d34,
        "latest_price":         latest_price,
        "latest_price_date":    latest_date,
        "short_potential":      short_pot,
        "invest_potential":     invest_pot,
    })

    return {
        "dcf_df": dcf_df,
        "fcf_per_share_avg3yr": latest_avg,
        "latest_price": latest_price,
        "short_potential": short_pot,
        "invest_potential": invest_pot,
    }


# ---------------------------------------------------------------------------
# EMA pre-computation
# ---------------------------------------------------------------------------

def compute_ema_series(values: pd.Series, span: int) -> pd.Series:
    """EMA of a price series using pandas .ewm(span, adjust=False).
    Pure helper exposed for unit testing — this is the only place the
    smoothing recipe lives, so chart and DB agree by construction."""
    return values.ewm(span=span, adjust=False).mean()


def compute_ema(ticker: str, conn: duckdb.DuckDBPyConnection) -> int:
    """Compute ema10 / ema250 over ticker's full ohlcv_daily history and
    write back into the same rows. Returns row count written.

    Computed eagerly over all rows (not incrementally) so a backfill or a
    re-run gives identical numbers — EMA depends on the full prior series."""
    df = conn.execute("""
        SELECT date, adj_close
        FROM ohlcv_daily
        WHERE ticker = ? AND adj_close IS NOT NULL
        ORDER BY date ASC
    """, [ticker]).fetch_df()
    if df.empty:
        return 0

    df["ema10"] = compute_ema_series(df["adj_close"], span=10)
    df["ema250"] = compute_ema_series(df["adj_close"], span=250)

    rows = [{"ticker": ticker, "date": r["date"],
             "ema10": float(r["ema10"]), "ema250": float(r["ema250"])}
            for _, r in df.iterrows()]
    upsert_ohlcv_ema(conn, rows)
    return len(rows)


# ---------------------------------------------------------------------------
# DCF historical step-lines  (writes one row per fiscal_year into dcf_history)
# ---------------------------------------------------------------------------

def _snap_to_trading_day(filing_date, ticker: str,
                         conn: duckdb.DuckDBPyConnection):
    """Anchor a filing_date to the nearest ohlcv_daily date ≤ filing_date.
    Returns filing_date itself if no OHLCV row exists ≤ filing_date."""
    if filing_date is None:
        return None
    row = conn.execute("""
        SELECT date FROM ohlcv_daily
        WHERE ticker = ? AND date <= ?
        ORDER BY date DESC LIMIT 1
    """, [ticker, filing_date]).fetchone()
    return row[0] if row else filing_date


def compute_dcf_history(ticker: str,
                        conn: duckdb.DuckDBPyConnection) -> int:
    """One row per fiscal_year into dcf_history.

    Same 3-year rolling avg + 5-year fallback as compute_dcf_lines, but
    materialised per year (not just the latest) so the chart can draw a
    historical step-line. anchor_date = filing_date snapped to nearest
    trading day so the line aligns with candle x-axis.
    """
    rows = conn.execute("""
        SELECT fiscal_year, filing_date, fcf_per_share
        FROM fundamentals_annual
        WHERE ticker = ? AND fcf_per_share IS NOT NULL
        ORDER BY fiscal_year ASC
    """, [ticker]).fetchall()
    if not rows:
        return 0

    years = [r[0] for r in rows]
    filings = [r[1] for r in rows]
    fcf_ps = [float(r[2]) for r in rows]

    out_rows = []
    for i, year in enumerate(years):
        window = fcf_ps[max(0, i - 2): i + 1]
        avg = float(np.mean(window))
        if avg <= 0:
            window5 = fcf_ps[max(0, i - 4): i + 1]
            avg5 = float(np.mean(window5))
            if avg5 > avg:
                avg = avg5
        out_rows.append({
            "ticker":         ticker,
            "fiscal_year":    year,
            "anchor_date":    _snap_to_trading_day(filings[i], ticker, conn),
            "fcf_ps_avg3yr":  avg,
            "dcf_14x":        14 * avg,
            "dcf_24x":        24 * avg,
            "dcf_34x":        34 * avg,
        })

    upsert_dcf_history(conn, out_rows)
    return len(out_rows)
