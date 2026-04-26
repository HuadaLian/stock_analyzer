"""
Read-only query layer. All Streamlit UI data access goes through here.
No network calls, no writes — pure SELECT against stock.db.
"""

from __future__ import annotations

import pandas as pd
from db.schema import get_conn


def _conn():
    return get_conn(readonly=True)


# ---------------------------------------------------------------------------
# Phase 1 — FCF + DCF chart (active)
# ---------------------------------------------------------------------------

def get_ohlcv(ticker: str, start_date: str = "2000-01-01") -> pd.DataFrame:
    """Daily OHLCV sorted ascending by date."""
    with _conn() as conn:
        return conn.execute("""
            SELECT date, open, high, low, close, volume, adj_close, ema10, ema250
            FROM ohlcv_daily
            WHERE ticker = ? AND date >= ?
            ORDER BY date
        """, [ticker, start_date]).df()


def get_fundamentals(ticker: str) -> pd.DataFrame:
    """All annual fundamental rows, newest first."""
    with _conn() as conn:
        return conn.execute("""
            SELECT fiscal_year, fiscal_end_date, filing_date,
                   fcf, fcf_per_share, shares_out, currency
            FROM fundamentals_annual
            WHERE ticker = ?
            ORDER BY fiscal_year DESC
        """, [ticker]).df()


def get_dcf_metrics(ticker: str) -> dict | None:
    """Pre-computed DCF lines for a ticker."""
    with _conn() as conn:
        cur = conn.execute(
            "SELECT * FROM dcf_metrics WHERE ticker = ?", [ticker]
        )
        cols = [d[0] for d in cur.description]
        row = cur.fetchone()
    return dict(zip(cols, row)) if row else None


def get_dcf_history(ticker: str) -> pd.DataFrame:
    """Historical DCF step-line rows sorted by fiscal_year ascending."""
    with _conn() as conn:
        return conn.execute("""
            SELECT ticker, fiscal_year, anchor_date,
                   fcf_ps_avg3yr, dcf_14x, dcf_24x, dcf_34x
            FROM dcf_history
            WHERE ticker = ?
            ORDER BY fiscal_year ASC
        """, [ticker]).df()


def get_fmp_dcf_history(ticker: str) -> pd.DataFrame:
    """FMP historical intrinsic value rows sorted by date ascending."""
    with _conn() as conn:
        return conn.execute("""
            SELECT ticker, date, dcf_value, stock_price
            FROM fmp_dcf_history
            WHERE ticker = ?
            ORDER BY date ASC
        """, [ticker]).df()


# ---------------------------------------------------------------------------
# Phase 2+ — companies / screener (stubs)
# ---------------------------------------------------------------------------

def get_company(ticker: str) -> dict | None:
    """Get company info (name, sector, shares_out, market_cap from latest OHLCV)."""
    with _conn() as conn:
        cur = conn.execute(
            "SELECT ticker, market, name, sector, shares_out FROM companies WHERE ticker = ?",
            [ticker]
        )
        row = cur.fetchone()
    if not row:
        return None
    
    # Get latest market cap from ohlcv_daily
    with _conn() as conn:
        cur = conn.execute(
            "SELECT market_cap FROM ohlcv_daily WHERE ticker = ? ORDER BY date DESC LIMIT 1",
            [ticker]
        )
        mcap_row = cur.fetchone()
    
    result = {
        "ticker": row[0],
        "market": row[1],
        "name": row[2],
        "sector": row[3],
        "shares_out": float(row[4]) if row[4] else None,
        "market_cap": float(mcap_row[0]) if mcap_row and mcap_row[0] else None,
    }
    return result


def get_all_tickers(market: str | None = None) -> pd.DataFrame:
    """Return ticker list (optionally filtered by market) for UI selectors."""
    with _conn() as conn:
        if market:
            df = conn.execute(
                """
                SELECT c.ticker, c.name, c.market
                FROM companies c
                WHERE c.market = ?
                ORDER BY c.ticker
                """,
                [market],
            ).df()
        else:
            df = conn.execute(
                """
                SELECT c.ticker, c.name, c.market
                FROM companies c
                ORDER BY c.ticker
                """
            ).df()

        # Fallback when companies table is not populated enough.
        if df.empty:
            if market:
                return conn.execute(
                    """
                    SELECT DISTINCT o.ticker, NULL::VARCHAR AS name, ? AS market
                    FROM ohlcv_daily o
                    ORDER BY o.ticker
                    """,
                    [market],
                ).df()
            return conn.execute(
                """
                SELECT DISTINCT o.ticker, NULL::VARCHAR AS name, NULL::VARCHAR AS market
                FROM ohlcv_daily o
                ORDER BY o.ticker
                """
            ).df()

        return df


def get_sectors(market: str = "US") -> list[str]:
    # TODO: implement in Phase 2
    pass


def get_screener(
    market: str = "US",
    sector: str | None = None,
    industry: str | None = None,
) -> pd.DataFrame:
    # TODO: implement in Phase 2
    pass


# ---------------------------------------------------------------------------
# Phase 5+ — dashboards (stubs)
# ---------------------------------------------------------------------------

def get_revenue_by_segment(ticker: str, fiscal_year: int) -> pd.DataFrame:
    # TODO: Dashboard 2
    pass


def get_revenue_by_geography(ticker: str, fiscal_year: int) -> pd.DataFrame:
    # TODO: Dashboard 2
    pass


def get_management(ticker: str) -> pd.DataFrame:
    # TODO: Dashboard 3
    pass


def get_competitors(ticker: str, sector: str | None = None, industry: str | None = None) -> pd.DataFrame:
    # TODO: Dashboard 4
    pass


def get_notes(ticker: str | None = None) -> pd.DataFrame:
    # TODO: notes dashboard
    pass


def get_active_alerts(ticker: str | None = None) -> pd.DataFrame:
    # TODO: price alerts
    pass
