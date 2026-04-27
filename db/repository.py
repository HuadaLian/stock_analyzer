"""
Read-only query layer. All Streamlit UI data access goes through here.
No network calls, no writes — pure SELECT against stock.db.
"""

from __future__ import annotations

from contextlib import contextmanager

import pandas as pd
from db.schema import get_conn


def _conn():
    return get_conn(readonly=True)


@contextmanager
def _resolve(conn):
    """Yield a readonly conn — reuse caller's if given, else open+close one.

    Lets a single Streamlit render path (e.g. render_d1_us) open one connection
    and thread it through every repository call, instead of paying the per-call
    DuckDB open/init cost ~10 times per page rerun.
    """
    if conn is not None:
        yield conn
    else:
        with _conn() as owned:
            yield owned


# ---------------------------------------------------------------------------
# Phase 1 — FCF + DCF chart (active)
# ---------------------------------------------------------------------------

def get_ohlcv(ticker: str, start_date: str = "2000-01-01", *, conn=None) -> pd.DataFrame:
    """Daily OHLCV sorted ascending by date."""
    with _resolve(conn) as c:
        return c.execute("""
            SELECT date, open, high, low, close, volume, adj_close, ema10, ema250
            FROM ohlcv_daily
            WHERE ticker = ? AND date >= ?
            ORDER BY date
        """, [ticker, start_date]).df()


def get_fundamentals(ticker: str, *, conn=None) -> pd.DataFrame:
    """All annual fundamental rows, newest first."""
    with _resolve(conn) as c:
        return c.execute("""
            SELECT fiscal_year, fiscal_end_date, filing_date,
                   fcf, fcf_per_share, shares_out, currency,
                   reporting_currency, fx_to_usd
            FROM fundamentals_annual
            WHERE ticker = ?
            ORDER BY fiscal_year DESC
        """, [ticker]).df()


def get_dcf_metrics(ticker: str, *, conn=None) -> dict | None:
    """Pre-computed DCF lines for a ticker."""
    with _resolve(conn) as c:
        cur = c.execute(
            "SELECT * FROM dcf_metrics WHERE ticker = ?", [ticker]
        )
        cols = [d[0] for d in cur.description]
        row = cur.fetchone()
    return dict(zip(cols, row)) if row else None


def get_dcf_history(ticker: str, *, conn=None) -> pd.DataFrame:
    """Historical DCF step-line rows sorted by fiscal_year ascending."""
    with _resolve(conn) as c:
        return c.execute("""
            SELECT ticker, fiscal_year, anchor_date,
                   fcf_ps_avg3yr, dcf_14x, dcf_24x, dcf_34x
            FROM dcf_history
            WHERE ticker = ?
            ORDER BY fiscal_year ASC
        """, [ticker]).df()


def get_fmp_dcf_history(ticker: str, *, conn=None) -> pd.DataFrame:
    """FMP historical intrinsic value rows sorted by date ascending."""
    with _resolve(conn) as c:
        return c.execute("""
            SELECT ticker, date, dcf_value, stock_price
            FROM fmp_dcf_history
            WHERE ticker = ?
            ORDER BY date ASC
        """, [ticker]).df()


# ---------------------------------------------------------------------------
# Phase 2+ — companies / screener (stubs)
# ---------------------------------------------------------------------------

def get_company(ticker: str, *, conn=None) -> dict | None:
    """Get company info (name, sector, currency, shares_out, latest market_cap)."""
    with _resolve(conn) as c:
        cur = c.execute(
            "SELECT ticker, market, name, sector, shares_out, currency FROM companies WHERE ticker = ?",
            [ticker]
        )
        row = cur.fetchone()
        if not row:
            return None

        # Latest market cap on the same connection — no second open.
        mcap_row = c.execute(
            "SELECT market_cap FROM ohlcv_daily WHERE ticker = ? ORDER BY date DESC LIMIT 1",
            [ticker]
        ).fetchone()

    return {
        "ticker": row[0],
        "market": row[1],
        "name": row[2],
        "sector": row[3],
        "shares_out": float(row[4]) if row[4] else None,
        "currency": row[5] or "USD",
        "market_cap": float(mcap_row[0]) if mcap_row and mcap_row[0] else None,
    }


def get_all_tickers(market: str | None = None, *, conn=None) -> pd.DataFrame:
    """Return ticker list (optionally filtered by market) for UI selectors.

    Returns empty when `companies` has no matching rows — callers fall back to
    a default ticker. The previous `SELECT DISTINCT ticker FROM ohlcv_daily`
    fallback was deleted after `db.us_data_audit.audit_orphan_ohlcv_tickers`
    confirmed every OHLCV ticker has a `companies` row (so the fallback was
    dead code that scaled badly with bulk size).
    """
    with _resolve(conn) as c:
        if market:
            return c.execute(
                """
                SELECT c.ticker, c.name, c.market
                FROM companies c
                WHERE c.market = ?
                ORDER BY c.ticker
                """,
                [market],
            ).df()
        return c.execute(
            """
            SELECT c.ticker, c.name, c.market
            FROM companies c
            ORDER BY c.ticker
            """
        ).df()


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
    with _conn() as conn:
        return conn.execute(
            """
            SELECT ticker, fiscal_year, segment, revenue, pct
            FROM revenue_by_segment
            WHERE ticker = ? AND fiscal_year = ?
            ORDER BY revenue DESC, segment ASC
            """,
            [ticker, fiscal_year],
        ).df()


def get_revenue_by_geography(ticker: str, fiscal_year: int) -> pd.DataFrame:
    with _conn() as conn:
        return conn.execute(
            """
            SELECT ticker, fiscal_year, region, revenue, pct
            FROM revenue_by_geography
            WHERE ticker = ? AND fiscal_year = ?
            ORDER BY revenue DESC, region ASC
            """,
            [ticker, fiscal_year],
        ).df()


def get_management(ticker: str) -> pd.DataFrame:
    with _conn() as conn:
        return conn.execute(
            """
            SELECT ticker, name, title, updated_at
            FROM management
            WHERE ticker = ?
            ORDER BY title ASC, name ASC
            """,
            [ticker],
        ).df()


def get_company_profile(ticker: str) -> dict | None:
    """Get company profile fields used by D2 dashboard."""
    with _conn() as conn:
        cur = conn.execute(
            """
            SELECT ticker, name, sector, industry, description
            FROM companies
            WHERE ticker = ?
            """,
            [ticker],
        )
        row = cur.fetchone()

    if not row:
        return None

    return {
        "ticker": row[0],
        "name": row[1],
        "sector": row[2],
        "industry": row[3],
        "description": row[4],
    }


def get_latest_revenue_year(ticker: str) -> int | None:
    """Get latest fiscal year available in segment/geography tables."""
    with _conn() as conn:
        cur = conn.execute(
            """
            SELECT MAX(fiscal_year)
            FROM (
                SELECT fiscal_year FROM revenue_by_segment WHERE ticker = ?
                UNION ALL
                SELECT fiscal_year FROM revenue_by_geography WHERE ticker = ?
            ) t
            """,
            [ticker, ticker],
        )
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def get_latest_total_revenue(ticker: str) -> tuple[int | None, float | None]:
    """Get latest annual total revenue in millions USD from fundamentals_annual."""
    with _conn() as conn:
        cur = conn.execute(
            """
            SELECT fiscal_year, revenue
            FROM fundamentals_annual
            WHERE ticker = ? AND revenue IS NOT NULL
            ORDER BY fiscal_year DESC
            LIMIT 1
            """,
            [ticker],
        )
        row = cur.fetchone()
    if not row:
        return None, None
    return int(row[0]), float(row[1]) if row[1] is not None else None


def get_ebitda_coverage_history(ticker: str) -> pd.DataFrame:
    """Get multi-year EBITDA and interest coverage inputs for D2 safety panel."""
    with _conn() as conn:
        return conn.execute(
            """
            SELECT
                fiscal_year,
                operating_income,
                depreciation,
                interest_expense
            FROM fundamentals_annual
            WHERE ticker = ?
            ORDER BY fiscal_year ASC
            """,
            [ticker],
        ).df()


def get_competitors(ticker: str, sector: str | None = None, industry: str | None = None) -> pd.DataFrame:
    # TODO: Dashboard 4
    pass


def get_industry_peers_revenue(ticker: str) -> pd.DataFrame:
    """Return latest annual revenue for every company sharing the target's
    sector AND industry (the target itself included).

    Output columns: ticker, name, sector, industry, currency (listing / OHLCV),
    fiscal_year, revenue, fund_currency, reporting_currency, market_cap.

    ``revenue`` is **USD millions** when ``fund_currency`` is ``USD`` (normal ETL
    path). ``market_cap`` is from the latest ``ohlcv_daily`` row, in **millions**
    of the listing currency (``companies.currency`` / OHLCV basis).
    """
    with _conn() as conn:
        return conn.execute(
            """
            WITH target AS (
                SELECT sector, industry FROM companies WHERE ticker = ?
            ),
            peer_companies AS (
                SELECT c.ticker, c.name, c.sector, c.industry, c.currency
                FROM companies c, target t
                WHERE c.sector = t.sector AND c.industry = t.industry
                  AND c.sector IS NOT NULL AND c.industry IS NOT NULL
            ),
            latest_rev AS (
                SELECT f.ticker,
                       f.fiscal_year,
                       f.revenue,
                       f.currency AS fund_currency,
                       f.reporting_currency,
                       ROW_NUMBER() OVER (PARTITION BY f.ticker ORDER BY f.fiscal_year DESC) AS rn
                FROM fundamentals_annual f
                JOIN peer_companies p USING (ticker)
                WHERE f.revenue IS NOT NULL AND f.revenue > 0
            ),
            latest_mc AS (
                SELECT o.ticker,
                       o.market_cap,
                       ROW_NUMBER() OVER (PARTITION BY o.ticker ORDER BY o.date DESC) AS rn
                FROM ohlcv_daily o
                JOIN peer_companies p ON p.ticker = o.ticker
                WHERE o.market_cap IS NOT NULL AND o.market_cap > 0
            )
            SELECT p.ticker, p.name, p.sector, p.industry, p.currency,
                   r.fiscal_year, r.revenue,
                   r.fund_currency, r.reporting_currency,
                   m.market_cap
            FROM peer_companies p
            LEFT JOIN latest_rev r ON r.ticker = p.ticker AND r.rn = 1
            LEFT JOIN latest_mc m ON m.ticker = p.ticker AND m.rn = 1
            ORDER BY r.revenue DESC NULLS LAST, p.ticker
            """,
            [ticker],
        ).df()


def get_notes(ticker: str | None = None) -> pd.DataFrame:
    # TODO: notes dashboard
    pass


def get_active_alerts(ticker: str | None = None) -> pd.DataFrame:
    # TODO: price alerts
    pass
