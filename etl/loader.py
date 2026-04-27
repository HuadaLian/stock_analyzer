"""
DuckDB upsert functions.

Each function uses INSERT ... ON CONFLICT DO UPDATE so ETL can be re-run
safely without duplicating rows. Only the columns being written in the
current phase are listed in the DO UPDATE clause, so later phases can
fill other columns without overwriting each other.

Bulk shape: list-of-dict rows are registered as a DataFrame view and inserted
in one INSERT … SELECT … ON CONFLICT statement. Row-by-row ``executemany``
on DuckDB is the documented slow path for upsert; this pattern stays
vectorised through the engine. Single-row writers (``upsert_company``,
``upsert_dcf_metrics``) keep the simple ``execute(?, ?, ?)`` form.
"""

from __future__ import annotations

from itertools import count

import duckdb
import pandas as pd


_stage_seq = count()


def _stage_name(prefix: str) -> str:
    """Unique view name per call so concurrent registrations don't collide.

    Apply phase is single-threaded today, but the cost is one ``next(count)``
    and it removes a foot-gun if we ever fan out.
    """
    return f"_stg_{prefix}_{next(_stage_seq)}"


def _bulk_upsert(
    conn: duckdb.DuckDBPyConnection,
    *,
    table: str,
    cols: list[str],
    rows: list[dict],
    update_clause: str,
    conflict: str,
    stage_prefix: str,
) -> None:
    """Stage list-of-dict rows as a DataFrame view, then INSERT … SELECT … ON CONFLICT.

    ``update_clause`` is the body after ``DO UPDATE SET`` (no trailing semicolon).
    ``conflict`` is the conflict target without parens, e.g. ``ticker, date``.
    """
    if not rows:
        return
    df = pd.DataFrame(rows, columns=cols)
    name = _stage_name(stage_prefix)
    conn.register(name, df)
    try:
        conn.execute(
            f"""
            INSERT INTO {table} ({", ".join(cols)})
            SELECT {", ".join(cols)} FROM {name}
            ON CONFLICT ({conflict}) DO UPDATE SET
                {update_clause}
            """
        )
    finally:
        conn.unregister(name)


# ---------------------------------------------------------------------------
# companies
# ---------------------------------------------------------------------------

def upsert_company(conn: duckdb.DuckDBPyConnection, data: dict) -> None:
    conn.execute("""
        INSERT INTO companies
            (ticker, market, name, exchange, exchange_full_name, country,
             sector, industry, currency, description, shares_out, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
        ON CONFLICT (ticker) DO UPDATE SET
            market               = excluded.market,
            name                 = excluded.name,
            exchange             = excluded.exchange,
            exchange_full_name   = excluded.exchange_full_name,
            country              = excluded.country,
            sector               = excluded.sector,
            industry             = excluded.industry,
            currency             = excluded.currency,
            description          = excluded.description,
            shares_out           = excluded.shares_out,
            updated_at           = excluded.updated_at
    """, [
        data["ticker"], data["market"], data["name"], data.get("exchange"),
        data.get("exchange_full_name"), data.get("country"),
        data["sector"], data["industry"], data["currency"], data["description"],
        data["shares_out"],
    ])


# ---------------------------------------------------------------------------
# ohlcv_daily
# ---------------------------------------------------------------------------

_OHLCV_BASE_COLS = ["ticker", "date", "open", "high", "low",
                    "close", "volume", "adj_close", "market_cap"]


def upsert_ohlcv_daily(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    """Batch upsert OHLCV base columns. ema10/ema250 untouched here — written
    separately by `upsert_ohlcv_ema` after compute step."""
    _bulk_upsert(
        conn,
        table="ohlcv_daily",
        cols=_OHLCV_BASE_COLS,
        rows=rows,
        conflict="ticker, date",
        update_clause="""
            open       = excluded.open,
            high       = excluded.high,
            low        = excluded.low,
            close      = excluded.close,
            volume     = excluded.volume,
            adj_close  = excluded.adj_close,
            market_cap = excluded.market_cap
        """,
        stage_prefix="ohlcv",
    )


_OHLCV_EMA_COLS = ["ticker", "date", "ema10", "ema250"]


def upsert_ohlcv_ema(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    """Update ema10/ema250 on existing ohlcv_daily rows. Each row dict needs
    keys: ticker, date, ema10, ema250. Rows whose (ticker,date) don't yet exist
    are inserted with the base columns as NULL — but in normal pipeline ordering
    EMA runs after OHLCV insert so this is the conflict path."""
    _bulk_upsert(
        conn,
        table="ohlcv_daily",
        cols=_OHLCV_EMA_COLS,
        rows=rows,
        conflict="ticker, date",
        update_clause="""
            ema10  = excluded.ema10,
            ema250 = excluded.ema250
        """,
        stage_prefix="ema",
    )


# ---------------------------------------------------------------------------
# fundamentals_annual  (Phase 2: FCF columns only)
# ---------------------------------------------------------------------------

_FCF_COLS = [
    "ticker", "fiscal_year", "fiscal_end_date", "filing_date", "currency",
    "reporting_currency", "fx_to_usd",
    "fcf", "fcf_per_share", "shares_out", "source",
    "revenue", "revenue_per_share", "gross_profit", "gross_margin",
    "operating_income", "operating_margin", "net_income", "profit_margin",
    "eps", "depreciation", "effective_tax_rate", "dividend_per_share",
    "total_equity", "long_term_debt", "working_capital",
    "book_value_per_share", "tangible_bv_per_share",
    "roic", "return_on_capital", "return_on_equity",
]


def upsert_fundamentals_annual(
    conn: duckdb.DuckDBPyConnection, rows: list[dict]
) -> None:
    # Note: only FCF/currency columns updated on conflict; other columns untouched
    # so a later income-statement upsert can fill them without being overwritten.
    _bulk_upsert(
        conn,
        table="fundamentals_annual",
        cols=_FCF_COLS,
        rows=rows,
        conflict="ticker, fiscal_year",
        update_clause="""
            fiscal_end_date    = excluded.fiscal_end_date,
            filing_date        = excluded.filing_date,
            currency           = excluded.currency,
            reporting_currency = excluded.reporting_currency,
            fx_to_usd          = excluded.fx_to_usd,
            fcf                = excluded.fcf,
            fcf_per_share      = excluded.fcf_per_share,
            shares_out         = excluded.shares_out,
            source             = excluded.source
        """,
        stage_prefix="fcf",
    )


# ---------------------------------------------------------------------------
# dcf_metrics
# ---------------------------------------------------------------------------

def upsert_dcf_metrics(conn: duckdb.DuckDBPyConnection, data: dict) -> None:
    conn.execute("""
        INSERT INTO dcf_metrics
            (ticker, fcf_per_share_avg3yr, dcf_14x, dcf_24x, dcf_34x,
             latest_price, latest_price_date, short_potential, invest_potential,
             computed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, now())
        ON CONFLICT (ticker) DO UPDATE SET
            fcf_per_share_avg3yr = excluded.fcf_per_share_avg3yr,
            dcf_14x              = excluded.dcf_14x,
            dcf_24x              = excluded.dcf_24x,
            dcf_34x              = excluded.dcf_34x,
            latest_price         = excluded.latest_price,
            latest_price_date    = excluded.latest_price_date,
            short_potential      = excluded.short_potential,
            invest_potential     = excluded.invest_potential,
            computed_at          = excluded.computed_at
    """, [
        data["ticker"], data["fcf_per_share_avg3yr"],
        data["dcf_14x"], data["dcf_24x"], data["dcf_34x"],
        data.get("latest_price"), data.get("latest_price_date"),
        data.get("short_potential"), data.get("invest_potential"),
    ])


# ---------------------------------------------------------------------------
# dcf_history  (one row per fiscal year per ticker)
# ---------------------------------------------------------------------------

_DCF_HISTORY_COLS = ["ticker", "fiscal_year", "anchor_date",
                     "fcf_ps_avg3yr", "dcf_14x", "dcf_24x", "dcf_34x"]


def upsert_dcf_history(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    _bulk_upsert(
        conn,
        table="dcf_history",
        cols=_DCF_HISTORY_COLS,
        rows=rows,
        conflict="ticker, fiscal_year",
        update_clause="""
            anchor_date    = excluded.anchor_date,
            fcf_ps_avg3yr  = excluded.fcf_ps_avg3yr,
            dcf_14x        = excluded.dcf_14x,
            dcf_24x        = excluded.dcf_24x,
            dcf_34x        = excluded.dcf_34x
        """,
        stage_prefix="dcfhist",
    )


# ---------------------------------------------------------------------------
# fmp_dcf_history
# ---------------------------------------------------------------------------

_FMP_DCF_COLS = ["ticker", "date", "dcf_value", "stock_price"]


def upsert_fmp_dcf_history(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    _bulk_upsert(
        conn,
        table="fmp_dcf_history",
        cols=_FMP_DCF_COLS,
        rows=rows,
        conflict="ticker, date",
        update_clause="""
            dcf_value   = excluded.dcf_value,
            stock_price = excluded.stock_price
        """,
        stage_prefix="fmpdcf",
    )


# ---------------------------------------------------------------------------
# management / revenue_by_segment / revenue_by_geography
#
# Each does DELETE WHERE ticker=? first to drop rows for segments/regions/titles
# that disappeared between fetches; the bulk insert then refills.
# ---------------------------------------------------------------------------

_MGMT_COLS = ["ticker", "name", "title", "updated_at"]


def upsert_management(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    """Upsert management rows keyed by (ticker, title)."""
    if not rows:
        return
    ticker = rows[0]["ticker"]
    conn.execute("DELETE FROM management WHERE ticker = ?", [ticker])
    _bulk_upsert(
        conn,
        table="management",
        cols=_MGMT_COLS,
        rows=rows,
        conflict="ticker, title",
        update_clause="""
            name       = excluded.name,
            updated_at = excluded.updated_at
        """,
        stage_prefix="mgmt",
    )


_SEG_COLS = ["ticker", "fiscal_year", "segment", "revenue", "pct"]


def upsert_revenue_by_segment(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    """Upsert revenue by segment keyed by (ticker, fiscal_year, segment)."""
    if not rows:
        return
    ticker = rows[0]["ticker"]
    conn.execute("DELETE FROM revenue_by_segment WHERE ticker = ?", [ticker])
    _bulk_upsert(
        conn,
        table="revenue_by_segment",
        cols=_SEG_COLS,
        rows=rows,
        conflict="ticker, fiscal_year, segment",
        update_clause="""
            revenue = excluded.revenue,
            pct     = excluded.pct
        """,
        stage_prefix="seg",
    )


_GEO_COLS = ["ticker", "fiscal_year", "region", "revenue", "pct"]


def upsert_revenue_by_geography(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    """Upsert revenue by geography keyed by (ticker, fiscal_year, region)."""
    if not rows:
        return
    ticker = rows[0]["ticker"]
    conn.execute("DELETE FROM revenue_by_geography WHERE ticker = ?", [ticker])
    _bulk_upsert(
        conn,
        table="revenue_by_geography",
        cols=_GEO_COLS,
        rows=rows,
        conflict="ticker, fiscal_year, region",
        update_clause="""
            revenue = excluded.revenue,
            pct     = excluded.pct
        """,
        stage_prefix="geo",
    )


# ---------------------------------------------------------------------------
# fundamentals_annual extras (interest_expense, income statement)
# ---------------------------------------------------------------------------

_INTEREST_COLS = ["ticker", "fiscal_year", "interest_expense"]


def upsert_interest_expense_annual(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    """Upsert annual interest expense into fundamentals_annual."""
    _bulk_upsert(
        conn,
        table="fundamentals_annual",
        cols=_INTEREST_COLS,
        rows=rows,
        conflict="ticker, fiscal_year",
        update_clause="""
            interest_expense = excluded.interest_expense
        """,
        stage_prefix="interest",
    )


_INCOME_COLS = ["ticker", "fiscal_year", "revenue", "operating_income",
                "depreciation", "interest_expense"]


def upsert_income_statement_annual(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    """Upsert annual revenue and EBITDA inputs into fundamentals_annual."""
    # interest_expense uses COALESCE so a later sweep doesn't blank out a value
    # already filled by upsert_interest_expense_annual.
    _bulk_upsert(
        conn,
        table="fundamentals_annual",
        cols=_INCOME_COLS,
        rows=rows,
        conflict="ticker, fiscal_year",
        update_clause="""
            revenue          = excluded.revenue,
            operating_income = excluded.operating_income,
            depreciation     = excluded.depreciation,
            interest_expense = COALESCE(excluded.interest_expense, fundamentals_annual.interest_expense)
        """,
        stage_prefix="income",
    )
