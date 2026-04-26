"""
DuckDB upsert functions.

Each function uses INSERT ... ON CONFLICT DO UPDATE so ETL can be re-run
safely without duplicating rows. Only the columns being written in the
current phase are listed in the DO UPDATE clause, so later phases can
fill other columns without overwriting each other.
"""

from __future__ import annotations

import duckdb


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
    if not rows:
        return

    placeholders = ", ".join(["?"] * len(_OHLCV_BASE_COLS))
    conn.executemany(f"""
        INSERT INTO ohlcv_daily ({", ".join(_OHLCV_BASE_COLS)})
        VALUES ({placeholders})
        ON CONFLICT (ticker, date) DO UPDATE SET
            open       = excluded.open,
            high       = excluded.high,
            low        = excluded.low,
            close      = excluded.close,
            volume     = excluded.volume,
            adj_close  = excluded.adj_close,
            market_cap = excluded.market_cap
    """, [[r[c] for c in _OHLCV_BASE_COLS] for r in rows])


def upsert_ohlcv_ema(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    """Update ema10/ema250 on existing ohlcv_daily rows. Each row dict needs
    keys: ticker, date, ema10, ema250. Rows whose (ticker,date) don't yet exist
    are inserted with the base columns as NULL — but in normal pipeline ordering
    EMA runs after OHLCV insert so this is the conflict path."""
    if not rows:
        return

    conn.executemany("""
        INSERT INTO ohlcv_daily (ticker, date, ema10, ema250)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (ticker, date) DO UPDATE SET
            ema10  = excluded.ema10,
            ema250 = excluded.ema250
    """, [[r["ticker"], r["date"], r["ema10"], r["ema250"]] for r in rows])


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
    if not rows:
        return

    placeholders = ", ".join(["?"] * len(_FCF_COLS))
    conn.executemany(f"""
        INSERT INTO fundamentals_annual ({", ".join(_FCF_COLS)})
        VALUES ({placeholders})
        ON CONFLICT (ticker, fiscal_year) DO UPDATE SET
            fiscal_end_date    = excluded.fiscal_end_date,
            filing_date        = excluded.filing_date,
            currency           = excluded.currency,
            reporting_currency = excluded.reporting_currency,
            fx_to_usd          = excluded.fx_to_usd,
            fcf                = excluded.fcf,
            fcf_per_share      = excluded.fcf_per_share,
            shares_out         = excluded.shares_out,
            source             = excluded.source
    """, [[r[c] for c in _FCF_COLS] for r in rows])
    # Note: only FCF/currency columns updated on conflict; other columns untouched.


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
    if not rows:
        return
    placeholders = ", ".join(["?"] * len(_DCF_HISTORY_COLS))
    conn.executemany(f"""
        INSERT INTO dcf_history ({", ".join(_DCF_HISTORY_COLS)})
        VALUES ({placeholders})
        ON CONFLICT (ticker, fiscal_year) DO UPDATE SET
            anchor_date    = excluded.anchor_date,
            fcf_ps_avg3yr  = excluded.fcf_ps_avg3yr,
            dcf_14x        = excluded.dcf_14x,
            dcf_24x        = excluded.dcf_24x,
            dcf_34x        = excluded.dcf_34x
    """, [[r[c] for c in _DCF_HISTORY_COLS] for r in rows])


# ---------------------------------------------------------------------------
# fmp_dcf_history
# ---------------------------------------------------------------------------

def upsert_fmp_dcf_history(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    if not rows:
        return
    conn.executemany("""
        INSERT INTO fmp_dcf_history (ticker, date, dcf_value, stock_price)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (ticker, date) DO UPDATE SET
            dcf_value   = excluded.dcf_value,
            stock_price = excluded.stock_price
    """, [[r["ticker"], r["date"], r["dcf_value"], r["stock_price"]] for r in rows])


def upsert_management(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    """Upsert management rows keyed by (ticker, title)."""
    if not rows:
        return
    ticker = rows[0]["ticker"]
    conn.execute("DELETE FROM management WHERE ticker = ?", [ticker])
    conn.executemany("""
        INSERT INTO management (ticker, name, title, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (ticker, title) DO UPDATE SET
            name       = excluded.name,
            updated_at = excluded.updated_at
    """, [[r["ticker"], r["name"], r["title"], r["updated_at"]] for r in rows])


def upsert_revenue_by_segment(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    """Upsert revenue by segment keyed by (ticker, fiscal_year, segment)."""
    if not rows:
        return
    ticker = rows[0]["ticker"]
    conn.execute("DELETE FROM revenue_by_segment WHERE ticker = ?", [ticker])
    conn.executemany("""
        INSERT INTO revenue_by_segment (ticker, fiscal_year, segment, revenue, pct)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (ticker, fiscal_year, segment) DO UPDATE SET
            revenue = excluded.revenue,
            pct     = excluded.pct
    """, [[r["ticker"], r["fiscal_year"], r["segment"], r["revenue"], r["pct"]] for r in rows])


def upsert_revenue_by_geography(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    """Upsert revenue by geography keyed by (ticker, fiscal_year, region)."""
    if not rows:
        return
    ticker = rows[0]["ticker"]
    conn.execute("DELETE FROM revenue_by_geography WHERE ticker = ?", [ticker])
    conn.executemany("""
        INSERT INTO revenue_by_geography (ticker, fiscal_year, region, revenue, pct)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (ticker, fiscal_year, region) DO UPDATE SET
            revenue = excluded.revenue,
            pct     = excluded.pct
    """, [[r["ticker"], r["fiscal_year"], r["region"], r["revenue"], r["pct"]] for r in rows])


def upsert_interest_expense_annual(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    """Upsert annual interest expense into fundamentals_annual."""
    if not rows:
        return
    conn.executemany("""
        INSERT INTO fundamentals_annual (ticker, fiscal_year, interest_expense)
        VALUES (?, ?, ?)
        ON CONFLICT (ticker, fiscal_year) DO UPDATE SET
            interest_expense = excluded.interest_expense
    """, [[r["ticker"], r["fiscal_year"], r["interest_expense"]] for r in rows])


def upsert_income_statement_annual(conn: duckdb.DuckDBPyConnection, rows: list[dict]) -> None:
    """Upsert annual revenue and EBITDA inputs into fundamentals_annual."""
    if not rows:
        return
    conn.executemany("""
        INSERT INTO fundamentals_annual (
            ticker, fiscal_year, revenue, operating_income, depreciation, interest_expense
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT (ticker, fiscal_year) DO UPDATE SET
            revenue          = excluded.revenue,
            operating_income = excluded.operating_income,
            depreciation     = excluded.depreciation,
            interest_expense = COALESCE(excluded.interest_expense, fundamentals_annual.interest_expense)
    """, [[
        r["ticker"],
        r["fiscal_year"],
        r.get("revenue"),
        r.get("operating_income"),
        r.get("depreciation"),
        r.get("interest_expense"),
    ] for r in rows])
