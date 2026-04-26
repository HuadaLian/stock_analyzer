"""
DuckDB schema definitions and connection factory.

Usage:
    from db.schema import get_conn, init_db

    conn = get_conn()                # read-write (ETL only)
    conn = get_conn(readonly=True)   # read-only (Streamlit UI)

Read replica (Windows / concurrent bulk):
    Set env ``STOCK_ANALYZER_READ_DB`` to a **copy** of ``stock.db`` (refresh the copy
    when bulk is idle). All ``get_conn(readonly=True)`` calls then use that file so the
    UI can run while ``us_bulk_run`` holds an exclusive lock on the primary file.
    Writes (``get_conn()``) always use the primary ``stock.db``.
"""

from __future__ import annotations

import os
import duckdb
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "stock.db"
READ_DB_ENV = "STOCK_ANALYZER_READ_DB"

_LOCK_HINT = (
    " If another process holds stock.db (e.g. `python -m etl.us_bulk_run`), on Windows "
    "DuckDB often cannot open a second connection. Copy stock.db to a second path when "
    "bulk is stopped, set env STOCK_ANALYZER_READ_DB to that path, then start Streamlit."
)


def get_conn(readonly: bool = False) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection to ``stock.db`` (write) or a read-only path."""
    if readonly:
        mirror = os.environ.get(READ_DB_ENV, "").strip()
        if mirror:
            p = Path(mirror).expanduser()
            if not p.is_file():
                raise FileNotFoundError(
                    f"{READ_DB_ENV}={mirror!r} is not an existing file. "
                    f"Copy {DB_PATH} to that path when the writer is not using the DB, "
                    "then point Streamlit at the copy."
                )
            return duckdb.connect(str(p.resolve()), read_only=True)
        try:
            return duckdb.connect(str(DB_PATH), read_only=True)
        except Exception as e:
            msg = str(e)
            if "Cannot open file" in msg or "另一个程序正在使用" in msg:
                raise RuntimeError(msg + _LOCK_HINT) from e
            raise
    return duckdb.connect(str(DB_PATH), read_only=False)


_DDL = """

-- ═══════════════════════════════════════════════
-- Company universe
-- ═══════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS companies (
    ticker              VARCHAR PRIMARY KEY,
    market              VARCHAR,        -- 'US' | 'CN' | 'HK'
    name                VARCHAR,
    exchange            VARCHAR,        -- short, e.g. 'NASDAQ', 'NYSE'
    exchange_full_name  VARCHAR,        -- e.g. 'NASDAQ Global Select'
    country             VARCHAR,        -- issuer / listing country
    sector              VARCHAR,
    industry            VARCHAR,
    currency            VARCHAR,        -- reporting currency, e.g. 'USD'
    description         TEXT,
    shares_out          DOUBLE,         -- latest shares outstanding (millions)
    updated_at          TIMESTAMP
);

-- ═══════════════════════════════════════════════
-- Price data
-- ═══════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ohlcv_daily (
    ticker      VARCHAR,
    date        DATE,
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    volume      BIGINT,
    adj_close   DOUBLE,     -- forward-adjusted close (前复权), used for DCF overlay
    market_cap  DOUBLE,     -- adj_close × shares_out (millions), pre-computed by ETL
    ema10       DOUBLE,     -- 10-day EMA of adj_close, pre-computed by ETL
    ema250      DOUBLE,     -- 250-day EMA of adj_close, pre-computed by ETL
    PRIMARY KEY (ticker, date)
);

CREATE TABLE IF NOT EXISTS ohlcv_minute (
    ticker      VARCHAR,
    ts          TIMESTAMP,
    resolution  VARCHAR,    -- '1m' | '5m' | '15m' | '30m' | '1h'
    open        DOUBLE,
    high        DOUBLE,
    low         DOUBLE,
    close       DOUBLE,
    volume      BIGINT,
    PRIMARY KEY (ticker, ts, resolution)
);

-- Tick data: Parquet files under ticks/<ticker>/<date>.parquet
-- Create this view once the ticks/ directory has data:
-- CREATE VIEW ticks AS SELECT * FROM read_parquet('ticks/*/*.parquet');

-- ═══════════════════════════════════════════════
-- Annual fundamentals
-- ═══════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS fundamentals_annual (
    ticker                  VARCHAR,
    fiscal_year             INTEGER,
    fiscal_end_date         DATE,
    filing_date             DATE,       -- actual SEC filing date (防前视偏差)
    currency                VARCHAR,    -- always 'USD' after Phase-2 normalization
    reporting_currency      VARCHAR,    -- original reportedCurrency from FMP, e.g. 'CNY' for BABA
    fx_to_usd               DOUBLE,     -- 1 reporting_currency = N USD on fiscal_end_date (1.0 if already USD)
    -- Income statement (source: /income-statement?period=annual)
    revenue                 DOUBLE,     -- Sales/Revenue/Turnover (millions)
    revenue_per_share       DOUBLE,     -- Revenue per Share
    gross_profit            DOUBLE,
    gross_margin            DOUBLE,     -- Gross Margin, ratio 0-1
    operating_income        DOUBLE,
    operating_margin        DOUBLE,     -- Operating Margin, ratio 0-1
    net_income              DOUBLE,     -- Net Income, GAAP (millions)
    profit_margin           DOUBLE,     -- Profit Margin, ratio 0-1
    eps                     DOUBLE,     -- Basic EPS, GAAP
    depreciation            DOUBLE,     -- Depreciation Expense (millions)
    effective_tax_rate      DOUBLE,     -- ratio 0-1
    -- Cash flow (source: /cash-flow-statement?period=annual)
    fcf                     DOUBLE,     -- Free Cash Flow (millions)
    fcf_per_share           DOUBLE,     -- Free Cash Flow per Share
    dividend_per_share      DOUBLE,     -- Dividend per Share
    -- Balance sheet (source: /balance-sheet-statement?period=annual)
    total_equity            DOUBLE,     -- Total Equity (millions)
    long_term_debt          DOUBLE,     -- LT Debt (millions)
    working_capital         DOUBLE,     -- Current Assets - Current Liabilities (millions)
    book_value_per_share    DOUBLE,     -- Book Value per Share
    tangible_bv_per_share   DOUBLE,     -- Tangible Book Value per Share
    shares_out              DOUBLE,     -- Basic Weighted Avg Shares (millions)
    -- Return metrics (source: /key-metrics?period=annual)
    roic                    DOUBLE,     -- Return on Invested Capital, ratio
    return_on_capital       DOUBLE,     -- Return on Capital Employed, ratio
    return_on_equity        DOUBLE,     -- Return on Common Equity, ratio
    -- Metadata
    source                  VARCHAR,    -- 'fmp' | 'llm'
    PRIMARY KEY (ticker, fiscal_year)
);

-- ═══════════════════════════════════════════════
-- Quarterly fundamentals
-- ═══════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS fundamentals_quarterly (
    ticker          VARCHAR,
    fiscal_year     INTEGER,
    quarter         INTEGER,    -- 1-4
    period_end      DATE,
    filing_date     DATE,       -- actual SEC filing date (防前视偏差)
    currency        VARCHAR,
    revenue         DOUBLE,
    gross_profit    DOUBLE,
    gross_margin    DOUBLE,
    operating_income DOUBLE,
    net_income      DOUBLE,
    eps             DOUBLE,
    fcf             DOUBLE,
    shares_out      DOUBLE,
    source          VARCHAR,
    PRIMARY KEY (ticker, fiscal_year, quarter)
);

-- ═══════════════════════════════════════════════
-- Forward estimates
-- ═══════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS estimates (
    ticker          VARCHAR,
    fiscal_year     INTEGER,
    quarter         INTEGER,    -- NULL = full-year estimate
    source          VARCHAR,    -- 'consensus' | 'self'
    published_at    DATE,
    revenue         DOUBLE,
    eps             DOUBLE,
    fcf             DOUBLE,
    net_income      DOUBLE,
    note            TEXT,
    PRIMARY KEY (ticker, fiscal_year, quarter, source, published_at)
);

-- ═══════════════════════════════════════════════
-- Revenue breakdown  (Dashboard 2)
-- ═══════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS revenue_by_segment (
    ticker          VARCHAR,
    fiscal_year     INTEGER,
    segment         VARCHAR,    -- e.g. 'Data Center', 'Gaming'
    revenue         DOUBLE,
    pct             DOUBLE,     -- fraction of total, 0-1
    PRIMARY KEY (ticker, fiscal_year, segment)
);

CREATE TABLE IF NOT EXISTS revenue_by_geography (
    ticker          VARCHAR,
    fiscal_year     INTEGER,
    region          VARCHAR,    -- e.g. 'United States', 'China'
    revenue         DOUBLE,
    pct             DOUBLE,
    PRIMARY KEY (ticker, fiscal_year, region)
);

-- ═══════════════════════════════════════════════
-- Management  (Dashboard 3)
-- ═══════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS management (
    ticker          VARCHAR,
    name            VARCHAR,
    title           VARCHAR,    -- 'CEO' | 'CFO' | 'COO' | 'CTO'
    updated_at      DATE,
    PRIMARY KEY (ticker, title)
);

-- ═══════════════════════════════════════════════
-- Pre-computed DCF lines  (derived, safe to recompute)
-- ═══════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS dcf_metrics (
    ticker                  VARCHAR PRIMARY KEY,
    fcf_per_share_avg3yr    DOUBLE,     -- 3-year rolling avg FCF per share (basis for lines)
    dcf_14x                 DOUBLE,     -- conservative valuation line
    dcf_24x                 DOUBLE,     -- mid valuation line
    dcf_34x                 DOUBLE,     -- optimistic valuation line
    latest_price            DOUBLE,     -- latest adj_close from ohlcv_daily (USD)
    latest_price_date       DATE,       -- date of latest_price (audit)
    short_potential         DOUBLE,     -- max(0, (latest_price - dcf_34x) / dcf_34x); 0 when underpriced
    invest_potential        DOUBLE,     -- (dcf_14x - latest_price) / dcf_24x; negative when overpriced
    computed_at             TIMESTAMP
);

-- ═══════════════════════════════════════════════
-- Pre-computed DCF history step-lines  (one row per fiscal year per ticker)
-- 用于 D1 价格图上叠加 14x/24x/34x 历史阶梯线。
-- 与 fundamentals_annual 分表：raw vs derived 隔离；anchor_date 是展示层概念
-- (filing_date 对齐到最近交易日)，不属于财报源数据。
-- ═══════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS dcf_history (
    ticker          VARCHAR,
    fiscal_year     INTEGER,
    anchor_date     DATE,       -- filing_date snapped to nearest trading day (用作图上 x 坐标)
    fcf_ps_avg3yr   DOUBLE,     -- 3-year rolling avg fcf_per_share (5-year fallback if ≤ 0)
    dcf_14x         DOUBLE,
    dcf_24x         DOUBLE,
    dcf_34x         DOUBLE,
    PRIMARY KEY (ticker, fiscal_year)
);

-- ═══════════════════════════════════════════════
-- FMP DCF history  (third-party intrinsic value, daily)
-- 来源：FMP /historical-discounted-cash-flow-statement/{ticker}
-- 作为参照线展示在 D1 图上，区别于我们自己的 14x/24x/34x。
-- ═══════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS fmp_dcf_history (
    ticker      VARCHAR,
    date        DATE,
    dcf_value   DOUBLE,     -- FMP 计算的内在价值
    stock_price DOUBLE,     -- FMP 当日报价（备审计）
    PRIMARY KEY (ticker, date)
);

-- ═══════════════════════════════════════════════
-- Factor scores  (derived, one row per ticker per rebalance date)
-- ═══════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS factor_scores (
    ticker                  VARCHAR,
    as_of_date              DATE,
    invest_score            DOUBLE,     -- 0-100
    short_score             DOUBLE,     -- 0-100
    roic_percentile         DOUBLE,     -- within sector, 0-1
    fcf_growth_3yr          DOUBLE,     -- 3-year FCF CAGR
    revenue_growth_3yr      DOUBLE,     -- 3-year revenue CAGR
    gross_margin_percentile DOUBLE,     -- within sector, 0-1
    pfcf_vs_history         DOUBLE,     -- current P/FCF vs historical avg
    computed_at             TIMESTAMP,
    PRIMARY KEY (ticker, as_of_date)
);

-- ═══════════════════════════════════════════════
-- Price alerts
-- ═══════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS price_alerts (
    id              VARCHAR PRIMARY KEY,
    ticker          VARCHAR,
    direction       VARCHAR,    -- 'above' | 'below'
    price           DOUBLE,
    note            VARCHAR,
    active          BOOLEAN DEFAULT true,
    created_at      TIMESTAMP
);

-- ═══════════════════════════════════════════════
-- Investment notes  (user-generated, UI writes directly)
-- ═══════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS notes (
    id          VARCHAR PRIMARY KEY,
    ticker      VARCHAR,    -- NULL = global note
    raw_text    TEXT,       -- original user input, written before LLM call
    markdown    TEXT,       -- LLM-formatted result, NULL if LLM failed
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP
);

-- ═══════════════════════════════════════════════
-- Backtest results
-- ═══════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS backtest_runs (
    run_id      VARCHAR PRIMARY KEY,
    strategy    VARCHAR,
    tickers     VARCHAR[],
    resolution  VARCHAR,    -- '1m' | '1d' etc.
    start_date  DATE,
    end_date    DATE,
    params      JSON,
    metrics     JSON,       -- sharpe, max_drawdown, total_return, win_rate, etc.
    created_at  TIMESTAMP
);
"""


_MIGRATIONS = [
    # Idempotent ALTERs — added when columns are introduced after the initial DDL.
    "ALTER TABLE fundamentals_annual ADD COLUMN IF NOT EXISTS reporting_currency VARCHAR",
    "ALTER TABLE fundamentals_annual ADD COLUMN IF NOT EXISTS fx_to_usd DOUBLE",
    "ALTER TABLE fundamentals_annual ADD COLUMN IF NOT EXISTS interest_expense DOUBLE",
    "ALTER TABLE dcf_metrics ADD COLUMN IF NOT EXISTS latest_price DOUBLE",
    "ALTER TABLE dcf_metrics ADD COLUMN IF NOT EXISTS latest_price_date DATE",
    "ALTER TABLE dcf_metrics ADD COLUMN IF NOT EXISTS short_potential DOUBLE",
    "ALTER TABLE dcf_metrics ADD COLUMN IF NOT EXISTS invest_potential DOUBLE",
    "ALTER TABLE ohlcv_daily ADD COLUMN IF NOT EXISTS ema10 DOUBLE",
    "ALTER TABLE ohlcv_daily ADD COLUMN IF NOT EXISTS ema250 DOUBLE",
    "ALTER TABLE companies ADD COLUMN IF NOT EXISTS country VARCHAR",
    "ALTER TABLE companies ADD COLUMN IF NOT EXISTS exchange_full_name VARCHAR",
    """
    CREATE TABLE IF NOT EXISTS etl_us_bulk_state (
        ticker      VARCHAR PRIMARY KEY,
        status      VARCHAR,        -- pending | running | done | failed | skipped
        step        VARCHAR,
        last_error  VARCHAR,
        updated_at  TIMESTAMP DEFAULT now()
    )
    """,
]


def init_db() -> None:
    """Create all tables if they don't exist, then apply idempotent migrations."""
    with get_conn() as conn:
        conn.execute(_DDL)
        for stmt in _MIGRATIONS:
            try:
                conn.execute(stmt)
            except duckdb.Error:
                pass
    print(f"Database initialised at {DB_PATH}")


if __name__ == "__main__":
    init_db()
