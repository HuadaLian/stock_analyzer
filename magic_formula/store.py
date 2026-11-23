"""Local SQLite storage for Magic Formula screening data."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Iterable

import pandas as pd

DATA_DIR = Path(__file__).parent
DB_PATH = DATA_DIR / "magic_formula.db"

_COLUMNS = (
    "market", "ticker", "name", "industry", "list_date", "report_date",
    "ebit", "current_assets", "cash", "current_liabilities", "current_debt",
    "fixed_assets", "total_debt", "market_cap", "price", "price_date",
    "avg_daily_turnover", "currency", "dividend_years", "audit_opinion",
    "audit_report_date", "source", "updated_at", "quote_updated_at",
    "fundamentals_updated_at", "quote_currency", "quote_fx_to_listing",
    "financial_currency", "financial_fx_to_listing",
    "equity", "goodwill", "intangible_assets", "roe", "price_to_book", "dividend_yield",
    "payout_ratio", "eps_growth", "cet1_ratio", "npl_ratio",
    "provision_coverage", "ffo_to_debt", "currency_assumption",
)
_ANNUAL_COLUMNS = (
    "market", "ticker", "fiscal_date", "ebit", "net_income", "equity", "eps",
    "source", "updated_at",
)

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS fundamentals (
    market TEXT NOT NULL,
    ticker TEXT NOT NULL,
    name TEXT,
    industry TEXT,
    list_date TEXT,
    report_date TEXT,
    ebit REAL,
    current_assets REAL,
    cash REAL,
    current_liabilities REAL,
    current_debt REAL,
    fixed_assets REAL,
    total_debt REAL,
    market_cap REAL,
    price REAL,
    price_date TEXT,
    avg_daily_turnover REAL,
    currency TEXT,
    dividend_years INTEGER,
    audit_opinion TEXT,
    audit_report_date TEXT,
    source TEXT,
    updated_at TEXT NOT NULL,
    quote_updated_at TEXT,
    fundamentals_updated_at TEXT,
    quote_currency TEXT,
    quote_fx_to_listing REAL,
    financial_currency TEXT,
    financial_fx_to_listing REAL,
    equity REAL,
    goodwill REAL,
    intangible_assets REAL,
    roe REAL,
    price_to_book REAL,
    dividend_yield REAL,
    payout_ratio REAL,
    eps_growth REAL,
    cet1_ratio REAL,
    npl_ratio REAL,
    provision_coverage REAL,
    ffo_to_debt REAL,
    currency_assumption TEXT,
    PRIMARY KEY (market, ticker)
)
"""


def connect() -> sqlite3.Connection:
    """Open the dedicated SQLite database and ensure its schema exists."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(_CREATE_TABLE)
    columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(fundamentals)")
    }
    column_types = {
        "quote_updated_at": "TEXT",
        "fundamentals_updated_at": "TEXT",
        "quote_currency": "TEXT",
        "quote_fx_to_listing": "REAL",
        "financial_currency": "TEXT",
        "financial_fx_to_listing": "REAL",
        "equity": "REAL",
        "goodwill": "REAL",
        "intangible_assets": "REAL",
        "roe": "REAL",
        "price_to_book": "REAL",
        "dividend_yield": "REAL",
        "payout_ratio": "REAL",
        "eps_growth": "REAL",
        "cet1_ratio": "REAL",
        "npl_ratio": "REAL",
        "provision_coverage": "REAL",
        "ffo_to_debt": "REAL",
        "currency_assumption": "TEXT",
    }
    missing_columns = [column for column in column_types if column not in columns]
    for column in missing_columns:
        conn.execute(
            f"ALTER TABLE fundamentals ADD COLUMN {column} {column_types[column]}"
        )
    timestamp_columns = [
        column for column in ("quote_updated_at", "fundamentals_updated_at")
        if column in missing_columns
    ]
    if timestamp_columns:
        assignments = ", ".join(f"{column} = updated_at" for column in timestamp_columns)
        conn.execute(f"UPDATE fundamentals SET {assignments}")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS annual_financials (
            market TEXT NOT NULL,
            ticker TEXT NOT NULL,
            fiscal_date TEXT NOT NULL,
            ebit REAL,
            net_income REAL,
            equity REAL,
            eps REAL,
            source TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (market, ticker, fiscal_date)
        )
        """
    )
    annual_columns = {
        row["name"] for row in conn.execute("PRAGMA table_info(annual_financials)")
    }
    for column in ("net_income", "equity", "eps"):
        if column not in annual_columns:
            conn.execute(f"ALTER TABLE annual_financials ADD COLUMN {column} REAL")
    conn.commit()
    return conn


def upsert_rows(rows: Iterable[dict]) -> int:
    """Insert or replace fetched company records in one transaction."""
    records = [tuple(row.get(col) for col in _COLUMNS) for row in rows]
    if not records:
        return 0
    placeholders = ", ".join("?" for _ in _COLUMNS)
    updates = ", ".join(
        f"{col}=excluded.{col}" for col in _COLUMNS if col not in ("market", "ticker")
    )
    sql = (
        f"INSERT INTO fundamentals ({', '.join(_COLUMNS)}) "
        f"VALUES ({placeholders}) ON CONFLICT(market, ticker) DO UPDATE SET {updates}"
    )
    with closing(connect()) as conn:
        conn.executemany(sql, records)
        conn.commit()
    return len(records)


def backfill_cn_currency_metadata() -> int:
    """Assume CNY only for missing currency metadata on cached A-share rows."""
    currency_missing = "(currency IS NULL OR TRIM(currency) = '')"
    quote_currency_missing = "(quote_currency IS NULL OR TRIM(quote_currency) = '')"
    financial_currency_missing = (
        "(financial_currency IS NULL OR TRIM(financial_currency) = '')"
    )
    quote_fx_missing = "quote_fx_to_listing IS NULL"
    financial_fx_missing = "financial_fx_to_listing IS NULL"
    missing_metadata = " OR ".join((
        currency_missing,
        quote_currency_missing,
        financial_currency_missing,
        quote_fx_missing,
        financial_fx_missing,
    ))
    assumption = "A股缺失币种/汇率→CNY/1"
    with closing(connect()) as conn:
        cursor = conn.execute(
            f"""
            UPDATE fundamentals SET
                currency = CASE WHEN {currency_missing} THEN 'CNY' ELSE currency END,
                quote_currency = CASE
                    WHEN {quote_currency_missing} THEN 'CNY' ELSE quote_currency
                END,
                quote_fx_to_listing = COALESCE(quote_fx_to_listing, 1.0),
                financial_currency = CASE
                    WHEN {financial_currency_missing} THEN 'CNY'
                    ELSE financial_currency
                END,
                financial_fx_to_listing = COALESCE(financial_fx_to_listing, 1.0),
                currency_assumption = CASE
                    WHEN instr(COALESCE(currency_assumption, ''), ?) = 0 THEN
                        CASE WHEN currency_assumption IS NULL
                                  OR TRIM(currency_assumption) = ''
                             THEN ?
                             ELSE currency_assumption || '；' || ?
                        END
                    ELSE currency_assumption
                END
            WHERE market = 'CN' AND ({missing_metadata})
            """,
            (assumption, assumption, assumption),
        )
        conn.commit()
        return cursor.rowcount


def upsert_annual_rows(rows: Iterable[dict]) -> int:
    """Insert or replace historical annual EBIT observations."""
    records = [tuple(row.get(col) for col in _ANNUAL_COLUMNS) for row in rows]
    if not records:
        return 0
    placeholders = ", ".join("?" for _ in _ANNUAL_COLUMNS)
    sql = (
        f"INSERT INTO annual_financials ({', '.join(_ANNUAL_COLUMNS)}) "
        f"VALUES ({placeholders}) "
        "ON CONFLICT(market, ticker, fiscal_date) DO UPDATE SET "
        "ebit=excluded.ebit, net_income=excluded.net_income, "
        "equity=excluded.equity, eps=excluded.eps, "
        "source=excluded.source, updated_at=excluded.updated_at"
    )
    with closing(connect()) as conn:
        conn.executemany(sql, records)
        conn.commit()
    return len(records)


def load_market(market: str) -> pd.DataFrame:
    """Load a market's cached fundamentals."""
    with closing(connect()) as conn:
        return pd.read_sql_query(
            """
            SELECT *, CASE WHEN market = 'HK'
                THEN COALESCE(NULLIF(audit_opinion, ''), 'yfinance 未审计')
                ELSE audit_opinion END AS audit_label
            FROM fundamentals WHERE market = ?
            """,
            conn,
            params=(market.upper(),),
        )


def latest_update(market: str) -> str | None:
    with closing(connect()) as conn:
        row = conn.execute(
            "SELECT MAX(updated_at) FROM fundamentals WHERE market = ?",
            (market.upper(),),
        ).fetchone()
    return row[0] if row and row[0] else None


def latest_quote_update(market: str) -> str | None:
    with closing(connect()) as conn:
        row = conn.execute(
            "SELECT MAX(quote_updated_at) FROM fundamentals WHERE market = ?",
            (market.upper(),),
        ).fetchone()
    return row[0] if row and row[0] else None


def latest_fundamentals_update(market: str) -> str | None:
    with closing(connect()) as conn:
        row = conn.execute(
            "SELECT MAX(fundamentals_updated_at) FROM fundamentals WHERE market = ?",
            (market.upper(),),
        ).fetchone()
    return row[0] if row and row[0] else None


def load_annual_financials(market: str) -> pd.DataFrame:
    """Load historical annual EBIT rows for a market."""
    with closing(connect()) as conn:
        return pd.read_sql_query(
            """
            SELECT ticker, fiscal_date, ebit, net_income, equity, eps
            FROM annual_financials WHERE market = ?
            """,
            conn,
            params=(market.upper(),),
        )
