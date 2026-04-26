"""
测试目标：schema 初始化后，所有预期的表和关键字段都存在且类型正确。

验证 db/schema.py 的 init_db() 没有遗漏任何表或字段。
每次修改 schema 后都应重跑此测试。
"""

import pytest

EXPECTED_TABLES = {
    "etl_us_bulk_state",
    "companies",
    "ohlcv_daily",
    "ohlcv_minute",
    "fundamentals_annual",
    "fundamentals_quarterly",
    "estimates",
    "revenue_by_segment",
    "revenue_by_geography",
    "management",
    "dcf_metrics",
    "dcf_history",
    "fmp_dcf_history",
    "factor_scores",
    "price_alerts",
    "notes",
    "backtest_runs",
}

# 每张表至少必须包含这些字段（类型也验证）
EXPECTED_COLUMNS = {
    "companies": {
        "ticker": "VARCHAR",
        "market": "VARCHAR",
        "exchange": "VARCHAR",
        "exchange_full_name": "VARCHAR",
        "country": "VARCHAR",
        "sector": "VARCHAR",
        "industry": "VARCHAR",
        "shares_out": "DOUBLE",
    },
    "etl_us_bulk_state": {
        "ticker": "VARCHAR",
        "status": "VARCHAR",
        "step": "VARCHAR",
    },
    "ohlcv_daily": {
        "ticker": "VARCHAR",
        "date": "DATE",
        "adj_close": "DOUBLE",
        "market_cap": "DOUBLE",
        "ema10": "DOUBLE",
        "ema250": "DOUBLE",
    },
    "dcf_history": {
        "ticker": "VARCHAR",
        "fiscal_year": "INTEGER",
        "anchor_date": "DATE",
        "fcf_ps_avg3yr": "DOUBLE",
        "dcf_14x": "DOUBLE",
        "dcf_24x": "DOUBLE",
        "dcf_34x": "DOUBLE",
    },
    "fmp_dcf_history": {
        "ticker": "VARCHAR",
        "date": "DATE",
        "dcf_value": "DOUBLE",
        "stock_price": "DOUBLE",
    },
    "fundamentals_annual": {
        "ticker": "VARCHAR",
        "fiscal_year": "INTEGER",
        "filing_date": "DATE",
        "fcf": "DOUBLE",
        "fcf_per_share": "DOUBLE",
        "shares_out": "DOUBLE",
        "roic": "DOUBLE",
    },
    "fundamentals_quarterly": {
        "ticker": "VARCHAR",
        "fiscal_year": "INTEGER",
        "quarter": "INTEGER",
        "filing_date": "DATE",
        "fcf": "DOUBLE",
    },
    "estimates": {
        "ticker": "VARCHAR",
        "source": "VARCHAR",
        "published_at": "DATE",
    },
    "dcf_metrics": {
        "ticker": "VARCHAR",
        "fcf_per_share_avg3yr": "DOUBLE",
        "dcf_14x": "DOUBLE",
        "dcf_24x": "DOUBLE",
        "dcf_34x": "DOUBLE",
    },
    "factor_scores": {
        "ticker": "VARCHAR",
        "as_of_date": "DATE",
        "invest_score": "DOUBLE",
        "short_score": "DOUBLE",
    },
    "notes": {
        "id": "VARCHAR",
        "ticker": "VARCHAR",
        "raw_text": "VARCHAR",
        "markdown": "VARCHAR",
    },
}


def test_all_expected_tables_exist(in_memory_db):
    actual = {row[0] for row in in_memory_db.execute("SHOW TABLES").fetchall()}
    missing = EXPECTED_TABLES - actual
    assert not missing, f"Missing tables: {missing}"


@pytest.mark.parametrize("table,columns", EXPECTED_COLUMNS.items())
def test_table_has_correct_columns(in_memory_db, table, columns):
    rows = in_memory_db.execute(f"DESCRIBE {table}").fetchall()
    actual = {row[0]: row[1] for row in rows}
    for col, expected_type in columns.items():
        assert col in actual, f"{table}.{col} does not exist"
        assert actual[col] == expected_type, (
            f"{table}.{col} expected type {expected_type}, got {actual[col]}"
        )
