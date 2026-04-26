"""Idempotency tests for FMP DCF history loader."""

import pytest
from etl.sources.fmp_dcf import load_fmp_dcf_history


def test_load_fmp_dcf_history_is_idempotent_and_updates(in_memory_db):
    state = {"version": 1}

    def _fake_fetch(ticker: str, api_key: str):
        assert ticker == "NVDA"
        assert api_key == "TEST_KEY"
        if state["version"] == 1:
            return [
                {"ticker": "NVDA", "date": "2025-01-01", "dcf_value": 10.0, "stock_price": 100.0},
                {"ticker": "NVDA", "date": "2025-01-02", "dcf_value": 11.0, "stock_price": 101.0},
            ]
        return [
            {"ticker": "NVDA", "date": "2025-01-01", "dcf_value": 10.0, "stock_price": 100.0},
            {"ticker": "NVDA", "date": "2025-01-02", "dcf_value": 12.5, "stock_price": 102.0},
        ]

    load_fmp_dcf_history("NVDA", in_memory_db, fetch_fn=_fake_fetch, api_key="TEST_KEY")
    load_fmp_dcf_history("NVDA", in_memory_db, fetch_fn=_fake_fetch, api_key="TEST_KEY")

    count_before = in_memory_db.execute(
        "SELECT COUNT(*) FROM fmp_dcf_history WHERE ticker='NVDA'"
    ).fetchone()[0]
    assert count_before == 2

    state["version"] = 2
    load_fmp_dcf_history("NVDA", in_memory_db, fetch_fn=_fake_fetch, api_key="TEST_KEY")

    count_after = in_memory_db.execute(
        "SELECT COUNT(*) FROM fmp_dcf_history WHERE ticker='NVDA'"
    ).fetchone()[0]
    assert count_after == 2

    row = in_memory_db.execute(
        """
        SELECT dcf_value, stock_price
        FROM fmp_dcf_history
        WHERE ticker='NVDA' AND date='2025-01-02'
        """
    ).fetchone()
    assert row[0] == pytest.approx(12.5)
    assert row[1] == pytest.approx(102.0)
