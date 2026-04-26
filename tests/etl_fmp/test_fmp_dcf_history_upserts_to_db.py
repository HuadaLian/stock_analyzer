"""Unit tests for loading FMP DCF history into DB."""

from etl.sources.fmp_dcf import load_fmp_dcf_history


def test_load_fmp_dcf_history_upserts_rows(in_memory_db):
    def _fake_fetch(ticker: str, api_key: str):
        assert ticker == "NVDA"
        assert api_key == "TEST_KEY"
        return [
            {"ticker": "NVDA", "date": "2025-01-01", "dcf_value": 10.0, "stock_price": 100.0},
            {"ticker": "NVDA", "date": "2025-01-02", "dcf_value": 11.0, "stock_price": 101.0},
        ]

    n = load_fmp_dcf_history("NVDA", in_memory_db, fetch_fn=_fake_fetch, api_key="TEST_KEY")
    assert n == 2

    rows = in_memory_db.execute(
        """
        SELECT date, dcf_value, stock_price
        FROM fmp_dcf_history
        WHERE ticker = 'NVDA'
        ORDER BY date ASC
        """
    ).fetchall()
    assert [(str(d), v, p) for d, v, p in rows] == [
        ("2025-01-01", 10.0, 100.0),
        ("2025-01-02", 11.0, 101.0),
    ]
