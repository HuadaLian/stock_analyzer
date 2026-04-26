"""Contract: repository.get_ohlcv should expose ema10/ema250 columns."""

from unittest.mock import patch

import db.repository as repo


def _patch_repo_conn(in_memory_db):
    return patch("db.repository._conn", autospec=False)


def test_get_ohlcv_returns_ema_columns(in_memory_db):
    in_memory_db.execute(
        """
        INSERT INTO ohlcv_daily
            (ticker, date, open, high, low, close, volume, adj_close, ema10, ema250)
        VALUES
            ('NVDA', '2025-01-02', 100, 105, 99, 104, 1000, 104, 103.5, 90.0)
        """
    )

    with _patch_repo_conn(in_memory_db) as mock:
        mock.return_value.__enter__ = lambda s: in_memory_db
        mock.return_value.__exit__ = lambda s, *a: None

        df = repo.get_ohlcv("NVDA")

    assert len(df) == 1
    assert "ema10" in df.columns
    assert "ema250" in df.columns
    assert float(df.iloc[0]["ema10"]) == 103.5
    assert float(df.iloc[0]["ema250"]) == 90.0
