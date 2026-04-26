"""Contract: repository.get_fmp_dcf_history returns rows in ascending date order."""

from unittest.mock import patch

import db.repository as repo


def test_get_fmp_dcf_history_sorted_by_date(in_memory_db):
    in_memory_db.execute(
        """
        INSERT INTO fmp_dcf_history (ticker, date, dcf_value, stock_price)
        VALUES
            ('NVDA', '2025-01-03', 13.0, 130.0),
            ('NVDA', '2025-01-01', 11.0, 110.0),
            ('NVDA', '2025-01-02', 12.0, 120.0)
        """
    )

    with patch("db.repository._conn") as mock:
        mock.return_value.__enter__ = lambda s: in_memory_db
        mock.return_value.__exit__ = lambda s, *a: None
        df = repo.get_fmp_dcf_history("NVDA")

    assert [str(d)[:10] for d in df["date"]] == ["2025-01-01", "2025-01-02", "2025-01-03"]
