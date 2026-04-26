"""Contract: repository.get_dcf_history returns fiscal years in ascending order."""

from unittest.mock import patch

import db.repository as repo


def test_get_dcf_history_sorted_by_fiscal_year(in_memory_db):
    in_memory_db.execute(
        """
        INSERT INTO dcf_history
            (ticker, fiscal_year, anchor_date, fcf_ps_avg3yr, dcf_14x, dcf_24x, dcf_34x)
        VALUES
            ('NVDA', 2024, '2025-03-01', 3.0, 42.0, 72.0, 102.0),
            ('NVDA', 2022, '2023-03-01', 2.0, 28.0, 48.0, 68.0),
            ('NVDA', 2023, '2024-03-01', 2.5, 35.0, 60.0, 85.0)
        """
    )

    with patch("db.repository._conn") as mock:
        mock.return_value.__enter__ = lambda s: in_memory_db
        mock.return_value.__exit__ = lambda s, *a: None
        df = repo.get_dcf_history("NVDA")

    assert list(df["fiscal_year"]) == [2022, 2023, 2024]
