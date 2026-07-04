"""DCF 派生只使用可信 fcf_quality 行（与 D1 一致）。"""

import pytest

from etl.compute import compute_dcf_history, compute_dcf_lines


def _insert_row(conn, *, fy: int, fcf_ps: float, quality: str | None) -> None:
    conn.execute(
        """
        INSERT INTO fundamentals_annual (
            ticker, fiscal_year, fiscal_end_date, filing_date, currency,
            fcf, fcf_per_share, shares_out, source,
            fcf_quality, fcf_scale_divisor, fcf_fmp_unit_notes
        ) VALUES (?, ?, ?, ?, 'USD', 1.0, ?, 1.0, 'fmp', ?, 1, NULL)
        """,
        ["QZ", fy, f"{fy}-12-31", f"{fy + 1}-03-01", fcf_ps, quality],
    )


def test_compute_dcf_history_ignores_flagged_magnitude_row(in_memory_db):
    conn = in_memory_db
    _insert_row(conn, fy=2023, fcf_ps=2.0, quality="ok")
    _insert_row(conn, fy=2024, fcf_ps=2.0, quality="ok")
    _insert_row(conn, fy=2025, fcf_ps=9999.0, quality="flagged_magnitude")

    n = compute_dcf_history("QZ", conn)
    assert n == 2

    years = [r[0] for r in conn.execute(
        "SELECT fiscal_year FROM dcf_history WHERE ticker='QZ' ORDER BY fiscal_year"
    ).fetchall()]
    assert years == [2023, 2024]


def test_compute_dcf_lines_keeps_scaled_quality(in_memory_db):
    conn = in_memory_db
    _insert_row(conn, fy=2024, fcf_ps=5.0, quality="ok")
    _insert_row(conn, fy=2025, fcf_ps=5.2, quality="scaled_1000")

    out = compute_dcf_lines("QZ", conn)
    assert out
    assert out["fcf_per_share_avg3yr"] == pytest.approx(5.1)
