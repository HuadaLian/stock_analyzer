"""FCF YoY / magnitude auto-scale (FMP-style spikes)."""

import pytest

from db.fcf_quality import (
    apply_fcf_native_adjustments,
    build_fcf_work_row,
    fmp_entry_unit_hints,
    suppressed_flagged_fcf_notes,
)


def test_unit_hints_empty_on_typical_fmp_row():
    entry = {"date": "2024-12-31", "freeCashFlow": 5_000_000, "reportedCurrency": "USD"}
    assert fmp_entry_unit_hints(entry) is None


def test_scaled_1000_when_spike_vs_three_year_avg():
    rows = []
    for fy, fcf in [(2021, 5e6), (2022, 5.2e6), (2023, 5.5e6), (2024, 5.1e6)]:
        rows.append(
            build_fcf_work_row(
                fiscal_year=fy,
                fcf_native=fcf,
                ocf_native=5.5e6,
                reporting_currency="USD",
                fx_to_usd=1.0,
                entry={"date": f"{fy}-12-31"},
            )
        )
    rows.append(
        build_fcf_work_row(
            fiscal_year=2025,
            fcf_native=5.0e9,
            ocf_native=5.1e9,
            reporting_currency="USD",
            fx_to_usd=1.0,
            entry={"date": "2025-12-31"},
        )
    )
    out = apply_fcf_native_adjustments(rows)
    assert out[-1]["scale_divisor"] == 1000
    assert out[-1]["quality"] == "scaled_1000"
    assert out[-1]["fcf_native_adjusted"] == pytest.approx(5.0e6)


def test_suppressed_flagged_notes_appends_raw_snapshot():
    s = suppressed_flagged_fcf_notes(
        "yoy_or_ocf_mismatch",
        fcf_millions=878442.0,
        fcf_per_share=225281.1,
    )
    assert "yoy_or_ocf_mismatch" in s
    assert "suppressed_raw_fcf_m=878442" in s
    assert "suppressed_raw_fcf_ps=225281" in s
    assert "values_cleared_not_trusted" in s


def test_currency_shift_skips_numeric_scale():
    rows = [
        build_fcf_work_row(
            fiscal_year=2022,
            fcf_native=5e6,
            ocf_native=5e6,
            reporting_currency="USD",
            fx_to_usd=1.0,
            entry={"date": "2022-12-31"},
        ),
        build_fcf_work_row(
            fiscal_year=2023,
            fcf_native=5e9,
            ocf_native=5e9,
            reporting_currency="EUR",
            fx_to_usd=1.0,
            entry={"date": "2023-12-31"},
        ),
    ]
    out = apply_fcf_native_adjustments(rows)
    assert out[1]["quality"] == "currency_shift"
    assert out[1]["scale_divisor"] == 1


def test_scaled_when_recent_years_zero_but_older_fcf_exists():
    """GYRO-like: trailing years ~0 then a huge FCF year; baseline must not be None."""
    rows = []
    for fy in range(2010, 2016):
        rows.append(
            build_fcf_work_row(
                fiscal_year=fy,
                fcf_native=5e6,
                ocf_native=5e6,
                reporting_currency="USD",
                fx_to_usd=1.0,
                entry={"date": f"{fy}-12-31"},
            )
        )
    for fy in range(2016, 2021):
        rows.append(
            build_fcf_work_row(
                fiscal_year=fy,
                fcf_native=0.0,
                ocf_native=0.0,
                reporting_currency="USD",
                fx_to_usd=1.0,
                entry={"date": f"{fy}-12-31"},
            )
        )
    rows.append(
        build_fcf_work_row(
            fiscal_year=2022,
            fcf_native=7.5e11,
            ocf_native=7.5e11,
            reporting_currency="USD",
            fx_to_usd=1.0,
            entry={"date": "2022-12-31"},
        )
    )
    out = apply_fcf_native_adjustments(rows)
    assert out[-1]["scale_divisor"] == 1000
    assert out[-1]["quality"] == "scaled_1000"
    assert out[-1]["fcf_native_adjusted"] == pytest.approx(7.5e8)
