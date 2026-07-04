"""FMP annual FCF sanity checks: YoY spikes, OCF magnitude, optional auto-scale.

FMP JSON does not document per-field Million/Billion flags; we scan keys for
unit-like hints.  When a row is an obvious outlier vs the trailing 3-year
average (and not a reporting-currency change), we try dividing by 1000 or 100
so the adjusted value lands near the historical band.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Iterable, Sequence


def fmp_entry_unit_hints(entry: dict[str, Any]) -> str | None:
    """Collect FMP fields whose names suggest explicit units (rare on stable API)."""
    hints: list[str] = []
    for k, v in (entry or {}).items():
        kl = str(k).lower()
        if any(
            s in kl
            for s in (
                "unit",
                "scale",
                "denominator",
                "thousand",
                "million",
                "billion",
                "kilo",
                "inmillions",
            )
        ):
            hints.append(f"{k}={v}")
    return "|".join(hints[:12]) if hints else None


@dataclass
class _FcfWork:
    fiscal_year: int
    fcf_native: float | None
    ocf_native: float | None
    reporting_currency: str
    unit_hints: str | None
    fx_to_usd: float


def _to_usd(native: float | None, fx: float) -> float | None:
    if native is None:
        return None
    return float(native) * float(fx or 1.0)


def _baseline_from_prior(abs_fcfs: Sequence[float]) -> float | None:
    """Trailing-3 positive mean; if recent years are ~0 (shells), use last-24 positives
    then median of all prior positives so YoY scale still triggers vs older FCF."""
    if not abs_fcfs:
        return None
    tail = [float(x) for x in abs_fcfs[-3:] if x is not None and math.isfinite(x) and x > 0]
    if tail:
        return float(sum(tail) / len(tail))
    window = [float(x) for x in abs_fcfs[-24:] if x is not None and math.isfinite(x) and x > 0]
    if window:
        return float(sum(window) / len(window))
    all_pos = [float(x) for x in abs_fcfs if x is not None and math.isfinite(x) and x > 0]
    if not all_pos:
        return None
    all_pos.sort()
    return float(all_pos[len(all_pos) // 2])


def _fits_band(adj_abs: float, baseline: float) -> bool:
    if baseline <= 0 or not math.isfinite(adj_abs) or adj_abs <= 0:
        return False
    r = adj_abs / baseline
    return 0.18 <= r <= 6.0


def _pick_scale_divisor(
    *,
    fcf_usd: float,
    baseline: float | None,
    ocf_usd: float | None,
) -> tuple[int, str]:
    """Return (divisor, quality) where divisor is 1, 10, 100, or 1000."""
    abs_f = abs(float(fcf_usd))
    if abs_f <= 0 or not math.isfinite(abs_f):
        return 1, "ok"

    # Strong hint: FCF >> OCF in same filing often means wrong decimal tier.
    ocf_hint = False
    if ocf_usd is not None and math.isfinite(ocf_usd):
        ao = abs(float(ocf_usd))
        if ao > 1e-9 and abs_f / ao > 35.0:
            ocf_hint = True

    spike_vs_prior = False
    if baseline and baseline > 0:
        if abs_f / baseline >= 14.0:
            spike_vs_prior = True

    if not spike_vs_prior and not ocf_hint:
        return 1, "ok"

    candidates: list[int] = []
    for d in (1000, 100, 10):
        adj = abs_f / d
        if baseline and _fits_band(adj, baseline):
            candidates.append(d)
        elif baseline is None and ocf_usd and math.isfinite(ocf_usd):
            ao = abs(float(ocf_usd))
            if ao > 1e-9 and _fits_band(adj, ao):
                candidates.append(d)

    if not candidates and baseline:
        # Ratio-only tiers when ``_fits_band`` rejects all divisors (common after
        # years of ~0 FCF so baseline comes from a longer lookback).
        ratio = abs_f / float(baseline)
        if ratio >= 250.0:
            candidates.append(1000)
        elif ratio >= 35.0:
            candidates.append(100)
        elif ratio >= 14.0:
            candidates.append(10)

    if candidates:
        d = min(candidates)
        return d, f"scaled_{d}"

    return 1, "flagged_magnitude"


def apply_fcf_native_adjustments(
    rows_oldest_first: Iterable[_FcfWork],
) -> list[dict[str, Any]]:
    """Walk oldest→newest; return one result dict per input row (same order).

    Each output dict:
      scale_divisor (int), quality (str), fcf_native_adjusted (float|None),
      fcf_fmp_unit_notes (str|None)
    """
    rows = list(rows_oldest_first)
    out: list[dict[str, Any]] = []
    prior_abs_fcfs: list[float] = []

    for i, w in enumerate(rows):
        prev_rep = rows[i - 1].reporting_currency if i > 0 else None
        rep = (w.reporting_currency or "USD").upper()
        cur_f = _to_usd(w.fcf_native, w.fx_to_usd)
        cur_ocf = _to_usd(w.ocf_native, w.fx_to_usd)

        unit_notes = w.unit_hints
        quality = "ok"
        divisor = 1
        adjusted_native = w.fcf_native

        currency_shift = bool(
            prev_rep
            and rep
            and prev_rep.upper() != rep
        )
        if currency_shift:
            quality = "currency_shift"
            divisor = 1
        elif cur_f is not None and math.isfinite(cur_f):
            baseline = _baseline_from_prior(prior_abs_fcfs)
            div, q = _pick_scale_divisor(
                fcf_usd=cur_f,
                baseline=baseline,
                ocf_usd=cur_ocf,
            )
            if div > 1:
                divisor = div
                quality = q
                if w.fcf_native is not None:
                    adjusted_native = float(w.fcf_native) / float(div)
            elif q == "flagged_magnitude":
                quality = "flagged_magnitude"
                if unit_notes:
                    unit_notes = f"{unit_notes};yoy_or_ocf_mismatch"
                else:
                    unit_notes = "yoy_or_ocf_mismatch"

        if cur_f is not None and math.isfinite(cur_f) and divisor > 0:
            prior_abs_fcfs.append(abs(float(cur_f)) / float(divisor))

        out.append(
            {
                "fiscal_year": w.fiscal_year,
                "scale_divisor": divisor,
                "quality": quality,
                "fcf_native_adjusted": adjusted_native,
                "fcf_fmp_unit_notes": unit_notes,
            }
        )

    return out


def dcf_source_fundamentals_quality_sql() -> str:
    """DuckDB ``WHERE`` 片段：与 D1 ``_fundamentals_rows_for_dcf_chart`` 一致，仅可信 FCF 参与 DCF。

    排除 ``flagged_magnitude`` 等；``NULL`` 质检列视为历史兼容（按 ok）。"""
    return """(
        fcf_quality IS NULL
        OR lower(trim(cast(fcf_quality AS VARCHAR))) IN ('ok', 'none', 'nan', '')
        OR lower(trim(cast(fcf_quality AS VARCHAR))) LIKE 'scaled%'
        OR lower(trim(cast(fcf_quality AS VARCHAR))) = 'currency_shift'
    )"""


def suppressed_flagged_fcf_notes(
    existing_notes: str | None,
    *,
    fcf_millions: float | None,
    fcf_per_share: float | None,
    max_len: int = 480,
) -> str:
    """When ``flagged_magnitude`` FCF is cleared for storage/DCF, append raw snapshot + sentinel.

    ``fcf_fmp_unit_notes`` remains the audit trail (FMP hints + suppressed raw values).
    """
    parts: list[str] = []
    if existing_notes and str(existing_notes).strip():
        parts.append(str(existing_notes).strip())
    snap: list[str] = []
    if fcf_millions is not None and math.isfinite(float(fcf_millions)):
        snap.append(f"suppressed_raw_fcf_m={float(fcf_millions):.8g}")
    if fcf_per_share is not None and math.isfinite(float(fcf_per_share)):
        snap.append(f"suppressed_raw_fcf_ps={float(fcf_per_share):.8g}")
    snap.append("values_cleared_not_trusted")
    parts.append(";".join(snap))
    out = ";".join(parts)
    if len(out) > max_len:
        out = out[: max_len - 3] + "..."
    return out


def build_fcf_work_row(
    *,
    fiscal_year: int,
    fcf_native: float | None,
    ocf_native: float | None,
    reporting_currency: str,
    fx_to_usd: float,
    entry: dict[str, Any],
) -> _FcfWork:
    return _FcfWork(
        fiscal_year=int(fiscal_year),
        fcf_native=fcf_native,
        ocf_native=ocf_native,
        reporting_currency=(reporting_currency or "USD").upper(),
        unit_hints=fmp_entry_unit_hints(entry),
        fx_to_usd=float(fx_to_usd or 1.0),
    )
