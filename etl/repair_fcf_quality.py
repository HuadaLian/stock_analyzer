"""Post-bulk: audit fundamentals_annual FCF rows, fix via FMP when out of sync with rules.

1. Rebuild each ticker's fiscal series (oldest→newest) and run ``apply_fcf_native_adjustments``
   on USD-equivalent natives derived from stored ``fcf`` (millions) / ``fx_to_usd``.
2. If inferred scale/FCF disagrees with DB → ``fetch_fcf_annual`` (single-ticker FMP) + upsert.
3. If numbers already match but ``fcf_quality`` (etc.) is missing → UPDATE quality fields only
   from inference (no HTTP).

Usage::

    conda activate stock_analyzer
    cd <repo>
    python -m etl.repair_fcf_quality --dry-run
    python -m etl.repair_fcf_quality --limit 500
    python -m etl.repair_fcf_quality --ticker LOAN
    python -m etl.repair_fcf_quality --recompute-dcf   # per touched ticker, after upsert
    python -m etl.repair_fcf_quality --suppress-flagged --recompute-dcf   # backfill DB clears
"""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from db.fcf_quality import (
    apply_fcf_native_adjustments,
    build_fcf_work_row,
    suppressed_flagged_fcf_notes,
)
from db.schema import get_conn, init_db
from etl.compute import compute_dcf_history, compute_dcf_lines
from etl.dotenv_local import merge_dotenv_into_environ
from etl.loader import upsert_fundamentals_annual
from etl.sources.fmp import fetch_fcf_annual, fetch_profile


def _list_tickers(conn, *, ticker: str | None, limit: int | None) -> list[str]:
    if ticker:
        return [ticker.strip().upper()]
    q = """
        SELECT DISTINCT ticker
        FROM fundamentals_annual
        WHERE fcf IS NOT NULL
          AND (source IS NULL OR lower(source) = 'fmp')
        ORDER BY ticker
    """
    rows = conn.execute(q).fetchall()
    out = [str(r[0]).upper() for r in rows if r and r[0]]
    if limit is not None and limit > 0:
        out = out[:limit]
    return out


def _load_fund_rows_chrono(conn, ticker: str) -> list[dict]:
    cur = conn.execute(
        "SELECT * FROM fundamentals_annual WHERE ticker = ? ORDER BY fiscal_year ASC",
        [ticker],
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _infer_meta_from_db_rows(rows: list[dict]) -> list[dict]:
    """Oldest-first ``rows`` → same-length meta from ``apply_fcf_native_adjustments``."""
    work = []
    for r in rows:
        fy = int(r["fiscal_year"])
        fcf_m = r.get("fcf")
        if fcf_m is None or (isinstance(fcf_m, float) and math.isnan(fcf_m)):
            work.append(
                build_fcf_work_row(
                    fiscal_year=fy,
                    fcf_native=None,
                    ocf_native=None,
                    reporting_currency=str(r.get("reporting_currency") or "USD"),
                    fx_to_usd=float(r.get("fx_to_usd") or 1.0),
                    entry={"date": str(r.get("fiscal_end_date") or f"{fy}-12-31")[:10]},
                )
            )
            continue
        fx = float(r.get("fx_to_usd") or 1.0)
        fcf_usd = float(fcf_m) * 1_000_000.0
        native = fcf_usd / fx if fx else fcf_usd
        work.append(
            build_fcf_work_row(
                fiscal_year=fy,
                fcf_native=native,
                ocf_native=None,
                reporting_currency=str(r.get("reporting_currency") or "USD"),
                fx_to_usd=fx,
                entry={"date": str(r.get("fiscal_end_date") or f"{fy}-12-31")[:10]},
            )
        )
    return apply_fcf_native_adjustments(work)


def _expected_fcf_millions(r: dict, meta: dict) -> float | None:
    adj = meta.get("fcf_native_adjusted")
    if adj is None:
        return None
    fx = float(r.get("fx_to_usd") or 1.0)
    return float(adj) * fx / 1_000_000.0


def _numeric_mismatch(rows: list[dict], meta: list[dict]) -> bool:
    for r, m in zip(rows, meta):
        exp = _expected_fcf_millions(r, m)
        if exp is None:
            continue
        cur = r.get("fcf")
        if cur is None:
            continue
        cur_f = float(cur)
        tol = 1e-4 + 0.02 * max(abs(exp), abs(cur_f), 1e-9)
        if abs(cur_f - exp) > tol:
            return True
        db_div = int(r.get("fcf_scale_divisor") or 1)
        if int(m.get("scale_divisor") or 1) != db_div:
            return True
    return False


def _quality_needs_fill(rows: list[dict], meta: list[dict]) -> bool:
    for r, m in zip(rows, meta):
        q = (str(r.get("fcf_quality") or "").strip().lower())
        # Cleared flagged rows: keep DB flags/notes; re-inference from NULL FCF would wrongly say "ok".
        if q == "flagged_magnitude" and r.get("fcf") is None and r.get("fcf_per_share") is None:
            continue
        if not q or q == "none":
            return True
        if q != str(m.get("quality") or "").strip().lower():
            return True
        if int(r.get("fcf_scale_divisor") or 1) != int(m.get("scale_divisor") or 1):
            return True
    return False


def _merge_row_quality_from_meta(row: dict, meta: dict) -> dict:
    out = dict(row)
    out["fcf_quality"] = str(meta.get("quality") or "ok")
    out["fcf_scale_divisor"] = int(meta.get("scale_divisor") or 1)
    notes = meta.get("fcf_fmp_unit_notes")
    out["fcf_fmp_unit_notes"] = notes if notes else None
    return out


def apply_flagged_magnitude_suppression(
    conn,
    *,
    dry_run: bool = False,
    recompute_dcf: bool = False,
) -> dict[str, object]:
    """Clear ``fcf`` / ``fcf_per_share`` for ``flagged_magnitude`` rows; append raw snapshot to notes.

    Run after schema exists. Use ``--suppress-flagged`` on this module's CLI to backfill.
    """
    rows = conn.execute(
        """
        SELECT ticker, fiscal_year, fcf, fcf_per_share, fcf_fmp_unit_notes
        FROM fundamentals_annual
        WHERE lower(trim(coalesce(fcf_quality, ''))) = 'flagged_magnitude'
          AND (fcf IS NOT NULL OR fcf_per_share IS NOT NULL)
        """
    ).fetchall()
    tickers: set[str] = set()
    n = 0
    for ticker, fy, fcf, fcf_ps, notes in rows:
        tickers.add(str(ticker).upper())
        fm = float(fcf) if fcf is not None and math.isfinite(float(fcf)) else None
        fps = float(fcf_ps) if fcf_ps is not None and math.isfinite(float(fcf_ps)) else None
        new_notes = suppressed_flagged_fcf_notes(notes, fcf_millions=fm, fcf_per_share=fps)
        if not dry_run:
            conn.execute(
                """
                UPDATE fundamentals_annual
                SET fcf = NULL, fcf_per_share = NULL, fcf_fmp_unit_notes = ?
                WHERE ticker = ? AND fiscal_year = ?
                """,
                [new_notes, ticker, fy],
            )
        n += 1
    if recompute_dcf and not dry_run:
        for t in sorted(tickers):
            compute_dcf_history(t, conn)
            compute_dcf_lines(t, conn)
    return {"rows_touched": n, "tickers": sorted(tickers)}


def _repair_one_ticker(
    conn,
    ticker: str,
    *,
    dry_run: bool,
    recompute_dcf: bool,
) -> tuple[str, str]:
    """Returns (status, detail) where status in ok_skip, updated_meta, refetched_fmp, error."""
    rows = _load_fund_rows_chrono(conn, ticker)
    if not rows:
        return "ok_skip", "no fundamentals rows"

    meta = _infer_meta_from_db_rows(rows)
    need_fmp = _numeric_mismatch(rows, meta)
    need_meta = _quality_needs_fill(rows, meta)

    if not need_fmp and not need_meta:
        return "ok_skip", "aligned with inference"

    if need_fmp:
        if dry_run:
            return "would_refetch", "numeric or divisor mismatch vs inference"
        try:
            prof = fetch_profile(ticker)
            if not prof:
                return "error", "fetch_profile returned empty"
            shares = prof.get("_shares_out_raw")
            fresh = fetch_fcf_annual(ticker, shares)
            upsert_fundamentals_annual(conn, fresh)
            if recompute_dcf:
                compute_dcf_history(ticker, conn)
                compute_dcf_lines(ticker, conn)
            return "refetched_fmp", f"upsert {len(fresh)} fiscal rows"
        except Exception as exc:
            return "error", str(exc)[:500]

    # Only metadata / quality flags — no HTTP
    if dry_run:
        return "would_update_meta", "fill fcf_quality / divisor / notes from inference"
    merged = [_merge_row_quality_from_meta(r, m) for r, m in zip(rows, meta)]
    upsert_fundamentals_annual(conn, merged)
    if recompute_dcf:
        compute_dcf_history(ticker, conn)
        compute_dcf_lines(ticker, conn)
    return "updated_meta", f"upsert {len(merged)} rows (quality only)"


def main() -> None:
    merge_dotenv_into_environ(_REPO)
    p = argparse.ArgumentParser(description="Repair FCF quality flags / refetch FMP when mismatched")
    p.add_argument("--dry-run", action="store_true", help="Print actions only; no DB writes")
    p.add_argument("--limit", type=int, default=None, help="Max tickers to process (ordered)")
    p.add_argument("--ticker", type=str, default=None, help="Single ticker, e.g. LOAN")
    p.add_argument(
        "--recompute-dcf",
        action="store_true",
        help="After write, run compute_dcf_history + compute_dcf_lines for touched tickers",
    )
    p.add_argument("--skip-init-db", action="store_true", help="Do not run init_db() at startup")
    p.add_argument(
        "--suppress-flagged",
        action="store_true",
        help="Clear fcf/fcf_per_share for flagged_magnitude rows; append suppressed_* to notes; optional --recompute-dcf",
    )
    args = p.parse_args()

    if not args.skip_init_db:
        init_db()

    if args.suppress_flagged:
        with get_conn() as conn:
            res = apply_flagged_magnitude_suppression(
                conn,
                dry_run=args.dry_run,
                recompute_dcf=args.recompute_dcf,
            )
        print("[repair_fcf_quality] suppress_flagged:", res)
        return

    stats: dict[str, int] = {}
    with get_conn() as conn:
        tickers = _list_tickers(conn, ticker=args.ticker, limit=args.limit)
        total = len(tickers)
        print(f"[repair_fcf_quality] tickers={total} dry_run={args.dry_run} recompute_dcf={args.recompute_dcf}")
        for i, t in enumerate(tickers, 1):
            if total > 1 and i % 2000 == 0:
                print(f"[repair_fcf_quality] progress {i}/{total}")
            st, detail = _repair_one_ticker(
                conn, t, dry_run=args.dry_run, recompute_dcf=args.recompute_dcf
            )
            stats[st] = stats.get(st, 0) + 1
            if st != "ok_skip" or args.ticker:
                print(f"  {t:12s}  {st:20s}  {detail}")

    print("[repair_fcf_quality] summary:", stats)


if __name__ == "__main__":
    main()
