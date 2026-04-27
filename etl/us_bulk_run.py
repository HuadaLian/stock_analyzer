"""
US/global-style ETL with checkpointing (universe source from ``us_universe``).

Usage:
    python -m etl.us_bulk_run --init-db
    python -m etl.us_bulk_run --rate-limit-ms 400 --skip-optional
    python -m etl.us_bulk_run --limit 50 --retry-failed
    python -m etl.us_bulk_run --force

Requires FMP_API_KEY. Logs to logs/us_bulk_<timestamp>.log

Concurrent Streamlit (read replica) + parallel fetch (recommended)::

    python -m etl.us_bulk_run --reconnect-every 50 --fetch-workers 20 \\
        --reconnect-pause-ms 1200

``--fetch-workers`` auto-enables ``--snapshot-every 1`` so ``stock_read.db`` is
refreshed after each batch while the writer is closed; start the UI with
``launch_app.py`` or ``app.py`` (bootstrap picks up ``stock_read.db``).

While the single writer applies batch *N*, HTTP fetch for batch *N+1* is already
in flight on the same thread pool (in-memory double-buffer; no disk staging).
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from db.schema import get_conn, init_db, DB_PATH
from etl.dotenv_local import merge_dotenv_into_environ
from etl.pipeline import USRunOptions, run_us_ticker
from etl.snapshot import snapshot_db
from etl.us_ticker_bundle import apply_ticker_bundle, fetch_ticker_bundle, load_batch_fetch_context
from us_universe import fetch_us_universe

_REPO_ROOT = Path(__file__).resolve().parents[1]
_LOG_DIR = _REPO_ROOT / "logs"
_REPORTS_DIR = _REPO_ROOT / "reports"

_SKIP_MSG = "skip (not operating common stock)"


def _ensure_initial_read_replica(log: logging.Logger, *, src: Path, dst: Path) -> None:
    """Create ``dst`` once if missing so Streamlit can set STOCK_ANALYZER_READ_DB.

    Safe to call while no bulk write connection is open on ``src``.
    """
    try:
        if dst.resolve() == src.resolve():
            return
    except OSError:
        return
    if not src.is_file():
        return
    if dst.is_file():
        return
    ok, _, msg = snapshot_db(src, dst)
    if ok:
        log.info("Initial read replica for UI: %s (%s)", dst, msg)
    else:
        log.warning("Initial read replica not created: %s", msg)


def _setup_logging(log_path: Path) -> None:
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def _submit_fetch_futures(
    ex: ThreadPoolExecutor,
    work_list: list[str],
    ctx_map: dict,
    opts: USRunOptions,
) -> dict[str, Future]:
    """Schedule one ``fetch_ticker_bundle`` task per ticker (HTTP-only; no DB)."""
    out: dict[str, Future] = {}
    for t in work_list:
        tu = t.upper()
        out[tu] = ex.submit(fetch_ticker_bundle, tu, ctx_map[tu], opts)
    return out


def _gather_fetch_results(futs: dict[str, Future]) -> tuple[dict[str, object], dict[str, Exception], float]:
    """Join fetch futures (wall time includes overlap when other threads run)."""
    bundles: dict[str, object] = {}
    exc_map: dict[str, Exception] = {}
    t0 = time.perf_counter()
    rev = {fu: sym for sym, fu in futs.items()}
    for fu in as_completed(futs.values()):
        sym = rev[fu]
        try:
            bundles[sym] = fu.result()
        except Exception as e:
            exc_map[sym] = e
    return bundles, exc_map, time.perf_counter() - t0


def _mark_state(conn, ticker: str, status: str, step: str = "", last_error: str | None = None) -> None:
    err = (last_error or "")[:2000]
    conn.execute(
        """
        INSERT INTO etl_us_bulk_state (ticker, status, step, last_error, updated_at)
        VALUES (?, ?, ?, ?, now())
        ON CONFLICT (ticker) DO UPDATE SET
            status = excluded.status,
            step = excluded.step,
            last_error = excluded.last_error,
            updated_at = now()
        """,
        [ticker, status, step, err or None],
    )


def _iter_batches(items, batch_size: int):
    """Yield successive batches; batch_size <= 0 yields the whole list once.

    Used to slice the ticker stream so bulk can periodically close the DuckDB
    write connection and let readers (Streamlit / audit / snapshot) grab the
    file. batch_size=0 preserves the legacy "one long-lived connection" behavior.
    """
    if batch_size <= 0:
        yield list(items)
        return
    buf: list = []
    for it in items:
        buf.append(it)
        if len(buf) >= batch_size:
            yield buf
            buf = []
    if buf:
        yield buf


def _open_conn_with_retry(log, max_attempts: int = 6, base_delay_s: float = 1.0):
    """Open a write connection, retrying with exponential backoff if Windows
    reports the file is locked (another process grabbed the snapshot window).

    DuckDB on Windows enforces single-writer file locking; in the gap between
    `conn.close()` and the next `get_conn()` a reader (or another bulk) may
    briefly hold the file. Backoff: 1s, 2s, 4s, 8s, 16s, 32s — total ~63s.
    """
    last_err: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return get_conn()
        except Exception as e:
            msg = str(e)
            looks_like_lock = (
                "Cannot open file" in msg
                or "另一个程序正在使用" in msg
                or "being used by another process" in msg
            )
            if not looks_like_lock or attempt == max_attempts:
                raise
            wait = base_delay_s * (2 ** (attempt - 1))
            log.warning(
                "DB locked on reopen (attempt %d/%d): %s — sleeping %.1fs",
                attempt, max_attempts, msg[:120], wait,
            )
            last_err = e
            time.sleep(wait)
    # Defensive: should never reach here (loop either returns or raises)
    raise RuntimeError(f"_open_conn_with_retry exhausted: {last_err}")


def _cleanup_stale_running(conn) -> int:
    """Mark rows stuck in `running` as `interrupted` so a previous crash
    doesn't masquerade as an active run. `interrupted` falls through
    `_should_process`'s default branch and gets re-attempted next run, while
    remaining distinguishable from `failed` (which only retries with the flag).
    """
    n = conn.execute(
        "SELECT COUNT(*) FROM etl_us_bulk_state WHERE status = 'running'"
    ).fetchone()[0]
    if n:
        conn.execute(
            """
            UPDATE etl_us_bulk_state
            SET status     = 'interrupted',
                last_error = COALESCE(last_error, '') || ' [cleared stale running on startup]',
                updated_at = now()
            WHERE status = 'running'
            """
        )
    return int(n)


_OHLCV_ONLY_FAILURE_MARKERS = (
    "no price history",
    "no price_history",
)


def _last_error_looks_ohlcv_only(last_error: str | None) -> bool:
    """True when ``last_error`` is likely a transient / incremental OHLCV-only issue."""
    if not last_error:
        return False
    low = last_error.lower()
    return any(m in low for m in _OHLCV_ONLY_FAILURE_MARKERS)


def _done_stale_for_refresh(conn, ticker: str, stale_days: int) -> bool:
    """True if any of (latest OHLCV, latest annual fiscal end, latest FMP DCF, company row date) is >= stale_days old."""
    from datetime import date as date_cls

    t = ticker.upper()
    row = conn.execute(
        """
        SELECT
            (SELECT MAX(date) FROM ohlcv_daily WHERE ticker = ?) AS mx_oh,
            (SELECT MAX(fiscal_end_date) FROM fundamentals_annual WHERE ticker = ?) AS mx_fa,
            (SELECT MAX(date) FROM fmp_dcf_history WHERE ticker = ?) AS mx_fmp,
            (SELECT CAST(updated_at AS DATE) FROM companies WHERE ticker = ?) AS mx_co
        """,
        [t, t, t, t],
    ).fetchone()
    today = date_cls.today()

    def age_days(mx: object) -> int:
        if mx is None:
            return stale_days + 1
        try:
            d = date_cls.fromisoformat(str(mx)[:10])
        except ValueError:
            return stale_days + 1
        return (today - d).days

    ages = [age_days(row[i]) for i in range(4)]
    return max(ages) >= stale_days


def _should_process(
    conn,
    ticker: str,
    *,
    force: bool,
    retry_failed: bool,
    stale_days: int | None,
    retry_failed_include_ohlcv: bool,
) -> bool:
    if force:
        return True
    row = conn.execute(
        "SELECT status, last_error FROM etl_us_bulk_state WHERE ticker = ?",
        [ticker],
    ).fetchone()
    if not row:
        return True
    st, last_err = row[0], row[1]
    if st == "done":
        if stale_days is not None and stale_days > 0:
            return _done_stale_for_refresh(conn, ticker, stale_days)
        return False
    if st == "skipped":
        return False
    if st == "failed":
        if not retry_failed:
            return False
        if not retry_failed_include_ohlcv and _last_error_looks_ohlcv_only(last_err):
            return False
        return True
    if st == "running":
        return True
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="US bulk ETL (checkpointed)")
    parser.add_argument("--init-db", action="store_true", help="Run init_db() before starting")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-run every ticker including done/skipped (full FMP+DB path; OHLCV still incremental from DB). "
        "For normal runs omit this — done tickers are skipped by default.",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Re-run tickers in failed state. By default skips failures whose last_error looks OHLCV-only "
        "(incremental daily window / empty bars); use --retry-failed-include-ohlcv to retry those too.",
    )
    parser.add_argument(
        "--retry-failed-include-ohlcv",
        action="store_true",
        help="With --retry-failed, also retry failures classified as OHLCV-only (see --retry-failed).",
    )
    parser.add_argument(
        "--stale-days",
        type=int,
        default=0,
        help="Re-process done tickers when fundamentals / OHLCV / FMP DCF / company row is at least this many "
        "calendar days behind today (0 = never refresh done; use e.g. 7 for weekly catch-up).",
    )
    parser.add_argument(
        "--refresh-mode",
        choices=("full", "ohlcv", "fundamentals", "fmp_dcf"),
        default="full",
        help="Subset of FMP fetch+apply: full (default), ohlcv (profile + incremental OHLCV + EMA/DCF lines), "
        "fundamentals (no OHLCV fetch; statements + FMP DCF + compute), fmp_dcf (profile + FMP DCF history only).",
    )
    parser.add_argument("--skip-optional", action="store_true", help="Skip management / segment / geo / interest")
    parser.add_argument("--limit", type=int, default=0, help="Max tickers to process (0 = no limit)")
    parser.add_argument("--rate-limit-ms", type=int, default=350, help="Sleep between tickers (ms)")
    parser.add_argument("--universe-refresh", action="store_true", help="Force refresh SEC universe cache")
    parser.add_argument(
        "--reconnect-every", type=int, default=0,
        help="Close+reopen the DuckDB write connection every N tickers (0 = never). "
             "Opens a periodic window for readers/snapshot to grab stock.db. "
             "Recommend 50-100; smaller values increase per-batch reconnect overhead.",
    )
    parser.add_argument(
        "--reconnect-pause-ms", type=int, default=200,
        help="Sleep between batches after closing the write conn (only with --reconnect-every>0). "
             "Default 200ms is enough on SSDs for readers to grab the file; bump to 1000 only if "
             "you see read attempts losing the race during snapshot windows.",
    )
    parser.add_argument(
        "--snapshot-every", type=int, default=0,
        help="Copy stock.db → --snapshot-path every K batches (requires --reconnect-every>0). "
             "0 = disabled. Snapshot runs in the reconnect window so readers see a consistent file.",
    )
    parser.add_argument(
        "--snapshot-path", type=str, default="",
        help=f"Destination for --snapshot-every (default: {DB_PATH.parent / 'stock_read.db'}). "
             "Streamlit reads this file when STOCK_ANALYZER_READ_DB points at it.",
    )
    parser.add_argument(
        "--fetch-workers",
        type=int,
        default=0,
        help="Parallel FMP-only fetch threads per reconnect batch (0 = legacy serial run_us_ticker). "
        "Requires --reconnect-every > 0 so each batch is bounded; apply stays single-writer.",
    )
    args = parser.parse_args()

    merge_dotenv_into_environ(_REPO_ROOT)

    stale_days = max(0, int(args.stale_days))
    stale_for_should = stale_days if stale_days > 0 else None

    if args.init_db:
        init_db()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = _LOG_DIR / f"us_bulk_{ts}.log"
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    _setup_logging(log_path)
    log = logging.getLogger("us_bulk")

    with get_conn() as _cleanup_conn:
        n_stale = _cleanup_stale_running(_cleanup_conn)
    if n_stale:
        log.warning(
            "Cleared %d stale 'running' rows from etl_us_bulk_state → 'interrupted'",
            n_stale,
        )

    log.info("Loading active company universe (FMP stable/search-symbol + filters)...")
    universe = fetch_us_universe(
        force_refresh=args.universe_refresh,
        filter_mode="ordinary_common_stock",
    )
    universe_tickers = list(universe.keys())
    log.info("Universe size: %s tickers", f"{len(universe_tickers):,}")
    log.info(
        "Bulk options: refresh_mode=%s stale_days=%s retry_failed_include_ohlcv=%s",
        args.refresh_mode,
        stale_days,
        bool(args.retry_failed_include_ohlcv),
    )

    opts = USRunOptions(
        skip_optional=args.skip_optional,
        verbose=False,
        refresh_mode=str(args.refresh_mode),
    )
    processed = 0
    ok = 0
    failed = 0
    skipped_state = 0
    skipped_etf = 0

    # Pre-filter to actionable tickers up front so reconnect/snapshot overhead is
    # amortised across real work, not over batches that turn out 90% skipped.
    # The inline _should_process inside each batch loop stays as a safety net
    # (status can still flip mid-run if another process touches the table).
    with _open_conn_with_retry(log) as _filter_conn:
        actionable: list[str] = []
        for t in universe_tickers:
            if _should_process(
                _filter_conn,
                t,
                force=args.force,
                retry_failed=args.retry_failed,
                stale_days=stale_for_should,
                retry_failed_include_ohlcv=bool(args.retry_failed_include_ohlcv),
            ):
                actionable.append(t)
    pre_skipped = len(universe_tickers) - len(actionable)
    log.info(
        "Actionable: %s tickers (pre-filtered %s as already-done/skipped/etc)",
        f"{len(actionable):,}",
        f"{pre_skipped:,}",
    )
    skipped_state += pre_skipped
    tickers = actionable

    delay_s = max(0, args.rate_limit_ms) / 1000.0
    pause_s = max(0, args.reconnect_pause_ms) / 1000.0
    reconnect_every = max(0, args.reconnect_every)
    snapshot_every = max(0, args.snapshot_every)
    snapshot_path = (
        Path(args.snapshot_path).expanduser()
        if args.snapshot_path
        else (DB_PATH.parent / "stock_read.db")
    )

    if snapshot_every and not reconnect_every:
        log.warning(
            "--snapshot-every=%d ignored: requires --reconnect-every > 0 (need a write-lock release window)",
            snapshot_every,
        )
        snapshot_every = 0

    fetch_workers = max(0, args.fetch_workers)
    if fetch_workers > 0 and reconnect_every <= 0:
        log.warning(
            "--fetch-workers=%d ignored: requires --reconnect-every > 0 (bounded batches)",
            fetch_workers,
        )
        fetch_workers = 0
    elif fetch_workers > 0:
        log.info(
            "Parallel FMP fetch: %d worker threads (writes stay single-threaded); "
            "HTTP for batch N+1 overlaps apply for batch N (in-memory double-buffer).",
            fetch_workers,
        )
        # Read/write split for Streamlit: keep stock_read.db fresh while bulk holds stock.db.
        # Snapshot every 10 batches (≈ 250 tickers @ reconnect-every=25): one full DB copy is
        # ~200 MiB, copying after every batch was wasting hundreds of GB over a full bulk run.
        # 10 batches keeps the read replica < 5 minutes stale in practice.
        if snapshot_every < 1:
            snapshot_every = 10
            log.info(
                "Auto-enabled --snapshot-every=10 (read replica refresh cadence with --fetch-workers). "
                "Pass --snapshot-every 1 if you need a snapshot after every batch.",
            )

    if reconnect_every:
        log.info(
            "Short-connection mode: reopen every %d tickers, pause %.1fs between batches",
            reconnect_every, pause_s,
        )
    if snapshot_every:
        log.info(
            "Snapshot mode: copy stock.db → %s every %d batch(es)",
            snapshot_path, snapshot_every,
        )

    if snapshot_every > 0:
        _ensure_initial_read_replica(log, src=DB_PATH, dst=snapshot_path)
    elif reconnect_every > 0:
        log.info(
            "Tip: add --snapshot-every 1 (on by default with --fetch-workers) so the UI can "
            "read stock_read.db while bulk holds stock.db (see STOCK_ANALYZER_READ_DB / db/README.md).",
        )

    wall_t0 = time.perf_counter()
    total_parallel_fetch_s = 0.0
    total_parallel_apply_net_s = 0.0
    total_parallel_pre_s = 0.0
    parallel_ticker_count = 0
    serial_run_us_ticker_s = 0.0

    stop_loop = False

    if fetch_workers > 0:
        batch_list = list(_iter_batches(tickers, reconnect_every))
        pipelined_wl: list[str] | None = None
        pipelined_ctx: dict = {}
        pipelined_futs: dict[str, Future] | None = None

        with ThreadPoolExecutor(max_workers=fetch_workers) as fetch_ex:
            for batch_idx, batch in enumerate(batch_list):
                if stop_loop:
                    break

                batch_did_write_work = False
                t_pre0 = time.perf_counter()

                if pipelined_futs is not None:
                    work_list = list(pipelined_wl or [])
                    ctx_map = pipelined_ctx
                    pipelined_wl = None
                    pipelined_ctx = {}
                    futs_cur = pipelined_futs
                    pipelined_futs = None
                else:
                    work_list = []
                    with _open_conn_with_retry(log) as conn:
                        for ticker in batch:
                            if args.limit and processed >= args.limit:
                                stop_loop = True
                                break
                            if not _should_process(
                                conn,
                                ticker,
                                force=args.force,
                                retry_failed=args.retry_failed,
                                stale_days=stale_for_should,
                                retry_failed_include_ohlcv=bool(args.retry_failed_include_ohlcv),
                            ):
                                skipped_state += 1
                                continue
                            processed += 1
                            _mark_state(conn, ticker, "running", "start")
                            work_list.append(ticker)
                        ctx_map = (
                            load_batch_fetch_context(conn, [t.upper() for t in work_list])
                            if work_list
                            else {}
                        )
                    futs_cur = _submit_fetch_futures(fetch_ex, work_list, ctx_map, opts)

                pre_s = time.perf_counter() - t_pre0

                bundles: dict[str, object] = {}
                exc_map: dict[str, Exception] = {}
                fetch_s = 0.0
                if work_list:
                    bundles, exc_map, fetch_s = _gather_fetch_results(futs_cur)

                if (
                    batch_idx + 1 < len(batch_list)
                    and not stop_loop
                    and (not args.limit or processed < args.limit)
                ):
                    nxt = batch_list[batch_idx + 1]
                    nwl: list[str] = []
                    with _open_conn_with_retry(log) as conn:
                        for ticker in nxt:
                            if args.limit and processed >= args.limit:
                                stop_loop = True
                                break
                            if not _should_process(
                                conn,
                                ticker,
                                force=args.force,
                                retry_failed=args.retry_failed,
                                stale_days=stale_for_should,
                                retry_failed_include_ohlcv=bool(args.retry_failed_include_ohlcv),
                            ):
                                skipped_state += 1
                                continue
                            processed += 1
                            _mark_state(conn, ticker, "running", "start")
                            nwl.append(ticker)
                        nctx = (
                            load_batch_fetch_context(conn, [t.upper() for t in nwl])
                            if nwl
                            else {}
                        )
                    if nwl:
                        pipelined_wl = nwl
                        pipelined_ctx = nctx
                        pipelined_futs = _submit_fetch_futures(fetch_ex, nwl, nctx, opts)

                apply_net_s = 0.0
                t_apply_wall0 = time.perf_counter()
                with _open_conn_with_retry(log) as conn:
                    for ticker in work_list:
                        tu = ticker.upper()
                        if tu in exc_map:
                            e = exc_map[tu]
                            if isinstance(e, ValueError) and _SKIP_MSG in str(e):
                                _mark_state(conn, ticker, "skipped", "etf_or_fund", str(e))
                                skipped_etf += 1
                                log.info("%s skipped (ETF/fund per FMP)", ticker)
                            else:
                                _mark_state(conn, ticker, "failed", "error", str(e))
                                failed += 1
                                if isinstance(e, ValueError):
                                    log.error("FAIL %s: %s", ticker, e)
                                else:
                                    log.exception("FAIL %s: %s", ticker, e)
                            time.sleep(delay_s)
                            continue
                        t_apply0 = time.perf_counter()
                        try:
                            apply_ticker_bundle(conn, bundles[tu], opts)
                            _mark_state(conn, ticker, "done", "complete", None)
                            ok += 1
                            log.info("OK %s", ticker)
                        except ValueError as e:
                            if _SKIP_MSG in str(e):
                                _mark_state(conn, ticker, "skipped", "etf_or_fund", str(e))
                                skipped_etf += 1
                                log.info("%s skipped (ETF/fund per FMP)", ticker)
                            else:
                                _mark_state(conn, ticker, "failed", "error", str(e))
                                failed += 1
                                log.error("FAIL %s: %s", ticker, e)
                        except Exception as e:
                            _mark_state(conn, ticker, "failed", "error", str(e))
                            failed += 1
                            log.exception("FAIL %s: %s", ticker, e)
                        finally:
                            apply_net_s += time.perf_counter() - t_apply0
                        time.sleep(delay_s)
                apply_wall_s = time.perf_counter() - t_apply_wall0

                if work_list:
                    n = len(work_list)
                    parallel_ticker_count += n
                    total_parallel_pre_s += pre_s
                    total_parallel_fetch_s += fetch_s
                    total_parallel_apply_net_s += apply_net_s
                    log.info(
                        "Batch %d throughput: tickers=%d prefetch_mark=%.2fs fetch_parallel=%.2fs "
                        "apply_db=%.2fs apply_loop_wall=%.2fs | wall rates: fetch %.2f t/s, apply_db %.2f t/s, "
                        "loop_incl_rate_limit_sleep %.2f t/s",
                        batch_idx,
                        n,
                        pre_s,
                        fetch_s,
                        apply_net_s,
                        apply_wall_s,
                        n / fetch_s if fetch_s > 0 else 0.0,
                        n / apply_net_s if apply_net_s > 0 else 0.0,
                        n / apply_wall_s if apply_wall_s > 0 else 0.0,
                    )
                batch_did_write_work = bool(work_list)

                if snapshot_every and (batch_idx + 1) % snapshot_every == 0 and batch_did_write_work:
                    ok_snap, bytes_done, msg = snapshot_db(DB_PATH, snapshot_path)
                    if ok_snap:
                        log.info("Snapshot batch %d: %s", batch_idx, msg)
                    else:
                        log.warning("Snapshot batch %d failed: %s", batch_idx, msg)

                if reconnect_every and not stop_loop and pause_s > 0:
                    log.info("Batch %d done — pausing %.1fs (window for readers)", batch_idx, pause_s)
                    time.sleep(pause_s)
    else:
        for batch_idx, batch in enumerate(_iter_batches(tickers, reconnect_every)):
            if stop_loop:
                break

            batch_did_write_work = False

            with _open_conn_with_retry(log) as conn:
                for ticker in batch:
                    if args.limit and processed >= args.limit:
                        stop_loop = True
                        break
                    if not _should_process(
                        conn,
                        ticker,
                        force=args.force,
                        retry_failed=args.retry_failed,
                        stale_days=stale_for_should,
                        retry_failed_include_ohlcv=bool(args.retry_failed_include_ohlcv),
                    ):
                        skipped_state += 1
                        continue

                    batch_did_write_work = True
                    processed += 1
                    _mark_state(conn, ticker, "running", "start")

                    t_run0 = time.perf_counter()
                    try:
                        run_us_ticker(conn, ticker, opts)
                        _mark_state(conn, ticker, "done", "complete", None)
                        ok += 1
                        log.info("OK %s", ticker)
                    except ValueError as e:
                        if _SKIP_MSG in str(e):
                            _mark_state(conn, ticker, "skipped", "etf_or_fund", str(e))
                            skipped_etf += 1
                            log.info("%s skipped (ETF/fund per FMP)", ticker)
                        else:
                            _mark_state(conn, ticker, "failed", "error", str(e))
                            failed += 1
                            log.error("FAIL %s: %s", ticker, e)
                    except Exception as e:
                        _mark_state(conn, ticker, "failed", "error", str(e))
                        failed += 1
                        log.exception("FAIL %s: %s", ticker, e)
                    finally:
                        serial_run_us_ticker_s += time.perf_counter() - t_run0

                    time.sleep(delay_s)

            if snapshot_every and (batch_idx + 1) % snapshot_every == 0 and batch_did_write_work:
                ok_snap, bytes_done, msg = snapshot_db(DB_PATH, snapshot_path)
                if ok_snap:
                    log.info("Snapshot batch %d: %s", batch_idx, msg)
                else:
                    log.warning("Snapshot batch %d failed: %s", batch_idx, msg)

            if reconnect_every and not stop_loop and pause_s > 0:
                log.info("Batch %d done — pausing %.1fs (window for readers)", batch_idx, pause_s)
                time.sleep(pause_s)

    wall_total = time.perf_counter() - wall_t0
    log.info(
        "Finished: attempted=%s ok=%s failed=%s skipped_done_or_skipped=%s skipped_etf_fund=%s log=%s",
        processed,
        ok,
        failed,
        skipped_state,
        skipped_etf,
        log_path,
    )
    if processed > 0 and wall_total > 0:
        log.info(
            "Global wall-clock: %.2f min, attempted tickers=%d → %.4f tickers/s overall "
            "(includes snapshot copies, reconnect pauses, --rate-limit-ms sleeps).",
            wall_total / 60.0,
            processed,
            processed / wall_total,
        )
    if fetch_workers > 0 and parallel_ticker_count > 0:
        log.info(
            "Global parallel phases (sum over batches): tickers=%d | "
            "fetch_parallel total=%.1fs → %.4f tickers/s | "
            "apply_db total=%.1fs → %.4f tickers/s | prefetch_mark total=%.1fs",
            parallel_ticker_count,
            total_parallel_fetch_s,
            parallel_ticker_count / total_parallel_fetch_s if total_parallel_fetch_s > 0 else 0.0,
            total_parallel_apply_net_s,
            parallel_ticker_count / total_parallel_apply_net_s if total_parallel_apply_net_s > 0 else 0.0,
            total_parallel_pre_s,
        )
    elif fetch_workers == 0 and processed > 0 and serial_run_us_ticker_s > 0:
        log.info(
            "Global serial run_us_ticker CPU+DB time: %.1fs for %d attempts → %.4f tickers/s "
            "(excludes rate-limit sleep between tickers; full wall in line above).",
            serial_run_us_ticker_s,
            processed,
            processed / serial_run_us_ticker_s,
        )
    print(f"\nLog file: {log_path}")


if __name__ == "__main__":
    main()
