"""全库重算 DCF 派生表（与当前 ``fundamentals_annual`` 一致）。

会按与 D1 / ``etl.compute`` 相同的 **FCF 质检可信规则** 选取财报行，再写入：

- ``dcf_history``（先删该 ticker 旧行再 upsert，避免剔除不可信年后残留财年）
- ``dcf_metrics``（含 invest/short potential；无可信 FCF 时删除该 ticker 行）

前置：财报与 FCF 质检已由 ETL / ``repair_fcf_quality`` 等写好即可；**不必**再单独跑质检，
本脚本只读库中现状。随时可重复执行（幂等）。

用法::

    conda activate stock_analyzer
    cd <repo>
    python -m etl.recompute_factor_potentials
    python -m etl.recompute_factor_potentials --log-file logs/my.log
    python -m etl.recompute_factor_potentials --limit 500 --progress-every 50
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from db.schema import get_conn, init_db
from etl.compute import compute_dcf_history, compute_dcf_lines


def _all_tickers_to_recompute(conn) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT upper(trim(cast(ticker AS VARCHAR))) AS t
        FROM (
            SELECT ticker FROM companies WHERE ticker IS NOT NULL
            UNION
            SELECT ticker FROM fundamentals_annual
            WHERE fcf_per_share IS NOT NULL
        ) AS u
        WHERE trim(coalesce(ticker, '')) != ''
        ORDER BY 1
        """
    ).fetchall()
    return [str(r[0]) for r in rows if r and r[0]]


def _setup_logging(log_path: Path) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    root.handlers.clear()
    root.addHandler(fh)
    root.addHandler(ch)


def main() -> None:
    p = argparse.ArgumentParser(description="Recompute dcf_history + dcf_metrics for all tickers")
    p.add_argument("--limit", type=int, default=None, help="Max tickers (ordered); default = all")
    p.add_argument("--skip-init-db", action="store_true", help="Do not run init_db()")
    p.add_argument(
        "--log-file",
        type=str,
        default=None,
        help="Log file path (UTF-8). Default: logs/recompute_dcf_YYYYMMDD_HHMMSS.log under repo",
    )
    p.add_argument(
        "--progress-every",
        type=int,
        default=100,
        metavar="N",
        help="Emit progress log every N tickers (default: 100)",
    )
    args = p.parse_args()

    if args.log_file:
        log_path = Path(args.log_file).expanduser()
        if not log_path.is_absolute():
            log_path = (_REPO / log_path).resolve()
    else:
        log_path = _REPO / "logs" / f"recompute_dcf_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    _setup_logging(log_path)
    log = logging.getLogger("recompute_dcf")

    log.info("LOG_FILE=%s", log_path)
    print(f"LOG_FILE={log_path}", flush=True)

    if not args.skip_init_db:
        try:
            init_db()
            log.info("init_db() done")
        except Exception as exc:
            log.exception(
                "init_db() failed（常见原因: Windows 上 stock.db 被 Streamlit / 其它 DuckDB 连接占用；"
                "请先结束占用进程后再跑本脚本）"
            )
            raise SystemExit(1) from exc

    t0 = time.perf_counter()
    try:
        with get_conn() as conn:
            tickers = _all_tickers_to_recompute(conn)
            if args.limit is not None and args.limit > 0:
                tickers = tickers[: args.limit]
            total = len(tickers)
            log.info("Ticker list built: total=%d (limit=%s)", total, args.limit)
            ok_hist = ok_metrics = 0
            err = 0
            pe = max(1, int(args.progress_every))
            for i, t in enumerate(tickers, start=1):
                try:
                    n = compute_dcf_history(t, conn)
                    if n > 0:
                        ok_hist += 1
                    out = compute_dcf_lines(t, conn)
                    if out:
                        ok_metrics += 1
                except Exception as exc:
                    err += 1
                    log.warning("FAIL ticker=%s err=%s", t, exc)
                    log.debug("Traceback for %s:\n%s", t, traceback.format_exc())
                if i == 1 or i == total or i % pe == 0:
                    elapsed = time.perf_counter() - t0
                    rate = i / elapsed if elapsed > 0 else 0.0
                    log.info(
                        "progress i=%d/%d rate=%.2f tickers/s dcf_history_tickers=%d "
                        "dcf_metrics_tickers=%d errors=%d elapsed_s=%.1f",
                        i,
                        total,
                        rate,
                        ok_hist,
                        ok_metrics,
                        err,
                        elapsed,
                    )
            elapsed = time.perf_counter() - t0
            log.info(
                "SUMMARY tickers=%d dcf_history_tickers=%d dcf_metrics_tickers=%d errors=%d elapsed_s=%.1f",
                total,
                ok_hist,
                ok_metrics,
                err,
                elapsed,
            )
            print(
                f"SUMMARY tickers={total} dcf_history_tickers={ok_hist} "
                f"dcf_metrics_tickers={ok_metrics} errors={err} elapsed_s={elapsed:.1f}",
                flush=True,
            )
    except Exception:
        log.exception(
            "无法以读写方式打开或使用 stock.db（Windows 上请先关闭 Streamlit / 其它占用主库的 DuckDB 连接）"
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
