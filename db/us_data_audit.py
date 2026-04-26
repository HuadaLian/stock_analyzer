"""
Post–US-bulk audit: currency sanity + coverage. Writes Markdown report.

Usage:
    python -m db.us_data_audit
    python -m db.us_data_audit --out reports/us_etl_audit.md
"""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from db.schema import get_conn, DB_PATH

_REPO = Path(__file__).resolve().parents[1]


def audit_orphan_ohlcv_tickers(conn) -> tuple[int, list[str]]:
    """Find tickers present in ohlcv_daily but missing from companies.

    These are the rows that would force `repository.get_all_tickers` to fall back
    to a full `SELECT DISTINCT ticker FROM ohlcv_daily` scan. If this returns
    `(0, [])` the fallback is dead code and can be deleted; otherwise the gap
    needs to be backfilled (insert companies rows) or pruned (delete the orphan
    OHLCV) before removal is safe.

    Returns: (orphan_count, sample_up_to_20_tickers)
    """
    n = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT DISTINCT ticker FROM ohlcv_daily
            EXCEPT
            SELECT ticker FROM companies
        )
        """
    ).fetchone()[0]
    sample: list[str] = []
    if n:
        rows = conn.execute(
            """
            SELECT ticker FROM (
                SELECT DISTINCT ticker FROM ohlcv_daily
                EXCEPT
                SELECT ticker FROM companies
            )
            ORDER BY ticker
            LIMIT 20
            """
        ).fetchall()
        sample = [r[0] for r in rows]
    return int(n), sample


def _md_table(headers: list[str], rows: list[tuple]) -> str:
    h = "| " + " | ".join(headers) + " |\n"
    sep = "| " + " | ".join("---" for _ in headers) + " |\n"
    body = ""
    for row in rows:
        body += "| " + " | ".join(str(c) if c is not None else "" for c in row) + " |\n"
    return h + sep + body


def run_audit(out_path: Path | None) -> tuple[str, Path]:
    lines: list[str] = []
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines.append(f"# US 数据审计报告\n\n生成时间: {ts}\n\n数据库: `{DB_PATH}`\n\n")

    with get_conn(readonly=True) as conn:
        lines.append("## 1. companies（market=US）\n\n")
        cur = conn.execute(
            """
            SELECT COUNT(*) FROM companies WHERE market = 'US'
            """
        ).fetchone()
        lines.append(f"- 行数: **{cur[0]}**\n\n")

        cur = conn.execute(
            """
            SELECT
              SUM(CASE WHEN currency IS NULL OR TRIM(CAST(currency AS VARCHAR)) = '' THEN 1 ELSE 0 END),
              SUM(CASE WHEN country IS NULL OR TRIM(CAST(country AS VARCHAR)) = '' THEN 1 ELSE 0 END),
              SUM(CASE WHEN exchange IS NULL OR TRIM(CAST(exchange AS VARCHAR)) = '' THEN 1 ELSE 0 END),
              SUM(CASE WHEN exchange_full_name IS NULL OR TRIM(CAST(exchange_full_name AS VARCHAR)) = '' THEN 1 ELSE 0 END)
            FROM companies WHERE market = 'US'
            """
        ).fetchone()
        lines.append("### 空值统计\n\n")
        lines.append(
            f"- `currency` 空: {cur[0]}\n"
            f"- `country` 空: {cur[1]}\n"
            f"- `exchange` 空: {cur[2]}\n"
            f"- `exchange_full_name` 空: {cur[3]}\n\n"
        )

        dist = conn.execute(
            """
            SELECT COALESCE(currency, '(null)'), COUNT(*)
            FROM companies WHERE market = 'US'
            GROUP BY 1 ORDER BY 2 DESC LIMIT 15
            """
        ).fetchall()
        lines.append("### currency 分布（前 15）\n\n")
        lines.append(_md_table(["currency", "count"], dist))

        lines.append("\n## 2. fundamentals_annual（US 公司）\n\n")
        fa = conn.execute(
            """
            SELECT COUNT(DISTINCT f.ticker)
            FROM fundamentals_annual f
            JOIN companies c ON c.ticker = f.ticker AND c.market = 'US'
            """
        ).fetchone()
        lines.append(f"- 至少有一行年报的 US ticker 数: **{fa[0]}**\n\n")

        cur = conn.execute(
            """
            SELECT
              SUM(CASE WHEN currency IS NULL OR currency != 'USD' THEN 1 ELSE 0 END),
              SUM(CASE WHEN reporting_currency IS NULL THEN 1 ELSE 0 END),
              SUM(CASE WHEN fx_to_usd IS NULL THEN 1 ELSE 0 END),
              COUNT(*)
            FROM fundamentals_annual f
            JOIN companies c ON c.ticker = f.ticker AND c.market = 'US'
            """
        ).fetchone()
        non_usd, null_rep, null_fx, total = cur
        lines.append(
            f"- 总行数: {total}\n"
            f"- `currency` 非 USD 或 NULL: {non_usd}（ETL 归一后应为 0）\n"
            f"- `reporting_currency` NULL: {null_rep}\n"
            f"- `fx_to_usd` NULL: {null_fx}\n\n"
        )

        big = conn.execute(
            """
            SELECT f.ticker, f.fiscal_year, f.revenue, f.currency, f.reporting_currency
            FROM fundamentals_annual f
            JOIN companies c ON c.ticker = f.ticker AND c.market = 'US'
            WHERE f.revenue > 1e6
            ORDER BY f.revenue DESC
            LIMIT 25
            """
        ).fetchall()
        if big:
            lines.append("### revenue > 1_000_000（百万）异常候选（前 25）\n\n")
            lines.append(_md_table(["ticker", "fiscal_year", "revenue", "currency", "reporting_currency"], big))
        else:
            lines.append("### revenue > 1e6 候选\n\n（无）\n\n")

        lines.append("\n## 3. ohlcv_daily（US）\n\n")
        o1 = conn.execute(
            """
            SELECT COUNT(DISTINCT o.ticker)
            FROM ohlcv_daily o
            JOIN companies c ON c.ticker = o.ticker AND c.market = 'US'
            """
        ).fetchone()
        o2 = conn.execute(
            """
            SELECT
              COUNT(*),
              SUM(CASE WHEN market_cap IS NULL THEN 1 ELSE 0 END),
              SUM(CASE WHEN ema10 IS NULL THEN 1 ELSE 0 END),
              SUM(CASE WHEN ema250 IS NULL THEN 1 ELSE 0 END)
            FROM ohlcv_daily o
            JOIN companies c ON c.ticker = o.ticker AND c.market = 'US'
            """
        ).fetchone()
        lines.append(f"- 有日线的 US ticker 数: **{o1[0]}**\n")
        lines.append(
            f"- 日线总行数: {o2[0]}；`market_cap` NULL: {o2[1]}；`ema10` NULL: {o2[2]}；`ema250` NULL: {o2[3]}\n\n"
        )

        lines.append("\n### 孤儿 ticker（在 ohlcv_daily 但不在 companies）\n\n")
        n_orphan, sample = audit_orphan_ohlcv_tickers(conn)
        lines.append(
            f"- 孤儿数: **{n_orphan}**（>0 时 `get_all_tickers` 仍可能落到 ohlcv 全表 DISTINCT；"
            "需先回填 companies 或清理 ohlcv 才能安全删 fallback）\n"
        )
        if sample:
            lines.append(f"- 样本（前 20）: {', '.join(sample)}\n\n")
        else:
            lines.append("\n")

        lines.append("## 4. etl_us_bulk_state\n\n")
        try:
            st = conn.execute(
                """
                SELECT status, COUNT(*) FROM etl_us_bulk_state GROUP BY status ORDER BY 2 DESC
                """
            ).fetchall()
            lines.append(_md_table(["status", "count"], st))
        except Exception as e:
            lines.append(f"（无表或错误: {e}）\n")

    lines.append("\n---\n\n结论：请重点查看 `currency` 非 USD、`revenue` 极大行、以及 `country`/`exchange` 空值比例。\n")

    text = "".join(lines)
    out = out_path or (_REPO / "reports" / f"us_etl_audit_{datetime.now().strftime('%Y%m%d_%H%M')}.md")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text, encoding="utf-8")
    return text, out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out", type=str, default="", help="Output Markdown path")
    args = p.parse_args()
    op = Path(args.out) if args.out else None
    text, written = run_audit(op)
    print(text[:2500])
    print(f"\nWritten: {written}")


if __name__ == "__main__":
    main()
