"""全库企业分布与基本面完整度统计（供质量审查脚本写入 report.json / Markdown）。

口径以 ``companies`` 为主表；市值取 ``ohlcv_daily`` 每 ticker 最新一条非空 ``market_cap``
（单位与 ETL 一致：百万）。高市值缺口列表按市值降序。
"""

from __future__ import annotations

from typing import Any


def _scalar(conn, sql: str, params: list | None = None) -> Any:
    row = conn.execute(sql, params or []).fetchone()
    return row[0] if row else None


def compute_company_portrait(
    conn,
    *,
    high_mcap_millions: float = 5000.0,
    country_chart_top_n: int = 14,
) -> dict[str, Any]:
    """单次只读连接内跑完；返回可 JSON 序列化的 dict。"""
    companies_total = int(_scalar(conn, "SELECT COUNT(*) FROM companies") or 0)

    by_market_rows = conn.execute(
        """
        SELECT COALESCE(NULLIF(TRIM(market), ''), '(未知)') AS mk, COUNT(*) AS n
        FROM companies
        GROUP BY 1
        ORDER BY n DESC
        """
    ).fetchall()
    by_market = [{"market": str(r[0]), "count": int(r[1])} for r in by_market_rows]

    country_rows = conn.execute(
        """
        SELECT COALESCE(NULLIF(TRIM(country), ''), '(未知国家/地区)') AS ct, COUNT(*) AS n
        FROM companies
        GROUP BY 1
        ORDER BY n DESC
        """
    ).fetchall()
    country_list = [{"country": str(r[0]), "count": int(r[1])} for r in country_rows]
    top_ct = country_list[:country_chart_top_n]
    other_n = sum(r["count"] for r in country_list[country_chart_top_n:])
    by_country_chart = list(top_ct)
    if other_n > 0:
        by_country_chart.append({"country": "其他（长尾）", "count": other_n})

    # --- fundamentals (annual): existence / revenue / FCF ---
    dist_any_fa = int(
        _scalar(
            conn,
            """
            SELECT COUNT(DISTINCT c.ticker)
            FROM companies c
            WHERE EXISTS (SELECT 1 FROM fundamentals_annual f WHERE f.ticker = c.ticker)
            """,
        )
        or 0
    )
    dist_rev_any = int(
        _scalar(
            conn,
            """
            SELECT COUNT(DISTINCT c.ticker)
            FROM companies c
            WHERE EXISTS (
              SELECT 1 FROM fundamentals_annual f
              WHERE f.ticker = c.ticker AND f.revenue IS NOT NULL
            )
            """,
        )
        or 0
    )
    dist_fcf_any = int(
        _scalar(
            conn,
            """
            SELECT COUNT(DISTINCT c.ticker)
            FROM companies c
            WHERE EXISTS (
              SELECT 1 FROM fundamentals_annual f
              WHERE f.ticker = c.ticker
                AND (f.fcf IS NOT NULL OR f.fcf_per_share IS NOT NULL)
            )
            """,
        )
        or 0
    )
    dist_rev_fcf_same_year = int(
        _scalar(
            conn,
            """
            SELECT COUNT(DISTINCT c.ticker)
            FROM companies c
            WHERE EXISTS (
              SELECT 1 FROM fundamentals_annual f
              WHERE f.ticker = c.ticker
                AND f.revenue IS NOT NULL
                AND (f.fcf IS NOT NULL OR f.fcf_per_share IS NOT NULL)
            )
            """,
        )
        or 0
    )

    fa_rows_total = int(_scalar(conn, "SELECT COUNT(*) FROM fundamentals_annual") or 0)
    fa_rows_with_rev = int(
        _scalar(
            conn,
            "SELECT COUNT(*) FROM fundamentals_annual WHERE revenue IS NOT NULL",
        )
        or 0
    )
    fa_rows_with_fcf = int(
        _scalar(
            conn,
            """
            SELECT COUNT(*) FROM fundamentals_annual
            WHERE fcf IS NOT NULL OR fcf_per_share IS NOT NULL
            """,
        )
        or 0
    )
    fa_rows_rev_fcf = int(
        _scalar(
            conn,
            """
            SELECT COUNT(*) FROM fundamentals_annual
            WHERE revenue IS NOT NULL
              AND (fcf IS NOT NULL OR fcf_per_share IS NOT NULL)
            """,
        )
        or 0
    )

    denom = max(companies_total, 1)

    def _pct(n: int) -> float:
        return round(100.0 * n / denom, 2)

    # --- latest market cap (one window scan); stats + gap sample in two statements ---
    _LM_CTE = """
        WITH latest AS (
          SELECT ticker, market_cap,
            ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
          FROM ohlcv_daily
          WHERE market_cap IS NOT NULL AND market_cap > 0
        ),
        lm AS (SELECT ticker, market_cap FROM latest WHERE rn = 1),
        joined AS (
          SELECT c.ticker, c.market, c.name, lm.market_cap,
            EXISTS (
              SELECT 1 FROM fundamentals_annual f
              WHERE f.ticker = c.ticker
                AND f.revenue IS NOT NULL
                AND (f.fcf IS NOT NULL OR f.fcf_per_share IS NOT NULL)
            ) AS has_rev_fcf
          FROM companies c
          INNER JOIN lm ON lm.ticker = c.ticker
          WHERE lm.market_cap >= ?
        )
    """
    row_h = conn.execute(
        _LM_CTE
        + """
        SELECT COUNT(*) AS n,
               SUM(CASE WHEN has_rev_fcf THEN 1 ELSE 0 END) AS ok
        FROM joined
        """,
        [high_mcap_millions],
    ).fetchone()
    high_n = int(row_h[0] or 0) if row_h else 0
    high_with_rev_fcf = int(row_h[1] or 0) if row_h else 0

    gap_rows = conn.execute(
        _LM_CTE
        + """
        SELECT ticker, market, name,
          ROUND(CAST(market_cap AS DOUBLE), 1) AS mcap_millions
        FROM joined
        WHERE NOT has_rev_fcf
        ORDER BY market_cap DESC
        LIMIT 50
        """,
        [high_mcap_millions],
    ).fetchall()
    high_mcap_missing_rev_fcf = [
        {
            "ticker": str(r[0]),
            "market": str(r[1]),
            "name": str(r[2] or ""),
            "mcap_millions": float(r[3]) if r[3] is not None else None,
        }
        for r in gap_rows
    ]

    high_denom = max(high_n, 1)
    high_missing_pct = round(100.0 * (high_n - high_with_rev_fcf) / high_denom, 2)

    return {
        "companies_total": companies_total,
        "by_market": by_market,
        "by_country_full": country_list[:80],
        "by_country_chart": by_country_chart,
        "fundamentals_annual": {
            "rows_total": fa_rows_total,
            "rows_with_revenue": fa_rows_with_rev,
            "rows_with_fcf": fa_rows_with_fcf,
            "rows_with_revenue_and_fcf": fa_rows_rev_fcf,
            "distinct_companies_any_row": dist_any_fa,
            "distinct_companies_with_revenue_any_year": dist_rev_any,
            "distinct_companies_with_fcf_any_year": dist_fcf_any,
            "distinct_companies_with_revenue_and_fcf_same_year": dist_rev_fcf_same_year,
            "pct_of_companies_with_any_annual": _pct(dist_any_fa),
            "pct_of_companies_with_revenue": _pct(dist_rev_any),
            "pct_of_companies_with_fcf": _pct(dist_fcf_any),
            "pct_of_companies_with_revenue_and_fcf": _pct(dist_rev_fcf_same_year),
        },
        "high_mcap": {
            "threshold_market_cap_millions": float(high_mcap_millions),
            "count_companies_at_or_above_threshold": high_n,
            "count_with_annual_revenue_and_fcf": high_with_rev_fcf,
            "pct_above_threshold_missing_rev_or_fcf": high_missing_pct,
            "sample_missing_rev_fcf": high_mcap_missing_rev_fcf,
        },
    }


def company_portrait_markdown(portrait: dict[str, Any]) -> str:
    """追加到 global_audit.md 的全库段落。"""
    if not portrait:
        return ""
    if portrait.get("error"):
        return f"\n\n> 企业库全景统计失败：**{portrait['error']}**\n\n"
    lines: list[str] = []
    lines.append("\n## 企业库全景与基本面完整度（全市场）\n\n")
    lines.append(f"- **companies 总行数**: {portrait.get('companies_total', 0):,}\n\n")

    lines.append("### 按市场（market）\n\n")
    for r in portrait.get("by_market") or []:
        lines.append(f"- `{r.get('market')}`: **{r.get('count', 0):,}**\n")
    lines.append("\n")

    lines.append("### 按国家/地区（country，issuer / listing）\n\n")
    lines.append("> 饼图用「前若干 + 其他」聚合；完整前 80 条见 `report.json` → `company_portrait.by_country_full`。\n\n")
    for r in (portrait.get("by_country_chart") or [])[:20]:
        lines.append(f"- {r.get('country')}: **{r.get('count', 0):,}**\n")
    lines.append("\n")

    fa = portrait.get("fundamentals_annual") or {}
    lines.append("### 年报基本面 `fundamentals_annual`（更关注基本面，非日 K）\n\n")
    lines.append(
        f"- 表总行数: **{fa.get('rows_total', 0):,}**；其中 `revenue` 非空行: **{fa.get('rows_with_revenue', 0):,}**；"
        f"`fcf` 或 `fcf_per_share` 非空行: **{fa.get('rows_with_fcf', 0):,}**；"
        f"同一财年行内 **收入+FCF 均有** 的行数: **{fa.get('rows_with_revenue_and_fcf', 0):,}**\n"
    )
    lines.append(
        f"- 在 **companies** 全量口径下，至少有一行年报的 **公司数**: **{fa.get('distinct_companies_any_row', 0):,}** "
        f"（{fa.get('pct_of_companies_with_any_annual', 0)}%）\n"
    )
    lines.append(
        f"- 至少一年 **有 revenue** 的公司数: **{fa.get('distinct_companies_with_revenue_any_year', 0):,}** "
        f"（{fa.get('pct_of_companies_with_revenue', 0)}%）\n"
    )
    lines.append(
        f"- 至少一年 **有 FCF**（fcf 或 fcf_per_share）的公司数: **{fa.get('distinct_companies_with_fcf_any_year', 0):,}** "
        f"（{fa.get('pct_of_companies_with_fcf', 0)}%）\n"
    )
    lines.append(
        f"- 至少一年 **同一行 revenue+FCF 均填** 的公司数: **{fa.get('distinct_companies_with_revenue_and_fcf_same_year', 0):,}** "
        f"（{fa.get('pct_of_companies_with_revenue_and_fcf', 0)}%）\n\n"
    )

    hm = portrait.get("high_mcap") or {}
    th = hm.get("threshold_market_cap_millions", 0)
    lines.append(f"### 高市值公司（最新市值 ≥ **{th:,.0f}** 百万）的基本面缺口\n\n")
    lines.append(
        f"- 达阈值公司数: **{hm.get('count_companies_at_or_above_threshold', 0):,}**；"
        f"其中已有「年报 revenue+FCF」的: **{hm.get('count_with_annual_revenue_and_fcf', 0):,}**；"
        f"缺口的占比（在达阈值集合内）: **{hm.get('pct_above_threshold_missing_rev_or_fcf', 0)}%**\n\n"
    )
    sample = hm.get("sample_missing_rev_fcf") or []
    if sample:
        lines.append("**缺口样本（按市值降序，最多 50）:**\n\n")
        lines.append("| ticker | market | mcap(百万) | name |\n| --- | --- | --- | --- |\n")
        for s in sample[:50]:
            nm = (s.get("name") or "").replace("|", " ")
            lines.append(
                f"| {s.get('ticker')} | {s.get('market')} | {s.get('mcap_millions')} | {nm} |\n"
            )
        lines.append("\n")

    lines.append(
        "_说明：日 K 覆盖未在此展开；全库 D1 就绪度仍以 checkpoint 的 per-ticker 扫描为准。_\n"
    )
    return "".join(lines)
