"""Streamlit tab: show cached DB quality report (fed by ``reports.run_db_quality_audit``)."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from db.data_quality_spec import QUALITY_DIMENSIONS

_REPO = Path(__file__).resolve().parents[1]
CACHE_DIR = _REPO / "reports" / "db_quality_cache"
REPORT_PATH = CACHE_DIR / "report.json"
STATE_PATH = CACHE_DIR / "state.json"
GLOBAL_AUDIT_PATH = CACHE_DIR / "global_audit.md"


def _read_json(path: Path) -> dict | None:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _pie_from_pairs(rows: list[dict], label_key: str, title: str) -> go.Figure | None:
    if not rows:
        return None
    labels = [str(r.get(label_key) or "") for r in rows]
    values = [int(r.get("count") or 0) for r in rows]
    if sum(values) <= 0:
        return None
    fig = go.Figure(
        data=[
            go.Pie(
                labels=labels,
                values=values,
                hole=0.32,
                textinfo="label+percent",
                textposition="inside",
                hovertemplate="%{label}<br>家数: %{value:,}<br>占比: %{percent}<extra></extra>",
            )
        ]
    )
    fig.update_layout(
        title=dict(text=title, font=dict(size=15, color="#e0e7ff"), x=0.5, xanchor="center"),
        paper_bgcolor="rgba(15,22,41,0.6)",
        plot_bgcolor="rgba(15,22,41,0.3)",
        font=dict(color="#cbd5e1"),
        margin=dict(t=48, b=24, l=24, r=24),
        height=360,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.22, x=0.5, xanchor="center"),
    )
    return fig


def render_db_quality_tab(st) -> None:
    """Render the database quality tab (read-only cache; no heavy DB scans here)."""

    def _body() -> None:
        report = _read_json(REPORT_PATH)
        state = _read_json(STATE_PATH)

        st.subheader("数据库质量检测")
        st.caption(
            "本页只读取 `reports/db_quality_cache/` 下的缓存结果，不在 UI 内跑全库扫描。"
            " 下方区域约每 20 秒自动重读磁盘缓存；后台脚本写入后无需整页刷新即可更新进度。"
        )

        st.markdown(
            "**一键启动（与 app.py 同级）：**\n```bash\npython launch_app.py\n```\n"
            "**仅后台审查：**\n```bash\npython -m reports.run_db_quality_audit\n```\n"
            "- **断点续跑**：同一轮审查在 **24 小时内** 未跑完时，再次执行同一命令会从上次 checkpoint 继续。\n"
            "- **强制全量重跑**：`python -m reports.run_db_quality_audit --force`\n"
            "- **24 小时内已完整跑完**：再次执行会提示跳过（需 `--force` 才重跑）。"
        )

        with st.expander("质量维度与检测形式（与 `db/data_quality_spec.py` 一致）", expanded=False):
            rows = [
                {
                    "代码": d.code,
                    "维度": d.title_zh,
                    "层级": d.layer_zh,
                    "检测要点": d.detection_zh,
                    "实现位置": d.implements,
                }
                for d in QUALITY_DIMENSIONS
            ]
            st.dataframe(rows, use_container_width=True, hide_index=True)

        if state:
            tot = int(state.get("total") or 0)
            cur = int(state.get("cursor") or 0)
            pct = min(100.0, 100.0 * cur / tot) if tot > 0 else 0.0
            st.progress(pct / 100.0, text=f"审查进度：{cur} / {tot} ticker（checkpoint）")
            st.caption(
                f"run_started: {state.get('run_started_at', '—')} · "
                f"last_checkpoint: {state.get('last_checkpoint_at', '—')} · "
                f"complete: {state.get('complete', False)}"
            )

        if not report:
            st.warning(
                "尚未找到 `report.json`。请先在项目根目录运行：\n\n"
                "`python -m reports.run_db_quality_audit`\n\n"
                "全量扫描可能较久，可边跑边在本页等待自动刷新。"
            )
            return

        agg = report.get("aggregates") or {}
        total = int(report.get("total_tickers") or 0)
        proc = int(agg.get("processed") or 0)

        st.markdown("### 汇总（D1 就绪度）")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("已处理 ticker", f"{proc}" + (f" / {total}" if total else ""))
        m2.metric("D1 全就绪 (EMA+DCF+FMP)", agg.get("d1_all_ready", "—"))
        m3.metric("缺 company 行", agg.get("company_missing", "—"))
        m4.metric("报告生成", report.get("generated_at", "—"))

        c_ema, c_dcf, c_fmp = st.columns(3)
        c_ema.metric("EMA 就绪（日 K 衍生）", agg.get("d1_ema_ready", "—"))
        c_dcf.metric("DCF 历史就绪", agg.get("d1_dcf_ready", "—"))
        c_fmp.metric("FMP DCF 就绪", agg.get("d1_fmp_dcf_ready", "—"))

        portrait = report.get("company_portrait")
        if portrait:
            st.markdown("---")
            st.markdown("### 企业库分布与基本面完整度（全 `companies` 口径）")
            st.caption(
                "由审查脚本对数据库即时聚合写入 `report.json` → `company_portrait`；"
                "与上方「FMP 普通股 universe」逐 ticker 扫描互补。"
            )
            if portrait.get("error"):
                st.error(f"企业全景统计失败：{portrait['error']}")
            else:
                pc1, pc2 = st.columns(2, gap="medium")
                fig_m = _pie_from_pairs(portrait.get("by_market") or [], "market", "按市场 market")
                fig_c = _pie_from_pairs(portrait.get("by_country_chart") or [], "country", "按国家/地区 country（长尾并入「其他」）")
                with pc1:
                    if fig_m:
                        st.plotly_chart(fig_m, width="stretch")
                    else:
                        st.caption("暂无 market 分布数据")
                with pc2:
                    if fig_c:
                        st.plotly_chart(fig_c, width="stretch")
                    else:
                        st.caption("暂无 country 分布数据")

                fa = portrait.get("fundamentals_annual") or {}
                st.markdown("#### 年报 `fundamentals_annual`（收入 / FCF）")
                f1, f2, f3, f4 = st.columns(4)
                f1.metric("表总行数", f"{fa.get('rows_total', 0):,}")
                f2.metric("行含 revenue", f"{fa.get('rows_with_revenue', 0):,}")
                f3.metric("行含 FCF", f"{fa.get('rows_with_fcf', 0):,}")
                f4.metric("同行 revenue+FCF", f"{fa.get('rows_with_revenue_and_fcf', 0):,}")
                g1, g2, g3, g4 = st.columns(4)
                g1.metric("公司有任意年报", f"{fa.get('distinct_companies_any_row', 0):,}", f"{fa.get('pct_of_companies_with_any_annual', 0)}%")
                g2.metric("公司有 revenue", f"{fa.get('distinct_companies_with_revenue_any_year', 0):,}", f"{fa.get('pct_of_companies_with_revenue', 0)}%")
                g3.metric("公司有 FCF", f"{fa.get('distinct_companies_with_fcf_any_year', 0):,}", f"{fa.get('pct_of_companies_with_fcf', 0)}%")
                g4.metric("公司同年 revenue+FCF", f"{fa.get('distinct_companies_with_revenue_and_fcf_same_year', 0):,}", f"{fa.get('pct_of_companies_with_revenue_and_fcf', 0)}%")

                comp_rows = max(int(portrait.get("companies_total") or 0), 1)
                bar_df = pd.DataFrame(
                    {
                        "指标": [
                            "至少一行年报",
                            "至少一年有 revenue",
                            "至少一年有 FCF",
                            "至少一年同年 revenue+FCF",
                        ],
                        "公司占比 %": [
                            float(fa.get("pct_of_companies_with_any_annual") or 0),
                            float(fa.get("pct_of_companies_with_revenue") or 0),
                            float(fa.get("pct_of_companies_with_fcf") or 0),
                            float(fa.get("pct_of_companies_with_revenue_and_fcf") or 0),
                        ],
                    }
                )
                fig_bar = go.Figure(
                    data=[
                        go.Bar(
                            x=bar_df["指标"],
                            y=bar_df["公司占比 %"],
                            marker_color=["#6366f1", "#22c55e", "#f59e0b", "#e879f9"],
                            text=[f"{v:.1f}%" for v in bar_df["公司占比 %"]],
                            textposition="outside",
                        )
                    ]
                )
                fig_bar.update_layout(
                    title="占全库 companies 比例（基本面覆盖）",
                    paper_bgcolor="rgba(15,22,41,0.6)",
                    plot_bgcolor="rgba(15,22,41,0.3)",
                    font=dict(color="#cbd5e1"),
                    yaxis=dict(title="占比（%）", range=[0, max(105.0, float(bar_df["公司占比 %"].max()) * 1.15)]),
                    height=340,
                    margin=dict(t=56, b=80, l=48, r=24),
                )
                st.plotly_chart(fig_bar, width="stretch")

                hm = portrait.get("high_mcap") or {}
                th = hm.get("threshold_market_cap_millions", 0)
                st.markdown(f"#### 高市值基本面缺口（最新市值 ≥ **{th:,.0f}** 百万）")
                h1, h2, h3 = st.columns(3)
                h1.metric("达阈值公司数", f"{hm.get('count_companies_at_or_above_threshold', 0):,}")
                h2.metric("其中年报 revenue+FCF 齐全", f"{hm.get('count_with_annual_revenue_and_fcf', 0):,}")
                h3.metric("达阈值集合内缺口径占比", f"{hm.get('pct_above_threshold_missing_rev_or_fcf', 0)}%")
                miss = hm.get("sample_missing_rev_fcf") or []
                if miss:
                    st.caption("以下在达阈值下仍无「同年 revenue+FCF」年报行（按市值降序，可优先补数）")
                    st.dataframe(pd.DataFrame(miss), use_container_width=True, hide_index=True)
        elif int(report.get("version") or 0) < 3:
            st.info("当前 `report.json` 为旧版（无 `company_portrait`）。请运行 `python -m reports.run_db_quality_audit --force` 生成企业分布与基本面统计。")

        if report.get("complete"):
            st.success("本轮审查已完成。")
            if report.get("finished_at"):
                st.caption(f"finished_at: {report['finished_at']}")
        else:
            st.info("审查仍在进行中；后台脚本在每个 checkpoint 会更新缓存文件。")

        st.markdown("---")
        st.markdown("### 全局数据审计（Markdown 摘要）")
        excerpt = (report.get("us_audit_excerpt") or "").strip()
        if excerpt:
            st.markdown(excerpt)
        else:
            st.caption("暂无摘要；完整内容见本地文件：" + str(GLOBAL_AUDIT_PATH))
        if GLOBAL_AUDIT_PATH.is_file():
            with st.expander("查看完整 global_audit.md（本地文件，截断显示）"):
                st.code(GLOBAL_AUDIT_PATH.read_text(encoding="utf-8")[:120_000], language="markdown")

        sample = report.get("not_ready_sample") or []
        if sample:
            st.markdown("### 未全就绪样本（最多 200 条）")
            st.dataframe(sample, use_container_width=True, hide_index=True)

    frag = getattr(st, "fragment", None)
    if callable(frag):
        frag(run_every=20)(_body)()
    else:
        _body()
