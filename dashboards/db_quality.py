"""Streamlit tab: show cached DB quality report (fed by ``reports.run_db_quality_audit``)."""

from __future__ import annotations

import json
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
CACHE_DIR = _REPO / "reports" / "db_quality_cache"
REPORT_PATH = CACHE_DIR / "report.json"
STATE_PATH = CACHE_DIR / "state.json"
US_AUDIT_PATH = CACHE_DIR / "us_audit.md"


def _read_json(path: Path) -> dict | None:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


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
        c_ema.metric("EMA 就绪", agg.get("d1_ema_ready", "—"))
        c_dcf.metric("DCF 历史就绪", agg.get("d1_dcf_ready", "—"))
        c_fmp.metric("FMP DCF 就绪", agg.get("d1_fmp_dcf_ready", "—"))

        if report.get("complete"):
            st.success("本轮审查已完成。")
            if report.get("finished_at"):
                st.caption(f"finished_at: {report['finished_at']}")
        else:
            st.info("审查仍在进行中；后台脚本在每个 checkpoint 会更新缓存文件。")

        st.markdown("---")
        st.markdown("### US 数据审计（Markdown 摘要）")
        excerpt = (report.get("us_audit_excerpt") or "").strip()
        if excerpt:
            st.markdown(excerpt)
        else:
            st.caption("暂无摘要；完整内容见本地文件：" + str(US_AUDIT_PATH))
        if US_AUDIT_PATH.is_file():
            with st.expander("查看完整 us_audit.md（本地文件，截断显示）"):
                st.code(US_AUDIT_PATH.read_text(encoding="utf-8")[:120_000], language="markdown")

        sample = report.get("not_ready_sample") or []
        if sample:
            st.markdown("### 未全就绪样本（最多 200 条）")
            st.dataframe(sample, use_container_width=True, hide_index=True)

    frag = getattr(st, "fragment", None)
    if callable(frag):
        frag(run_every=20)(_body)()
    else:
        _body()
