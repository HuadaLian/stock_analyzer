"""CN (A-share) market analyzer — akshare + 巨潮资讯."""

import streamlit as st
from .base import MarketAnalyzer
from data_provider import get_cn_data
from downloader import CninfoDownloader
from cn_universe import fetch_cn_universe
from analysis_tracker import get_analyzed_tickers, mark_analyzed, get_next_unanalyzed
import os


@st.cache_data(ttl=600, show_spinner=False)
def _fetch_cn_data(code):
    return get_cn_data(code)


class CNAnalyzer(MarketAnalyzer):
    market = "CN"
    default_currency = "CNY"
    ticker_input_label = "A股代码 (6位)"
    ticker_input_default = ""
    analyze_button_label = "📊 一键分析 (K线 + 下载 + 财务)"
    data_source_desc = "akshare K线 + 巨潮年报"

    def normalize_ticker(self, ticker):
        t = ticker.strip()
        if t.isdigit():
            t = t.zfill(6)   # pad to 6 digits (e.g. "600" → "000600")
        return t

    def fetch_data(self, ticker):
        return _fetch_cn_data(ticker)

    def on_analysis_complete(self, ticker, data):
        try:
            mcap = data.get("market_cap")
            meta = {"market_cap": mcap, "market": "CN"} if mcap else {"market": "CN"}
            try:
                from data_provider import compute_dcf_lines
                price = data.get("last_price")
                fcf_ps = data.get("fcf_per_share_by_year", {})
                if price and fcf_ps:
                    dcf_df = compute_dcf_lines(fcf_ps)
                    if not dcf_df.empty:
                        dcf_14x = dcf_df["dcf_14x"].iloc[-1]
                        dcf_34x = dcf_df["dcf_34x"].iloc[-1]
                        if dcf_14x > 0:
                            meta["last_price"] = price
                            meta["dcf_14x"] = dcf_14x
                            meta["dcf_34x"] = dcf_34x
            except Exception:
                pass
            mark_analyzed(ticker, metadata=meta)
        except Exception:
            mark_analyzed(ticker)
        try:
            from chart_store import save_chart
            save_chart(ticker, self.market, data)
        except Exception:
            pass

    def download_filings_ui(self, ticker):
        """Automatically download A-share annual reports from 巨潮资讯 (no user widgets)."""
        log_container = st.empty()
        log_data = []

        def cn_logger(msg):
            log_data.append(msg)
            log_container.text_area("📥 巨潮下载日志", value="\n".join(log_data), height=150)

        with st.spinner(f"正在检索并下载 {ticker} A股年报 (巨潮资讯)..."):
            try:
                dl = CninfoDownloader()
                count = dl.download_cn_reports(ticker, "年度报告", cn_logger)
                cn_logger(f"🎉 年报下载结束，共处理 {count} 份文件")
            except Exception as e:
                cn_logger(f"⚠️ 年报下载遇到问题 (分析将继续): {e}")

    def run(self):
        """Render tracker at the top, then proceed with the normal analysis flow."""
        try:
            self._render_tracker_ui()
        except Exception:
            pass
        super().run()

    # ── helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _has_saved_fcf(ticker: str) -> bool:
        """Check whether a saved FCF CSV exists for this CN ticker."""
        ticker_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "saved_tables", f"{ticker}_CN",
        )
        if not os.path.isdir(ticker_dir):
            return False
        return any(f.endswith(".csv") for f in os.listdir(ticker_dir))

    def _render_tracker_ui(self):
        st.markdown(
            """
            <style>
            div[data-testid="stExpander"] summary,
            div[role="button"].st-expanderHeader {
                background: transparent !important;
                color: #e0e7ff !important;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
        st.divider()
        st.subheader("📋 A 股分析进度追踪")

        analyzed = get_analyzed_tickers()
        n_analyzed = len(analyzed)

        if "cn_universe" not in st.session_state:
            st.session_state["cn_universe"] = None

        universe = st.session_state.get("cn_universe")

        col_load, col_refresh = st.columns([3, 1])
        with col_refresh:
            force = st.button("🔄 刷新列表", key="cn_univ_refresh", use_container_width=True)

        if universe is None or force:
            log_ph = st.empty()
            def _univ_log(msg):
                log_ph.caption(msg)
            try:
                universe = fetch_cn_universe(force_refresh=force, progress_callback=_univ_log)
                st.session_state["cn_universe"] = universe
                log_ph.empty()
            except Exception as e:
                st.error(f"获取 A 股列表失败: {e}")
                return

        if not universe:
            st.info(
                "A 股列表为空。请在 .env 中设置 TUSHARE_TOKEN，"
                "或先放置 CN_Filings 数据后点击刷新。"
            )
            return

        n_total = len(universe)
        pct = n_analyzed / n_total if n_total > 0 else 0
        with col_load:
            st.progress(pct, text=f"已分析 {n_analyzed:,} / {n_total:,} ({pct:.1%})")

        sc1, sc2, sc3, sc4, sc5 = st.columns(5)
        sc1.metric("总上市公司", f"{n_total:,}")
        sc2.metric("已分析", f"{n_analyzed:,}")
        sc3.metric("未分析", f"{n_total - n_analyzed:,}")
        # FCF data coverage
        n_with_fcf = sum(1 for tk in analyzed if self._has_saved_fcf(tk))
        sc4.metric("含 FCF 数据", f"{n_with_fcf:,}")
        if analyzed:
            last_ticker = max(analyzed, key=lambda t: analyzed[t].get("timestamp", ""))
            last_ts = analyzed[last_ticker].get("timestamp", "?")
            sc5.metric("最近分析", f"{last_ticker} ({last_ts[:10]})")
        else:
            sc5.metric("最近分析", "无")

        # Auto-analyze controls
        auto_c1, auto_c2, auto_c3 = st.columns([2, 2, 1])

        with auto_c1:
            next_ticker = get_next_unanalyzed(universe, analyzed)
            univ_info = universe.get(next_ticker, {}) if next_ticker else {}
            next_label = f"{univ_info.get('name', '')} ({univ_info.get('industry', '')})" if next_ticker else ""
            st.caption(
                f"下一个待分析: **{next_ticker}** {next_label}"
                if next_ticker
                else "🎉 所有股票已分析完毕！"
            )

        with auto_c2:
            batch_size = st.number_input(
                "连续分析数量", min_value=1, max_value=50, value=1,
                key="cn_auto_batch",
                help="点击自动分析后将连续分析指定数量的股票",
            )

        with auto_c3:
            st.markdown("<br>", unsafe_allow_html=True)
            auto_btn = st.button(
                "🚀 自动分析下一个",
                key="cn_auto_analyze",
                use_container_width=True,
                disabled=(next_ticker is None),
            )

        if auto_btn and next_ticker:
            batch = []
            remaining = dict(analyzed)
            for _ in range(int(batch_size)):
                t = get_next_unanalyzed(universe, remaining)
                if t is None:
                    break
                batch.append(t)
                remaining[t] = {"timestamp": "", "status": "queued"}

            # Capture Gemini config and launch background worker
            import background_worker as _bw
            session_id = st.session_state.get("_sid", "default")
            gemini_cfg = {
                "api_key":        st.session_state.get("gemini_api_key", ""),
                "model_name":     st.session_state.get("gemini_model_name", ""),
                "enabled_models": st.session_state.get("enabled_models"),
            }
            _bw.start(f"cn_{session_id}", "CN", batch, gemini_cfg)

        # Background worker polling fragment — runs every 2s, never blocks the UI
        @st.fragment(run_every=2)
        def _cn_worker_fragment():
            import background_worker as _bw
            sid = "cn_" + st.session_state.get("_sid", "default")
            state = _bw.get_state(sid)
            if state is None:
                return

            queue = state["queue"]
            idx   = state["idx"]
            logs  = state["logs"]
            done  = state["status"] == "done"

            if done:
                pct = 1.0
                label_text = f"✅ 批量完成，共 {len(queue)} 只"
            else:
                pct = idx / len(queue) if queue else 0
                current = queue[idx] if idx < len(queue) else "?"
                label_text = f"🤖 后台分析中: **{current}** ({idx + 1}/{len(queue)})"

            st.progress(pct, text=label_text)
            if logs:
                recent = "\n".join(logs[-8:])
                st.text_area("后台日志 (最近8条)", value=recent, height=160,
                             key="cn_bg_log_area", disabled=True)

            if done:
                # Keep result visible for one extra cycle, then clear
                if st.session_state.get("_cn_bg_done_shown"):
                    _bw.clear(sid)
                    st.session_state.pop("_cn_bg_done_shown", None)
                else:
                    st.session_state["_cn_bg_done_shown"] = True

        _cn_worker_fragment()

        st.caption("📋 已分析股票的完整列表与图表查看，请切换到「已分析股票」标签页。")

    def _run_single_auto_analysis(self, ticker):
        """Run a complete analysis for one CN ticker (used by auto-analyze)."""
        from gemini_chat import load_fcf_table

        ticker = self.normalize_ticker(ticker)
        key = self.market.lower()

        # Step 1: Automatically download annual reports
        self.download_filings_ui(ticker)

        # Step 2: Fetch data
        data = None
        with st.spinner(f"正在获取 {ticker} 数据 ({self.data_source_desc})..."):
            try:
                data = self.fetch_data(ticker)
                saved_tbl = load_fcf_table(ticker, self.market)
                if saved_tbl is not None and not saved_tbl.empty:
                    data = dict(data)
                    data["fcf_table"] = saved_tbl
            except Exception as e:
                st.error(f"数据获取出错: {e}")

        # Step 3: AI fill
        tbl_placeholder = None
        if data is not None:
            try:
                tbl_placeholder = self._run_ai_fill(data, ticker)
            except Exception as e:
                st.error(f"AI 填充出错 (分析将继续): {e}")

            # Step 4: Store
            data = self._apply_adjusted_fcf(data)
            st.session_state[f"{key}_chart_data"] = data
            st.session_state[f"{key}_chart_label"] = self.format_label(ticker)
            if tbl_placeholder:
                tbl_placeholder.empty()

            self.on_analysis_complete(ticker, data)

    def render_extra_ui(self, ticker):
        with st.expander("🔧 手动下载年报 (巨潮资讯, 自定义关键词)"):
            cn_c1, cn_c2, cn_c3 = st.columns([2, 2, 1])
            with cn_c1:
                cn_keyword = st.text_input(
                    "报告关键词", key="cn_kw",
                    placeholder="年度报告 / 半年报 / 留空下载全部",
                )
            with cn_c2:
                cn_start_year = st.text_input(
                    "起始年份 (可选)", key="cn_start_year",
                    placeholder="如 2018，留空不限",
                )
            with cn_c3:
                st.markdown("<br>", unsafe_allow_html=True)
                cn_dl_btn = st.button("🔍 下载", key="cn_dl", use_container_width=True)

            if cn_dl_btn:
                if not ticker:
                    st.warning("请输入 A 股代码。")
                else:
                    log_container_cn = st.empty()
                    log_data_cn = []

                    def cn_logger(msg):
                        log_data_cn.append(msg)
                        log_container_cn.text_area("实时日志", value="\n".join(log_data_cn), height=250)

                    with st.spinner("正在检索巨潮资讯，请稍候..."):
                        try:
                            dl_cn = CninfoDownloader()
                            count = dl_cn.download_cn_reports(
                                ticker,
                                cn_keyword.strip() if cn_keyword else "",
                                cn_logger,
                            )
                            cn_logger(f"🎉 任务结束! 总计处理 {count} 份文件。")
                            st.success("A股报告下载完毕！")
                        except Exception as e:
                            cn_logger(f"❌ 发生错误: {str(e)}")
                            st.error("下载中断。")
