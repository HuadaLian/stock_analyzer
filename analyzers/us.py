"""US market analyzer — SEC filings + yfinance + XBRL + analysis tracker."""

import streamlit as st
from .base import MarketAnalyzer
from data_provider import get_us_data
from downloader import SmartSECDownloader
from us_universe import fetch_us_universe
from analysis_tracker import get_analyzed_tickers, mark_analyzed, get_next_unanalyzed


@st.cache_data(ttl=600, show_spinner=False)
def _fetch_us_data(ticker):
    return get_us_data(ticker)


class USAnalyzer(MarketAnalyzer):
    market = "US"
    default_currency = "USD"
    ticker_input_label = "美股代码 (Ticker)"
    ticker_input_default = "AAPL"
    analyze_button_label = "📊 一键分析 (K线 + 下载 + 财务)"
    data_source_desc = "yfinance K线 + SEC XBRL"

    def normalize_ticker(self, ticker):
        return ticker.strip().upper()

    def fetch_data(self, ticker):
        return _fetch_us_data(ticker)

    def on_analysis_complete(self, ticker, data):
        try:
            mcap = data.get("market_cap")
            meta = {"market_cap": mcap, "market": "US"} if mcap else {"market": "US"}
            mark_analyzed(ticker, metadata=meta)
        except Exception:
            mark_analyzed(ticker)
        try:
            from chart_store import save_chart
            save_chart(ticker, self.market, data)
        except Exception:
            pass

    def download_filings_ui(self, ticker):
        log_container = st.empty()
        log_data = []

        def sec_logger(msg):
            log_data.append(msg)
            log_container.text_area("📥 SEC 下载日志", value="\n".join(log_data), height=150)

        with st.spinner(f"正在索引并下载 {ticker} SEC 报告..."):
            try:
                dl = SmartSECDownloader(email="lianhdff@gmail.com")
                dl.smart_download_us(ticker, sec_logger)
            except Exception as e:
                sec_logger(f"⚠️ SEC 报告下载遇到问题 (分析将继续): {e}")

    def render_extra_ui(self, ticker):
        # Manual download expander
        with st.expander("🔧 手动下载 SEC 报告 (自定义报告类型)"):
            dl_c1b, dl_c2b, dl_c3b = st.columns([2, 2, 1])
            with dl_c1b:
                sec_url_m = st.text_input("SEC URL", key="us_sec_url_m")
            with dl_c2b:
                us_form_kw = st.text_input("报告关键词 (如 10-K, 20-F)", key="us_form_kw",
                                            placeholder="留空下载全部类型")
            with dl_c3b:
                us_dl_btn = st.button("下载", key="us_dl", use_container_width=True)

            if us_dl_btn:
                if not ticker:
                    st.warning("请输入有效的 Ticker。")
                else:
                    ticker_up = self.normalize_ticker(ticker)
                    log_container2 = st.empty()
                    log_data2 = []

                    def sec_logger2(msg):
                        log_data2.append(msg)
                        log_container2.text_area("实时日志", value="\n".join(log_data2), height=250)

                    with st.spinner("任务执行中..."):
                        try:
                            dl = SmartSECDownloader(email="lianhdff@gmail.com")
                            cik = dl.get_cik(ticker_up, sec_url_m)
                            sec_logger2(f"✅ 锁定目标 CIK: {cik}")
                            form_filter = us_form_kw.strip() if us_form_kw else None
                            count = dl.download_all(cik, ticker_up, sec_logger2,
                                                    form_filter=form_filter)
                            sec_logger2(f"🎉 任务结束! 总计处理 {count} 份文件。")
                            st.success("SEC 报告下载完毕！")
                        except Exception as e:
                            sec_logger2(f"❌ 发生错误: {str(e)}")
                            st.error("下载中断。")

        # Note: tracker UI is rendered at the top via run();
        # keep render_extra_ui focused on downloads and extras.

    @staticmethod
    def _has_saved_fcf(ticker: str) -> bool:
        """Check whether a saved FCF CSV exists for this US ticker."""
        import os
        ticker_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "saved_tables", f"{ticker}_US",
        )
        if not os.path.isdir(ticker_dir):
            return False
        return any(f.endswith(".csv") for f in os.listdir(ticker_dir))

    # ── Analysis Tracker UI ──────────────────────────────────────────

    def _render_tracker_ui(self):
        """Render the stock-scanning / analysis-progress tracker."""
        # Fix expander header background when expanded (keep dark theme)
        st.markdown(
            """
            <style>
            /* Streamlit expander header - keep dark bg when expanded */
            .st-expander > .st-expanderHeader, .st-expanderHeader {
                background: transparent !important;
                color: #e0e7ff !important;
            }
            /* Additional selectors for different streamlit versions */
            div[data-testid="stExpander"] summary, div[role="button"].st-expanderHeader {
                background: transparent !important;
                color: #e0e7ff !important;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
        st.divider()
        st.subheader("📋 美股分析进度追踪")

        # Load universe + analyzed state
        analyzed = get_analyzed_tickers()
        n_analyzed = len(analyzed)

        # Universe loading (cached in session state to avoid re-fetching on rerun)
        if "us_universe" not in st.session_state:
            st.session_state["us_universe"] = None

        universe = st.session_state.get("us_universe")

        col_load, col_refresh = st.columns([3, 1])
        with col_refresh:
            force = st.button("🔄 刷新列表", key="us_univ_refresh", use_container_width=True)

        if universe is None or force:
            log_ph = st.empty()
            def _univ_log(msg):
                log_ph.caption(msg)
            try:
                universe = fetch_us_universe(
                    force_refresh=force,
                    progress_callback=_univ_log,
                )
                st.session_state["us_universe"] = universe
                log_ph.empty()
            except Exception as e:
                st.error(f"获取美股列表失败: {e}")
                return

        if not universe:
            st.info("美股列表为空，请点击刷新。")
            return

        n_total = len(universe)

        # Progress bar
        pct = n_analyzed / n_total if n_total > 0 else 0
        with col_load:
            st.progress(pct, text=f"已分析 {n_analyzed:,} / {n_total:,} ({pct:.1%})")

        # Stats row
        sc1, sc2, sc3, sc4, sc5 = st.columns(5)
        sc1.metric("总上市公司", f"{n_total:,}")
        sc2.metric("已分析", f"{n_analyzed:,}")
        sc3.metric("未分析", f"{n_total - n_analyzed:,}")
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
            st.caption(
                f"下一个待分析: **{next_ticker}** "
                f"({universe.get(next_ticker, {}).get('name', '')})"
                if next_ticker
                else "🎉 所有股票已分析完毕！"
            )

        with auto_c2:
            batch_size = st.number_input(
                "连续分析数量", min_value=1, max_value=50, value=1,
                key="us_auto_batch",
                help="点击自动分析后将连续分析指定数量的股票",
            )

        with auto_c3:
            st.markdown("<br>", unsafe_allow_html=True)
            auto_btn = st.button(
                "🚀 自动分析下一个",
                key="us_auto_analyze",
                use_container_width=True,
                disabled=(next_ticker is None),
            )

        if auto_btn and next_ticker:
            # Build a batch queue of tickers to analyze
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
            _bw.start(f"us_{session_id}", "US", batch, gemini_cfg)

        # Background worker polling fragment — runs every 2s, never blocks the UI
        @st.fragment(run_every=2)
        def _us_worker_fragment():
            import background_worker as _bw
            sid = "us_" + st.session_state.get("_sid", "default")
            state = _bw.get_state(sid)
            if state is None:
                return

            queue = state["queue"]
            idx   = state["idx"]
            logs  = state["logs"]
            done  = state["status"] == "done"

            st.divider()
            if done:
                pct = 1.0
                label_text = f"✅ 批量完成，共 {len(queue)} 只"
                st.success(label_text)
            else:
                pct = idx / len(queue) if queue else 0
                current = queue[idx] if idx < len(queue) else "?"
                label_text = f"🤖 后台分析中: **{current}** ({idx + 1}/{len(queue)})"
                st.info(label_text)

            st.progress(pct)

            n_show = min(len(logs), 20)
            st.caption(f"📋 后台日志（最近 {n_show}/{len(logs)} 条）")
            log_text = "\n".join(logs[-20:]) if logs else "（等待日志...）"
            st.code(log_text, language=None)

            # Show latest AI-filled FCF table if available
            fcf_tbl = state.get("last_fcf_table")
            fcf_ticker = state.get("last_fcf_ticker")
            fcf_market = state.get("last_fcf_market", "US")
            if fcf_tbl is not None and fcf_ticker:
                currency = {"US": "USD", "CN": "CNY", "HK": "HKD"}.get(fcf_market, "USD")
                st.caption(f"📊 最新 AI 填表结果 — {fcf_ticker}")
                st.markdown(
                    self._build_fcf_table_html(fcf_tbl, currency),
                    unsafe_allow_html=True,
                )

            if done:
                # Keep result visible for one extra cycle, then clear
                if st.session_state.get("_us_bg_done_shown"):
                    _bw.clear(sid)
                    st.session_state.pop("_us_bg_done_shown", None)
                else:
                    st.session_state["_us_bg_done_shown"] = True

        _us_worker_fragment()

        st.caption("📋 已分析股票的完整列表与图表查看，请切换到「已分析股票」标签页。")

    def _run_single_auto_analysis(self, ticker):
        """Run a complete analysis for one ticker (used by auto-analyze)."""
        from gemini_chat import load_fcf_table

        ticker = self.normalize_ticker(ticker)
        key = self.market.lower()

        # Step 1: Download
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

    # ── Override run to show tracker at top ─────────────────────────
    def run(self):
        """Render tracker at the top, then proceed with the normal flow."""
        # Render tracker first so it stays visible while lower controls run
        try:
            self._render_tracker_ui()
        except Exception:
            # Fall back to normal flow if tracker fails
            pass

        # Continue with the base implementation
        super().run()
