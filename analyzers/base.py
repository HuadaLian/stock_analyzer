"""Base market analyzer with shared rendering & AI fill logic."""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import re
import time
from data_provider import compute_dcf_lines
from gemini_chat import (
    fill_fcf_table_with_llm, save_fcf_table, load_fcf_table,
    recompute_fcf_per_share,
)


class MarketAnalyzer:
    """Base class for market-specific stock analysis.

    Subclasses must set class attributes and implement ``fetch_data``.
    """

    # ── Override in subclass ─────────────────────────────────────────
    market: str = ""              # "US", "CN", "HK"
    default_currency: str = "USD"
    ticker_input_label: str = "代码"
    ticker_input_default: str = ""
    analyze_button_label: str = "📊 一键分析"
    data_source_desc: str = ""    # shown in the spinner

    # ── Static helpers ───────────────────────────────────────────────

    @staticmethod
    def fmt_val(val, currency="USD"):
        """Format a number as compact string with currency symbol."""
        symbols = {"USD": "$", "CNY": "¥", "HKD": "HK$"}
        sym = symbols.get(currency, f"{currency} ")
        if val is None or val == 0:
            return "N/A"
        abs_v = abs(val)
        sign = "-" if val < 0 else ""
        if abs_v >= 1e12:
            return f"{sign}{sym}{abs_v/1e12:.2f}T"
        if abs_v >= 1e9:
            return f"{sign}{sym}{abs_v/1e9:.2f}B"
        if abs_v >= 1e6:
            return f"{sign}{sym}{abs_v/1e6:.1f}M"
        return f"{sign}{sym}{abs_v:,.0f}"

    @staticmethod
    def _apply_adjusted_fcf(data: dict) -> dict:
        """Recompute per-share FCF using latest shares outstanding.

        Since stock prices are adjusted (前复权), FCF per share must also use
        the latest total shares to be on the same basis as the adjusted price.
        """
        data = dict(data)  # shallow copy to avoid mutating cached data
        fcf_table = data.get("fcf_table")
        if fcf_table is None or fcf_table.empty:
            return data

        latest_shares = data.get("shares_outstanding")
        if latest_shares and latest_shares > 0:
            fcf_table = recompute_fcf_per_share(fcf_table, latest_shares)
            data["fcf_table"] = fcf_table

        # Always rebuild the per-share dict from the table (covers AI-filled years)
        new_fcf_ps = {}
        for _, row in fcf_table.iterrows():
            if pd.notna(row.get("每股FCF")):
                yr = str(row["年份"])[:4]
                new_fcf_ps[pd.Timestamp(f"{yr}-12-31")] = row["每股FCF"]
        if new_fcf_ps:
            data["fcf_per_share_by_year"] = new_fcf_ps
        return data

    @staticmethod
    def _build_fcf_table_html(fcf_table, currency, source="", prev_table=None):
        """Build styled HTML for the FCF table. Highlight cells that changed from prev_table."""
        currency_sym = {"USD": "$", "CNY": "¥", "HKD": "HK$"}.get(currency, currency)
        display_tbl = fcf_table.copy()
        raw_tbl = fcf_table.copy()
        unit_label = "亿" if currency == "CNY" else ""
        divisor = 1e8 if currency == "CNY" else 1

        # Build previous values lookup: {(year_prefix, col): raw_value}
        prev_vals = {}
        if prev_table is not None:
            for _, row in prev_table.iterrows():
                yr = str(row["年份"])[:4]
                for col in prev_table.columns:
                    if col != "年份":
                        prev_vals[(yr, col)] = row.get(col)

        def _fmt_big(x):
            if pd.isna(x):
                return "N/A"
            return f"{currency_sym}{x/divisor:>14,.2f}{unit_label}"

        def _fmt_ps(x):
            if pd.isna(x):
                return "N/A"
            return f"{currency_sym}{x:>10,.3f}"

        for col in ["OCF", "CapEx", "FCF"]:
            if col in display_tbl.columns:
                display_tbl[col] = display_tbl[col].apply(_fmt_big)
        for col in ["每股FCF", "3年均每股FCF", "5年均每股FCF", "yf每股FCF"]:
            if col in display_tbl.columns:
                display_tbl[col] = display_tbl[col].apply(_fmt_ps)

        _tbl_cols = list(display_tbl.columns)
        header_html = "".join(f"<th>{c}</th>" for c in _tbl_cols)
        rows_html = ""
        for row_idx, (_, row) in enumerate(display_tbl.iterrows()):
            yr = str(raw_tbl.iloc[row_idx]["年份"])[:4]
            cells = ""
            for c in _tbl_cols:
                val = str(row[c]) if pd.notna(row[c]) else "N/A"
                cls_list = []
                if val == "N/A":
                    cls_list.append("na")
                elif c != "年份" and prev_table is not None:
                    raw_val = raw_tbl.iloc[row_idx].get(c)
                    prev_val = prev_vals.get((yr, c))
                    if prev_val is not None and pd.notna(raw_val):
                        if pd.isna(prev_val):
                            cls_list.append("changed")
                        elif isinstance(prev_val, (int, float)) and isinstance(raw_val, (int, float)):
                            if abs(float(prev_val) - float(raw_val)) / max(abs(float(prev_val)), 1) > 1e-6:
                                cls_list.append("changed")
                cls = f' class="{" ".join(cls_list)}"' if cls_list else ""
                cells += f"<td{cls}>{val}</td>"
            rows_html += f"<tr>{cells}</tr>"

        source_html = ""
        if source:
            source_html = f" <span style='color:#94a3b8;font-size:.8rem'>({source})</span>"

        return f"""
        <div style="margin-bottom:8px;"><strong style="color:#e0e7ff;">📊 历年自由现金流明细</strong>{source_html}</div>
        <div style="overflow-x:auto; border-radius:10px; border:1px solid #1e3a5f;">
        <table style="width:100%; border-collapse:collapse; font-size:.85rem; font-family:'Cascadia Mono','Consolas','SF Mono',monospace;">
        <thead><tr style="background:#131b2e; color:#00d4ff; text-align:right;">
            {header_html}
        </tr></thead>
        <tbody style="color:#e0e7ff;">
            {rows_html}
        </tbody>
        </table>
        </div>
        <style>
            div[data-testid="stMarkdownContainer"] table th {{
                padding: 8px 12px; border-bottom: 2px solid #1e3a5f;
                white-space: nowrap; font-weight: 600; text-align: right;
            }}
            div[data-testid="stMarkdownContainer"] table td {{
                padding: 6px 12px; border-bottom: 1px solid #1a2235;
                text-align: right; white-space: nowrap;
            }}
            div[data-testid="stMarkdownContainer"] table tr:hover {{
                background: #1a2540 !important;
            }}
            div[data-testid="stMarkdownContainer"] table tr:nth-child(even) {{
                background: #0f1627;
            }}
            div[data-testid="stMarkdownContainer"] table tr:nth-child(odd) {{
                background: #0a0e17;
            }}
            div[data-testid="stMarkdownContainer"] table td.na {{
                color: #64748b; font-style: italic;
            }}
            div[data-testid="stMarkdownContainer"] table td.changed {{
                background: rgba(16, 185, 129, 0.25) !important;
                color: #10b981 !important;
                font-weight: 600;
            }}
        </style>
        """

    # ── Subclass interface ───────────────────────────────────────────

    def fetch_data(self, ticker):
        """Fetch market data. Override in subclass."""
        raise NotImplementedError

    def normalize_ticker(self, ticker):
        """Normalize ticker input. Override if needed."""
        return ticker.strip()

    def format_label(self, ticker):
        """Format ticker for chart title. Override if needed."""
        return ticker

    def download_filings_ui(self, ticker):
        """Download filings with UI feedback. Override in subclass if applicable."""
        pass

    def render_extra_ui(self, ticker):
        """Render extra UI after chart/price alert. Override in subclass."""
        pass

    def on_analysis_complete(self, ticker, data):
        """Called after a ticker is fully analyzed. Override for hooks like tracking."""
        pass

    # ── Chart ────────────────────────────────────────────────────────

    _DOWNSAMPLE_THRESHOLD = 1500  # above this -> resample older data to weekly

    @staticmethod
    def _downsample_ohlcv(ohlcv, threshold=1500):
        """Keep recent *threshold* daily rows; resample older portion to weekly."""
        n = len(ohlcv)
        if n <= threshold:
            return ohlcv
        recent = ohlcv.iloc[-threshold:]
        older = ohlcv.iloc[: n - threshold].copy()
        older["_week"] = pd.to_datetime(older["Date"]).dt.to_period("W").dt.start_time
        weekly = older.groupby("_week", sort=True).agg({
            "Date": "last", "Open": "first", "High": "max",
            "Low": "min", "Close": "last", "Volume": "sum",
        }).reset_index(drop=True)
        return pd.concat([weekly, recent], ignore_index=True)

    def render_chart(self, data, ticker_label, show_fcf_table: bool = True):
        """Draw an interactive Plotly candlestick chart with EMA + DCF lines."""
        ohlcv = data["ohlcv"]
        currency = data.get("currency", self.default_currency)
        currency_sym = {"USD": "$", "CNY": "¥", "HKD": "HK$"}.get(currency, currency)
        last_price = data.get("last_price")
        if last_price is None and ohlcv is not None and not ohlcv.empty:
            last_price = float(ohlcv["Close"].iloc[-1])

        if ohlcv is None or ohlcv.empty:
            st.error("无可用数据。")
            return

        ohlcv = self._downsample_ohlcv(ohlcv.copy())
        ohlcv["EMA10"] = ohlcv["Close"].ewm(span=10, adjust=False).mean()
        ohlcv["EMA250"] = ohlcv["Close"].ewm(span=250, adjust=False).mean()

        dcf_df = compute_dcf_lines(data.get("fcf_per_share_by_year", {}))
        if not dcf_df.empty:
            trade_dates = pd.to_datetime(ohlcv["Date"]).dt.normalize().sort_values().reset_index(drop=True)

            def _align_to_trade_day(dt):
                ts = pd.Timestamp(dt).normalize()
                idx = trade_dates.searchsorted(ts, side="right") - 1
                if idx < 0:
                    idx = 0
                return trade_dates.iloc[idx]

            dcf_df = dcf_df.copy()
            dcf_df["plot_date"] = dcf_df["date"].apply(_align_to_trade_day)

        fig = go.Figure()

        # Candlestick
        fig.add_trace(go.Candlestick(
            x=ohlcv["Date"], open=ohlcv["Open"], high=ohlcv["High"],
            low=ohlcv["Low"], close=ohlcv["Close"],
            name="K线",
            increasing_line_color="#ef5350", increasing_fillcolor="#ef5350",
            decreasing_line_color="#26a69a", decreasing_fillcolor="#26a69a",
        ))

        # EMA (WebGL for speed)
        fig.add_trace(go.Scattergl(
            x=ohlcv["Date"], y=ohlcv["EMA10"], name="EMA 10",
            line=dict(color="#f94144", width=1), mode="lines",
            hoverinfo="skip",
        ))
        fig.add_trace(go.Scattergl(
            x=ohlcv["Date"], y=ohlcv["EMA250"], name="EMA 250",
            line=dict(color="#7209b7", width=2), mode="lines",
            hoverinfo="skip",
        ))

        # DCF valuation lines
        if not dcf_df.empty:
            for col, name, color in [
                ("dcf_14x", "DCF 14x", "#3b82f6"),
                ("dcf_24x", "DCF 24x", "#10b981"),
                ("dcf_34x", "DCF 34x", "#f59e0b"),
            ]:
                fig.add_trace(go.Scatter(
                    x=dcf_df["plot_date"], y=dcf_df[col], name=name,
                    line=dict(color=color, width=2.5, dash="dash"),
                    mode="lines+markers",
                    marker=dict(size=10, symbol="diamond", line=dict(width=1, color="white")),
                    connectgaps=True,
                ))

        # Latest price annotation
        annotations = []
        if last_price and not ohlcv.empty:
            last_date = ohlcv["Date"].iloc[-1]
            annotations.append(dict(
                x=last_date, y=last_price,
                text=f"  {currency_sym}{last_price:,.2f}",
                showarrow=False,
                font=dict(color="#00d4ff", size=14, family="monospace"),
                xanchor="left", yanchor="middle",
                bgcolor="rgba(10,14,23,0.8)",
                bordercolor="#00d4ff", borderwidth=1, borderpad=4,
            ))

        # Axis ranges
        price_max = ohlcv["High"].max()
        price_min = ohlcv["Low"].min()
        y_top = price_max * 1.12
        y_bottom = max(0, price_min * 0.88)

        date_min = ohlcv["Date"].min()
        date_max = ohlcv["Date"].max()
        date_span = (date_max - date_min)
        x_right = date_max + date_span * 0.20

        fig.update_layout(
            title=dict(text=f"{ticker_label} 日K线", font=dict(color="#e0e7ff")),
            yaxis_title=f"价格 ({currency})",
            xaxis_rangeslider_visible=True,
            xaxis_rangeslider_thickness=0.06,
            template="plotly_dark",
            paper_bgcolor="#0a0e17",
            plot_bgcolor="#0f1629",
            height=650,
            dragmode="pan",
            legend=dict(orientation="h", yanchor="bottom", y=1.02,
                        xanchor="right", x=1, font=dict(color="#94a3b8")),
            margin=dict(l=60, r=100, t=60, b=40),
            xaxis=dict(
                gridcolor="#1e3a5f", zerolinecolor="#1e3a5f",
                range=[date_min, x_right],
            ),
            yaxis=dict(
                gridcolor="#1e3a5f", zerolinecolor="#1e3a5f",
                range=[y_bottom, y_top],
                fixedrange=False,
            ),
            annotations=annotations,
        )
        fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])

        # FCF report table (suppressed in reviewed/browse mode)
        fcf_table = data.get("fcf_table")
        if show_fcf_table:
            if fcf_table is not None and not fcf_table.empty:
                source = data.get("source", "")
                st.markdown(
                    self._build_fcf_table_html(fcf_table, currency, source=source),
                    unsafe_allow_html=True,
                )

        st.plotly_chart(fig, use_container_width=True, config={"scrollZoom": True})

        # Metric cards
        market_cap = data.get("market_cap")

        latest_fcf = None
        latest_fcf_year = None
        avg_fcf_3y = None
        fcf_ps_latest = None
        if fcf_table is not None and not fcf_table.empty:
            for _, row in fcf_table.iterrows():
                if pd.notna(row.get("FCF")):
                    latest_fcf = row["FCF"]
                    latest_fcf_year = str(row["年份"])[:4]
                    break
            fcf_vals = []
            for _, row in fcf_table.iterrows():
                if pd.notna(row.get("FCF")) and len(fcf_vals) < 3:
                    fcf_vals.append(row["FCF"])
            if fcf_vals:
                avg_fcf_3y = sum(fcf_vals) / len(fcf_vals)
            for _, row in fcf_table.iterrows():
                if pd.notna(row.get("每股FCF")):
                    fcf_ps_latest = row["每股FCF"]
                    break

        p_fcf = None
        if last_price and fcf_ps_latest and fcf_ps_latest > 0:
            p_fcf = last_price / fcf_ps_latest

        fcf_label = f"最近年度 FCF ({latest_fcf_year})" if latest_fcf_year else "最近年度 FCF"
        n_metrics = 5 if last_price else 4
        cols = st.columns(n_metrics)
        idx = 0
        if last_price:
            cols[idx].metric("最新价", f"{currency_sym}{last_price:,.2f}")
            idx += 1
        cols[idx].metric("市值", self.fmt_val(market_cap, currency))
        cols[idx + 1].metric(fcf_label, self.fmt_val(latest_fcf, currency))
        cols[idx + 2].metric("3年平均 FCF", self.fmt_val(avg_fcf_3y, currency))
        cols[idx + 3].metric("P/FCF (基于最近年度)", f"{p_fcf:.1f}x" if p_fcf else "N/A")

        # Note: P/FCF uses the latest single-year per-share FCF; DCF 曲线使用 3 年滚动均值
        st.caption("提示：图上的 DCF 曲线基于每股 FCF 的 3 年滚动均值；P/FCF 指标使用最新一年的每股 FCF，与曲线值不同。")

    # ── Price alert ──────────────────────────────────────────────────

    def render_price_alert(self, ticker, data=None, key_suffix=""):
        """Render moomoo OpenD price alert subscription UI with DCF quick-subscribe."""
        key_prefix = self.market.lower() + (f"_{key_suffix}" if key_suffix else "")
        st.divider()
        st.subheader("📢 价格提醒订阅 (moomoo OpenD)")

        # DCF Quick-Subscribe
        dcf_df = None
        fcf_ps_latest = None
        fcf_ps_date = None
        currency = data.get("currency", self.default_currency) if data else self.default_currency
        currency_sym = {"USD": "$", "CNY": "¥", "HKD": "HK$"}.get(currency, currency)

        if data:
            dcf_df = compute_dcf_lines(data.get("fcf_per_share_by_year", {}))
            if dcf_df is not None and not dcf_df.empty:
                fcf_ps_by_year = data.get("fcf_per_share_by_year", {})
                if fcf_ps_by_year:
                    latest_date = max(fcf_ps_by_year.keys())
                    fcf_ps_latest = fcf_ps_by_year[latest_date]
                    fcf_ps_date = pd.Timestamp(latest_date).strftime("%Y-%m-%d")

                p14 = dcf_df["dcf_14x"].iloc[-1]
                p24 = dcf_df["dcf_24x"].iloc[-1]
                p34 = dcf_df["dcf_34x"].iloc[-1]

                st.markdown("##### ⚡ DCF 估值快捷订阅")
                dc1, dc2, dc3 = st.columns(3)
                dc1.metric("DCF 14x", f"{currency_sym}{p14:,.2f}")
                dc2.metric("DCF 24x", f"{currency_sym}{p24:,.2f}")
                dc3.metric("DCF 34x", f"{currency_sym}{p34:,.2f}")

                if st.button("🔔 一键订阅 3 个 DCF 价格提醒", key=f"{key_prefix}_dcf_subscribe",
                             use_container_width=True):
                    if not ticker:
                        st.warning("请先输入股票代码。")
                    else:
                        try:
                            from futu_client import FutuClient
                            code = FutuClient.build_code(ticker, self.market)
                            results = []
                            for mult, price in [("14x", p14), ("24x", p24), ("34x", p34)]:
                                note = f"DCF{mult} FCF{currency_sym}{fcf_ps_latest:.2f}"
                                with FutuClient() as fc:
                                    ok, msg = fc.set_price_alert(
                                        code, price, note, reminder_type="PRICE_DOWN",
                                    )
                                results.append((mult, price, ok, msg))
                            for mult, price, ok, msg in results:
                                if ok:
                                    st.success(f"✅ {mult} ({currency_sym}{price:,.2f}): {msg}")
                                else:
                                    st.error(f"❌ {mult}: {msg}")
                        except Exception as e:
                            st.error(f"设置提醒失败 (请确认 moomoo OpenD 已启动): {e}")

                st.caption(
                    f"基于 5 年滚动均值 FCF/share = {currency_sym}{fcf_ps_latest:.3f} "
                    f"(数据截至 {fcf_ps_date})"
                    if fcf_ps_latest else ""
                )

        # Manual alert
        with st.expander("🔧 自定义价格提醒"):
            ac1, ac2, ac3, ac4 = st.columns([1.5, 1.5, 2, 1])
            with ac1:
                alert_price = st.number_input(
                    "目标价格", min_value=0.01, step=0.01,
                    key=f"{key_prefix}_alert_price",
                )
            with ac2:
                alert_type = st.selectbox(
                    "提醒类型", ["价格跌到", "价格涨到"],
                    key=f"{key_prefix}_alert_type",
                )
            with ac3:
                alert_note = st.text_input(
                    "备注", key=f"{key_prefix}_alert_note",
                    placeholder="如: DCF 24x 估值点",
                )
            with ac4:
                st.markdown("<br>", unsafe_allow_html=True)
                alert_btn = st.button(
                    "🔔 设置提醒", key=f"{key_prefix}_alert_btn",
                    use_container_width=True,
                )

            if alert_btn:
                if not ticker:
                    st.warning("请先输入股票代码。")
                    return
                try:
                    from futu_client import FutuClient
                    code = FutuClient.build_code(ticker, self.market)
                    rt = "PRICE_UP" if alert_type == "价格涨到" else "PRICE_DOWN"
                    with FutuClient() as fc:
                        ok, msg = fc.set_price_alert(
                            code, alert_price, alert_note or "", reminder_type=rt,
                        )
                    if ok:
                        st.success(msg)
                    else:
                        st.error(msg)
                except Exception as e:
                    st.error(f"设置提醒失败 (请确认 moomoo OpenD 已启动): {e}")

    # ── AI fill flow ─────────────────────────────────────────────────

    def _run_ai_fill(self, data, ticker, filing_store=None):
        """Run AI fill + validate flow. Returns st.empty placeholder or None."""
        gemini_api_key = st.session_state.get("gemini_api_key", "")
        gemini_model = st.session_state.get("gemini_model_name", "")
        enabled_models = st.session_state.get("enabled_models")

        fcf_tbl = data.get("fcf_table") if isinstance(data, dict) else None
        if fcf_tbl is None or fcf_tbl.empty or not gemini_api_key:
            return None

        st.divider()
        st.markdown("#### 🤖 AI 自动读取年报填表 + 验证中...")

        currency = data.get("currency", self.default_currency)
        tbl_placeholder = st.empty()
        prev_tbl = [fcf_tbl.copy()]
        tbl_placeholder.markdown(
            self._build_fcf_table_html(fcf_tbl, currency),
            unsafe_allow_html=True,
        )

        def _table_callback(updated_tbl):
            tbl_placeholder.markdown(
                self._build_fcf_table_html(updated_tbl, currency, prev_table=prev_tbl[0]),
                unsafe_allow_html=True,
            )
            prev_tbl[0] = updated_tbl.copy()

        progress_bar = st.progress(0, text="准备中...")
        log_area = st.empty()
        all_logs = []

        prog = {"start_ts": time.time(), "pct": 0.0, "tokens": 0, "est_total": 0}

        def _fmt_counter(p):
            elapsed = int(max(0, time.time() - p["start_ts"]))
            mm, ss = divmod(elapsed, 60)
            est = p["est_total"]
            est_str = f" / 估算总计 ~{est:,}" if est else ""
            return f"已用时 {mm:02d}:{ss:02d} | 已消耗 ~{p['tokens']:,}{est_str} tokens"

        def _progress(msg=None, step=None, total=None):
            if msg:
                all_logs.append(msg)
                log_area.text_area("📋 处理日志", "\n".join(reversed(all_logs)), height=300)
                if "正在等待回复" in msg and "tokens" in msg:
                    m = re.search(r"~\s*([\d,]+)\s*tokens", msg)
                    if m:
                        prog["tokens"] += int(m.group(1).replace(",", ""))
                elif "合计 ~" in msg:
                    m = re.search(r"合计 ~([\d,]+)\s*tokens", msg)
                    if m:
                        prog["est_total"] += int(m.group(1).replace(",", ""))
            if step is not None and total and total > 0:
                prog["pct"] = min(step / total, 1.0)
            progress_bar.progress(prog["pct"], text=_fmt_counter(prog))

        try:
            filled, logs, prompt_info = fill_fcf_table_with_llm(
                api_key=gemini_api_key,
                model_name=gemini_model,
                fcf_table=fcf_tbl.copy(),
                ticker=ticker,
                market=self.market,
                progress_callback=_progress,
                table_update_callback=_table_callback,
                enabled_models=enabled_models,
                filing_store=filing_store,
            )
            progress_bar.progress(1.0, text=f"完成 | {_fmt_counter(prog)}")
            # Delete raw filings whose excerpts are now cached
            if filing_store is not None:
                n = filing_store.delete_raw_filings()
                if n:
                    _progress(msg=f"🗑 已删除 {n} 份原始年报文件（节选已缓存，可随时重新下载）")
            # Show prompt used
            with st.expander("📜 查看发送给 Gemini 的 Prompt", expanded=False):
                st.markdown(f"**System Prompt:**\n```\n{prompt_info['system_prompt']}\n```")
                st.markdown(f"**规则文件:** `{prompt_info['rules_path']}`")
                st.markdown(f"**规则内容:**\n```\n{prompt_info['rules']}\n```")
                for i, bp in enumerate(prompt_info.get("batch_prompts", [])):
                    st.markdown(f"**批次 {i+1} Prompt:**\n```\n{bp[:2000]}{'...(截断)' if len(bp) > 2000 else ''}\n```")
            # Update data with filled table
            latest_shares = data.get("shares_outstanding")
            if latest_shares and latest_shares > 0:
                filled = recompute_fcf_per_share(filled, latest_shares)
            data["fcf_table"] = filled
            _table_callback(filled)
            # Save
            try:
                saved_path = save_fcf_table(filled, ticker, self.market)
                st.caption(f"📁 表格已保存: {saved_path}")
            except Exception:
                pass
            st.success("AI 年报验证完成!")
        except Exception as e:
            st.error(f"AI 补全失败: {e}")

        return tbl_placeholder

    # ── Main orchestrator ────────────────────────────────────────────

    def run(self):
        """Run the full analysis flow within a Streamlit tab."""
        key = self.market.lower()
        ticker_raw = st.text_input(
            self.ticker_input_label,
            value=self.ticker_input_default,
            key=f"{key}_ticker",
        )

        if st.button(self.analyze_button_label, key=f"{key}_chart", use_container_width=True):
            if not ticker_raw:
                st.warning("请输入有效的代码。")
            else:
                ticker = self.normalize_ticker(ticker_raw)

                # Step 1: Download filings (optional, subclass overrides)
                filing_store = self.download_filings_ui(ticker)

                # Step 2: Fetch data
                data = None
                spinner_msg = (
                    f"正在获取 {ticker} 数据 ({self.data_source_desc})..."
                    if self.data_source_desc
                    else f"正在获取 {ticker} 数据..."
                )
                with st.spinner(spinner_msg):
                    try:
                        data = self.fetch_data(ticker)
                        # Load previously saved FCF table if available
                        saved_tbl = load_fcf_table(ticker, self.market)
                        if saved_tbl is not None and not saved_tbl.empty:
                            data = dict(data)
                            data["fcf_table"] = saved_tbl
                    except Exception as e:
                        st.error(f"数据获取出错: {e}")

                # Step 3: AI fill + validate
                tbl_placeholder = None
                if data is not None:
                    tbl_placeholder = self._run_ai_fill(data, ticker, filing_store=filing_store)

                    # Step 4: Apply adjusted FCF + store in session state
                    data = self._apply_adjusted_fcf(data)
                    st.session_state[f"{key}_chart_data"] = data
                    st.session_state[f"{key}_chart_label"] = self.format_label(ticker)
                    # Clear live table to avoid duplication with render_chart
                    if tbl_placeholder:
                        tbl_placeholder.empty()

                    # Step 5: Notify subclass of completion
                    self.on_analysis_complete(ticker, data)

        # Always render chart if data is available
        if f"{key}_chart_data" in st.session_state:
            self.render_chart(
                st.session_state[f"{key}_chart_data"],
                st.session_state[f"{key}_chart_label"],
            )

        self.render_price_alert(
            ticker_raw,
            data=st.session_state.get(f"{key}_chart_data"),
        )

        self.render_extra_ui(ticker_raw)
