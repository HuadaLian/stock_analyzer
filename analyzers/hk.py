"""HK market analyzer — Futu OpenD kline + yfinance financials."""

import streamlit as st
from .base import MarketAnalyzer
from data_provider import get_hk_data
from analysis_tracker import mark_analyzed


@st.cache_data(ttl=600, show_spinner=False)
def _fetch_hk_data(code):
    return get_hk_data(code)


class HKAnalyzer(MarketAnalyzer):
    market = "HK"
    default_currency = "HKD"
    ticker_input_label = "港股代码 (如 00700)"
    ticker_input_default = ""
    analyze_button_label = "📊 生成价格图表"
    data_source_desc = "Futu OpenD"

    def format_label(self, ticker):
        return f"HK.{ticker.zfill(5)}"

    def fetch_data(self, ticker):
        return _fetch_hk_data(ticker)

    def on_analysis_complete(self, ticker, data):
        try:
            mcap = data.get("market_cap")
            meta = {"market_cap": mcap, "market": "HK"} if mcap else {"market": "HK"}
            mark_analyzed(ticker, metadata=meta)
        except Exception:
            mark_analyzed(ticker)
        try:
            from chart_store import save_chart
            save_chart(ticker, self.market, data)
        except Exception:
            pass

    def render_extra_ui(self, ticker):
        st.divider()
        st.info("港股报告下载功能即将上线。")
