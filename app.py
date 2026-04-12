# app.py
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import re
import time
from data_provider import get_us_data, get_cn_data, get_hk_data, compute_dcf_lines
from downloader import SmartSECDownloader, CninfoDownloader
from gemini_chat import (
    MODELS, MODEL_RATE_LIMITS, DEFAULT_ENABLED_MODELS, RULES_PATH,
    list_sec_filings, list_cn_filings,
    extract_text, estimate_tokens, init_chat, send_message,
    fill_fcf_table_with_llm, save_fcf_table, load_fcf_table,
    load_fcf_rules, save_fcf_rules, recompute_fcf_per_share,
    get_model_call_status, reset_model_call_status,
)

# ── Page config ──────────────────────────────────────────────────────────
st.set_page_config(page_title="Stock Analyzer", layout="wide")

# ── Modern CSS ───────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* ── Base dark sci-fi palette ─────────────────────────────────────── */
    :root {
        --bg-primary:   #0a0e17;
        --bg-card:      #111827;
        --bg-card-alt:  #1a2035;
        --border:       #1e3a5f;
        --accent-cyan:  #00d4ff;
        --accent-blue:  #3b82f6;
        --accent-purple:#8b5cf6;
        --text-primary: #e0e7ff;
        --text-muted:   #94a3b8;
        --glow-cyan:    0 0 12px rgba(0,212,255,.25);
        --glow-purple:  0 0 12px rgba(139,92,246,.25);
    }

    /* hide chrome */
    #MainMenu, header, footer {visibility: hidden;}
    .block-container {padding-top: 1.2rem; padding-bottom: 1rem;}

    /* overall background */
    .stApp, [data-testid="stAppViewContainer"],
    [data-testid="stHeader"],
    section[data-testid="stSidebar"] {
        background-color: var(--bg-primary) !important;
    }

    /* ── Metric cards ──────────────────────────────────────────────── */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, var(--bg-card) 0%, var(--bg-card-alt) 100%);
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: 18px 22px;
        box-shadow: var(--glow-cyan);
        transition: box-shadow .3s;
    }
    div[data-testid="stMetric"]:hover {
        box-shadow: var(--glow-purple);
    }
    div[data-testid="stMetric"] label {
        color: var(--text-muted) !important; font-size: .82rem;
    }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        color: var(--accent-cyan) !important; font-size: 1.35rem; font-weight: 600;
    }

    /* ── Tab bar ───────────────────────────────────────────────────── */
    button[data-baseweb="tab"] {
        font-size: 1.05rem; font-weight: 600;
        color: var(--text-muted) !important;
        border-bottom: 2px solid transparent;
        transition: all .25s;
    }
    button[data-baseweb="tab"]:hover {
        color: var(--accent-cyan) !important;
    }
    button[data-baseweb="tab"][aria-selected="true"] {
        color: var(--accent-cyan) !important;
        border-bottom: 2px solid var(--accent-cyan);
        text-shadow: 0 0 8px rgba(0,212,255,.45);
    }

    /* ── Buttons ───────────────────────────────────────────────────── */
    .stButton > button {
        background: linear-gradient(135deg, #1e3a5f 0%, #0f2440 100%) !important;
        color: var(--accent-cyan) !important;
        border: 1px solid var(--border) !important;
        border-radius: 10px !important;
        font-weight: 600 !important;
        letter-spacing: .3px;
        transition: all .3s !important;
        box-shadow: 0 0 6px rgba(0,212,255,.15);
    }
    .stButton > button:hover {
        background: linear-gradient(135deg, #1e3a5f 0%, #162d50 100%) !important;
        box-shadow: var(--glow-cyan) !important;
        border-color: var(--accent-cyan) !important;
    }

    /* ── Text inputs / number inputs / selects ─────────────────────── */
    .stTextInput > div > div > input,
    .stNumberInput > div > div > input,
    .stSelectbox > div > div,
    .stTextArea textarea {
        background-color: var(--bg-card) !important;
        color: var(--text-primary) !important;
        border: 1px solid var(--border) !important;
        border-radius: 8px !important;
    }
    .stTextInput > div > div > input:focus,
    .stNumberInput > div > div > input:focus {
        border-color: var(--accent-cyan) !important;
        box-shadow: var(--glow-cyan) !important;
    }

    /* ── Subheaders ────────────────────────────────────────────────── */
    h3, .stSubheader {
        color: var(--accent-cyan) !important;
        text-shadow: 0 0 6px rgba(0,212,255,.3);
    }
    h1, h2 { color: var(--text-primary) !important; }
    p, span, label, .stMarkdown { color: var(--text-primary) !important; }

    /* ── Dividers ──────────────────────────────────────────────────── */
    hr {
        border-color: var(--border) !important;
        opacity: .4;
    }

    /* ── Info / success / error boxes ──────────────────────────────── */
    .stAlert {
        background-color: var(--bg-card) !important;
        border-left: 4px solid var(--accent-blue);
        border-radius: 8px;
    }

    /* ── Dataframe / table ─────────────────────────────────────────── */
    .stDataFrame, [data-testid="stTable"] {
        border: 1px solid var(--border);
        border-radius: 10px;
        overflow: hidden;
    }

    /* ── Cell highlight animation for updated values ──────────────── */
    @keyframes cellHighlight {
        0%   {
            background-color: rgba(0, 212, 255, 0.60);
            box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.35);
        }
        15%  {
            background-color: rgba(0, 212, 255, 0.52);
            box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.30);
        }
        40%  {
            background-color: rgba(0, 212, 255, 0.34);
            box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.22);
        }
        70%  {
            background-color: rgba(0, 212, 255, 0.18);
            box-shadow: inset 0 0 0 1px rgba(255, 255, 255, 0.12);
        }
        100% {
            background-color: transparent;
            box-shadow: none;
        }
    }
    td.cell-updated {
        animation: cellHighlight 60s linear forwards;
        font-weight: 700;
        color: #f8fdff !important;
        text-shadow: 0 1px 0 rgba(0, 0, 0, 0.45);
    }
</style>
""", unsafe_allow_html=True)


# ── Helpers ──────────────────────────────────────────────────────────────
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


@st.cache_data(ttl=600, show_spinner=False)
def fetch_us_data(ticker):
    return get_us_data(ticker)

@st.cache_data(ttl=600, show_spinner=False)
def fetch_cn_data(code):
    return get_cn_data(code)

@st.cache_data(ttl=600, show_spinner=False)
def fetch_hk_data(code):
    return get_hk_data(code)


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


def render_chart(data, ticker_label):
    """Draw an interactive Plotly candlestick chart with EMA + DCF lines."""
    ohlcv = data["ohlcv"]
    currency = data.get("currency", "USD")
    currency_sym = {"USD": "$", "CNY": "¥", "HKD": "HK$"}.get(currency, currency)
    last_price = data.get("last_price")

    if ohlcv is None or ohlcv.empty:
        st.error("无可用数据。")
        return

    ohlcv = ohlcv.copy()
    ohlcv["EMA10"] = ohlcv["Close"].ewm(span=10, adjust=False).mean()
    ohlcv["EMA250"] = ohlcv["Close"].ewm(span=250, adjust=False).mean()

    dcf_df = compute_dcf_lines(data.get("fcf_per_share_by_year", {}))
    if not dcf_df.empty:
        # DCF annual dates can fall on weekends; map to nearest previous
        # trading day so points are not removed by weekend rangebreaks.
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

    # ── Candlestick ──────────────────────────────────────────────────
    fig.add_trace(go.Candlestick(
        x=ohlcv["Date"], open=ohlcv["Open"], high=ohlcv["High"],
        low=ohlcv["Low"], close=ohlcv["Close"],
        name="K线",
        increasing_line_color="#ef5350", increasing_fillcolor="#ef5350",
        decreasing_line_color="#26a69a", decreasing_fillcolor="#26a69a",
    ))

    # ── EMA ───────────────────────────────────────────────────────────
    fig.add_trace(go.Scatter(
        x=ohlcv["Date"], y=ohlcv["EMA10"], name="EMA 10",
        line=dict(color="#f94144", width=1), mode="lines",
    ))
    fig.add_trace(go.Scatter(
        x=ohlcv["Date"], y=ohlcv["EMA250"], name="EMA 250",
        line=dict(color="#7209b7", width=2), mode="lines",
    ))

    # ── DCF valuation lines ───────────────────────────────────────
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

    # ── Latest price annotation ──────────────────────────────────────
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

    # ── Compute default axis ranges ──────────────────────────────────
    # Y-range based on stock price ONLY; DCF lines may exceed visible range
    price_max = ohlcv["High"].max()
    price_min = ohlcv["Low"].min()
    y_top = price_max * 1.12       # ~12% headroom above price peak
    y_bottom = max(0, price_min * 0.88)

    date_min = ohlcv["Date"].min()
    date_max = ohlcv["Date"].max()
    date_span = (date_max - date_min)
    x_right = date_max + date_span * 0.20  # 1.2x total width

    fig.update_layout(
        title=dict(text=f"{ticker_label} 日K线", font=dict(color="#e0e7ff")),
        yaxis_title=f"价格 ({currency})",
        xaxis_rangeslider_visible=True,
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

    # ── FCF report table ─────────────────────────────────────────────
    fcf_table = data.get("fcf_table")
    if fcf_table is not None and not fcf_table.empty:
        source = data.get("source", "")
        st.markdown(
            _build_fcf_table_html(fcf_table, currency, source=source),
            unsafe_allow_html=True,
        )

    st.plotly_chart(fig, use_container_width=True, config={"scrollZoom": True})

    # ── Metric cards ─────────────────────────────────────────────────
    market_cap = data.get("market_cap")

    # Compute latest FCF, 5-year avg, and per-share from the table
    latest_fcf = None
    latest_fcf_year = None
    avg_fcf_5y = None
    fcf_ps_latest = None
    if fcf_table is not None and not fcf_table.empty:
        for _, row in fcf_table.iterrows():
            if pd.notna(row.get("FCF")):
                latest_fcf = row["FCF"]
                latest_fcf_year = str(row["年份"])[:4]
                break
        fcf_vals = []
        for _, row in fcf_table.iterrows():
            if pd.notna(row.get("FCF")) and len(fcf_vals) < 5:
                fcf_vals.append(row["FCF"])
        if fcf_vals:
            avg_fcf_5y = sum(fcf_vals) / len(fcf_vals)
        for _, row in fcf_table.iterrows():
            if pd.notna(row.get("每股FCF")):
                fcf_ps_latest = row["每股FCF"]
                break

    # P/FCF = latest price / FCF per share
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
    cols[idx].metric("市值", fmt_val(market_cap, currency))
    cols[idx + 1].metric(fcf_label, fmt_val(latest_fcf, currency))
    cols[idx + 2].metric("5年平均 FCF", fmt_val(avg_fcf_5y, currency))
    cols[idx + 3].metric("P/FCF", f"{p_fcf:.1f}x" if p_fcf else "N/A")


def render_price_alert(ticker, market, key_prefix, data=None):
    """Render moomoo OpenD price alert subscription UI with DCF quick-subscribe."""
    st.divider()
    st.subheader("📢 价格提醒订阅 (moomoo OpenD)")

    # ── DCF Quick-Subscribe ───────────────────────────────────────────
    dcf_df = None
    fcf_ps_latest = None
    fcf_ps_date = None
    currency = data.get("currency", "USD") if data else "USD"
    currency_sym = {"USD": "$", "CNY": "¥", "HKD": "HK$"}.get(currency, currency)

    if data:
        dcf_df = compute_dcf_lines(data.get("fcf_per_share_by_year", {}))
        if dcf_df is not None and not dcf_df.empty:
            # Get the latest (last row before today-extension) FCF per share info
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
                        code = FutuClient.build_code(ticker, market)
                        results = []
                        for mult, price in [("14x", p14), ("24x", p24), ("34x", p34)]:
                            note = f"DCF{mult} FCF{currency_sym}{fcf_ps_latest:.2f}"
                            with FutuClient() as fc:
                                ok, msg = fc.set_price_alert(
                                    code, price, note, reminder_type="PRICE_DOWN",
                                )
                            results.append((mult, price, ok, msg))
                        # Show results
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

    # ── Manual alert ──────────────────────────────────────────────────
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
                code = FutuClient.build_code(ticker, market)
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


# ── Gemini API config (persisted in session state) ──────────────────────
import os
_default_key = os.environ.get("GEMINI_API_KEY", "")
if not _default_key:
    _env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(_env_path):
        with open(_env_path, "r") as _f:
            for _line in _f:
                if _line.startswith("GEMINI_API_KEY="):
                    _default_key = _line.split("=", 1)[1].strip()

if "gemini_api_key" not in st.session_state:
    st.session_state["gemini_api_key"] = _default_key
if "gemini_model_name" not in st.session_state:
    st.session_state["gemini_model_name"] = list(MODELS.keys())[0]
if "enabled_models" not in st.session_state:
    st.session_state["enabled_models"] = list(DEFAULT_ENABLED_MODELS)

gemini_api_key = st.session_state["gemini_api_key"]
gemini_model = st.session_state["gemini_model_name"]

# ── Tabs ─────────────────────────────────────────────────────────────────
tab_us, tab_cn, tab_hk, tab_chat, tab_settings = st.tabs([
    "🇺🇸 美股分析中心", "🇨🇳 A股分析中心", "🇭🇰 港股分析中心",
    "📝 AI 年报问答", "⚙️ 设置",
])

# =====================================================================
#  美股
# =====================================================================
with tab_us:
    us_ticker = st.text_input("美股代码 (Ticker)", value="AAPL", key="us_ticker")

    if st.button("📊 一键分析 (K线 + 下载 + 财务)", key="us_chart", use_container_width=True):
        if not us_ticker:
            st.warning("请输入有效的 Ticker。")
        else:
            ticker = us_ticker.upper()

            # Step 1: Download SEC filings (with real-time log)
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

            # Step 2: Fetch & analyze data
            data = None
            with st.spinner(f"正在获取 {ticker} 价格和财务数据 (yfinance K线 + SEC XBRL)..."):
                try:
                    data = fetch_us_data(ticker)
                    # Load previously saved FCF table if available
                    saved_tbl = load_fcf_table(ticker, "US")
                    if saved_tbl is not None and not saved_tbl.empty:
                        data = dict(data)
                        data["fcf_table"] = saved_tbl
                except Exception as e:
                    st.error(f"数据获取出错: {e}")

            # Step 3: AI fill + validate (always run, whether loaded or fresh)
            if data is not None:
                _us_fcf_tbl = data.get("fcf_table") if isinstance(data, dict) else None
                if _us_fcf_tbl is not None and not _us_fcf_tbl.empty and gemini_api_key:
                    st.divider()
                    st.markdown("#### 🤖 AI 自动读取年报填表 + 验证中...")

                    # Live table: render initial table before AI starts
                    _us_currency = data.get("currency", "USD") if isinstance(data, dict) else "USD"
                    us_tbl_placeholder = st.empty()
                    _us_prev_tbl = [_us_fcf_tbl.copy()]
                    us_tbl_placeholder.markdown(
                        _build_fcf_table_html(_us_fcf_tbl, _us_currency),
                        unsafe_allow_html=True,
                    )

                    def _us_table_callback(updated_tbl):
                        us_tbl_placeholder.markdown(
                            _build_fcf_table_html(updated_tbl, _us_currency, prev_table=_us_prev_tbl[0]),
                            unsafe_allow_html=True,
                        )
                        _us_prev_tbl[0] = updated_tbl.copy()

                    progress_bar = st.progress(0, text="准备中...")
                    log_area = st.empty()
                    all_logs = []

                    us_prog = {
                        "start_ts": time.time(),
                        "pct": 0.0,
                        "tokens": 0,
                        "est_total": 0,
                    }

                    def _fmt_ai_counter(prog_state):
                        elapsed = int(max(0, time.time() - prog_state["start_ts"]))
                        mm, ss = divmod(elapsed, 60)
                        est = prog_state["est_total"]
                        est_str = f" / 估算总计 ~{est:,}" if est else ""
                        return f"已用时 {mm:02d}:{ss:02d} | 已消耗 ~{prog_state['tokens']:,}{est_str} tokens"

                    def us_progress(msg=None, step=None, total=None):
                        if msg:
                            all_logs.append(msg)
                            log_area.text_area("📋 处理日志", "\n".join(reversed(all_logs)), height=300)
                            if "正在等待回复" in msg and "tokens" in msg:
                                m = re.search(r"~\s*([\d,]+)\s*tokens", msg)
                                if m:
                                    us_prog["tokens"] += int(m.group(1).replace(",", ""))
                            elif "合计 ~" in msg:
                                m = re.search(r"合计 ~([\d,]+)\s*tokens", msg)
                                if m:
                                    us_prog["est_total"] += int(m.group(1).replace(",", ""))
                        if step is not None and total and total > 0:
                            us_prog["pct"] = min(step / total, 1.0)
                        progress_bar.progress(us_prog["pct"], text=_fmt_ai_counter(us_prog))

                    try:
                        filled, logs, prompt_info = fill_fcf_table_with_llm(
                            api_key=gemini_api_key,
                            model_name=gemini_model,
                            fcf_table=_us_fcf_tbl.copy(),
                            ticker=ticker,
                            market="US",
                            progress_callback=us_progress,
                            table_update_callback=_us_table_callback,
                            enabled_models=st.session_state.get("enabled_models"),
                        )
                        progress_bar.progress(1.0, text=f"完成 | {_fmt_ai_counter(us_prog)}")
                        # Show prompt used (in expander)
                        with st.expander("📜 查看发送给 Gemini 的 Prompt", expanded=False):
                            st.markdown(f"**System Prompt:**\n```\n{prompt_info['system_prompt']}\n```")
                            st.markdown(f"**规则文件:** `{prompt_info['rules_path']}`")
                            st.markdown(f"**规则内容:**\n```\n{prompt_info['rules']}\n```")
                            for i, bp in enumerate(prompt_info.get("batch_prompts", [])):
                                st.markdown(f"**批次 {i+1} Prompt:**\n```\n{bp[:2000]}{'...(截断)' if len(bp) > 2000 else ''}\n```")
                        # Update data with filled table
                        data = dict(data) if not isinstance(data, dict) else data
                        latest_shares = data.get("shares_outstanding")
                        if latest_shares and latest_shares > 0:
                            filled = recompute_fcf_per_share(filled, latest_shares)
                        data["fcf_table"] = filled
                        # Final update of live table (with per-share values)
                        _us_table_callback(filled)
                        # Save filled table to disk
                        try:
                            saved_path = save_fcf_table(filled, ticker, "US")
                            st.caption(f"📁 表格已保存: {saved_path}")
                        except Exception:
                            pass
                        st.success("AI 年报验证完成!")
                    except Exception as e:
                        st.error(f"AI 补全失败: {e}")

                # Step 4: Apply adjusted per-share FCF + save to session state
                data = _apply_adjusted_fcf(data)
                st.session_state["us_chart_data"] = data
                st.session_state["us_chart_label"] = ticker
                # Clear live table to avoid duplication with render_chart
                us_tbl_placeholder.empty()

    if "us_chart_data" in st.session_state:
        render_chart(st.session_state["us_chart_data"],
                     st.session_state["us_chart_label"])

    render_price_alert(us_ticker, "US", "us",
                        data=st.session_state.get("us_chart_data"))

    # Legacy manual download (collapsed)
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
            if not us_ticker:
                st.warning("请输入有效的 Ticker。")
            else:
                log_container2 = st.empty()
                log_data2 = []

                def sec_logger2(msg):
                    log_data2.append(msg)
                    log_container2.text_area("实时日志", value="\n".join(log_data2), height=250)

                with st.spinner("任务执行中..."):
                    try:
                        dl = SmartSECDownloader(email="lianhdff@gmail.com")
                        cik = dl.get_cik(us_ticker, sec_url_m)
                        sec_logger2(f"✅ 锁定目标 CIK: {cik}")
                        form_filter = us_form_kw.strip() if us_form_kw else None
                        count = dl.download_all(cik, us_ticker, sec_logger2,
                                                form_filter=form_filter)
                        sec_logger2(f"🎉 任务结束! 总计处理 {count} 份文件。")
                        st.success("SEC 报告下载完毕！")
                    except Exception as e:
                        sec_logger2(f"❌ 发生错误: {str(e)}")
                        st.error("下载中断。")

# =====================================================================
#  A股
# =====================================================================
with tab_cn:
    cn_code = st.text_input("A股代码 (6位)", value="", key="cn_code")

    if st.button("📊 生成价格图表", key="cn_chart", use_container_width=True):
        if not cn_code:
            st.warning("请输入有效的 A 股代码。")
        else:
            # Step 1: Fetch data
            data = None
            with st.spinner(f"正在通过 akshare 获取 {cn_code} 数据..."):
                try:
                    data = fetch_cn_data(cn_code)
                    # Load previously saved FCF table if available
                    saved_tbl = load_fcf_table(cn_code, "CN")
                    if saved_tbl is not None and not saved_tbl.empty:
                        data = dict(data)
                        data["fcf_table"] = saved_tbl
                except Exception as e:
                    st.error(f"数据获取出错: {e}")

            # Step 2: AI fill + validate (always run)
            if data is not None:
                _cn_fcf_tbl = data.get("fcf_table") if isinstance(data, dict) else None
                if _cn_fcf_tbl is not None and not _cn_fcf_tbl.empty and gemini_api_key:
                    st.divider()
                    st.markdown("#### 🤖 AI 自动读取年报填表 + 验证中...")

                    # Live table: render initial table before AI starts
                    _cn_currency = data.get("currency", "CNY") if isinstance(data, dict) else "CNY"
                    cn_tbl_placeholder = st.empty()
                    _cn_prev_tbl = [_cn_fcf_tbl.copy()]
                    cn_tbl_placeholder.markdown(
                        _build_fcf_table_html(_cn_fcf_tbl, _cn_currency),
                        unsafe_allow_html=True,
                    )

                    def _cn_table_callback(updated_tbl):
                        cn_tbl_placeholder.markdown(
                            _build_fcf_table_html(updated_tbl, _cn_currency, prev_table=_cn_prev_tbl[0]),
                            unsafe_allow_html=True,
                        )
                        _cn_prev_tbl[0] = updated_tbl.copy()

                    progress_bar = st.progress(0, text="准备中...")
                    log_area = st.empty()
                    all_logs = []

                    cn_prog = {
                        "start_ts": time.time(),
                        "pct": 0.0,
                        "tokens": 0,
                        "est_total": 0,
                    }

                    def _fmt_ai_counter_cn(prog_state):
                        elapsed = int(max(0, time.time() - prog_state["start_ts"]))
                        mm, ss = divmod(elapsed, 60)
                        est = prog_state["est_total"]
                        est_str = f" / 估算总计 ~{est:,}" if est else ""
                        return f"已用时 {mm:02d}:{ss:02d} | 已消耗 ~{prog_state['tokens']:,}{est_str} tokens"

                    def cn_progress(msg=None, step=None, total=None):
                        if msg:
                            all_logs.append(msg)
                            log_area.text_area("📋 处理日志", "\n".join(reversed(all_logs)), height=300)
                            if "正在等待回复" in msg and "tokens" in msg:
                                m = re.search(r"~\s*([\d,]+)\s*tokens", msg)
                                if m:
                                    cn_prog["tokens"] += int(m.group(1).replace(",", ""))
                            elif "合计 ~" in msg:
                                m = re.search(r"合计 ~([\d,]+)\s*tokens", msg)
                                if m:
                                    cn_prog["est_total"] += int(m.group(1).replace(",", ""))
                        if step is not None and total and total > 0:
                            cn_prog["pct"] = min(step / total, 1.0)
                        progress_bar.progress(cn_prog["pct"], text=_fmt_ai_counter_cn(cn_prog))

                    try:
                        filled, logs, prompt_info = fill_fcf_table_with_llm(
                            api_key=gemini_api_key,
                            model_name=gemini_model,
                            fcf_table=_cn_fcf_tbl.copy(),
                            ticker=cn_code,
                            market="CN",
                            progress_callback=cn_progress,
                            table_update_callback=_cn_table_callback,
                            enabled_models=st.session_state.get("enabled_models"),
                        )
                        progress_bar.progress(1.0, text=f"完成 | {_fmt_ai_counter_cn(cn_prog)}")
                        # Show prompt used (in expander)
                        with st.expander("📜 查看发送给 Gemini 的 Prompt", expanded=False):
                            st.markdown(f"**System Prompt:**\n```\n{prompt_info['system_prompt']}\n```")
                            st.markdown(f"**规则文件:** `{prompt_info['rules_path']}`")
                            st.markdown(f"**规则内容:**\n```\n{prompt_info['rules']}\n```")
                            for i, bp in enumerate(prompt_info.get("batch_prompts", [])):
                                st.markdown(f"**批次 {i+1} Prompt:**\n```\n{bp[:2000]}{'...(截断)' if len(bp) > 2000 else ''}\n```")
                        # Update data with filled table
                        data = dict(data) if not isinstance(data, dict) else data
                        latest_shares = data.get("shares_outstanding")
                        if latest_shares and latest_shares > 0:
                            filled = recompute_fcf_per_share(filled, latest_shares)
                        data["fcf_table"] = filled
                        # Final update of live table (with per-share values)
                        _cn_table_callback(filled)
                        # Save filled table to disk
                        try:
                            saved_path = save_fcf_table(filled, cn_code, "CN")
                            st.caption(f"📁 表格已保存: {saved_path}")
                        except Exception:
                            pass
                        st.success("AI 年报验证完成!")
                    except Exception as e:
                        st.error(f"AI 补全失败: {e}")

                # Step 3: Apply adjusted per-share FCF + save to session state
                data = _apply_adjusted_fcf(data)
                st.session_state["cn_chart_data"] = data
                st.session_state["cn_chart_label"] = cn_code
                # Clear live table to avoid duplication with render_chart
                cn_tbl_placeholder.empty()

    if "cn_chart_data" in st.session_state:
        render_chart(st.session_state["cn_chart_data"],
                     st.session_state["cn_chart_label"])

    render_price_alert(cn_code, "CN", "cn",
                        data=st.session_state.get("cn_chart_data"))

    st.divider()
    st.subheader("巨潮资讯报告下载")
    cn_c1, cn_c2 = st.columns(2)
    with cn_c1:
        cn_keyword = st.text_input("报告关键词 (如 年报, 半年报)", key="cn_kw",
                                   placeholder="留空下载全部定期报告")
    with cn_c2:
        cn_dl_btn = st.button("🔍 批量下载", key="cn_dl", use_container_width=True)

    if cn_dl_btn:
        if not cn_code:
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
                    count = dl_cn.download_cn_reports(cn_code,
                                                      cn_keyword.strip() if cn_keyword else "",
                                                      cn_logger)
                    cn_logger(f"🎉 任务结束! 总计处理 {count} 份文件。")
                    st.success("A股报告下载完毕！")
                except Exception as e:
                    cn_logger(f"❌ 发生错误: {str(e)}")
                    st.error("下载中断。")

# =====================================================================
#  港股 (placeholder)
# =====================================================================
with tab_hk:
    hk_code = st.text_input("港股代码 (如 00700)", value="", key="hk_code")

    if st.button("📊 生成价格图表", key="hk_chart", use_container_width=True):
        if not hk_code:
            st.warning("请输入有效的港股代码。")
        else:
            with st.spinner(f"正在通过 OpenD 获取 HK.{hk_code.zfill(5)} 数据..."):
                try:
                    data = fetch_hk_data(hk_code)
                    # Apply adjusted per-share FCF
                    data = _apply_adjusted_fcf(data)
                    st.session_state["hk_chart_data"] = data
                    st.session_state["hk_chart_label"] = f"HK.{hk_code.zfill(5)}"
                except Exception as e:
                    st.error(f"数据获取出错: {e}")

    if "hk_chart_data" in st.session_state:
        render_chart(st.session_state["hk_chart_data"],
                     st.session_state["hk_chart_label"])

    render_price_alert(hk_code, "HK", "hk",
                        data=st.session_state.get("hk_chart_data"))

    st.divider()
    st.info("港股报告下载功能即将上线。")

# =====================================================================
#  AI 年报问答 (Gemini)
# =====================================================================
with tab_chat:
    st.subheader("📝 AI 年报问答")
    st.caption("选择已下载的年报/季报，直接和 Gemini 讨论报告内容")

    # ── Filing selection ──────────────────────────────────────────────
    chat_c1, chat_c2 = st.columns([1, 1])
    with chat_c1:
        chat_market = st.radio(
            "市场", ["美股 (SEC)", "A股 (巨潮)"], horizontal=True, key="chat_market",
        )
    with chat_c2:
        if chat_market == "美股 (SEC)":
            chat_ticker = st.text_input("Ticker", value="", key="chat_ticker",
                                        placeholder="如 BILI, AAPL")
        else:
            chat_ticker = st.text_input("股票代码", value="", key="chat_cn_code",
                                        placeholder="如 002110")

    # List available filings
    if chat_ticker:
        if chat_market == "美股 (SEC)":
            available = list_sec_filings(chat_ticker)
        else:
            available = list_cn_filings(chat_ticker)

        if not available:
            st.info(f"未找到 {chat_ticker} 的已下载报告。请先在分析中心下载报告。")
        else:
            # Separate annual vs quarterly
            annual = [f for f in available if f.get("is_annual")]
            quarterly = [f for f in available if not f.get("is_annual")]

            st.markdown("#### 选择要讨论的报告")
            sel_c1, sel_c2 = st.columns(2)

            selected_paths = []
            selected_labels = []

            with sel_c1:
                st.markdown("**📘 年报 (Annual)**")
                for i, f in enumerate(annual):
                    if st.checkbox(f["label"], key=f"ann_{i}_{chat_ticker}"):
                        selected_paths.append(f["path"])
                        selected_labels.append(f["label"])

            with sel_c2:
                st.markdown("**📗 季报 (Quarterly)**")
                for i, f in enumerate(quarterly):
                    if st.checkbox(f["label"], key=f"qtr_{i}_{chat_ticker}"):
                        selected_paths.append(f["path"])
                        selected_labels.append(f["label"])

            if selected_paths:
                # Show selection summary + token estimate
                context_key = "|".join(sorted(selected_paths))

                # Extract text (cached in session state)
                if st.session_state.get("_chat_ctx_key") != context_key:
                    filing_texts = []
                    total_tokens = 0
                    with st.spinner("正在提取报告文本..."):
                        for path, label in zip(selected_paths, selected_labels):
                            text = extract_text(path)
                            tokens = estimate_tokens(text)
                            total_tokens += tokens
                            filing_texts.append((label, text))
                    st.session_state["_chat_filing_texts"] = filing_texts
                    st.session_state["_chat_total_tokens"] = total_tokens
                    st.session_state["_chat_ctx_key"] = context_key
                    # Reset chat when context changes
                    st.session_state["_chat_session"] = None
                    st.session_state["_chat_messages"] = []

                total_tokens = st.session_state["_chat_total_tokens"]
                filing_texts = st.session_state["_chat_filing_texts"]

                st.info(
                    f"已选 **{len(selected_paths)}** 份报告 | "
                    f"预估 **~{total_tokens:,}** tokens"
                )

                # ── Start / Reset chat button ────────────────────────
                btn_c1, btn_c2 = st.columns([1, 1])
                with btn_c1:
                    start_btn = st.button(
                        "🚀 开始对话" if not st.session_state.get("_chat_session")
                        else "🔄 重新开始",
                        key="chat_start", use_container_width=True,
                    )
                with btn_c2:
                    if st.session_state.get("_chat_session"):
                        if st.button("🗑️ 清空对话", key="chat_clear", use_container_width=True):
                            st.session_state["_chat_session"] = None
                            st.session_state["_chat_messages"] = []
                            st.rerun()

                if start_btn:
                    if not gemini_api_key:
                        st.error("请在 ⚙️ 设置 标签页中输入 Gemini API Key。")
                    else:
                        with st.spinner("正在初始化 Gemini 对话 (发送报告内容)..."):
                            try:
                                chat_session = init_chat(
                                    gemini_api_key, gemini_model, filing_texts,
                                )
                                st.session_state["_chat_session"] = chat_session
                                st.session_state["_chat_messages"] = []
                                st.success("对话已就绪！请在下方输入你的问题。")
                            except Exception as e:
                                st.error(f"初始化 Gemini 失败: {e}")

                # ── Chat interface ───────────────────────────────────
                if st.session_state.get("_chat_session"):
                    st.divider()

                    # Display history
                    for msg in st.session_state.get("_chat_messages", []):
                        with st.chat_message(msg["role"]):
                            st.markdown(msg["content"])

                    # Chat input
                    if prompt := st.chat_input("询问年报内容...", key="chat_input"):
                        # Show user message
                        st.session_state["_chat_messages"].append(
                            {"role": "user", "content": prompt}
                        )
                        with st.chat_message("user"):
                            st.markdown(prompt)

                        # Get Gemini response
                        with st.chat_message("assistant"):
                            with st.spinner("思考中..."):
                                try:
                                    reply = send_message(
                                        st.session_state["_chat_session"], prompt,
                                    )
                                    st.markdown(reply)
                                    st.session_state["_chat_messages"].append(
                                        {"role": "assistant", "content": reply}
                                    )
                                except Exception as e:
                                    err_msg = f"Gemini 请求失败: {e}"
                                    st.error(err_msg)
                                    st.session_state["_chat_messages"].append(
                                        {"role": "assistant", "content": f"⚠️ {err_msg}"}
                                    )

# =====================================================================
#  ⚙️ 设置
# =====================================================================
with tab_settings:
    st.subheader("🤖 Gemini AI 设置")
    st.caption("在此配置 Gemini API Key 和模型。设置会在整个会话中持久生效。")

    _set_c1, _set_c2 = st.columns([2, 1])
    with _set_c1:
        _new_key = st.text_input(
            "Gemini API Key",
            value=st.session_state["gemini_api_key"],
            type="password",
            key="settings_api_key",
            help="从 aistudio.google.com 获取免费 API Key",
        )
    with _set_c2:
        # Only show models with actual quota as primary model options
        _primary_options = [m for m in MODELS
                           if MODEL_RATE_LIMITS.get(m, {}).get("rpd", 0) != 0]
        if not _primary_options:
            _primary_options = list(MODELS.keys())
        _cur_model = st.session_state["gemini_model_name"]
        _primary_idx = _primary_options.index(_cur_model) if _cur_model in _primary_options else 0
        _new_model = st.selectbox(
            "主模型 (优先使用)",
            _primary_options,
            index=_primary_idx,
            format_func=lambda x: MODELS[x],
            key="settings_model",
        )

    # ── Model rotation pool + status dashboard ───────────────────────
    st.divider()
    st.markdown("##### 🔄 模型轮转池 + 调用状态")
    st.caption(
        "勾选启用的模型加入轮转池。触发 429 限流时自动切换到下一个。"
        " ⚠️ Gemma 3 系列 TPM 仅 15K，只适合轻量验证任务，不适合发送完整年报。"
    )

    _current_enabled = set(st.session_state["enabled_models"])
    _call_status = get_model_call_status()
    _new_enabled = []

    # Build status table as HTML
    def _fmt_tpm(tpm):
        if tpm == -1:
            return "无限"
        if tpm == 0:
            return "—"
        if tpm >= 1_000_000:
            return f"{tpm // 1_000_000}M"
        if tpm >= 1_000:
            return f"{tpm // 1_000}K"
        return str(tpm)

    def _fmt_rpd(rpd):
        if rpd == 0:
            return "—"
        return f"{rpd:,}"

    _status_html = """
    <table style="width:100%;border-collapse:collapse;font-size:0.82rem;margin-bottom:8px;">
    <tr style="background:#1a2035;color:#00d4ff;">
        <th style="padding:5px 8px;border:1px solid #1e3a5f;text-align:left;">模型</th>
        <th style="padding:5px 8px;border:1px solid #1e3a5f;text-align:center;">RPM</th>
        <th style="padding:5px 8px;border:1px solid #1e3a5f;text-align:center;">TPM</th>
        <th style="padding:5px 8px;border:1px solid #1e3a5f;text-align:center;">RPD</th>
        <th style="padding:5px 8px;border:1px solid #1e3a5f;text-align:center;">今日调用</th>
        <th style="padding:5px 8px;border:1px solid #1e3a5f;text-align:center;">今日tokens</th>
        <th style="padding:5px 8px;border:1px solid #1e3a5f;text-align:left;">最近状态</th>
    </tr>
    """
    for model_id, model_name_str in MODELS.items():
        lim = MODEL_RATE_LIMITS.get(model_id, {})
        rpm = lim.get("rpm", 0)
        tpm = lim.get("tpm", 0)
        rpd = lim.get("rpd", 0)
        no_quota = (rpm == 0 and rpd == 0)

        # Status
        st_info = _call_status.get(model_id)
        calls_today = 0
        tokens_today = 0
        if st_info:
            st_text = st_info["detail"]
            calls_today = st_info.get("calls_today", 0)
            tokens_today = st_info.get("tokens_today", 0)
            st_color = {
                "ok": "#22c55e",
                "rate_limited": "#f59e0b",
                "cooldown": "#f97316",
                "error": "#ef4444",
            }.get(st_info["status"], "#94a3b8")
        elif no_quota:
            st_text = "⬜ 无配额"
            st_color = "#475569"
        else:
            st_text = "⬜ 未使用"
            st_color = "#94a3b8"

        row_bg = "#0f1729" if no_quota else "#111827"
        name_color = "#475569" if no_quota else "#e0e7ff"
        # Highlight calls approaching RPD limit
        calls_str = f"{calls_today}"
        if rpd > 0 and calls_today > 0:
            pct = calls_today / rpd
            calls_color = "#ef4444" if pct >= 0.9 else "#f59e0b" if pct >= 0.6 else name_color
            calls_str = f"<span style='color:{calls_color}'>{calls_today}/{rpd}</span>"
        tokens_str = f"{tokens_today:,}" if tokens_today else "—"

        _status_html += f"""<tr style="background:{row_bg};">
            <td style="padding:4px 8px;border:1px solid #1e3a5f;color:{name_color};">{model_name_str}</td>
            <td style="padding:4px 8px;border:1px solid #1e3a5f;text-align:center;color:{name_color};">{rpm if rpm else '—'}</td>
            <td style="padding:4px 8px;border:1px solid #1e3a5f;text-align:center;color:{name_color};">{_fmt_tpm(tpm)}</td>
            <td style="padding:4px 8px;border:1px solid #1e3a5f;text-align:center;color:{name_color};">{_fmt_rpd(rpd)}</td>
            <td style="padding:4px 8px;border:1px solid #1e3a5f;text-align:center;">{calls_str}</td>
            <td style="padding:4px 8px;border:1px solid #1e3a5f;text-align:center;color:{name_color};">{tokens_str}</td>
            <td style="padding:4px 8px;border:1px solid #1e3a5f;color:{st_color};">{st_text}</td>
        </tr>"""
    _status_html += "</table>"
    st.markdown(_status_html, unsafe_allow_html=True)

    # Reset status button
    _st_c1, _st_c2 = st.columns([3, 1])
    with _st_c2:
        if st.button("🔄 重置状态", key="reset_model_status", use_container_width=True):
            reset_model_call_status()
            st.rerun()

    # Model selection checkboxes (grouped)
    st.markdown("###### 选择启用的模型:")

    # Group: Gemini models
    st.caption("**Gemini 系列** — 大上下文 (250K TPM)，适合发送完整年报")
    _gemini_cols = st.columns(4)
    _gemini_models = [m for m in MODELS if m.startswith("gemini-")]
    for i, model_id in enumerate(_gemini_models):
        lim = MODEL_RATE_LIMITS.get(model_id, {})
        rpm = lim.get("rpm", 0)
        rpd = lim.get("rpd", 0)
        no_quota = (rpm == 0 and rpd == 0)
        _label = MODELS[model_id]
        if no_quota:
            _label += " (无配额)"

        # Add status icon prefix
        st_info = _call_status.get(model_id)
        if st_info and st_info["status"] == "rate_limited":
            _label = "⚠️ " + _label
        elif st_info and st_info["status"] == "ok":
            _label = "✅ " + _label

        with _gemini_cols[i % 4]:
            _checked = st.checkbox(
                _label,
                value=(model_id in _current_enabled) and not no_quota,
                key=f"model_toggle_{model_id}",
                disabled=no_quota,
            )
            if _checked and not no_quota:
                _new_enabled.append(model_id)

    # Group: Gemma 4 models
    st.caption("**Gemma 4 系列** — 无限 TPM, 15 RPM, 1.5K RPD")
    _gemma4_cols = st.columns(4)
    _gemma4_models = [m for m in MODELS if m.startswith("gemma-4")]
    for i, model_id in enumerate(_gemma4_models):
        lim = MODEL_RATE_LIMITS.get(model_id, {})
        _label = MODELS[model_id]
        st_info = _call_status.get(model_id)
        if st_info and st_info["status"] == "rate_limited":
            _label = "⚠️ " + _label
        elif st_info and st_info["status"] == "ok":
            _label = "✅ " + _label

        with _gemma4_cols[i % 4]:
            _checked = st.checkbox(
                _label,
                value=(model_id in _current_enabled),
                key=f"model_toggle_{model_id}",
            )
            if _checked:
                _new_enabled.append(model_id)

    # Group: Gemma 3 models
    st.caption("**Gemma 3 系列** — 30 RPM, 14.4K RPD, ⚠️ 仅 15K TPM (不适合发大量年报文本)")
    _gemma3_cols = st.columns(4)
    _gemma3_models = [m for m in MODELS if m.startswith("gemma-3")]
    for i, model_id in enumerate(_gemma3_models):
        _label = MODELS[model_id]
        st_info = _call_status.get(model_id)
        if st_info and st_info["status"] == "rate_limited":
            _label = "⚠️ " + _label
        elif st_info and st_info["status"] == "ok":
            _label = "✅ " + _label

        with _gemma3_cols[i % 4]:
            _checked = st.checkbox(
                _label,
                value=(model_id in _current_enabled),
                key=f"model_toggle_{model_id}",
            )
            if _checked:
                _new_enabled.append(model_id)

    if st.button("💾 保存设置", key="save_settings", use_container_width=True):
        st.session_state["gemini_api_key"] = _new_key
        st.session_state["gemini_model_name"] = _new_model
        st.session_state["enabled_models"] = _new_enabled if _new_enabled else [_new_model]
        st.success(f"设置已保存! 轮转池: {len(_new_enabled)} 个模型已启用")
        st.rerun()

    st.divider()
    st.subheader("� FCF 提取规则 (Prompt 核心规则)")
    st.caption(
        "以下规则会作为 Prompt 的一部分发送给 Gemini。你可以直接编辑来调整 AI 的提取行为。"
        f" 文件位置: `{RULES_PATH}`"
    )
    _current_rules = load_fcf_rules()
    _edited_rules = st.text_area(
        "规则内容",
        value=_current_rules,
        height=400,
        key="settings_rules_editor",
    )
    if st.button("💾 保存规则", key="save_rules", use_container_width=True):
        save_fcf_rules(_edited_rules)
        st.success("规则已保存!")

    st.divider()
    st.subheader("📁 已保存的 FCF 表格")
    _saved_dir = os.path.join(os.path.dirname(__file__), "saved_tables")
    if os.path.isdir(_saved_dir):
        _ticker_dirs = sorted(
            [d for d in os.listdir(_saved_dir)
             if os.path.isdir(os.path.join(_saved_dir, d))],
            reverse=True,
        )
        if _ticker_dirs:
            for _td in _ticker_dirs:
                _td_path = os.path.join(_saved_dir, _td)
                _files = sorted(os.listdir(_td_path), reverse=True)
                st.markdown(f"**{_td}/**")
                for sf in _files[:10]:
                    st.text(f"  {sf}")
        else:
            st.info("暂无已保存的表格。运行 AI 填表后将自动保存。")
    else:
        st.info("暂无已保存的表格。运行 AI 填表后将自动保存。")
