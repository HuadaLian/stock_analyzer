# app.py
import streamlit as st
import pandas as pd
import os
from analyzers import USAnalyzer, CNAnalyzer, HKAnalyzer
from chart_store import load_chart
from analysis_tracker import get_analyzed_tickers
from gemini_chat import (
    MODELS, MODEL_RATE_LIMITS, DEFAULT_ENABLED_MODELS, RULES_PATH,
    _MODEL_CAPABILITY_RANK,
    load_fcf_rules, save_fcf_rules,
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

# ── Session ID (stable across reruns, unique per browser session) ─────────
import uuid as _uuid
if "_sid" not in st.session_state:
    st.session_state["_sid"] = str(_uuid.uuid4())

# ── Tabs ─────────────────────────────────────────────────────────────────
tab_us, tab_cn, tab_hk, tab_reviewed, tab_settings = st.tabs([
    "🇺🇸 美股分析中心", "🇨🇳 A股分析中心", "🇭🇰 港股分析中心",
    "📋 已分析股票", "⚙️ 设置",
])

# =====================================================================
#  美股
# =====================================================================
with tab_us:
    USAnalyzer().run()

# =====================================================================
#  A股
# =====================================================================
with tab_cn:
    CNAnalyzer().run()

# =====================================================================
#  港股
# =====================================================================
with tab_hk:
    HKAnalyzer().run()

# =====================================================================
#  已分析股票 — cross-market browsing
# =====================================================================
def _infer_market(ticker: str) -> str:
    """Guess the market from ticker format when no 'market' tag is stored."""
    t = ticker.strip()
    if t.replace(".", "").isdigit():
        return "HK" if len(t) <= 5 else "CN"
    return "US"


def _has_fcf_table(ticker: str, market: str) -> bool:
    ticker_dir = os.path.join(os.path.dirname(__file__), "saved_tables", f"{ticker}_{market}")
    return os.path.isdir(ticker_dir) and any(
        f.endswith(".csv") for f in os.listdir(ticker_dir)
    )


def _dcf_metrics_from_data(data: dict) -> tuple[float, float]:
    """Return (inv_potential_pct, short_potential_pct) or (nan, nan).

    inv_potential_pct   = (dcf_14x - price) / price * 100  (positive = undervalued vs 14x)
    short_potential_pct = (price - dcf_34x) / price * 100  (positive = overvalued vs 34x)
    Both are nan when latest 3-yr avg FCF <= 0.
    """
    from data_provider import compute_dcf_lines
    price = data.get("last_price")
    fcf_ps = data.get("fcf_per_share_by_year", {})
    if not price or not fcf_ps:
        return float("nan"), float("nan")
    dcf_df = compute_dcf_lines(fcf_ps)
    if dcf_df.empty:
        return float("nan"), float("nan")
    dcf_14x = dcf_df["dcf_14x"].iloc[-1]
    dcf_34x = dcf_df["dcf_34x"].iloc[-1]
    if dcf_14x <= 0:
        return float("nan"), float("nan")
    inv_pot   = (dcf_14x - price) / price * 100
    short_pot = (price - dcf_34x) / price * 100
    return inv_pot, short_pot


def _render_reviewed_market(market: str, analyzer, universe_key: str | None):
    from gemini_chat import load_fcf_table

    analyzed = get_analyzed_tickers()
    currency = {"US": "USD", "CN": "CNY", "HK": "HKD"}.get(market, "USD")

    # Filter tickers belonging to this market
    market_items = {}
    for tk, info in analyzed.items():
        stored_mkt = info.get("market", "").upper()
        inferred = stored_mkt if stored_mkt else _infer_market(tk)
        if inferred == market:
            market_items[tk] = info

    if not market_items:
        label = {"US": "美股", "CN": "A股", "HK": "港股"}.get(market, market)
        st.info(f"尚未分析任何{label}。请在对应分析中心中完成首次分析后再来查看。")
        return

    universe = st.session_state.get(universe_key, {}) if universe_key else {}

    # ── Sort controls ────────────────────────────────────────────────
    sc1, sc2 = st.columns([3, 1])
    with sc1:
        sort_by = st.radio(
            "排列方式",
            ["分析时间 (最新)", "市值 (最大)", "投资潜力 (最高)", "做空潜力 (最高)", "代码 (升序)"],
            horizontal=True,
            key=f"rev_{market}_sort",
            label_visibility="collapsed",
        )
    with sc2:
        show_limit = st.selectbox(
            "显示条数", [50, 100, 200, 500], index=1,
            key=f"rev_{market}_limit",
            label_visibility="collapsed",
        )

    # ── Compute DCF metrics for each ticker ──────────────────────────
    import math

    def _get_dcf_metrics(tk, info):
        # Try cached tracker metadata first (stored after each analysis)
        dcf_14x = info.get("dcf_14x")
        dcf_34x = info.get("dcf_34x")
        price   = info.get("last_price")
        if dcf_14x and dcf_34x and price and dcf_14x > 0:
            inv_pot   = (dcf_14x - price) / price * 100
            short_pot = (price - dcf_34x) / price * 100
            return inv_pot, short_pot
        # Fallback: load from pickle (cached in session_state to avoid re-loading)
        cache_key = f"_dcf_cache_{market}_{tk}"
        if cache_key in st.session_state:
            return st.session_state[cache_key]
        try:
            chart_data = load_chart(tk, market)
            if chart_data:
                result = _dcf_metrics_from_data(chart_data)
                # Write back to tracker so future sessions skip the pickle load
                if not math.isnan(result[0]):
                    try:
                        from data_provider import compute_dcf_lines
                        from analysis_tracker import patch_metadata
                        p = chart_data.get("last_price")
                        fcf_ps = chart_data.get("fcf_per_share_by_year", {})
                        if p and fcf_ps:
                            dcf_df = compute_dcf_lines(fcf_ps)
                            if not dcf_df.empty:
                                patch_metadata(tk, {
                                    "last_price": float(p),
                                    "dcf_14x": float(dcf_df["dcf_14x"].iloc[-1]),
                                    "dcf_34x": float(dcf_df["dcf_34x"].iloc[-1]),
                                })
                    except Exception:
                        pass
            else:
                result = (float("nan"), float("nan"))
        except Exception:
            result = (float("nan"), float("nan"))
        st.session_state[cache_key] = result
        return result

    items = [
        (tk, info, float(info.get("market_cap") or 0), *_get_dcf_metrics(tk, info))
        for tk, info in market_items.items()
    ]

    _nan_last_desc = lambda v: v if not math.isnan(v) else -math.inf

    if sort_by == "市值 (最大)":
        items.sort(key=lambda x: x[2], reverse=True)
    elif sort_by == "投资潜力 (最高)":
        items.sort(key=lambda x: _nan_last_desc(x[3]), reverse=True)
    elif sort_by == "做空潜力 (最高)":
        items.sort(key=lambda x: _nan_last_desc(x[4]), reverse=True)
    elif sort_by == "代码 (升序)":
        items.sort(key=lambda x: x[0])
    else:
        items.sort(key=lambda x: x[1].get("timestamp", ""), reverse=True)

    visible_items = items[:show_limit]

    # ── Build display DataFrame ───────────────────────────────────────
    rows = []
    for tk, info, mcap_num, inv_pot, short_pot in visible_items:
        univ = universe.get(tk, {}) if universe else {}
        name    = univ.get("name", info.get("name", ""))
        ts      = info.get("timestamp", "?")[:16]
        status  = info.get("status", "?")
        status_str = "✅ 完成" if status == "complete" else "⚠️ 错误" if status == "error" else "🔄"
        mcap_str = analyzer.fmt_val(mcap_num, currency=currency) if mcap_num else "—"
        row = {"代码": tk, "公司名": name}
        if market == "CN":
            row["行业"] = univ.get("industry", "")
            row["地区"] = univ.get("area", "")
        row["市值"] = mcap_str
        row["投资潜力%"] = None if math.isnan(inv_pot) else round(inv_pot, 2)
        row["做空潜力%"] = None if math.isnan(short_pot) else round(short_pot, 2)
        row["状态"] = status_str
        row["分析时间"] = ts
        rows.append(row)

    df = pd.DataFrame(rows)

    st.caption("点击任意行查看该股票图表 ↓")
    event = st.dataframe(
        df,
        column_config={
            "投资潜力%": st.column_config.NumberColumn("投资潜力%", format="%.1f%%"),
            "做空潜力%": st.column_config.NumberColumn("做空潜力%", format="%.1f%%"),
        },
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        height=min(420, 38 + len(df) * 35),
        key=f"rev_{market}_df",
    )

    if len(items) > show_limit:
        st.caption(f"显示 {show_limit} / {len(items)} 条")

    # ── Row click → load chart ────────────────────────────────────────
    sel_rows = event.selection.rows if hasattr(event, "selection") else []
    if sel_rows:
        sel_ticker = visible_items[sel_rows[0]][0]
        if st.session_state.get(f"rev_{market}_ticker") != sel_ticker:
            data = None
            with st.spinner(f"正在加载 {sel_ticker}..."):
                data = load_chart(sel_ticker, market)
                if data is None:
                    try:
                        data = analyzer.fetch_data(sel_ticker)
                        saved_tbl = load_fcf_table(sel_ticker, market)
                        if saved_tbl is not None and not saved_tbl.empty:
                            data = dict(data)
                            data["fcf_table"] = saved_tbl
                        data = analyzer._apply_adjusted_fcf(data)
                    except Exception as e:
                        st.error(f"数据加载失败: {e}")
                        data = None
            if data is not None:
                st.session_state[f"rev_{market}_data"] = data
                st.session_state[f"rev_{market}_ticker"] = sel_ticker
                st.rerun()
            else:
                st.warning(f"⚠️ 无法加载 {sel_ticker} 的图表，请重试或在对应分析中心重新分析")
                st.session_state.pop(f"rev_{market}_ticker", None)
                st.session_state.pop(f"rev_{market}_data", None)

    # ── Render chart for selected ticker ─────────────────────────────
    cached_ticker = st.session_state.get(f"rev_{market}_ticker")
    cached_data   = st.session_state.get(f"rev_{market}_data")
    if cached_data and cached_ticker:
        st.divider()
        label = analyzer.format_label(cached_ticker)
        analyzer.render_chart(cached_data, label, show_fcf_table=False)

        # FCF table — shown between chart and price alert
        fcf_table = cached_data.get("fcf_table")
        if fcf_table is not None and not fcf_table.empty:
            currency = cached_data.get("currency", {"US": "USD", "CN": "CNY", "HK": "HKD"}.get(market, "USD"))
            source = cached_data.get("source", "")
            st.markdown(
                analyzer._build_fcf_table_html(fcf_table, currency, source=source),
                unsafe_allow_html=True,
            )

        analyzer.render_price_alert(cached_ticker, data=cached_data, key_suffix="rev")


with tab_reviewed:
    st.subheader("📋 已分析股票")
    rev_us, rev_cn, rev_hk = st.tabs(["🇺🇸 美股", "🇨🇳 A股", "🇭🇰 港股"])

    with rev_us:
        _render_reviewed_market("US", USAnalyzer(), "us_universe")

    with rev_cn:
        _render_reviewed_market("CN", CNAnalyzer(), "cn_universe")

    with rev_hk:
        _render_reviewed_market("HK", HKAnalyzer(), None)


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
    import time as _time_mod
    st.divider()
    st.markdown("##### 🔄 模型轮转池 + 调用状态")
    st.caption(
        "模型按优先级排序 (Priority #0 = 最先尝试)。"
        "触发 429 限流时自动按优先级切换到下一个可用模型。"
    )

    _current_enabled = set(st.session_state["enabled_models"])
    _call_status = get_model_call_status()
    _new_enabled = []

    # ── Sort MODELS by _MODEL_CAPABILITY_RANK ────────────────────────
    _sorted_model_ids = sorted(
        MODELS.keys(),
        key=lambda m: (_MODEL_CAPABILITY_RANK.get(m, 99), m)
    )

    def _fmt_tpm(tpm):
        if tpm == -1: return "无限"
        if tpm == 0:  return "—"
        if tpm >= 1_000_000: return f"{tpm // 1_000_000}M"
        if tpm >= 1_000:     return f"{tpm // 1_000}K"
        return str(tpm)

    def _fmt_ago(ts):
        if not ts: return "—"
        secs = int(_time_mod.time() - float(ts))
        if secs < 60:   return f"{secs}秒前"
        if secs < 3600: return f"{secs // 60}分钟前"
        return f"{secs // 3600}小时前"

    def _fmt_cooldown(until_ts):
        if not until_ts: return ""
        remaining = int(float(until_ts) - _time_mod.time())
        if remaining <= 0: return ""
        if remaining < 60:   return f"冷却 {remaining}s"
        return f"冷却 {remaining // 60}分钟"

    _status_html = """
    <table style="width:100%;border-collapse:collapse;font-size:0.82rem;margin-bottom:8px;">
    <tr style="background:#1a2035;color:#00d4ff;">
        <th style="padding:5px 8px;border:1px solid #1e3a5f;text-align:center;">优先级</th>
        <th style="padding:5px 8px;border:1px solid #1e3a5f;text-align:left;">模型</th>
        <th style="padding:5px 8px;border:1px solid #1e3a5f;text-align:center;">RPM</th>
        <th style="padding:5px 8px;border:1px solid #1e3a5f;text-align:center;">TPM</th>
        <th style="padding:5px 8px;border:1px solid #1e3a5f;text-align:center;">RPD</th>
        <th style="padding:5px 8px;border:1px solid #1e3a5f;text-align:center;">今日调用</th>
        <th style="padding:5px 8px;border:1px solid #1e3a5f;text-align:center;">今日 tokens</th>
        <th style="padding:5px 8px;border:1px solid #1e3a5f;text-align:left;">最近调用</th>
        <th style="padding:5px 8px;border:1px solid #1e3a5f;text-align:left;">状态 / 详情</th>
    </tr>
    """
    for model_id in _sorted_model_ids:
        model_name_str = MODELS[model_id]
        priority = _MODEL_CAPABILITY_RANK.get(model_id, 99)
        lim = MODEL_RATE_LIMITS.get(model_id, {})
        rpm  = lim.get("rpm", 0)
        tpm  = lim.get("tpm", 0)
        rpd  = lim.get("rpd", 0)
        no_quota = (rpm == 0 and rpd == 0)
        is_enabled = model_id in _current_enabled

        st_info       = _call_status.get(model_id)
        calls_today   = st_info.get("calls_today",  0) if st_info else 0
        tokens_today  = st_info.get("tokens_today", 0) if st_info else 0
        last_ts       = st_info.get("time",          0) if st_info else 0
        cooldown_ts   = st_info.get("cooldown_until",0) if st_info else 0
        st_status     = st_info.get("status", "unused") if st_info else "unused"
        st_detail     = st_info.get("detail", "") if st_info else ""

        # Status colour + text
        if no_quota:
            st_text  = "⬜ 无配额"
            st_color = "#475569"
        elif st_status == "ok":
            st_text  = st_detail or "✅ 正常"
            st_color = "#22c55e"
        elif st_status == "rate_limited":
            cd = _fmt_cooldown(cooldown_ts)
            st_text  = f"⚠️ 限流 {cd}".strip()
            st_color = "#f59e0b"
        elif st_status == "cooldown":
            cd = _fmt_cooldown(cooldown_ts)
            st_text  = f"🧊 冷却 {cd}".strip()
            st_color = "#f97316"
        elif st_status in ("error", "not_found"):
            st_text  = st_detail[:60] if st_detail else "❌ 错误"
            st_color = "#ef4444"
        else:
            st_text  = "⬜ 未使用"
            st_color = "#64748b"

        row_bg     = "#0f1729" if no_quota else ("#1a2540" if is_enabled else "#111827")
        name_color = "#475569" if no_quota else ("#00d4ff" if is_enabled else "#94a3b8")
        priority_badge = (
            f'<span style="color:#00d4ff;font-weight:700">#{priority}</span>'
            if not no_quota else
            f'<span style="color:#475569">#{priority}</span>'
        )

        # Calls today with RPD bar
        calls_str = f"{calls_today}"
        if rpd > 0 and calls_today > 0:
            pct = calls_today / rpd
            c_clr = "#ef4444" if pct >= 0.9 else "#f59e0b" if pct >= 0.6 else name_color
            calls_str = f"<span style='color:{c_clr}'>{calls_today}/{rpd}</span>"

        tokens_str  = f"{tokens_today:,}" if tokens_today else "—"
        last_called = _fmt_ago(last_ts) if last_ts else "—"

        _status_html += f"""<tr style="background:{row_bg};">
            <td style="padding:4px 8px;border:1px solid #1e3a5f;text-align:center;">{priority_badge}</td>
            <td style="padding:4px 8px;border:1px solid #1e3a5f;color:{name_color};white-space:nowrap;">{model_name_str}</td>
            <td style="padding:4px 8px;border:1px solid #1e3a5f;text-align:center;color:{name_color};">{rpm or '—'}</td>
            <td style="padding:4px 8px;border:1px solid #1e3a5f;text-align:center;color:{name_color};">{_fmt_tpm(tpm)}</td>
            <td style="padding:4px 8px;border:1px solid #1e3a5f;text-align:center;color:{name_color};">{f'{rpd:,}' if rpd else '—'}</td>
            <td style="padding:4px 8px;border:1px solid #1e3a5f;text-align:center;">{calls_str}</td>
            <td style="padding:4px 8px;border:1px solid #1e3a5f;text-align:center;color:{name_color};">{tokens_str}</td>
            <td style="padding:4px 8px;border:1px solid #1e3a5f;color:#64748b;white-space:nowrap;">{last_called}</td>
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

    # ── Model selection checkboxes — in priority order ────────────────
    st.markdown("###### 选择启用的模型 (按优先级排列):")

    # Group: Gemma 4 (priority #0–#1) — highest priority
    st.caption("**Gemma 4 系列** ⭐ 最高优先级 — 无限 TPM, 30 RPM, 14.4K RPD")
    _gemma4_cols = st.columns(4)
    _gemma4_models = [m for m in _sorted_model_ids if m.startswith("gemma-4")]
    for i, model_id in enumerate(_gemma4_models):
        _label = f"#{_MODEL_CAPABILITY_RANK.get(model_id,99)} {MODELS[model_id]}"
        _si = _call_status.get(model_id)
        if _si:
            if _si["status"] == "rate_limited": _label = "⚠️ " + _label
            elif _si["status"] == "ok":         _label = "✅ " + _label
            elif _si["status"] == "cooldown":   _label = "🧊 " + _label
        with _gemma4_cols[i % 4]:
            if st.checkbox(_label, value=(model_id in _current_enabled),
                           key=f"model_toggle_{model_id}"):
                _new_enabled.append(model_id)

    # Group: Gemma 3 (priority #2)
    st.caption("**Gemma 3 系列** — 60 RPM, 30K TPM, 14.4K RPD")
    _gemma3_cols = st.columns(4)
    _gemma3_models = [m for m in _sorted_model_ids if m.startswith("gemma-3")]
    for i, model_id in enumerate(_gemma3_models):
        _label = f"#{_MODEL_CAPABILITY_RANK.get(model_id,99)} {MODELS[model_id]}"
        _si = _call_status.get(model_id)
        if _si:
            if _si["status"] == "rate_limited": _label = "⚠️ " + _label
            elif _si["status"] == "ok":         _label = "✅ " + _label
            elif _si["status"] == "cooldown":   _label = "🧊 " + _label
        with _gemma3_cols[i % 4]:
            if st.checkbox(_label, value=(model_id in _current_enabled),
                           key=f"model_toggle_{model_id}"):
                _new_enabled.append(model_id)

    # Group: Gemini (priority #3–#10) — last resort
    st.caption("**Gemini 系列** — 最后备用，1M TPM，RPD 较低 (1,500)")
    _gemini_cols = st.columns(4)
    _gemini_models = [m for m in _sorted_model_ids if m.startswith("gemini-")]
    for i, model_id in enumerate(_gemini_models):
        lim = MODEL_RATE_LIMITS.get(model_id, {})
        no_quota = (lim.get("rpm", 0) == 0 and lim.get("rpd", 0) == 0)
        _label = f"#{_MODEL_CAPABILITY_RANK.get(model_id,99)} {MODELS[model_id]}"
        if no_quota: _label += " (无配额)"
        _si = _call_status.get(model_id)
        if _si and not no_quota:
            if _si["status"] == "rate_limited": _label = "⚠️ " + _label
            elif _si["status"] == "ok":         _label = "✅ " + _label
            elif _si["status"] == "cooldown":   _label = "🧊 " + _label
        with _gemini_cols[i % 4]:
            if st.checkbox(_label,
                           value=(model_id in _current_enabled) and not no_quota,
                           key=f"model_toggle_{model_id}",
                           disabled=no_quota):
                if not no_quota:
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
