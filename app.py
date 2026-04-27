# app.py
import streamlit as st
import pandas as pd
import os
import time
from pathlib import Path
from db.schema import get_conn, DB_PATH, READ_DB_ENV
from etl.pipeline import USRunOptions, run_us_ticker
from etl.snapshot import snapshot_db

# Auto-detect read replica BEFORE anything else can call get_conn(readonly=True).
# If STOCK_ANALYZER_READ_DB is unset and stock_read.db exists next to stock.db,
# point the env at it so this Streamlit session reads the snapshot (lets bulk
# keep running concurrently). User-set env vars are preserved.
from dashboards.db_status import bootstrap_read_replica, render_status_caption
from dashboards.db_quality import render_db_quality_tab
from dashboards.symbol_registry import search_registry_options
from dashboards.factor_lab import render_factor_lab
bootstrap_read_replica()

from analyzers import USAnalyzer
from dashboards.d1_fcf_multiple import render_d1_stock
from dashboards.d2_business import render_d2_stock
from dashboards.d3_industry import render_d3_stock
from chart_store import load_chart
from analysis_tracker import get_analyzed_tickers
from core.symbol_router import parse_global_symbol, apply_global_selection
from gemini_chat import (
    MODELS, MODEL_RATE_LIMITS, DEFAULT_ENABLED_MODELS, RULES_PATH,
    _MODEL_CAPABILITY_RANK,
    load_fcf_rules, save_fcf_rules,
    get_model_call_status, reset_model_call_status,
)

# ── Page config ──────────────────────────────────────────────────────────
st.set_page_config(page_title="Stock Analyzer", layout="wide")

# ── Data-source status (副本/主库 + freshness) ─────────────────────────────
render_status_caption(st)

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
tab_stock, tab_reviewed, tab_db_quality, tab_settings = st.tabs([
    "📈 个股分析中心",
    "🧪 因子分析",
    "🧪 数据库质量检测",
    "⚙️ 设置",
])

# Keep reviewed-page implementation in codebase, but disable in UI for now.
ENABLE_REVIEWED_TAB = False


def _needs_fmp_fill(ticker: str) -> bool:
    """True when ticker is missing or only partially populated in DB."""
    t = (ticker or "").strip().upper()
    if not t:
        return False
    with get_conn(readonly=True) as conn:
        c_row = conn.execute("SELECT COUNT(*) FROM companies WHERE ticker = ?", [t]).fetchone()
        o_row = conn.execute("SELECT COUNT(*) FROM ohlcv_daily WHERE ticker = ?", [t]).fetchone()
        f_row = conn.execute("SELECT COUNT(*) FROM fundamentals_annual WHERE ticker = ?", [t]).fetchone()
    has_company = bool(c_row and int(c_row[0]) > 0)
    ohlcv_n = int(o_row[0]) if o_row else 0
    fund_n = int(f_row[0]) if f_row else 0
    return (not has_company) or ohlcv_n == 0 or fund_n == 0


def _fill_single_ticker_from_fmp(ticker: str) -> tuple[bool, str]:
    t = (ticker or "").strip().upper()
    if not t:
        return False, "ticker 为空"
    try:
        with get_conn() as conn:
            run_us_ticker(
                conn,
                t,
                USRunOptions(skip_optional=True, verbose=False, refresh_mode="full"),
            )
        return True, f"已从 FMP 填充 {t}"
    except Exception as e:
        return False, f"FMP 填充失败: {e}"


def _sync_read_replica_after_fill() -> tuple[bool, str]:
    """Best-effort sync: stock.db -> read replica after single-ticker fill."""
    mirror = os.environ.get(READ_DB_ENV, "").strip()
    if mirror:
        dst = Path(mirror).expanduser()
    else:
        dst = DB_PATH.parent / "stock_read.db"
        if not dst.is_file():
            return False, "未检测到 read 副本，已仅更新主库"
    ok, _bytes, msg = snapshot_db(DB_PATH, dst)
    if ok:
        return True, f"read 副本已刷新（{dst.name}）"
    return False, f"read 副本刷新失败：{msg}"

# =====================================================================
#  个股分析中心（D1 + D2 + D3）
# =====================================================================
with tab_stock:
    st.subheader("📈 个股分析中心")
    st.caption("一个搜索框覆盖全球普通股（US/ADR/OTC、A股、港股）。")

    # Streamlit: widget key must only be mutated before widget instantiation.
    pending_query = st.session_state.pop("_global_symbol_query_pending", None)
    if pending_query is not None:
        st.session_state["global_symbol_query"] = pending_query

    q_col, btn_col = st.columns([5, 1])
    with q_col:
        symbol_query = st.text_input(
            "搜索代码",
            value=st.session_state.get("global_symbol_query", ""),
            placeholder="例如: AAPL / BABA / 600519 / 000001.SZ / 00700 / 0700.HK",
            key="global_symbol_query",
        )
        options = search_registry_options(symbol_query, limit=80)
        if options:
            picked = st.selectbox(
                "快速联想",
                options=[""] + [o["label"] for o in options],
                index=0,
                key="global_symbol_pick",
                help="可选：从候选列表直接选中",
            )
            if picked:
                sel = next((o for o in options if o["label"] == picked), None)
                last_picked = st.session_state.get("_global_symbol_last_pick_label", "")
                if sel and picked != last_picked:
                    st.session_state["_global_symbol_query_pending"] = sel["ticker"]
                    apply_global_selection(st.session_state, sel["market"], sel["ticker"])
                    st.session_state["_global_symbol_last_pick_label"] = picked
                    st.rerun()
            else:
                st.session_state.pop("_global_symbol_last_pick_label", None)
    with btn_col:
        st.markdown("<div style='margin-top: 28px'></div>", unsafe_allow_html=True)
        do_analyze_global = st.button("分析", key="global_analyze_btn", use_container_width=True)

    if "global_selected_market" not in st.session_state:
        st.session_state["global_selected_market"] = "US"
    if "global_selected_ticker" not in st.session_state:
        st.session_state["global_selected_ticker"] = "NVDA"
    if "active_market" not in st.session_state:
        st.session_state["active_market"] = st.session_state["global_selected_market"]
    if "active_ticker" not in st.session_state:
        st.session_state["active_ticker"] = st.session_state["global_selected_ticker"]

    if do_analyze_global and symbol_query.strip():
        mkt, tk = parse_global_symbol(symbol_query)
        apply_global_selection(st.session_state, mkt, tk)
        with st.spinner(f"正在检查并补全 {tk} 数据..."):
            if _needs_fmp_fill(tk):
                ok, msg = _fill_single_ticker_from_fmp(tk)
                if ok:
                    rep_ok, rep_msg = _sync_read_replica_after_fill()
                    if rep_ok:
                        st.success(f"{msg}；{rep_msg}")
                    else:
                        st.warning(f"{msg}；{rep_msg}")
                else:
                    st.warning(msg)

    selected_market = st.session_state.get("global_selected_market", "US")
    selected_ticker = st.session_state.get("global_selected_ticker", "NVDA")
    st.caption(f"当前目标: `{selected_market}` · `{selected_ticker}`")

    if selected_market in ("US", "CN", "HK"):
        ticker = render_d1_stock(
            market=selected_market,
            ticker_override=selected_ticker,
        )
        st.divider()
        col_d2, col_d3 = st.columns([1, 1], gap="medium")
        with col_d2:
            render_d2_stock(ticker=ticker, market=selected_market)
        with col_d3:
            render_d3_stock(ticker=ticker, market=selected_market)

# =====================================================================
#  已分析股票 — cross-market browsing
# =====================================================================
def _infer_market(ticker: str) -> str:
    """Guess the market from ticker format when no 'market' tag is stored."""
    t = ticker.strip()
    if t.replace(".", "").isdigit():
        return "GLOBAL"
    return "GLOBAL"


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

    def _resolve_chart_market(tk: str, info: dict) -> str:
        """Map mixed legacy/new tracker market tags to chart-store market folder."""
        stored = str(info.get("market") or "").upper()
        if stored in ("US", "CN", "HK"):
            return stored
        inferred = _infer_market(tk)
        if inferred in ("US", "CN", "HK"):
            return inferred
        # GLOBAL mode currently reuses US chart/store path for single-stock UI.
        return "US"

    analyzed = get_analyzed_tickers()
    currency = {"US": "USD", "CN": "CNY", "HK": "HKD", "GLOBAL": "USD"}.get(market, "USD")

    # Filter tickers belonging to this market
    market_items = {}
    for tk, info in analyzed.items():
        stored_mkt = info.get("market", "").upper()
        inferred = stored_mkt if stored_mkt else _infer_market(tk)
        if market == "GLOBAL":
            # Global reviewed page keeps single-stock (US/GLOBAL) flow only.
            if inferred in ("CN", "HK"):
                continue
            market_items[tk] = info
        elif inferred == market:
            market_items[tk] = info

    if not market_items:
        label = {"US": "美股", "CN": "A股", "HK": "港股", "GLOBAL": "个股"}.get(market, market)
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
        page_size = st.selectbox(
            "每页条数", [50, 100, 200], index=0,
            key=f"rev_{market}_page_size",
            label_visibility="collapsed",
        )

    # ── Build or reuse cached items list ─────────────────────────────
    # Version key: invalidates when a new analysis is added or the latest timestamp changes.
    # This means the expensive per-ticker loop runs at most once per session (or after
    # a new analysis completes).  All sort/limit changes operate on the cached list.
    import math

    last_ts = max((info.get("timestamp", "") for info in market_items.values()), default="")
    _cache_ver = (len(market_items), last_ts)
    _items_key = f"_rev_{market}_items"
    _items_ver_key = f"_rev_{market}_items_ver"

    if st.session_state.get(_items_ver_key) != _cache_ver:
        # Cache miss — build the full list (expensive on first load with many tickers)
        def _get_dcf_metrics(tk, info):
            # Fast path: all three metrics already stored in tracker.json
            dcf_14x = info.get("dcf_14x")
            dcf_34x = info.get("dcf_34x")
            price   = info.get("last_price")
            if dcf_14x and dcf_34x and price and dcf_14x > 0:
                inv_pot   = (dcf_14x - price) / price * 100
                short_pot = (price - dcf_34x) / price * 100
                return inv_pot, short_pot
            # Slow path: load pickle (also cached in session_state per-ticker)
            dcf_cache_key = f"_dcf_cache_{market}_{tk}"
            if dcf_cache_key in st.session_state:
                return st.session_state[dcf_cache_key]
            try:
                chart_data = load_chart(tk, _resolve_chart_market(tk, info))
                if chart_data:
                    result = _dcf_metrics_from_data(chart_data)
                    # Write back to tracker so future sessions always hit the fast path
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
            st.session_state[dcf_cache_key] = result
            return result

        items = [
            (tk, info, float(info.get("market_cap") or 0), *_get_dcf_metrics(tk, info))
            for tk, info in market_items.items()
        ]
        st.session_state[_items_key] = items
        st.session_state[_items_ver_key] = _cache_ver
    else:
        # Cache hit — reuse without any I/O or per-ticker computation
        items = st.session_state[_items_key]

    # ── Sort (always fast: in-memory sort of cached list) ─────────────
    _nan_last_desc = lambda v: v if not math.isnan(v) else -math.inf

    if sort_by == "市值 (最大)":
        items = sorted(items, key=lambda x: x[2], reverse=True)
    elif sort_by == "投资潜力 (最高)":
        items = sorted(items, key=lambda x: _nan_last_desc(x[3]), reverse=True)
    elif sort_by == "做空潜力 (最高)":
        items = sorted(items, key=lambda x: _nan_last_desc(x[4]), reverse=True)
    elif sort_by == "代码 (升序)":
        items = sorted(items, key=lambda x: x[0])
    else:
        items = sorted(items, key=lambda x: x[1].get("timestamp", ""), reverse=True)

    # Store sorted ticker order so the autoplay fragment can find the next one
    _sorted_tickers = [tk for tk, *_ in items]
    st.session_state[f"_rev_{market}_sorted_tickers"] = _sorted_tickers

    # ── Pagination ────────────────────────────────────────────────────
    _page_key  = f"rev_{market}_page"
    _total     = len(items)
    _n_pages   = max(1, (_total + page_size - 1) // page_size)

    # Reset to page 1 when sort order or page size changes
    _sort_prev_key = f"_rev_{market}_sort_prev"
    _size_prev_key = f"_rev_{market}_size_prev"
    if (st.session_state.get(_sort_prev_key) != sort_by
            or st.session_state.get(_size_prev_key) != page_size):
        st.session_state[_page_key] = 1
        st.session_state[_sort_prev_key] = sort_by
        st.session_state[_size_prev_key] = page_size

    # Clamp page to valid range (e.g. after filtering)
    if st.session_state.get(_page_key, 1) > _n_pages:
        st.session_state[_page_key] = _n_pages

    pg_c1, pg_c2, pg_c3, pg_c4, pg_c5 = st.columns([1, 1, 2, 1, 3])
    with pg_c1:
        _prev_clicked = st.button(
            "◀ 上一页", key=f"rev_{market}_prev_pg",
            disabled=(st.session_state.get(_page_key, 1) <= 1),
            use_container_width=True,
        )
    with pg_c2:
        _next_clicked = st.button(
            "下一页 ▶", key=f"rev_{market}_next_pg",
            disabled=(st.session_state.get(_page_key, 1) >= _n_pages),
            use_container_width=True,
        )
    with pg_c3:
        _cur_page = st.number_input(
            f"/ {_n_pages} 页",
            min_value=1, max_value=_n_pages, step=1,
            key=_page_key,
        )
    with pg_c4:
        pass  # spacer keeps layout balanced
    with pg_c5:
        st.caption(f"共 {_total:,} 条 · 第 {_cur_page}/{_n_pages} 页 · 每页 {page_size} 条")

    if _prev_clicked:
        st.session_state[_page_key] = _cur_page - 1
        st.rerun()
    if _next_clicked:
        st.session_state[_page_key] = _cur_page + 1
        st.rerun()

    _start = (_cur_page - 1) * page_size
    visible_items = items[_start : _start + page_size]

    # ── Build display DataFrame ───────────────────────────────────────
    rows = []
    for tk, info, mcap_num, inv_pot, short_pot in visible_items:
        univ = universe.get(tk, {}) if universe else {}
        name    = univ.get("name", info.get("name", ""))
        ts      = info.get("timestamp", "?")[:16]
        status  = info.get("status", "?")
        status_str = "✅ 完成" if status == "complete" else "⚠️ 错误" if status == "error" else "🔄"
        mcap_m = round(mcap_num / 1_000_000, 1) if mcap_num else None
        row = {"代码": tk, "公司名": name}
        if market == "CN":
            row["行业"] = univ.get("industry", "")
            row["地区"] = univ.get("area", "")
        row["市值(百万USD)"] = mcap_m
        row["投资潜力%"] = None if math.isnan(inv_pot) else round(inv_pot, 2)
        row["做空潜力%"] = None if math.isnan(short_pot) else round(short_pot, 2)
        row["状态"] = status_str
        row["分析时间"] = ts
        rows.append(row)

    df = pd.DataFrame(rows)

    # ── Table header: caption + autoplay controls ─────────────────────
    hdr_c1, hdr_c2, hdr_c3, hdr_c4 = st.columns([4, 2, 2, 1])
    with hdr_c1:
        st.caption("点击任意行查看该股票图表 ↓")
    with hdr_c2:
        autoplay = st.toggle(
            "自动播放下一个",
            key=f"rev_{market}_autoplay",
            value=False,
            help="按顺序自动切换图表",
        )
    with hdr_c3:
        ap_interval = st.number_input(
            "间隔（秒）",
            min_value=3,
            max_value=300,
            value=st.session_state.get(f"rev_{market}_ap_interval", 10),
            step=1,
            key=f"rev_{market}_ap_interval",
            disabled=not autoplay,
        )
    with hdr_c4:
        st.markdown("<div style='margin-top:4px'></div>", unsafe_allow_html=True)
        next_btn = st.button(
            "⏭ 下一个",
            key=f"rev_{market}_next_btn",
            use_container_width=True,
            help="手动跳到下一个",
        )

    event = st.dataframe(
        df,
        column_config={
            "市值(百万USD)": st.column_config.NumberColumn("市值(百万USD)", format="%.0f M"),
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

    # ── Shared helper: load and switch to a specific ticker ───────────
    def _load_and_switch(target_tk: str):
        """Load chart data for target_tk and store in session_state."""
        _info = market_items.get(target_tk, {})
        _data = load_chart(target_tk, _resolve_chart_market(target_tk, _info))
        if _data is None:
            try:
                _data = analyzer.fetch_data(target_tk)
                _tbl = load_fcf_table(target_tk, market)
                if _tbl is not None and not _tbl.empty:
                    _data = dict(_data)
                    _data["fcf_table"] = _tbl
                _data = analyzer._apply_adjusted_fcf(_data)
            except Exception:
                return False
        st.session_state[f"rev_{market}_data"] = _data
        st.session_state[f"rev_{market}_ticker"] = target_tk
        # Reset autoplay countdown whenever we switch
        _iv = st.session_state.get(f"rev_{market}_ap_interval", 10)
        st.session_state[f"_rev_{market}_ap_next_time"] = time.time() + _iv
        return True

    def _next_ticker() -> str | None:
        """Return the next ticker after the current one in the sorted list."""
        _sorted = st.session_state.get(f"_rev_{market}_sorted_tickers", [])
        if not _sorted:
            return None
        _cur = st.session_state.get(f"rev_{market}_ticker")
        _idx = _sorted.index(_cur) if _cur in _sorted else -1
        return _sorted[(_idx + 1) % len(_sorted)]

    # ── Manual "next" button ──────────────────────────────────────────
    if next_btn:
        _nt = _next_ticker()
        if _nt and _load_and_switch(_nt):
            st.rerun()

    # ── Row click → load chart ────────────────────────────────────────
    sel_rows = event.selection.rows if hasattr(event, "selection") else []
    if sel_rows:
        sel_ticker = df.iloc[sel_rows[0]]["代码"]
        if st.session_state.get(f"rev_{market}_ticker") != sel_ticker:
            with st.spinner(f"正在加载 {sel_ticker}..."):
                ok = _load_and_switch(sel_ticker)
            if ok:
                st.rerun()
            else:
                st.warning(f"⚠️ 无法加载 {sel_ticker} 的图表，请重试或在对应分析中心重新分析")
                st.session_state.pop(f"rev_{market}_ticker", None)
                st.session_state.pop(f"rev_{market}_data", None)

    # ── Autoplay fragment: polls every second, advances on schedule ───
    _ap_on = st.session_state.get(f"rev_{market}_autoplay", False)

    @st.fragment(run_every=1 if _ap_on else None)
    def _autoplay_frag():
        _on = st.session_state.get(f"rev_{market}_autoplay", False)
        _iv = int(st.session_state.get(f"rev_{market}_ap_interval", 10))
        if not _on:
            return

        _next_key = f"_rev_{market}_ap_next_time"
        _now = time.time()
        # Initialise timer on first run after enabling
        if _next_key not in st.session_state:
            st.session_state[_next_key] = _now + _iv

        _remaining = max(0.0, st.session_state[_next_key] - _now)
        _pct = min(1.0 - _remaining / max(_iv, 1), 1.0)
        _cur_tk = st.session_state.get(f"rev_{market}_ticker", "—")
        st.progress(
            _pct,
            text=f"自动播放中 · 当前: **{_cur_tk}** · {int(_remaining)}s 后切换下一个",
        )

        if _remaining > 0:
            return

        # Time's up — find and load the next ticker
        _sorted = st.session_state.get(f"_rev_{market}_sorted_tickers", [])
        if not _sorted:
            st.session_state[_next_key] = _now + _iv
            return
        _idx = _sorted.index(_cur_tk) if _cur_tk in _sorted else -1
        _next_tk = _sorted[(_idx + 1) % len(_sorted)]

        _info = market_items.get(_next_tk, {})
        _data = load_chart(_next_tk, _resolve_chart_market(_next_tk, _info))
        if _data is not None:
            st.session_state[f"rev_{market}_data"] = _data
            st.session_state[f"rev_{market}_ticker"] = _next_tk
            st.session_state[_next_key] = _now + _iv
            try:
                st.rerun(scope="app")
            except TypeError:
                st.rerun()
        else:
            # No pickle — skip this ticker, advance timer and try next cycle
            st.session_state[f"rev_{market}_ticker"] = _next_tk
            st.session_state[_next_key] = _now + _iv

    _autoplay_frag()

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
            original_currency   = cached_data.get("fmp_original_currency")
            currency_converted  = cached_data.get("fmp_currency_converted", True)
            st.markdown(
                analyzer._build_fcf_table_html(
                    fcf_table, currency, source=source,
                    original_currency=original_currency,
                    currency_converted=currency_converted,
                ),
                unsafe_allow_html=True,
            )

        analyzer.render_price_alert(cached_ticker, data=cached_data, key_suffix="rev")


with tab_reviewed:
    if not ENABLE_REVIEWED_TAB:
        render_factor_lab(st)
    else:
        st.subheader("📋 已分析股票")
        _render_reviewed_market("GLOBAL", USAnalyzer(), "us_universe")


# =====================================================================
#  数据库质量检测（读取后台脚本写入的缓存报告）
# =====================================================================
with tab_db_quality:
    render_db_quality_tab(st)


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
