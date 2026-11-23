"""Factor Lab: DCF 投资潜力 / 做空潜力排行榜（轻量 SQL + 分页，降低内存与首屏成本）。"""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import quote, unquote, urlencode

import pandas as pd
import streamlit as st

from core.symbol_router import apply_global_selection
from db.schema import get_conn

_PAGE_SIZE = 100
_FETCH_LIMIT = _PAGE_SIZE + 1  # 多取 1 行判断是否有下一页，避免全表 COUNT
_DEFAULT_MIN_MCAP_USD_M = 100.0
_MISSING_LABEL = "(未填写)"
# 浮动条数字页码：当前页左右各显示几页（0-based 页码链接为 1..N 展示）
_FACTOR_PAGER_PAGE_RADIUS = 3

# SQL：空白 sector/country/industry 与界面选项 ``_MISSING_LABEL`` 对齐
_SQL_COUNTRY_LABEL = (
    f"CASE WHEN trim(coalesce(c.country, '')) = '' THEN '{_MISSING_LABEL}' "
    f"ELSE trim(c.country) END"
)
_SQL_SECTOR_LABEL = (
    f"CASE WHEN trim(coalesce(c.sector, '')) = '' THEN '{_MISSING_LABEL}' "
    f"ELSE trim(c.sector) END"
)
_SQL_INDUSTRY_LABEL = (
    f"CASE WHEN trim(coalesce(c.industry, '')) = '' THEN '{_MISSING_LABEL}' "
    f"ELSE trim(c.industry) END"
)

# app.py 仍消费 ``?fp=&d=&fv=`` 以及 ``?finv_page=&fshort_page=&fv=&ffilt=``（浮动分页；
# ``ffilt`` 为 URL 编码 JSON：市场/国家/板块/行业/最小市值；旧链接可仍带 ``fmm``）。
# 主导航切换使用稳定的标签，而不是依赖位置索引。
_MAIN_NAV_FACTOR_LAB_LABEL = "🧪 因子分析"
FV_TO_VIEW_MODE = {"inv": "仅投资榜单", "sh": "仅做空榜单"}
VIEW_MODE_TO_FV = {v: k for k, v in FV_TO_VIEW_MODE.items()}

_DEFAULT_MARKETS_PREF = ("US", "CN", "HK")

# 主导航切走时 Streamlit 会清掉未渲染控件的 session；用独立快照保留因子页 UI。
_FACTOR_LAB_UI_SNAP_KEY = "_factor_lab_ui_snap"


def _factor_lab_filter_to_current_options(chosen: Any, options: list[str]) -> list[str]:
    """把上次保存的多选值过滤到当前 options，避免库结构变化后残留无效项。"""
    if not options:
        return []
    oset = set(options)
    if not isinstance(chosen, list):
        return []
    out: list[str] = []
    for x in chosen:
        t = str(x).strip()
        if t and t in oset:
            out.append(t)
    return out


def _factor_lab_restore_widget_keys_from_snap(
    st_mod,
    *,
    market_opts: list[str],
    country_opts: list[str],
    sector_opts: list[str],
    industry_opts: list[str],
) -> None:
    ss = st_mod.session_state
    snap = ss.get(_FACTOR_LAB_UI_SNAP_KEY)
    if not isinstance(snap, dict):
        snap = {}

    if "factor_market_scope" not in ss:
        mk = _factor_lab_filter_to_current_options(snap.get("markets"), market_opts)
        if not mk:
            mk = _default_scope_from_prefs(market_opts, _DEFAULT_MARKETS_PREF)
        ss["factor_market_scope"] = mk

    if "factor_country_scope" not in ss:
        ss["factor_country_scope"] = _factor_lab_filter_to_current_options(
            snap.get("countries"), country_opts
        )

    if "factor_sector_pick" not in ss:
        ss["factor_sector_pick"] = _factor_lab_filter_to_current_options(
            snap.get("sectors"), sector_opts
        )

    if "factor_industry_pick" not in ss:
        ss["factor_industry_pick"] = _factor_lab_filter_to_current_options(
            snap.get("industries"), industry_opts
        )

    _vm_opts = ("仅投资榜单", "仅做空榜单")
    if "factor_view_mode" not in ss:
        vm = snap.get("view_mode")
        ss["factor_view_mode"] = vm if isinstance(vm, str) and vm in _vm_opts else _vm_opts[0]

    if "factor_min_mcap_usd_m" not in ss:
        mmc = snap.get("min_mcap")
        try:
            v = float(mmc)
            if v >= 0.0 and v == v and abs(v) < 1e308:
                ss["factor_min_mcap_usd_m"] = v
            else:
                ss["factor_min_mcap_usd_m"] = float(_DEFAULT_MIN_MCAP_USD_M)
        except (TypeError, ValueError):
            ss["factor_min_mcap_usd_m"] = float(_DEFAULT_MIN_MCAP_USD_M)

    if "factor_page_invest" not in ss:
        try:
            ss["factor_page_invest"] = max(0, int(snap.get("page_invest", 0)))
        except (TypeError, ValueError):
            ss["factor_page_invest"] = 0
    if "factor_page_short" not in ss:
        try:
            ss["factor_page_short"] = max(0, int(snap.get("page_short", 0)))
        except (TypeError, ValueError):
            ss["factor_page_short"] = 0


def _factor_lab_save_ui_snap(st_mod) -> None:
    ss = st_mod.session_state
    try:
        mmc = float(ss.get("factor_min_mcap_usd_m", _DEFAULT_MIN_MCAP_USD_M))
    except (TypeError, ValueError):
        mmc = float(_DEFAULT_MIN_MCAP_USD_M)
    try:
        pi = max(0, int(ss.get("factor_page_invest", 0)))
    except (TypeError, ValueError):
        pi = 0
    try:
        ps = max(0, int(ss.get("factor_page_short", 0)))
    except (TypeError, ValueError):
        ps = 0
    ss[_FACTOR_LAB_UI_SNAP_KEY] = {
        "markets": list(ss.get("factor_market_scope") or []),
        "countries": list(ss.get("factor_country_scope") or []),
        "sectors": list(ss.get("factor_sector_pick") or []),
        "industries": list(ss.get("factor_industry_pick") or []),
        "view_mode": ss.get("factor_view_mode"),
        "min_mcap": mmc,
        "page_invest": pi,
        "page_short": ps,
    }

# 因子榜整页翻页时塞进 URL，避免 session 清空后多选/数字框回到默认。
_FFILT_JSON_MAX = 48_000


def pack_factor_pager_ffilt(
    *,
    markets: list[str],
    countries: list[str],
    sectors: list[str],
    industries: list[str],
    min_mcap_usd_m: float,
) -> str:
    obj = {
        "mk": [str(x).strip() for x in markets if str(x).strip()],
        "ct": [str(x).strip() for x in countries if str(x).strip()],
        "se": [str(x).strip() for x in sectors if str(x).strip()],
        "in": [str(x).strip() for x in industries if str(x).strip()],
        "mm": float(min_mcap_usd_m),
    }
    raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    return quote(raw, safe="")


def parse_factor_pager_ffilt(raw) -> dict[str, Any] | None:
    """解析 ``ffilt``；成功返回 ``markets, countries, sectors, industries, min_mcap_usd_m``。"""
    if raw is None:
        return None
    s0 = raw[0] if isinstance(raw, list) else raw
    s = str(s0).strip()
    if not s or len(s) > _FFILT_JSON_MAX:
        return None
    blob = unquote(s)
    try:
        obj = json.loads(blob)
    except (json.JSONDecodeError, TypeError):
        try:
            obj = json.loads(s)
        except (json.JSONDecodeError, TypeError):
            return None
    if not isinstance(obj, dict):
        return None

    def _norm_list(key: str) -> list[str] | None:
        v = obj.get(key)
        if v is None:
            return []
        if not isinstance(v, list):
            return None
        out: list[str] = []
        for it in v:
            t = str(it).strip()
            if len(t) > 512:
                return None
            if t:
                out.append(t)
        return out

    mk = _norm_list("mk")
    ct = _norm_list("ct")
    se = _norm_list("se")
    ind = _norm_list("in")
    if mk is None or ct is None or se is None or ind is None:
        return None
    if not mk:
        return None
    mm = obj.get("mm")
    try:
        mmf = float(mm)
    except (TypeError, ValueError):
        return None
    if mmf < 0.0 or mmf != mmf or abs(mmf) > 1e308:
        return None
    return {
        "markets": mk,
        "countries": ct,
        "sectors": se,
        "industries": ind,
        "min_mcap_usd_m": mmf,
    }


def _market_in_clause(markets: list[str]) -> tuple[str, list]:
    ph = ", ".join(["?"] * len(markets))
    return ph, list(markets)


def _where_normalized_in(case_sql: str, values_key: tuple[str, ...]) -> tuple[str, list[Any]]:
    if not values_key:
        return "", []
    ph = ", ".join(["?"] * len(values_key))
    return f"AND ({case_sql}) IN ({ph})", list(values_key)


@st.cache_data(ttl=90, show_spinner=False, max_entries=256)
def _fetch_factor_filter_options() -> dict[str, list[str]]:
    """来自 ``companies ∩ dcf_metrics`` 的去重选项（与榜单同源）。"""
    base = """
        FROM companies c
        INNER JOIN dcf_metrics d ON d.ticker = c.ticker
        WHERE trim(coalesce(c.market, '')) <> ''
    """
    q_market = f"SELECT DISTINCT trim(coalesce(c.market, '')) AS v {base} ORDER BY 1"
    q_country = f"SELECT DISTINCT {_SQL_COUNTRY_LABEL} AS v {base} ORDER BY 1"
    q_sector = f"SELECT DISTINCT {_SQL_SECTOR_LABEL} AS v {base} ORDER BY 1"
    q_industry = f"SELECT DISTINCT {_SQL_INDUSTRY_LABEL} AS v {base} ORDER BY 1"
    out: dict[str, list[str]] = {"markets": [], "countries": [], "sectors": [], "industries": []}
    with get_conn(readonly=True) as conn:
        out["markets"] = [str(r[0]) for r in conn.execute(q_market).fetchall() if r and r[0]]
        out["countries"] = [str(r[0]) for r in conn.execute(q_country).fetchall() if r and r[0]]
        out["sectors"] = [str(r[0]) for r in conn.execute(q_sector).fetchall() if r and r[0]]
        out["industries"] = [str(r[0]) for r in conn.execute(q_industry).fetchall() if r and r[0]]
    return out


def _default_scope_from_prefs(options: list[str], prefs: tuple[str, ...]) -> list[str]:
    """优先勾选 prefs 中存在于 options 的项；若没有交集则勾选全部可选项（市场通常很少）。"""
    hit = [p for p in prefs if p in options]
    return hit if hit else list(options)


@st.cache_data(ttl=90, show_spinner=False, max_entries=4096)
def _fetch_rank_slice(
    kind: str,
    markets_key: tuple[str, ...],
    countries_key: tuple[str, ...],
    sectors_key: tuple[str, ...],
    industries_key: tuple[str, ...],
    min_mcap_usd_m: float,
    page: int,
) -> tuple[list[tuple[Any, ...]], list[str]]:
    """返回 (rows, column_names)；rows 最多 _FETCH_LIMIT 条。仅用 cache 命中时复用，减小重复大查询。"""
    markets = list(markets_key)
    in_sql, params = _market_in_clause(markets)
    wc, cp = _where_normalized_in(_SQL_COUNTRY_LABEL, countries_key)
    ws, sp = _where_normalized_in(_SQL_SECTOR_LABEL, sectors_key)
    wi, ip = _where_normalized_in(_SQL_INDUSTRY_LABEL, industries_key)
    offset = int(page) * _PAGE_SIZE
    if kind == "invest":
        where_extra = "AND d.invest_potential IS NOT NULL AND d.dcf_14x IS NOT NULL AND d.dcf_14x > 0"
        order = "d.invest_potential DESC NULLS LAST, c.ticker"
    else:
        where_extra = "AND d.short_potential IS NOT NULL AND d.short_potential > 0 AND d.dcf_34x IS NOT NULL AND d.dcf_34x > 0"
        order = "d.short_potential DESC NULLS LAST, c.ticker"

    # 市值：单位 M（百万）；筛选统一用 USD 口径。
    market_cap_usd_expr = """
        COALESCE(
            (SELECT o.market_cap_usd FROM ohlcv_daily o
             WHERE o.ticker = c.ticker AND o.market_cap_usd IS NOT NULL
             ORDER BY o.date DESC LIMIT 1),
            CASE
                WHEN upper(coalesce(c.currency, '')) = 'USD' THEN
                    (SELECT o.market_cap FROM ohlcv_daily o
                     WHERE o.ticker = c.ticker AND o.market_cap IS NOT NULL
                     ORDER BY o.date DESC LIMIT 1)
                ELSE NULL
            END
        )
    """
    sql = f"""
        SELECT
            c.ticker,
            c.name,
            c.market,
            COALESCE(NULLIF(TRIM(c.country), ''), '—') AS country,
            COALESCE(NULLIF(TRIM(c.exchange), ''), '—') AS exchange,
            COALESCE(NULLIF(TRIM(c.sector), ''), '—') AS sector,
            COALESCE(NULLIF(TRIM(c.industry), ''), '—') AS industry,
            {market_cap_usd_expr} AS market_cap_usd,
            d.latest_price AS latest_price,
            d.invest_potential,
            d.short_potential
        FROM dcf_metrics d
        INNER JOIN companies c ON c.ticker = d.ticker
        WHERE c.market IN ({in_sql})
        {wc}
        {ws}
        {wi}
        AND {market_cap_usd_expr} >= ?
        {where_extra}
        ORDER BY {order}
        LIMIT ? OFFSET ?
    """
    qparams = params + cp + sp + ip + [float(min_mcap_usd_m), _FETCH_LIMIT, offset]
    with get_conn(readonly=True) as conn:
        cur = conn.execute(sql, qparams)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return rows, cols


def _cell_display(v: Any) -> str:
    if v is None or pd.isna(v):
        return "—"
    s = str(v).strip()
    return s if s else "—"


def _rows_to_display_df(rows: list[tuple[Any, ...]], cols: list[str], kind: str) -> pd.DataFrame:
    """只保留展示用列，控制内存。"""
    if not rows:
        return pd.DataFrame()
    want_cols = (
        "ticker",
        "name",
        "market",
        "country",
        "exchange",
        "sector",
        "industry",
        "market_cap_usd",
        "latest_price",
        "invest_potential",
        "short_potential",
    )
    slim = [cols.index(c) for c in want_cols if c in cols]
    data = []
    for r in rows:
        data.append([r[i] for i in slim])
    names = [cols[i] for i in slim]
    out = pd.DataFrame(data, columns=names)
    out["市值(USD, M)"] = pd.to_numeric(out["market_cap_usd"], errors="coerce").round(2)
    out["最新价"] = pd.to_numeric(out["latest_price"], errors="coerce").round(4)
    if kind == "invest":
        out["投资潜力分数"] = (pd.to_numeric(out["invest_potential"], errors="coerce") * 100.0).round(2)
        score_col = "投资潜力分数"
    else:
        out["做空潜力分数"] = (pd.to_numeric(out["short_potential"], errors="coerce") * 100.0).round(2)
        score_col = "做空潜力分数"
    out["上市地"] = (
        out["market"].astype(str)
        + " / "
        + out["country"].astype(str)
        + " / "
        + out["exchange"].astype(str)
    )
    out["板块"] = out["sector"].map(_cell_display)
    out["行业"] = out["industry"].map(_cell_display)
    return out[
        [
            "ticker",
            "name",
            "板块",
            "行业",
            score_col,
            "市值(USD, M)",
            "最新价",
            "上市地",
        ]
    ].rename(columns={"ticker": "代码", "name": "公司名"})


def _abs_pager_href(
    *,
    inv_page: int,
    sh_page: int,
    view_tag: str,
    markets: list[str],
    countries: list[str],
    sectors: list[str],
    industries: list[str],
    min_mcap_usd_m: float,
) -> str:
    """浮动条用整页跳转；页码放在 URL（绝对页码），新会话也能对齐，不依赖 fp/d 相对步进。

    ``ffilt`` 携带筛选条件，避免整页刷新后 session 清空导致控件回到默认。
    """
    ffilt = pack_factor_pager_ffilt(
        markets=markets,
        countries=countries,
        sectors=sectors,
        industries=industries,
        min_mcap_usd_m=min_mcap_usd_m,
    )
    qp: dict[str, str] = {
        "main_nav_tab": _MAIN_NAV_FACTOR_LAB_LABEL,
        "fv": view_tag,
        "ffilt": ffilt,
    }
    if view_tag != "sh":
        qp["finv_page"] = str(max(0, int(inv_page)))
    if view_tag != "inv":
        qp["fshort_page"] = str(max(0, int(sh_page)))
    return "?" + urlencode(qp, doseq=True)


def _emit_floating_pager_html(
    st_mod,
    *,
    jump_prefix: str,
    inv_page: int,
    short_page: int,
    current_page: int,
    view_tag: str,
    markets: list[str],
    countries: list[str],
    sectors: list[str],
    industries: list[str],
    min_mcap_usd_m: float,
    has_prev: bool,
    has_next: bool,
    bottom: str,
    pager_anchor: str = "right",
) -> None:
    """viewport 固定分页条（<a href> + 绝对页码 query）。"""

    def _targets_for(delta: int) -> tuple[int, int]:
        if jump_prefix == "finv":
            return max(0, inv_page + delta), max(0, short_page)
        return max(0, inv_page), max(0, short_page + delta)

    def _href(ti: int, ts: int) -> str:
        return _abs_pager_href(
            inv_page=ti,
            sh_page=ts,
            view_tag=view_tag,
            markets=markets,
            countries=countries,
            sectors=sectors,
            industries=industries,
            min_mcap_usd_m=min_mcap_usd_m,
        )

    def _link(label: str, delta: int, enabled: bool) -> str:
        if not enabled:
            return f'<span class="factor-pager-disabled">{label}</span>'
        ti, ts = _targets_for(delta)
        href = _href(ti, ts)
        return f'<a href="{href}" target="_self" class="factor-pager-link">{label}</a>'

    def _page_num_links() -> str:
        cp = max(0, int(current_page))
        lo = max(0, cp - _FACTOR_PAGER_PAGE_RADIUS)
        hi = cp + _FACTOR_PAGER_PAGE_RADIUS
        parts: list[str] = []
        for pg in range(lo, hi + 1):
            if jump_prefix == "finv":
                ti, ts = pg, max(0, short_page)
            else:
                ti, ts = max(0, inv_page), pg
            label = str(pg + 1)
            if pg == cp:
                parts.append(f'<span class="factor-pager-cur" title="当前页">{label}</span>')
            else:
                href = _href(ti, ts)
                parts.append(
                    f'<a href="{href}" target="_self" class="factor-pager-link factor-pager-num" '
                    f'title="第 {label} 页">{label}</a>'
                )
        return '<span class="factor-pager-pages">' + "".join(parts) + "</span>"

    anchor_cls = " factor-pager-anchor-left" if pager_anchor == "left" else ""
    which_zh = "投资榜" if jump_prefix == "finv" else "做空榜"
    page_links = _page_num_links()

    st_mod.markdown(
        f"""
<style>
div.factor-pager-float-{jump_prefix} {{
  position: fixed;
  bottom: {bottom};
  right: max(10px, env(safe-area-inset-right, 0px));
  z-index: 1000001;
  display: flex;
  flex-wrap: wrap;
  gap: 8px 10px;
  align-items: center;
  max-width: min(96vw, 560px);
  background: rgba(250, 250, 250, 0.96);
  border: 1px solid rgba(0, 0, 0, 0.1);
  border-radius: 11px;
  padding: 7px 11px;
  box-shadow: 0 2px 14px rgba(0, 0, 0, 0.12);
  font-size: 0.92rem;
  line-height: 1.25;
  color: #111827;
  font-weight: 600;
}}
div.factor-pager-float-{jump_prefix}.factor-pager-anchor-left {{
  left: max(10px, env(safe-area-inset-left, 0px));
  right: auto;
}}
div.factor-pager-float-{jump_prefix} a.factor-pager-link {{
  text-decoration: none;
  color: #0d47a1;
  font-weight: 700;
}}
div.factor-pager-float-{jump_prefix} a.factor-pager-link:hover {{
  text-decoration: underline;
  color: #1565c0;
}}
div.factor-pager-float-{jump_prefix} span.factor-pager-disabled {{
  color: #6b7280;
  opacity: 1;
  cursor: default;
  font-weight: 600;
}}
div.factor-pager-float-{jump_prefix} span.factor-pager-pages {{
  display: inline-flex;
  flex-wrap: wrap;
  gap: 4px 8px;
  align-items: center;
}}
div.factor-pager-float-{jump_prefix} a.factor-pager-num {{
  min-width: 1.35rem;
  text-align: center;
  font-variant-numeric: tabular-nums;
}}
div.factor-pager-float-{jump_prefix} span.factor-pager-cur {{
  min-width: 1.35rem;
  text-align: center;
  font-weight: 800;
  color: #111827;
  border-bottom: 2px solid #0d47a1;
  padding: 0 2px;
  font-variant-numeric: tabular-nums;
}}
@media (prefers-color-scheme: dark) {{
  div.factor-pager-float-{jump_prefix} {{
    background: rgba(30, 32, 40, 0.96);
    border-color: rgba(255, 255, 255, 0.12);
    color: #e5e7eb;
  }}
  div.factor-pager-float-{jump_prefix} a.factor-pager-link {{
    color: #90caf9;
  }}
  div.factor-pager-float-{jump_prefix} a.factor-pager-link:hover {{
    color: #bbdefb;
  }}
  div.factor-pager-float-{jump_prefix} span.factor-pager-disabled {{
    color: #9ca3af;
  }}
  div.factor-pager-float-{jump_prefix} span.factor-pager-cur {{
    color: #e5e7eb;
    border-bottom-color: #90caf9;
  }}
}}
div.factor-pager-float-{jump_prefix} span.factor-pager-which {{
  margin-right: 2px;
  color: #374151;
  font-weight: 700;
}}
@media (prefers-color-scheme: dark) {{
  div.factor-pager-float-{jump_prefix} span.factor-pager-which {{
    color: #d1d5db;
  }}
}}
</style>
<div class="factor-pager-float-{jump_prefix}{anchor_cls}">
  <span class="factor-pager-which">{which_zh}</span>
  {_link("上一页", -1, has_prev)}
  {page_links}
  {_link("下一页", 1, has_next)}
</div>
""",
        unsafe_allow_html=True,
    )


def _roic_quote_url(ticker: str) -> str:
    """ROIC.ai 个股页（仅 UI 拼接，不入库）。"""
    sym = str(ticker).strip()
    if not sym:
        return "https://roic.ai/quote/"
    return f"https://roic.ai/quote/{quote(sym, safe='.-_')}"


def _jump_to_d1(st_mod, market: str, ticker: str) -> None:
    apply_global_selection(st_mod.session_state, market, ticker)
    # 不可在本页直接改 global_symbol_query：tab_stock 的 text_input 已先实例化。
    # 与 app.py 约定：只写 pending，由「个股分析中心」在下一 run 在 widget 之前写入搜索框。
    st_mod.session_state["_global_symbol_query_pending"] = ticker
    # 与 app.py 主导航约定：下一 run 切到「个股分析中心」
    st_mod.session_state["_nav_main_tab"] = "📈 个股分析中心"
    st_mod.session_state["_factor_jump_notice"] = (
        f"已定位到 `{market}` / `{ticker}`，正在切换到个股分析中心…"
    )
    st_mod.rerun()


def _render_rank_block(
    st_mod,
    *,
    title: str,
    kind: str,
    markets: list[str],
    countries: list[str],
    sectors: list[str],
    industries: list[str],
    min_mcap_usd_m: float,
    page_key: str,
    jump_prefix: str,
    view_tag: str,
    inv_page: int,
    short_page: int,
    float_bottom: str = "4rem",
    pager_anchor: str = "right",
) -> None:
    st_mod.markdown(f"#### {title}")
    if page_key not in st_mod.session_state:
        st_mod.session_state[page_key] = 0
    p = max(0, int(st_mod.session_state[page_key]))

    mk = tuple(sorted({m.strip().upper() for m in markets}))
    ck = tuple(sorted(countries))
    sk = tuple(sorted(sectors))
    ik = tuple(sorted(industries))
    rows, cols = _fetch_rank_slice(kind, mk, ck, sk, ik, min_mcap_usd_m, p)
    has_next = len(rows) > _PAGE_SIZE
    rows = rows[:_PAGE_SIZE]
    has_prev = p > 0

    if not rows and p > 0:
        st_mod.session_state[page_key] = 0
        st_mod.rerun()
        return

    if not rows:
        st_mod.caption("暂无有效样本（或当前市场筛选下无数据）。")
        return

    out = _rows_to_display_df(rows, cols, kind)
    score_col = "投资潜力分数" if kind == "invest" else "做空潜力分数"
    st_mod.caption(
        f"第 {p + 1} 页 · 每页 {_PAGE_SIZE} 条"
        + (" · 后面还有数据" if has_next else " · 已到末尾")
        + " · 浮动条：上一页 / 数字跳页 / 下一页（随表格下滑仍可见）"
    )

    raw = pd.DataFrame(rows, columns=cols)
    score_hdr = "投潜%" if kind == "invest" else "空潜%"
    # 略收紧列宽；分页在 viewport 固定条，不占表格宽度。
    col_weights = [0.7, 0.21, 0.24, 0.72, 0.32, 0.38, 0.4, 0.36, 0.36, 0.98]
    hdr = st_mod.columns(col_weights)
    hdr[0].markdown("**代码**")
    hdr[1].markdown("**D1**")
    hdr[2].markdown("**ROIC**")
    hdr[3].markdown("**公司**")
    hdr[4].markdown("**板块**")
    hdr[5].markdown("**行业**")
    hdr[6].markdown(f"**{score_hdr}**")
    hdr[7].markdown("**市值M**")
    hdr[8].markdown("**最新价**")
    hdr[9].markdown("**上市地**")

    def _cell_str(v: Any) -> str:
        return "" if pd.isna(v) else str(v)

    for i in range(len(out)):
        r = out.iloc[i]
        mr = raw.iloc[i]
        ticker = str(mr["ticker"])
        market = str(mr["market"])
        cc = st_mod.columns(col_weights)
        with cc[0]:
            st_mod.markdown(f"**{ticker}**")
        with cc[1]:
            if st_mod.button(
                "D1",
                key=f"{jump_prefix}_d1_p{p}_r{i}",
                width="stretch",
                help=f"跳转个股分析中心 · {market}",
            ):
                _jump_to_d1(st_mod, market, ticker)
        with cc[2]:
            st_mod.link_button(
                "ROIC",
                url=_roic_quote_url(ticker),
                width="stretch",
                help="在浏览器打开 ROIC.ai（点击时按代码拼接链接）",
            )
        with cc[3]:
            st_mod.text(_cell_str(r["公司名"]))
        with cc[4]:
            st_mod.text(_cell_str(r["板块"]))
        with cc[5]:
            st_mod.text(_cell_str(r["行业"]))
        with cc[6]:
            st_mod.text(_cell_str(r[score_col]))
        with cc[7]:
            st_mod.text(_cell_str(r["市值(USD, M)"]))
        with cc[8]:
            lp = r["最新价"]
            if pd.isna(lp):
                st_mod.text("—")
            else:
                st_mod.text(f"{float(lp):.4f}".rstrip("0").rstrip("."))
        with cc[9]:
            st_mod.text(_cell_str(r["上市地"]))

    st_mod.markdown('<div style="height:3.25rem;"></div>', unsafe_allow_html=True)
    _emit_floating_pager_html(
        st_mod,
        jump_prefix=jump_prefix,
        inv_page=inv_page,
        short_page=short_page,
        current_page=p,
        view_tag=view_tag,
        markets=list(markets),
        countries=list(countries),
        sectors=list(sectors),
        industries=list(industries),
        min_mcap_usd_m=float(min_mcap_usd_m),
        has_prev=has_prev,
        has_next=has_next,
        bottom=float_bottom,
        pager_anchor=pager_anchor,
    )


def render_factor_lab(st_mod=st) -> None:
    # 旧版用 sig 比较清零页码；已改为 on_change，避免残留键干扰阅读 session。
    st_mod.session_state.pop("_factor_lab_filter_sig", None)

    if st_mod.session_state.get("_factor_jump_notice"):
        st_mod.success(st_mod.session_state.pop("_factor_jump_notice"))

    st_mod.subheader("🧪 因子分析：DCF 潜力排行")
    st_mod.caption(
        "预计算来自 `dcf_metrics`：投资潜力 = (14×DCF − 价格) / 14×DCF；"
        "做空潜力 = max(0, (价格 − 34×DCF) / 34×DCF)。分数为百分比。"
        " 切换至其他主导航再返回时，会保留上次的筛选条件与翻页位置。"
    )

    def _reset_factor_pager_pages() -> None:
        """仅在用户操作筛选条件时调用；勿用每 run 的 sig 比较，rerun 间易误判导致页码被清零。"""
        st_mod.session_state["factor_page_invest"] = 0
        st_mod.session_state["factor_page_short"] = 0

    fo = _fetch_factor_filter_options()
    market_opts = fo["markets"]
    country_opts = fo["countries"]
    sector_opts = fo["sectors"]
    industry_opts = fo["industries"]
    if not market_opts:
        st_mod.warning("当前数据库中没有同时存在于 `companies` 与 `dcf_metrics` 的标的，无法生成筛选选项与榜单。")
        return

    # 切到其他主导航时控件 session 会被 Streamlit 清掉；从快照预填，避免每次回到本页都回到默认。
    _factor_lab_restore_widget_keys_from_snap(
        st_mod,
        market_opts=market_opts,
        country_opts=country_opts,
        sector_opts=sector_opts,
        industry_opts=industry_opts,
    )
    if st_mod.session_state.get("factor_view_mode") not in VIEW_MODE_TO_FV:
        st_mod.session_state["factor_view_mode"] = "仅投资榜单"

    m1, m2 = st_mod.columns(2)
    with m1:
        market_scope = st_mod.multiselect(
            "市场",
            options=market_opts,
            key="factor_market_scope",
            help="数据来源于库内已有 DCF 潜力的标的；可多选。",
            on_change=_reset_factor_pager_pages,
        )
    with m2:
        country_scope = st_mod.multiselect(
            "国家/地区",
            options=country_opts,
            key="factor_country_scope",
            help="按公司 profile 的国家/地区字段筛选；留空表示不限国家。"
            + f" `{_MISSING_LABEL}` 表示库中该字段为空。",
            on_change=_reset_factor_pager_pages,
        )

    s1, s2 = st_mod.columns(2)
    with s1:
        sector_pick = st_mod.multiselect(
            "板块",
            options=sector_opts,
            key="factor_sector_pick",
            help="留空表示不限板块；勾选后仅保留所选板块。",
            on_change=_reset_factor_pager_pages,
        )
    with s2:
        industry_pick = st_mod.multiselect(
            "行业",
            options=industry_opts,
            key="factor_industry_pick",
            help="留空表示不限行业；勾选后仅保留所选行业。",
            on_change=_reset_factor_pager_pages,
        )

    v1, v2 = st_mod.columns([2, 1])
    with v1:
        view_mode = st_mod.radio(
            "显示",
            options=["仅投资榜单", "仅做空榜单"],
            horizontal=True,
            key="factor_view_mode",
            help="在同一页切换投资榜或做空榜；分页互不干扰。",
            on_change=_reset_factor_pager_pages,
        )
    with v2:
        min_mcap_usd_m = st_mod.number_input(
            "最小市值(USD, M)",
            min_value=0.0,
            value=_DEFAULT_MIN_MCAP_USD_M,
            step=0.1,
            key="factor_min_mcap_usd_m",
            help="单位：百万 USD；默认 100M，低于该市值的样本不进入榜单。",
            on_change=_reset_factor_pager_pages,
        )

    if not market_scope:
        _factor_lab_save_ui_snap(st_mod)
        st_mod.info("请至少选择一个市场。")
        return

    countries_filter = list(country_scope)
    sectors_filter = list(sector_pick)
    industries_filter = list(industry_pick)

    inv_p = max(0, int(st_mod.session_state.get("factor_page_invest", 0)))
    sh_p = max(0, int(st_mod.session_state.get("factor_page_short", 0)))
    vtag = VIEW_MODE_TO_FV[view_mode]

    if view_mode == "仅投资榜单":
        _render_rank_block(
            st_mod,
            title="投资潜力（相对 14×DCF）",
            kind="invest",
            markets=market_scope,
            countries=countries_filter,
            sectors=sectors_filter,
            industries=industries_filter,
            min_mcap_usd_m=float(min_mcap_usd_m),
            page_key="factor_page_invest",
            jump_prefix="finv",
            view_tag=vtag,
            inv_page=inv_p,
            short_page=sh_p,
            float_bottom="4rem",
            pager_anchor="right",
        )
        _factor_lab_save_ui_snap(st_mod)
    else:
        _render_rank_block(
            st_mod,
            title="做空潜力（相对 34×DCF）",
            kind="short",
            markets=market_scope,
            countries=countries_filter,
            sectors=sectors_filter,
            industries=industries_filter,
            min_mcap_usd_m=float(min_mcap_usd_m),
            page_key="factor_page_short",
            jump_prefix="fshort",
            view_tag=vtag,
            inv_page=inv_p,
            short_page=sh_p,
            float_bottom="4rem",
            pager_anchor="right",
        )
        _factor_lab_save_ui_snap(st_mod)
