"""Factor Lab: DCF 投资潜力 / 做空潜力排行榜（轻量 SQL + 分页，降低内存与首屏成本）。"""

from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from core.symbol_router import apply_global_selection
from db.schema import get_conn

_PAGE_SIZE = 100
_FETCH_LIMIT = _PAGE_SIZE + 1  # 多取 1 行判断是否有下一页，避免全表 COUNT

_FINANCIAL_SQL = """
  AND NOT (
    lower(coalesce(c.sector, '')) LIKE '%financial%'
    OR lower(coalesce(c.sector, '')) LIKE '%bank%'
    OR lower(coalesce(c.sector, '')) LIKE '%insurance%'
    OR lower(coalesce(c.sector, '')) LIKE '%broker%'
    OR lower(coalesce(c.industry, '')) LIKE '%financial%'
    OR lower(coalesce(c.industry, '')) LIKE '%bank%'
    OR lower(coalesce(c.industry, '')) LIKE '%insurance%'
    OR lower(coalesce(c.industry, '')) LIKE '%broker%'
  )
"""


def _market_in_clause(markets: list[str]) -> tuple[str, list]:
    ph = ", ".join(["?"] * len(markets))
    return ph, list(markets)


@st.cache_data(ttl=90, show_spinner=False, max_entries=256)
def _fetch_rank_slice(
    kind: str,
    markets_key: tuple[str, ...],
    exclude_financial: bool,
    page: int,
) -> tuple[list[tuple[Any, ...]], list[str]]:
    """返回 (rows, column_names)；rows 最多 _FETCH_LIMIT 条。仅用 cache 命中时复用，减小重复大查询。"""
    markets = list(markets_key)
    in_sql, params = _market_in_clause(markets)
    fin = _FINANCIAL_SQL if exclude_financial else ""
    offset = int(page) * _PAGE_SIZE
    if kind == "invest":
        where_extra = "AND d.invest_potential IS NOT NULL AND d.dcf_14x IS NOT NULL AND d.dcf_14x > 0"
        order = "d.invest_potential DESC NULLS LAST, c.ticker"
    else:
        where_extra = "AND d.short_potential IS NOT NULL AND d.short_potential > 0 AND d.dcf_34x IS NOT NULL AND d.dcf_34x > 0"
        order = "d.short_potential DESC NULLS LAST, c.ticker"

    # 市值：按当前页 ticker 做标量子查询，避免对全表 ohlcv_daily 做 ROW_NUMBER 窗口
    sql = f"""
        SELECT
            c.ticker,
            c.name,
            c.market,
            COALESCE(NULLIF(TRIM(c.country), ''), '—') AS country,
            COALESCE(NULLIF(TRIM(c.exchange), ''), '—') AS exchange,
            (SELECT o.market_cap FROM ohlcv_daily o
             WHERE o.ticker = c.ticker AND o.market_cap IS NOT NULL
             ORDER BY o.date DESC LIMIT 1) AS market_cap,
            d.invest_potential,
            d.short_potential
        FROM dcf_metrics d
        INNER JOIN companies c ON c.ticker = d.ticker
        WHERE c.market IN ({in_sql})
        {where_extra}
        {fin}
        ORDER BY {order}
        LIMIT ? OFFSET ?
    """
    qparams = params + [_FETCH_LIMIT, offset]
    with get_conn(readonly=True) as conn:
        cur = conn.execute(sql, qparams)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return rows, cols


def _rows_to_display_df(rows: list[tuple[Any, ...]], cols: list[str], kind: str) -> pd.DataFrame:
    """只保留展示用列，控制内存。"""
    if not rows:
        return pd.DataFrame()
    slim = [cols.index(c) for c in ("ticker", "name", "market", "country", "exchange", "market_cap", "invest_potential", "short_potential") if c in cols]
    data = []
    for r in rows:
        data.append([r[i] for i in slim])
    names = [cols[i] for i in slim]
    out = pd.DataFrame(data, columns=names)
    out["市值(百万)"] = pd.to_numeric(out["market_cap"], errors="coerce").round(2)
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
    return out[
        [
            "ticker",
            "name",
            score_col,
            "市值(百万)",
            "上市地",
        ]
    ].rename(columns={"ticker": "代码", "name": "公司名"})


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
    exclude_financial: bool,
    page_key: str,
    jump_prefix: str,
) -> None:
    st_mod.markdown(f"#### {title}")
    if page_key not in st_mod.session_state:
        st_mod.session_state[page_key] = 0
    p = max(0, int(st_mod.session_state[page_key]))

    mk = tuple(sorted({m.strip().upper() for m in markets}))
    rows, cols = _fetch_rank_slice(kind, mk, exclude_financial, p)
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
    )
    st_mod.dataframe(out, hide_index=True, width="stretch")

    pc1, pc2 = st_mod.columns(2)
    with pc1:
        if st_mod.button("上一页", key=f"{jump_prefix}_prev", disabled=not has_prev, width="stretch"):
            st_mod.session_state[page_key] = p - 1
            st_mod.rerun()
    with pc2:
        if st_mod.button("下一页", key=f"{jump_prefix}_next", disabled=not has_next, width="stretch"):
            st_mod.session_state[page_key] = p + 1
            st_mod.rerun()

    raw = pd.DataFrame(rows, columns=cols)
    labels = (
        raw["ticker"].astype(str)
        + " ["
        + raw["market"].astype(str)
        + "] "
        + raw["name"].fillna("").astype(str)
    ).tolist()
    sel = st_mod.selectbox(f"{title}：选择代码跳转 D1", options=labels, key=f"{jump_prefix}_pick")
    if st_mod.button(f"打开 D1（{title}）", key=f"{jump_prefix}_go", width="stretch"):
        idx = labels.index(sel)
        row = raw.iloc[idx]
        _jump_to_d1(st_mod, str(row["market"]), str(row["ticker"]))


def render_factor_lab(st_mod=st) -> None:
    if st_mod.session_state.get("_factor_jump_notice"):
        st_mod.success(st_mod.session_state.pop("_factor_jump_notice"))

    st_mod.subheader("🧪 因子分析：DCF 潜力排行")
    st_mod.caption(
        "预计算来自 `dcf_metrics`：投资潜力 = (14×DCF − 价格) / 14×DCF；"
        "做空潜力 = max(0, (价格 − 34×DCF) / 34×DCF)。分数为百分比。"
    )

    c1, c2, c3 = st_mod.columns([2, 1, 2])
    with c1:
        market_scope = st_mod.multiselect(
            "国家/市场",
            options=["US", "CN", "HK"],
            default=["US", "CN", "HK"],
            key="factor_market_scope",
            help="仅 US / CN / HK",
        )
    with c2:
        exclude_financial = st_mod.checkbox("排除金融/保险", value=True, key="factor_ex_fin")
    with c3:
        view_mode = st_mod.radio(
            "显示",
            options=["仅投资榜单", "仅做空榜单", "两个都显示"],
            index=0,
            horizontal=True,
            key="factor_view_mode",
            help="默认只加载一个榜单，减少查询与内存；需要对比时再选「两个都显示」。",
        )

    if not market_scope:
        st_mod.info("请至少选择一个国家/市场。")
        return

    sig = (tuple(sorted(market_scope)), bool(exclude_financial), view_mode)
    if st_mod.session_state.get("_factor_lab_filter_sig") != sig:
        st_mod.session_state["_factor_lab_filter_sig"] = sig
        st_mod.session_state["factor_page_invest"] = 0
        st_mod.session_state["factor_page_short"] = 0

    if view_mode == "仅投资榜单":
        _render_rank_block(
            st_mod,
            title="投资潜力（相对 14×DCF）",
            kind="invest",
            markets=market_scope,
            exclude_financial=exclude_financial,
            page_key="factor_page_invest",
            jump_prefix="finv",
        )
    elif view_mode == "仅做空榜单":
        _render_rank_block(
            st_mod,
            title="做空潜力（相对 34×DCF）",
            kind="short",
            markets=market_scope,
            exclude_financial=exclude_financial,
            page_key="factor_page_short",
            jump_prefix="fshort",
        )
    else:
        c_inv, c_short = st_mod.columns(2, gap="medium")
        with c_inv:
            _render_rank_block(
                st_mod,
                title="投资潜力（相对 14×DCF）",
                kind="invest",
                markets=market_scope,
                exclude_financial=exclude_financial,
                page_key="factor_page_invest",
                jump_prefix="finv",
            )
        with c_short:
            _render_rank_block(
                st_mod,
                title="做空潜力（相对 34×DCF）",
                kind="short",
                markets=market_scope,
                exclude_financial=exclude_financial,
                page_key="factor_page_short",
                jump_prefix="fshort",
            )
