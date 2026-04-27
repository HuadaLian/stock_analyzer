"""Factor Lab (MVP): cross-market quick factors from DB."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from db.schema import get_conn


def _load_factor_frame(market: str, limit: int) -> pd.DataFrame:
    with get_conn(readonly=True) as conn:
        df = conn.execute(
            """
            WITH latest_price AS (
                SELECT ticker, adj_close AS price, market_cap,
                       ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
                FROM ohlcv_daily
            ),
            latest_fund AS (
                SELECT ticker, revenue, fcf, fiscal_year,
                       ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY fiscal_year DESC) AS rn
                FROM fundamentals_annual
            )
            SELECT c.ticker, c.name, c.market,
                   p.price, p.market_cap,
                   f.revenue, f.fcf, f.fiscal_year
            FROM companies c
            LEFT JOIN latest_price p ON p.ticker = c.ticker AND p.rn = 1
            LEFT JOIN latest_fund f ON f.ticker = c.ticker AND f.rn = 1
            WHERE (? = 'ALL' OR c.market = ?)
            ORDER BY p.market_cap DESC NULLS LAST, c.ticker
            LIMIT ?
            """,
            [market, market, int(limit)],
        ).df()

    if df.empty:
        return df

    for col in ["price", "market_cap", "revenue", "fcf"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["ps_ratio"] = (df["market_cap"] / df["revenue"]).where(df["revenue"] > 0)
    df["pfcf_ratio"] = (df["market_cap"] / df["fcf"]).where(df["fcf"] > 0)
    df["fcf_margin"] = (df["fcf"] / df["revenue"]).where((df["fcf"] > 0) & (df["revenue"] > 0))
    return df


def render_factor_lab(st_mod=st) -> None:
    st_mod.subheader("🧪 因子分析（MVP）")
    st_mod.caption("第一版提供跨市场基础因子快照：规模、估值（P/S、P/FCF）、质量（FCF Margin）。")

    c1, c2 = st_mod.columns([2, 1])
    with c1:
        market = st_mod.selectbox("市场", ["ALL", "US", "CN", "HK"], index=0, key="factor_market")
    with c2:
        limit = st_mod.selectbox("样本上限", [100, 300, 500, 1000], index=1, key="factor_limit")

    df = _load_factor_frame(market, int(limit))
    if df.empty:
        st_mod.info("暂无可用于因子分析的数据。")
        return

    top = df.copy()
    top["market_cap_m"] = top["market_cap"]
    top["ps_ratio"] = top["ps_ratio"].round(2)
    top["pfcf_ratio"] = top["pfcf_ratio"].round(2)
    top["fcf_margin_pct"] = (top["fcf_margin"] * 100).round(2)

    show_cols = [
        "ticker", "name", "market", "market_cap_m", "price", "fiscal_year",
        "ps_ratio", "pfcf_ratio", "fcf_margin_pct",
    ]
    st_mod.dataframe(
        top[show_cols].rename(
            columns={
                "ticker": "代码",
                "name": "公司名",
                "market": "市场",
                "market_cap_m": "市值(百万)",
                "price": "价格",
                "fiscal_year": "财年",
                "ps_ratio": "P/S",
                "pfcf_ratio": "P/FCF",
                "fcf_margin_pct": "FCF Margin(%)",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )
