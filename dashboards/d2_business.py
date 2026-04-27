"""Dashboard D2: company business portrait (read-only)."""

from __future__ import annotations

from urllib.parse import quote_plus

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from db.repository import (
    get_company_profile,
    get_ebitda_coverage_history,
    get_latest_revenue_year,
    get_latest_total_revenue,
    get_management,
    get_revenue_by_geography,
    get_revenue_by_segment,
)
from dashboards.cache import get_all_tickers_cached


def _fallback_ticker() -> str:
    ticker_df = get_all_tickers_cached(market="US")
    ticker_options = sorted(ticker_df["ticker"].dropna().astype(str).unique().tolist()) if not ticker_df.empty else []
    return ticker_options[0] if ticker_options else "NVDA"


def _build_google_search_link(name: str, title: str, company_name: str | None) -> str:
    query = f"{name} {title} {company_name or ''}"
    return f"https://www.google.com/search?q={quote_plus(query.strip())}"


def _render_management_panel(ticker: str, profile: dict | None) -> None:
    st.markdown("### 管理层")
    df = get_management(ticker)
    if df.empty:
        st.info("暂无管理层数据")
        return

    company_name = (profile or {}).get("name")
    chips: list[str] = []
    for _, row in df.iterrows():
        name = str(row.get("name") or "").strip()
        title = str(row.get("title") or "").strip()
        if not name:
            continue
        url = _build_google_search_link(name, title, company_name)
        label = f"{name} / {title}" if title else name
        chips.append(
            '<a href="{url}" target="_blank" style="display:inline-block;margin:2px 4px 2px 0;'
            'padding:3px 8px;border:1px solid #1e3a5f;border-radius:999px;'
            'background:#0f1629;color:#cfe8ff;text-decoration:none;font-size:.94rem;font-weight:600;line-height:1.25;">{label}</a>'
            .format(url=url, label=label)
        )

    if not chips:
        st.info("暂无管理层数据")
        return

    st.markdown("<div style='margin-top:-2px'>{}</div>".format("".join(chips)), unsafe_allow_html=True)


def _render_description(profile: dict | None) -> None:
    st.markdown("### 公司业务描述")
    if not profile:
        st.info("暂无公司简介")
        return

    desc = (profile.get("description") or "").strip()
    if not desc:
        st.info("暂无公司简介")
        return

    st.markdown(
        '<div style="font-size:1.05rem; line-height:1.8; color:#e6efff;">{}</div>'.format(desc.replace("\n", "<br>")),
        unsafe_allow_html=True,
    )


def _build_pie(df: pd.DataFrame, label_col: str, title: str) -> go.Figure:
    fig = go.Figure(
        data=[
            go.Pie(
                labels=df[label_col],
                values=df["revenue"],
                textinfo="label+percent",
                textposition="inside",
                insidetextorientation="radial",
                hovertemplate="%{label}<br>Revenue: %{value:,.2f}M<extra></extra>",
                hole=0.28,
                domain=dict(x=[0.0, 0.72], y=[0.06, 0.94]),
            )
        ]
    )
    fig.update_layout(
        title=dict(
            text=title,
            font=dict(size=16, color="#e0e7ff"),
            x=0.36,
            xanchor="center",
            y=0.99,
            yanchor="top",
        ),
        margin=dict(l=4, r=4, t=44, b=8),
        height=400,
        font=dict(size=13, color="#cbd5e1"),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(
            orientation="v",
            yanchor="middle",
            y=0.5,
            xanchor="left",
            x=1.02,
            font=dict(size=12, color="#cbd5e1"),
            bgcolor="rgba(15,22,41,0.92)",
            bordercolor="#1e3a5f",
            borderwidth=1,
        ),
        uniformtext_minsize=10,
        uniformtext_mode="hide",
    )
    return fig


def _render_revenue_distribution(ticker: str) -> None:
    st.markdown("### 收入分布（最新财年）")

    latest_year = get_latest_revenue_year(ticker)
    if latest_year is None:
        st.info("暂无分部或地区收入数据")
        return

    total_rev_year, total_rev = get_latest_total_revenue(ticker)
    if total_rev is None:
        seg_total = pd.to_numeric(get_revenue_by_segment(ticker, latest_year).get("revenue"), errors="coerce").dropna().sum()
        geo_total = pd.to_numeric(get_revenue_by_geography(ticker, latest_year).get("revenue"), errors="coerce").dropna().sum()
        fallback_total = seg_total if seg_total > 0 else geo_total
        if fallback_total > 0:
            total_rev_year, total_rev = latest_year, float(fallback_total)
    total_text = f"${total_rev:,.2f}M" if total_rev is not None else "-"
    st.markdown(
        (
            '<div style="background:#0f1629;border:1px solid #1e3a5f;border-radius:10px;'
            "padding:10px 12px;margin-bottom:20px;margin-top:4px;\">"
            f'<span style="color:#94a3b8;">最新总收入 ({total_rev_year or latest_year})：</span>'
            f'<span style="color:#e0e7ff;font-weight:800;font-size:1.02rem;"> {total_text}</span>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )

    seg_df = get_revenue_by_segment(ticker, latest_year)
    geo_df = get_revenue_by_geography(ticker, latest_year)

    c1, c2 = st.columns(2, gap="medium")
    with c1:
        if seg_df.empty:
            st.info("暂无按板块收入")
        else:
            st.plotly_chart(_build_pie(seg_df, "segment", f"按板块 ({latest_year})"), width="stretch")
    with c2:
        if geo_df.empty:
            st.info("暂无按地区收入")
        else:
            st.plotly_chart(_build_pie(geo_df, "region", f"按地区 ({latest_year})"), width="stretch")


def _render_safety_panel(ticker: str) -> None:
    st.markdown("### 安全性：EBITDA / 利息覆盖率")

    df = get_ebitda_coverage_history(ticker)
    if df.empty:
        st.info("暂无安全性数据")
        return

    work = df.copy()
    for col in ["operating_income", "depreciation", "interest_expense"]:
        work[col] = pd.to_numeric(work[col], errors="coerce")

    work["ebitda"] = work["operating_income"].fillna(0) + work["depreciation"].fillna(0)
    work["coverage"] = work.apply(
        lambda r: (r["ebitda"] / abs(r["interest_expense"])) if pd.notna(r["interest_expense"]) and r["interest_expense"] not in (0, 0.0) else pd.NA,
        axis=1,
    )
    work = work.dropna(subset=["coverage"])

    if work.empty:
        st.info("暂无可计算覆盖率的数据")
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=work["fiscal_year"],
        y=work["coverage"],
        mode="lines+markers",
        name="EBITDA / Interest",
        line=dict(color="#22c55e", width=2.5),
        marker=dict(size=7),
        hovertemplate="FY %{x}<br>Coverage: %{y:.2f}x<extra></extra>",
    ))
    fig.add_hline(y=3, line_dash="dash", line_color="#f59e0b", annotation_text="3x 警戒线")
    fig.update_layout(
        height=300,
        margin=dict(l=10, r=10, t=16, b=10),
        font=dict(size=15),
        xaxis=dict(title="Fiscal Year", tickmode="linear", title_font=dict(size=16), tickfont=dict(size=14)),
        yaxis=dict(title="Coverage (x)", title_font=dict(size=16), tickfont=dict(size=14)),
    )
    st.plotly_chart(fig, width="stretch")


def render_d2_stock(ticker: str | None = None, market: str = "US") -> None:
    st.subheader("D2: 公司业务画像")
    m = (market or "US").strip().lower()
    ticker = str(ticker or st.session_state.get(f"d1_{m}_ticker") or "").strip().upper()
    if not ticker:
        ticker = _fallback_ticker()
        st.session_state[f"d1_{m}_ticker"] = ticker

    st.caption(f"当前股票：{ticker}（与 D1 共享）")

    profile = get_company_profile(ticker)

    _render_description(profile)
    _render_revenue_distribution(ticker)

    c1, c2 = st.columns(2, gap="small")
    with c1:
        _render_management_panel(ticker, profile)
    with c2:
        _render_safety_panel(ticker)


def render_d2_us(ticker: str | None = None) -> None:
    """Backward-compatible alias."""
    render_d2_stock(ticker=ticker, market="US")
