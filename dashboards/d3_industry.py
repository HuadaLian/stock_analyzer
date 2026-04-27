"""Dashboard D3: industry peer comparison (read-only).

Charts:
  * **Scatter (log–log)**: x = market cap (USD), y = latest total revenue (USD).
    Highlights the active ticker; labels the top 10 peers by revenue with
    shortened company names.
  * **CCDF**: y = revenue (USD log), x = % of peers with revenue ≥ y (survival).

Currency rules (scatter + CCDF):
  * **Revenue** (``fundamentals_annual``): values are **USD millions** when
    ``fund_currency == 'USD'`` (normal ETL); otherwise multiplied by FX from
    ``fund_currency`` → USD.
  * **Market cap** (``ohlcv_daily``): **millions** in **listing** currency
    (``companies.currency``); multiplied by FX from that code → USD.
  * **Checks**: ISO 3-letter codes, FMP FX success flags, optional notice when
    listing currency ≠ fund currency.
"""

from __future__ import annotations

import re
from datetime import date, timedelta

import math
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from db.repository import get_company_profile, get_industry_peers_revenue
from etl.sources.fmp import fetch_fx_to_usd


_TARGET_COLOR = "#fbbf24"
_LINE_COLOR = "#3b82f6"
_PEER_COLOR = "#60a5fa"
_BULK_COLOR = "#64748b"

# Anchor ticks (raw USD) and their human labels.
_TICK_VALUES = [1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13]
_TICK_LABELS = ["$1M", "$10M", "$100M", "$1B", "$10B", "$100B", "$1T", "$10T"]


def _format_revenue_usd(value: float | None) -> str:
    """Format raw USD value (not millions) to compact human string."""
    if value is None or pd.isna(value):
        return "-"
    if value >= 1e12:
        return f"${value / 1e12:.2f}T"
    if value >= 1e9:
        return f"${value / 1e9:.2f}B"
    if value >= 1e6:
        return f"${value / 1e6:.1f}M"
    return f"${value:,.0f}"


def _truncate_label(name: str, max_len: int = 18) -> str:
    s = (name or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


_ISO_CCY = re.compile(r"^[A-Za-z]{3}$")


def _normalize_currency_code(raw: str | None) -> str:
    """Uppercase ISO-like 3-letter code; invalid → USD."""
    c = (raw or "USD").strip().upper()
    if _ISO_CCY.match(c):
        return c
    return "USD"


@st.cache_data(ttl=21600)
def _cached_fx_to_usd(currency: str) -> tuple[float, bool]:
    """Latest spot: 1 *currency* = *rate* USD. Bool = FMP returned a quote (not assumed)."""
    cur = _normalize_currency_code(currency)
    if cur == "USD":
        return (1.0, True)
    today = date.today()
    start = today - timedelta(days=14)
    try:
        rates = fetch_fx_to_usd(cur, start.isoformat(), today.isoformat())
    except Exception:
        return (1.0, False)
    if not rates:
        return (1.0, False)
    latest_date = max(rates.keys())
    return (float(rates[latest_date]), True)


def _convert_to_usd(peers: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Revenue → USD (per ``fund_currency``); market cap → USD (per listing ``currency``)."""
    warnings: list[str] = []
    work = peers.copy()
    work["revenue"] = pd.to_numeric(work["revenue"], errors="coerce")

    if "currency" in work.columns:
        raw_listing = work["currency"].fillna("").astype(str).str.strip().str.upper()
    else:
        raw_listing = pd.Series(["USD"] * len(work), index=work.index, dtype=str)

    bad_listing = ~raw_listing.str.match(_ISO_CCY.pattern, na=False) & (raw_listing != "")
    for idx in work.loc[bad_listing].index:
        tk = work.at[idx, "ticker"] if "ticker" in work.columns else "?"
        warnings.append(f"{tk}: 挂牌货币「{raw_listing.at[idx]}」非 ISO 4217 三位码，已按 USD 处理市值换汇")

    work["listing_currency"] = raw_listing.where(raw_listing.str.match(_ISO_CCY.pattern, na=False), "USD").map(
        _normalize_currency_code
    )

    if "fund_currency" in work.columns:
        work["fund_currency"] = (
            work["fund_currency"].fillna("USD").astype(str).str.strip().str.upper().map(_normalize_currency_code)
        )
    else:
        work["fund_currency"] = pd.Series(["USD"] * len(work), index=work.index, dtype=str)

    # FX caches per code
    fund_codes = [c for c in work["fund_currency"].dropna().unique().tolist() if c != "USD"]
    list_codes = [c for c in work["listing_currency"].dropna().unique().tolist() if c != "USD"]
    fx_fund: dict[str, tuple[float, bool]] = {c: _cached_fx_to_usd(c) for c in fund_codes}
    fx_fund["USD"] = (1.0, True)
    fx_list: dict[str, tuple[float, bool]] = {c: _cached_fx_to_usd(c) for c in list_codes}
    fx_list["USD"] = (1.0, True)

    for _, row in work.iterrows():
        fc = row["fund_currency"]
        if fc != "USD":
            ok = fx_fund.get(fc, (1.0, False))[1]
            if not ok:
                tk = row.get("ticker", "?")
                warnings.append(f"{tk}: 财报币种 {fc} 无有效 FMP 汇率，收入按 1 {fc}=1 USD 近似（请核对数据）")

    for _, row in work.iterrows():
        lc = row["listing_currency"]
        if lc != "USD":
            ok = fx_list.get(lc, (1.0, False))[1]
            if not ok:
                tk = row.get("ticker", "?")
                warnings.append(f"{tk}: 挂牌币种 {lc} 无有效 FMP 汇率，市值按 1 {lc}=1 USD 近似（请核对数据）")

    work["fx_fund"] = work["fund_currency"].map(lambda c: fx_fund.get(c, (1.0, False))[0])
    work["fx_listing"] = work["listing_currency"].map(lambda c: fx_list.get(c, (1.0, False))[0])

    # Revenue: DB stores USD millions when fund_currency is USD (ETL default).
    work["revenue_usd"] = np.where(
        work["fund_currency"].eq("USD"),
        work["revenue"] * 1e6,
        work["revenue"] * 1e6 * work["fx_fund"],
    )

    if "market_cap" in work.columns:
        work["market_cap"] = pd.to_numeric(work["market_cap"], errors="coerce")
        work["market_cap_usd"] = work["market_cap"] * 1e6 * work["fx_listing"]
    else:
        work["market_cap_usd"] = np.nan

    mismatch = work["listing_currency"].ne(work["fund_currency"]) & work["revenue"].notna()
    for _, row in work[mismatch].iterrows():
        tk = row.get("ticker", "?")
        warnings.append(
            f"{tk}: 挂牌币 {row['listing_currency']} ≠ 财报库币种 {row['fund_currency']} "
            f"（收入按财报币种→USD；市值按挂牌币→USD）"
        )

    # de-dupe while preserving order
    seen: set[str] = set()
    out_warn: list[str] = []
    for w in warnings:
        if w not in seen:
            seen.add(w)
            out_warn.append(w)

    return work, out_warn


def _render_header(profile: dict | None, peer_count: int) -> None:
    sector = (profile or {}).get("sector") or "-"
    industry = (profile or {}).get("industry") or "-"
    st.markdown(
        (
            '<div style="background:#0f1629;border:1px solid #1e3a5f;border-radius:10px;'
            'padding:10px 12px;margin-bottom:8px;display:flex;flex-wrap:wrap;gap:18px;">'
            f'<div><span style="color:#94a3b8;">板块</span> '
            f'<span style="color:#00d4ff;font-weight:700;font-size:1.02rem;">{sector}</span></div>'
            f'<div><span style="color:#94a3b8;">行业</span> '
            f'<span style="color:#00d4ff;font-weight:700;font-size:1.02rem;">{industry}</span></div>'
            f'<div><span style="color:#94a3b8;">行业内公司数</span> '
            f'<span style="color:#e0e7ff;font-weight:700;font-size:1.02rem;">{peer_count}</span></div>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )


def _build_distribution_figure(peers: pd.DataFrame, ticker: str) -> tuple[go.Figure, dict]:
    work = peers.dropna(subset=["revenue_usd"]).copy()
    work = work[work["revenue_usd"] > 0].sort_values("revenue_usd", ascending=False).reset_index(drop=True)

    n = len(work)
    # CCDF: x[i] = (i+1)/n * 100 = percent of peers with revenue >= work.revenue_usd[i].
    work["rank"] = np.arange(1, n + 1)
    work["pct_at_or_above"] = work["rank"] / n * 100.0

    rev_min = float(work["revenue_usd"].min())
    rev_max = float(work["revenue_usd"].max())
    log_min = math.floor(math.log10(rev_min)) - 0.2
    log_max = math.ceil(math.log10(rev_max)) + 0.2

    # Step line through every peer (sorted descending by revenue).
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=work["pct_at_or_above"],
            y=work["revenue_usd"],
            mode="lines",
            line=dict(color=_LINE_COLOR, width=2.4, shape="hv"),
            name="同业累计分布",
            hoverinfo="skip",
            showlegend=False,
        )
    )

    target_row = work[work["ticker"] == ticker]
    has_target = not target_row.empty

    other = work[work["ticker"] != ticker]
    if not other.empty:
        fig.add_trace(
            go.Scatter(
                x=other["pct_at_or_above"],
                y=other["revenue_usd"],
                mode="markers",
                marker=dict(size=10, color=_PEER_COLOR,
                            line=dict(color="#0a0e17", width=1)),
                name="同业公司",
                customdata=np.stack([other["ticker"], other["name"]], axis=-1),
                hovertemplate=(
                    "%{customdata[0]} · %{customdata[1]}<br>"
                    "总收入: $%{y:,.0f}<br>"
                    "≥ 此值的公司占比: %{x:.1f}%<extra></extra>"
                ),
                showlegend=False,
            )
        )

    target_rev = None
    target_pct = None
    target_rank = None
    if has_target:
        t = target_row.iloc[0]
        target_rev = float(t["revenue_usd"])
        target_pct = float(t["pct_at_or_above"])
        target_rank = int(t["rank"])
        fig.add_trace(
            go.Scatter(
                x=[target_pct],
                y=[target_rev],
                mode="markers+text",
                marker=dict(size=18, color=_TARGET_COLOR, symbol="star",
                            line=dict(color="#0a0e17", width=1.5)),
                text=[f"  {ticker}"],
                textposition="middle right",
                textfont=dict(color=_TARGET_COLOR, size=14, family="Cascadia Mono, monospace"),
                name=ticker,
                hovertemplate=(
                    f"{ticker}<br>总收入: ${target_rev:,.0f}<br>"
                    f"行业排名: {target_rank} / {n}<br>"
                    "≥ 此值的公司占比: %{x:.1f}%<extra></extra>"
                ),
                showlegend=False,
            )
        )

    # Reference lines + labels at $1M / $1B / $1T (only those inside y-range).
    shapes = []
    annotations = []
    for v, lbl in zip(_TICK_VALUES, _TICK_LABELS):
        if math.log10(v) < log_min or math.log10(v) > log_max:
            continue
        if lbl in {"$1M", "$1B", "$1T"}:
            shapes.append(dict(
                type="line", xref="paper", x0=0, x1=1,
                yref="y", y0=v, y1=v,
                line=dict(color="#1e3a5f", width=1, dash="dot"),
            ))

    fig.update_layout(
        height=440,
        margin=dict(l=18, r=24, t=24, b=18),
        font=dict(size=14, color="#cbd5e1"),
        showlegend=False,
        plot_bgcolor="#0a0e17",
        paper_bgcolor="#0a0e17",
        xaxis=dict(
            title="≥ 该总收入的公司占比 (%)",
            title_font=dict(size=15, color="#cbd5e1"),
            tickfont=dict(size=13, color="#cbd5e1"),
            gridcolor="#1e3a5f",
            range=[0, 105],
            ticksuffix="%",
        ),
        yaxis=dict(
            title="最新总收入 (USD, log scale)",
            title_font=dict(size=15, color="#cbd5e1"),
            tickfont=dict(size=13, color="#cbd5e1"),
            type="log",
            range=[log_min, log_max],
            tickmode="array",
            tickvals=_TICK_VALUES,
            ticktext=_TICK_LABELS,
            gridcolor="#1e3a5f",
        ),
        shapes=shapes,
        annotations=annotations,
    )

    summary = {
        "rank": target_rank if target_rank is not None else 0,
        "total": n,
        "percentile": (100.0 - target_pct) if target_pct is not None else float("nan"),
        "target_revenue": target_rev,
        "median": float(np.median(work["revenue_usd"])) if n else None,
        "max": float(work["revenue_usd"].max()) if n else None,
    }
    return fig, summary


def _build_market_cap_revenue_scatter(peers: pd.DataFrame, ticker: str) -> go.Figure | None:
    """Log–log scatter: market cap (x) vs revenue (y). Top 10 by revenue get name labels; target = star + ticker."""
    need = {"revenue_usd", "market_cap_usd"}
    if not need.issubset(peers.columns):
        return None
    work = peers.dropna(subset=["revenue_usd", "market_cap_usd"]).copy()
    work = work[(work["revenue_usd"] > 0) & (work["market_cap_usd"] > 0)]
    if len(work) < 2:
        return None

    work = work.sort_values("revenue_usd", ascending=False).reset_index(drop=True)
    k = min(10, len(work))
    top10 = work.head(k)
    top10_tickers = set(top10["ticker"].tolist())

    bulk = work[(~work["ticker"].isin(top10_tickers)) & (work["ticker"] != ticker)]
    top10_others = top10[top10["ticker"] != ticker]

    x_min, x_max = float(work["market_cap_usd"].min()), float(work["market_cap_usd"].max())
    y_min, y_max = float(work["revenue_usd"].min()), float(work["revenue_usd"].max())
    log_x0 = math.floor(math.log10(x_min)) - 0.18
    log_x1 = math.ceil(math.log10(x_max)) + 0.18
    log_y0 = math.floor(math.log10(y_min)) - 0.18
    log_y1 = math.ceil(math.log10(y_max)) + 0.18

    fig = go.Figure()

    if not bulk.empty:
        fig.add_trace(
            go.Scatter(
                x=bulk["market_cap_usd"],
                y=bulk["revenue_usd"],
                mode="markers",
                marker=dict(size=8, color=_BULK_COLOR, line=dict(width=1, color="#0a0e17")),
                customdata=np.stack(
                    [
                        bulk["ticker"],
                        bulk["name"],
                        bulk["listing_currency"],
                        bulk["fund_currency"],
                    ],
                    axis=-1,
                ),
                hovertemplate=(
                    "%{customdata[0]} · %{customdata[1]}<br>"
                    "挂牌: %{customdata[2]}　财报库: %{customdata[3]}<br>"
                    "市值(USD): $%{x:,.0f}<br>总收入(USD): $%{y:,.0f}<extra></extra>"
                ),
                showlegend=False,
            )
        )

    if not top10_others.empty:
        labels = [_truncate_label(str(n)) for n in top10_others["name"]]
        fig.add_trace(
            go.Scatter(
                x=top10_others["market_cap_usd"],
                y=top10_others["revenue_usd"],
                mode="markers+text",
                marker=dict(size=12, color=_PEER_COLOR, line=dict(width=1, color="#0a0e17")),
                text=labels,
                textposition="top center",
                textfont=dict(size=11, color="#e2e8f0"),
                customdata=np.stack(
                    [
                        top10_others["ticker"],
                        top10_others["name"],
                        top10_others["listing_currency"],
                        top10_others["fund_currency"],
                    ],
                    axis=-1,
                ),
                hovertemplate=(
                    "%{customdata[0]} · %{customdata[1]}<br>"
                    "挂牌: %{customdata[2]}　财报库: %{customdata[3]}<br>"
                    "市值(USD): $%{x:,.0f}<br>总收入(USD): $%{y:,.0f}<extra></extra>"
                ),
                showlegend=False,
            )
        )

    target_rows = work[work["ticker"] == ticker]
    if not target_rows.empty:
        row = target_rows.iloc[0]
        fig.add_trace(
            go.Scatter(
                x=[float(row["market_cap_usd"])],
                y=[float(row["revenue_usd"])],
                mode="markers+text",
                marker=dict(
                    size=22,
                    color=_TARGET_COLOR,
                    symbol="star",
                    line=dict(color="#0a0e17", width=1.5),
                ),
                text=[f"  {ticker}"],
                textposition="middle right",
                textfont=dict(color=_TARGET_COLOR, size=13, family="Cascadia Mono, monospace"),
                name=ticker,
                customdata=[
                    [
                        ticker,
                        row.get("name", ""),
                        row.get("listing_currency", ""),
                        row.get("fund_currency", ""),
                    ]
                ],
                hovertemplate=(
                    f"{ticker} · {row.get('name', '')}<br>"
                    "挂牌: %{customdata[2]}　财报库: %{customdata[3]}<br>"
                    "市值(USD): $%{x:,.0f}<br>总收入(USD): $%{y:,.0f}<extra></extra>"
                ),
                showlegend=False,
            )
        )

    axis_log = dict(
        type="log",
        tickmode="array",
        tickvals=_TICK_VALUES,
        ticktext=_TICK_LABELS,
        gridcolor="#1e3a5f",
        tickfont=dict(size=12, color="#cbd5e1"),
        title_font=dict(size=14, color="#cbd5e1"),
    )

    fig.update_layout(
        height=460,
        margin=dict(l=18, r=20, t=36, b=18),
        font=dict(size=13, color="#cbd5e1"),
        title=dict(
            text="市值 vs 最新总收入（USD；市值←挂牌币，收入←财报库币种）",
            font=dict(size=15, color="#e0e7ff"),
            x=0.02,
            xanchor="left",
        ),
        showlegend=False,
        plot_bgcolor="#0a0e17",
        paper_bgcolor="#0a0e17",
        xaxis=dict(
            title="市值（换算后 USD, log）",
            range=[log_x0, log_x1],
            **axis_log,
        ),
        yaxis=dict(
            title="最新总收入（换算后 USD, log）",
            range=[log_y0, log_y1],
            **axis_log,
        ),
    )
    return fig


def _render_peer_table(peers: pd.DataFrame, ticker: str) -> None:
    df = peers.copy()
    df["revenue_fmt"] = df["revenue_usd"].apply(_format_revenue_usd)
    df["fiscal_year"] = df["fiscal_year"].astype("Int64")
    df = df.rename(columns={
        "ticker": "代码",
        "name": "公司名",
        "currency": "货币",
        "fiscal_year": "财年",
        "revenue_fmt": "最新总收入 (USD)",
    })[["代码", "公司名", "货币", "财年", "最新总收入 (USD)"]]

    def _row_style(row):
        return [
            "background-color: rgba(245, 158, 11, 0.18); font-weight: 700;" if row["代码"] == ticker else ""
            for _ in row
        ]

    st.dataframe(
        df.style.apply(_row_style, axis=1),
        use_container_width=True,
        hide_index=True,
        height=min(360, 38 + len(df) * 35),
    )


def render_d3_stock(ticker: str | None = None, market: str = "US") -> None:
    st.subheader("D3: 行业内比较")
    m = (market or "US").strip().lower()
    ticker = str(ticker or st.session_state.get(f"d1_{m}_ticker") or "").strip().upper()
    if not ticker:
        st.info("尚未选择股票")
        return

    profile = get_company_profile(ticker)
    if not profile or not profile.get("sector") or not profile.get("industry"):
        st.warning(f"{ticker} 缺少板块/行业信息，无法做行业内比较。")
        return

    peers_raw = get_industry_peers_revenue(ticker)
    valid_peers = peers_raw.dropna(subset=["revenue"])

    _render_header(profile, len(valid_peers))

    if valid_peers.empty:
        st.info("数据库中暂无同板块同行业的公司收入数据。")
        return

    peers, currency_warnings = _convert_to_usd(valid_peers)

    if currency_warnings:
        with st.expander("币种 / 换汇校验（散点图）", expanded=False):
            for msg in currency_warnings:
                st.markdown(f"- {msg}")

    if len(peers) < 2:
        st.info(f"同板块同行业目前仅有 {len(peers)} 家公司，分布图需要至少 2 家。")
        _render_peer_table(peers, ticker)
        return

    fig_scatter = _build_market_cap_revenue_scatter(peers, ticker)
    if fig_scatter is not None:
        st.markdown("##### 市值 × 总收入（双对数，均为 USD）")
        st.caption(
            "市值：最新交易日 ohlcv_daily.market_cap（百万）× 挂牌货币→USD；"
            "收入：最新财年 revenue（百万）× 财报库 currency→USD（通常为已归一的 USD）。"
            "悬停可见挂牌/财报币种。"
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
    else:
        st.info("同业中暂无足够的「市值 + 收入」数据点，已跳过散点图（请确认 OHLCV 已拉取且含 market_cap）。")

    st.markdown("##### 收入累计分布（CCDF）")
    fig, summary = _build_distribution_figure(peers, ticker)

    rank_text = "-"
    if summary["target_revenue"] is not None:
        rank_text = (
            f"第 {summary['rank']} / {summary['total']} 名 "
            f"(P{summary['percentile']:.0f})"
        )
    st.markdown(
        (
            '<div style="display:flex;flex-wrap:wrap;gap:14px;margin-bottom:6px;">'
            f'<span style="color:#94a3b8;">{ticker} 排名:</span>'
            f'<span style="color:#fbbf24;font-weight:700;">{rank_text}</span>'
            f'<span style="color:#94a3b8;">中位数:</span>'
            f'<span style="color:#e0e7ff;font-weight:600;">{_format_revenue_usd(summary["median"])}</span>'
            f'<span style="color:#94a3b8;">行业最大:</span>'
            f'<span style="color:#e0e7ff;font-weight:600;">{_format_revenue_usd(summary["max"])}</span>'
            '</div>'
        ),
        unsafe_allow_html=True,
    )

    st.plotly_chart(fig, use_container_width=True)

    with st.expander("查看行业公司列表", expanded=True):
        _render_peer_table(peers, ticker)


def render_d3_us(ticker: str | None = None) -> None:
    """Backward-compatible alias."""
    render_d3_stock(ticker=ticker, market="US")
