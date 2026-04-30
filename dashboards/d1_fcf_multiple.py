"""Dashboard D1: price chart with EMA and DCF overlays."""

from __future__ import annotations

import json
import os
from datetime import datetime, date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from google import genai
from google.genai import types

from db.repository import (
    get_ohlcv,
    get_fundamentals,
    get_dcf_history,
    get_fmp_dcf_history,
    get_company,
    get_dcf_metrics,
)
from db.schema import get_conn
from core.symbol_router import apply_global_selection
from dashboards.market_features import get_market_features
from dashboards.symbol_registry import build_symbol_registry, find_registry_row
from etl.loader import upsert_ohlcv_daily, upsert_ohlcv_ema, upsert_fmp_dcf_history
from etl.sources.fmp_dcf import fetch_fmp_dcf_history
from etl.sources.fmp import fetch_profile, fetch_ohlcv, fetch_fx_to_usd
from data_provider import _fmp_analyst_data
from futu_client import FutuClient


_DCF_COLORS = {
    "dcf_14x": "#3b82f6",
    "dcf_24x": "#10b981",
    "dcf_34x": "#f59e0b",
}

_CCY_SYMBOL = {
    "USD": "$",
    "EUR": "EUR ",
    "GBP": "GBP ",
    "JPY": "JPY ",
    "CNY": "¥",
    "CNH": "CNH ",
    "HKD": "HK$",
    "SGD": "SGD ",
    "AUD": "AUD ",
    "NZD": "NZD ",
    "CAD": "CAD ",
    "CHF": "CHF ",
    "SEK": "SEK ",
    "NOK": "NOK ",
    "DKK": "DKK ",
    "KRW": "KRW ",
    "INR": "INR ",
    "TWD": "TWD ",
    "THB": "THB ",
    "MYR": "MYR ",
    "IDR": "IDR ",
    "PHP": "PHP ",
    "VND": "VND ",
    "BRL": "BRL ",
    "MXN": "MXN ",
    "ZAR": "ZAR ",
    "TRY": "TRY ",
    "RUB": "RUB ",
    "AED": "AED ",
    "SAR": "SAR ",
}


def _currency_symbol(ccy: str | None) -> str:
    return _CCY_SYMBOL.get(str(ccy or "USD").upper(), f"{str(ccy or 'USD').upper()} ")


def _rate_on_or_before(fx_dict: dict[str, float], target_date: str | date | datetime | pd.Timestamp) -> float | None:
    if not fx_dict:
        return None
    s = str(pd.Timestamp(target_date).date())
    for d in sorted(fx_dict.keys(), reverse=True):
        if d <= s:
            return float(fx_dict[d])
    oldest = min(fx_dict.keys())
    return float(fx_dict[oldest])


@st.cache_data(ttl=3600, show_spinner=False)
def _load_fx_to_usd_series(currency: str, date_from: str, date_to: str) -> dict[str, float]:
    return fetch_fx_to_usd(currency=currency, date_from=date_from, date_to=date_to)


def _convert_usd_to_listing_value(
    usd_value: float | None,
    *,
    listing_currency: str,
    reporting_currency: str | None,
    fx_to_usd: float | None,
    anchor_date: str | date | datetime | pd.Timestamp | None,
    listing_fx_dict: dict[str, float] | None = None,
) -> float | None:
    if usd_value is None or pd.isna(usd_value):
        return None
    listing = (listing_currency or "USD").upper()
    if listing == "USD":
        return float(usd_value)
    rep = (reporting_currency or "").upper()
    if rep == listing and fx_to_usd and float(fx_to_usd) > 0:
        return float(usd_value) / float(fx_to_usd)
    if listing_fx_dict and anchor_date is not None:
        r = _rate_on_or_before(listing_fx_dict, anchor_date)
        if r and r > 0:
            return float(usd_value) / float(r)
    return float(usd_value)


def _convert_dcf_history_for_listing(
    df_dcf_hist: pd.DataFrame,
    df_fund: pd.DataFrame,
    listing_currency: str,
) -> pd.DataFrame:
    if df_dcf_hist.empty or (listing_currency or "USD").upper() == "USD":
        return df_dcf_hist
    out = df_dcf_hist.copy()
    f = df_fund.copy() if df_fund is not None else pd.DataFrame()
    if f.empty:
        return out
    fy_map: dict[int, dict] = {}
    for _, r in f.iterrows():
        fy = pd.to_numeric(r.get("fiscal_year"), errors="coerce")
        if pd.isna(fy):
            continue
        fy_map[int(fy)] = {
            "reporting_currency": str(r.get("reporting_currency") or "").upper(),
            "fx_to_usd": pd.to_numeric(r.get("fx_to_usd"), errors="coerce"),
        }
    if not fy_map:
        return out

    listing_fx_dict: dict[str, float] | None = None
    if (listing_currency or "USD").upper() != "USD":
        start = str(pd.to_datetime(out["anchor_date"]).min().date())
        end = str(pd.to_datetime(out["anchor_date"]).max().date())
        try:
            listing_fx_dict = _load_fx_to_usd_series((listing_currency or "USD").upper(), start, end)
        except Exception:
            listing_fx_dict = None

    for idx, row in out.iterrows():
        fy = pd.to_numeric(row.get("fiscal_year"), errors="coerce")
        if pd.isna(fy):
            continue
        meta = fy_map.get(int(fy))
        if not meta:
            continue
        for col in ["fcf_ps_avg3yr", "dcf_14x", "dcf_24x", "dcf_34x"]:
            v = pd.to_numeric(row.get(col), errors="coerce")
            if pd.isna(v):
                continue
            out.at[idx, col] = _convert_usd_to_listing_value(
                float(v),
                listing_currency=listing_currency,
                reporting_currency=meta.get("reporting_currency"),
                fx_to_usd=meta.get("fx_to_usd"),
                anchor_date=row.get("anchor_date"),
                listing_fx_dict=listing_fx_dict,
            )
    return out


def _convert_fund_for_listing(df_fund: pd.DataFrame, listing_currency: str) -> pd.DataFrame:
    if df_fund.empty or (listing_currency or "USD").upper() == "USD":
        return df_fund
    out = df_fund.copy()
    for idx, row in out.iterrows():
        v = pd.to_numeric(row.get("fcf_per_share"), errors="coerce")
        if pd.isna(v):
            continue
        out.at[idx, "fcf_per_share"] = _convert_usd_to_listing_value(
            float(v),
            listing_currency=listing_currency,
            reporting_currency=row.get("reporting_currency"),
            fx_to_usd=pd.to_numeric(row.get("fx_to_usd"), errors="coerce"),
            anchor_date=row.get("fiscal_end_date") or row.get("filing_date"),
        )
    return out


def _convert_fmp_dcf_for_listing(
    df_fmp_dcf: pd.DataFrame,
    df_fund: pd.DataFrame,
    listing_currency: str,
) -> pd.DataFrame:
    """Normalize FMP DCF display into listing currency when needed."""
    if df_fmp_dcf.empty or (listing_currency or "USD").upper() == "USD":
        return df_fmp_dcf
    out = df_fmp_dcf.copy()
    f = df_fund.copy() if df_fund is not None else pd.DataFrame()
    if f.empty:
        return out

    listing = (listing_currency or "USD").upper()
    reporting_set = {
        str(v or "").upper()
        for v in f.get("reporting_currency", pd.Series(dtype=str)).tolist()
        if str(v or "").strip()
    }
    # If any annual row reports in listing currency, treat FMP DCF as already local.
    # This avoids double conversion for cases like CN/HK tickers where FMP DCF is local.
    if listing in reporting_set:
        return out

    start = str(pd.to_datetime(out["date"]).min().date())
    end = str(pd.to_datetime(out["date"]).max().date())
    try:
        listing_fx_dict = _load_fx_to_usd_series((listing_currency or "USD").upper(), start, end)
    except Exception:
        return out

    for idx, row in out.iterrows():
        v = pd.to_numeric(row.get("dcf_value"), errors="coerce")
        if pd.isna(v):
            continue
        out.at[idx, "dcf_value"] = _convert_usd_to_listing_value(
            float(v),
            listing_currency=listing_currency,
            reporting_currency="USD",
            fx_to_usd=None,
            anchor_date=row.get("date"),
            listing_fx_dict=listing_fx_dict,
        )
    return out


def _ensure_ema_columns(df_ohlcv: pd.DataFrame) -> pd.DataFrame:
    """Ensure EMA columns exist for plotting without mutating DB."""
    out = df_ohlcv.copy()
    ema10_calc = out["adj_close"].ewm(span=10, adjust=False).mean()
    ema250_calc = out["adj_close"].ewm(span=250, adjust=False).mean()

    if "ema10" not in out.columns:
        out["ema10"] = ema10_calc
    else:
        out["ema10"] = out["ema10"].fillna(ema10_calc)

    if "ema250" not in out.columns:
        out["ema250"] = ema250_calc
    else:
        out["ema250"] = out["ema250"].fillna(ema250_calc)

    return out


def _build_dcf_history_fallback(df_fund: pd.DataFrame,
                                df_ohlcv: pd.DataFrame,
                                ticker: str) -> pd.DataFrame:
    """Build DCF history in-memory from fundamentals when dcf_history is empty."""
    if df_fund.empty or "fcf_per_share" not in df_fund.columns:
        return pd.DataFrame()

    work = df_fund[["fiscal_year", "filing_date", "fcf_per_share"]].copy()
    work["fcf_per_share"] = pd.to_numeric(work["fcf_per_share"], errors="coerce")
    work = work.dropna(subset=["fcf_per_share"]).sort_values("fiscal_year")
    if work.empty:
        return pd.DataFrame()

    trade_dates = pd.to_datetime(df_ohlcv["date"]).dt.normalize().sort_values().reset_index(drop=True)

    def _snap_to_trade_day(filing_date):
        if pd.isna(filing_date):
            return pd.NaT
        ts = pd.Timestamp(filing_date).normalize()
        idx = trade_dates.searchsorted(ts, side="right") - 1
        if idx < 0:
            return ts
        return trade_dates.iloc[idx]

    values = list(work["fcf_per_share"].astype(float))
    years = list(work["fiscal_year"].astype(int))
    filings = list(work["filing_date"])

    rows = []
    for i, year in enumerate(years):
        window = values[max(0, i - 2): i + 1]
        avg = float(pd.Series(window).mean())
        if avg <= 0:
            window5 = values[max(0, i - 4): i + 1]
            avg5 = float(pd.Series(window5).mean())
            if avg5 > avg:
                avg = avg5

        rows.append({
            "ticker": ticker,
            "fiscal_year": year,
            "anchor_date": _snap_to_trade_day(filings[i]),
            "fcf_ps_avg3yr": avg,
            "dcf_14x": 14 * avg,
            "dcf_24x": 24 * avg,
            "dcf_34x": 34 * avg,
        })

    return pd.DataFrame(rows)


def _build_chart(df_ohlcv: pd.DataFrame,
                 df_dcf_hist: pd.DataFrame,
                 df_fmp_dcf: pd.DataFrame,
                 ticker: str,
                 display_currency: str = "USD",
                 currency_symbol: str = "$") -> go.Figure:
    latest_price = float(df_ohlcv["adj_close"].iloc[-1]) if not df_ohlcv.empty else None

    fig = go.Figure()

    fig.add_trace(go.Candlestick(
        x=df_ohlcv["date"],
        open=df_ohlcv["open"],
        high=df_ohlcv["high"],
        low=df_ohlcv["low"],
        close=df_ohlcv["close"],
        name="K线",
        increasing_line_color="#ef5350",
        increasing_fillcolor="#ef5350",
        decreasing_line_color="#26a69a",
        decreasing_fillcolor="#26a69a",
    ))

    fig.add_trace(go.Scattergl(
        x=df_ohlcv["date"],
        y=df_ohlcv["ema10"],
        name="EMA 10",
        line=dict(color="#f94144", width=1),
        mode="lines",
        hoverinfo="skip",
    ))
    fig.add_trace(go.Scattergl(
        x=df_ohlcv["date"],
        y=df_ohlcv["ema250"],
        name="EMA 250",
        line=dict(color="#7209b7", width=2),
        mode="lines",
        hoverinfo="skip",
    ))

    if not df_dcf_hist.empty:
        dcf = df_dcf_hist.sort_values("fiscal_year").copy()
        dcf["anchor_date"] = pd.to_datetime(dcf["anchor_date"])

        for col, name in [
            ("dcf_14x", "DCF 14x"),
            ("dcf_24x", "DCF 24x"),
            ("dcf_34x", "DCF 34x"),
        ]:
            vals = list(dcf[col])
            xs = list(dcf["anchor_date"])
            if vals:
                xs.append(pd.Timestamp(datetime.now().date()))
                vals.append(vals[-1])
            fig.add_trace(go.Scatter(
                x=xs,
                y=vals,
                name=name,
                line=dict(color=_DCF_COLORS[col], width=2.5, dash="dash"),
                mode="lines+markers",
                marker=dict(size=9, symbol="diamond", line=dict(width=1, color="white")),
                connectgaps=True,
            ))

    latest_fmp = None
    if not df_fmp_dcf.empty:
        fmp_vals = pd.to_numeric(df_fmp_dcf["dcf_value"], errors="coerce").dropna()
        if not fmp_vals.empty:
            latest_fmp = float(fmp_vals.iloc[-1])
            ohlcv_dates = pd.to_datetime(df_ohlcv["date"])
            x0 = ohlcv_dates.min()
            x1 = max(ohlcv_dates.max(), pd.Timestamp(datetime.now().date()))
            fig.add_trace(go.Scatter(
                x=[x0, x1],
                y=[latest_fmp, latest_fmp],
                name="FMP DCF",
                line=dict(color="#ff4dd2", width=2.6, dash="dot"),
                mode="lines",
                hovertemplate=f"FMP DCF: {currency_symbol}%{{y:,.2f}}<extra></extra>",
            ))

    annotations = []
    if latest_price is not None:
        annotations.append(dict(
            x=df_ohlcv["date"].iloc[-1],
            y=latest_price,
            text=f"  {currency_symbol}{latest_price:,.2f}",
            showarrow=False,
            font=dict(color="#00d4ff", size=14, family="monospace"),
            xanchor="left",
            yanchor="middle",
            bgcolor="rgba(10,14,23,0.8)",
            bordercolor="#00d4ff",
            borderwidth=1,
            borderpad=4,
        ))

    # Build y-range from all plotted series so DCF/FMP DCF lines are never clipped.
    y_candidates = []
    y_candidates.extend(pd.to_numeric(df_ohlcv["high"], errors="coerce").dropna().tolist())
    y_candidates.extend(pd.to_numeric(df_ohlcv["low"], errors="coerce").dropna().tolist())
    y_candidates.extend(pd.to_numeric(df_ohlcv["ema10"], errors="coerce").dropna().tolist())
    y_candidates.extend(pd.to_numeric(df_ohlcv["ema250"], errors="coerce").dropna().tolist())

    if not df_dcf_hist.empty:
        for col in ["dcf_14x", "dcf_24x", "dcf_34x"]:
            if col in df_dcf_hist.columns:
                y_candidates.extend(pd.to_numeric(df_dcf_hist[col], errors="coerce").dropna().tolist())

    if latest_fmp is not None:
        y_candidates.append(latest_fmp)

    if y_candidates:
        y_max = max(y_candidates)
        y_min = min(y_candidates)
        span = max(y_max - y_min, 1e-6)
        y_top = y_max + span * 0.12
        y_bottom = max(0.0, y_min - span * 0.12)
    else:
        price_max = float(df_ohlcv["high"].max())
        price_min = float(df_ohlcv["low"].min())
        y_top = price_max * 1.12
        y_bottom = max(0.0, price_min * 0.88)

    date_min = pd.to_datetime(df_ohlcv["date"]).min()
    date_max = pd.to_datetime(df_ohlcv["date"]).max()
    date_span = date_max - date_min
    x_right = date_max + date_span * 0.2

    fig.update_layout(
        title=dict(text=f"{ticker.upper()} 日K线", font=dict(color="#e0e7ff", size=20)),
        yaxis_title=f"价格 ({display_currency})",
        xaxis_rangeslider_visible=False,
        template="plotly_dark",
        paper_bgcolor="#0a0e17",
        plot_bgcolor="#0f1629",
        height=760,
        dragmode="pan",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            font=dict(color="#94a3b8", size=12),
        ),
        margin=dict(l=60, r=100, t=60, b=40),
        xaxis=dict(
            gridcolor="#1e3a5f",
            zerolinecolor="#1e3a5f",
            range=[date_min, x_right],
            tickfont=dict(size=13, color="#cbd5e1"),
            title=dict(font=dict(size=14, color="#cbd5e1")),
        ),
        yaxis=dict(
            gridcolor="#1e3a5f",
            zerolinecolor="#1e3a5f",
            range=[y_bottom, y_top],
            fixedrange=False,
            tickfont=dict(size=13, color="#cbd5e1"),
            title=dict(text=f"价格 ({display_currency})", font=dict(size=15, color="#cbd5e1")),
        ),
        annotations=annotations,
    )
    fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])

    return fig


def _load_gemini_api_key() -> str:
    """Load GEMINI_API_KEY from env/.env for note markdown formatting."""
    key = (os.environ.get("GEMINI_API_KEY") or "").strip()
    if key:
        return key
    try:
        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
        env_path = os.path.abspath(env_path)
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                txt = line.strip()
                if txt.startswith("GEMINI_API_KEY="):
                    return txt.split("=", 1)[1].strip()
    except OSError:
        pass
    return ""


def _get_note_row(ticker: str) -> dict | None:
    """Fetch latest note row for ticker from notes table."""
    with get_conn(readonly=True) as conn:
        cur = conn.execute(
            """
            SELECT id, ticker, raw_text, markdown, created_at, updated_at
            FROM notes
            WHERE ticker = ?
            ORDER BY updated_at DESC, created_at DESC
            LIMIT 1
            """,
            [ticker],
        )
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))


def _generate_markdown_from_raw_note(raw_text: str) -> str:
    """Send raw note to LLM and require strict JSON response containing markdown source."""
    api_key = _load_gemini_api_key()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY 未配置")

    prompt = (
        "你是一个专业投研笔记整理助手。\\n"
        "任务：把下面的原始笔记整理成结构清晰的 Markdown。\\n"
        "要求：\\n"
        "1) 保留原始信息，不编造事实。\\n"
        "2) 按 Markdown 语法组织为标题、要点、结论、待跟踪。\\n"
        "3) 只输出 JSON，不要输出任何解释文字。\\n"
        "4) JSON 格式必须是：{\"markdown\": \"...\"}\\n\\n"
        "原始笔记如下：\\n"
        f"{raw_text}"
    )

    client = genai.Client(api_key=api_key)
    resp = client.models.generate_content(
        model="gemini-2.5-flash-lite",
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0.2,
            response_mime_type="application/json",
        ),
    )

    text = (getattr(resp, "text", "") or "").strip()
    if not text:
        raise RuntimeError("LLM 未返回内容")

    try:
        payload = json.loads(text)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"LLM 返回非JSON: {str(e)[:80]}") from e

    markdown = str(payload.get("markdown") or "").strip()
    if not markdown:
        raise RuntimeError("LLM 返回JSON缺少 markdown 字段")
    return markdown


def _append_note_and_save(ticker: str, new_note_text: str) -> tuple[bool, str]:
    """Append raw note (not overwrite), generate markdown via LLM, save both into DB."""
    content = (new_note_text or "").strip()
    if not content:
        return False, "请输入笔记内容"

    row = _get_note_row(ticker)
    note_id = row["id"] if row else f"d1_note_{ticker.upper()}"
    raw_existing = (row.get("raw_text") or "") if row else ""
    md_existing = (row.get("markdown") or "") if row else ""

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    append_block = f"[{ts}]\\n{content}"
    merged_raw = f"{raw_existing}\\n\\n{append_block}".strip() if raw_existing else append_block

    # Always try LLM on each save; keep previous markdown if this call fails.
    try:
        md_text = _generate_markdown_from_raw_note(merged_raw)
    except Exception as e:
        md_text = md_existing
        llm_err = str(e)[:80]
    else:
        llm_err = ""

    with get_conn() as conn:
        exists = conn.execute("SELECT 1 FROM notes WHERE id = ? LIMIT 1", [note_id]).fetchone()
        if exists:
            conn.execute(
                """
                UPDATE notes
                SET ticker = ?, raw_text = ?, markdown = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                [ticker, merged_raw, md_text, note_id],
            )
        else:
            conn.execute(
                """
                INSERT INTO notes (id, ticker, raw_text, markdown, created_at, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                [note_id, ticker, merged_raw, md_text],
            )

    if llm_err:
        return True, f"已追加保存原始笔记；Markdown 生成失败: {llm_err}"
    return True, "已追加保存笔记，并更新 Markdown 笔记"


def _render_notes_panel_d1(ticker: str) -> None:
    """Render notes area below chart, same width as chart column."""
    show_md_key = f"d1_note_show_md_{ticker}"
    if show_md_key not in st.session_state:
        st.session_state[show_md_key] = False

    with st.form(key=f"d1_note_form_{ticker}", clear_on_submit=True):
        note_input = st.text_area(
            "追加笔记",
            key=f"d1_note_input_{ticker}",
            height=78,
            placeholder="输入本次新增笔记，保存后会追加到原始笔记并生成 Markdown",
            label_visibility="collapsed",
        )
        do_save_note = st.form_submit_button("保存笔记", type="primary")

    if do_save_note:
        with st.spinner("保存中：追加原始笔记并请求 LLM 生成 Markdown..."):
            ok, msg = _append_note_and_save(ticker, note_input)
        if ok:
            st.success(msg)
        else:
            st.error(msg)

    with st.expander("投研笔记（点击展开）", expanded=False):
        c_show, c_hide = st.columns([1, 1], gap="small")
        with c_show:
            if st.button("显示已保存笔记", key=f"d1_note_show_btn_{ticker}", width="stretch"):
                st.session_state[show_md_key] = True
        with c_hide:
            if st.button("收起内容", key=f"d1_note_hide_btn_{ticker}", width="stretch"):
                st.session_state[show_md_key] = False

        if st.session_state.get(show_md_key, False):
            row = _get_note_row(ticker)
            existing_md = (row.get("markdown") or "") if row else ""
            if existing_md:
                st.markdown(existing_md)
            else:
                st.caption("当前没有已保存的 Markdown 笔记")
        else:
            st.caption("点击“显示已保存笔记”后再渲染")


def _refresh_latest_fmp_data(ticker: str) -> tuple[bool, str]:
    """
    Lightweight refresh: fetch latest price and FMP DCF, write to DB.
    
    Returns: (success: bool, message: str)
    """
    try:
        ticker_upper = ticker.upper()
        
        # Fetch profile for shares_out
        profile = fetch_profile(ticker_upper)
        if not profile:
            return False, "无法获取公司信息"
        shares_out = profile.get("shares_out")
        
        # Fetch latest OHLCV only (date_from = today - 1 day to catch latest close)
        from_date = str(date.today())
        ohlcv_rows = fetch_ohlcv(ticker_upper, shares_out_raw=shares_out, date_from=from_date)
        
        if not ohlcv_rows:
            return False, "FMP 未返回最新价格数据"
        
        # Write OHLCV
        conn = get_conn()
        upsert_ohlcv_daily(conn, ohlcv_rows)
        
        # Compute and upsert EMA for new row
        df_new = pd.DataFrame(ohlcv_rows)
        if not df_new.empty:
            # Get all historical data for EMA calculation
            df_hist = get_ohlcv(ticker_upper)
            if not df_hist.empty:
                # Normalize date dtype before concat/sort to avoid str vs Timestamp comparison crash.
                df_hist["date"] = pd.to_datetime(df_hist["date"], errors="coerce")
                df_new["date"] = pd.to_datetime(df_new["date"], errors="coerce")
                df_new = df_new.dropna(subset=["date"])
                # Combine historical + new
                df_combined = pd.concat([df_hist, df_new], ignore_index=True)
                df_combined = df_combined.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
                
                # Calculate EMA
                df_combined["ema10"] = df_combined["adj_close"].ewm(span=10, adjust=False).mean()
                df_combined["ema250"] = df_combined["adj_close"].ewm(span=250, adjust=False).mean()
                
                # Upsert last row's EMA
                last_row = df_combined.iloc[-1]
                upsert_ohlcv_ema(conn, [{
                    "ticker": ticker_upper,
                    "date": last_row["date"],
                    "ema10": last_row["ema10"],
                    "ema250": last_row["ema250"],
                }])
        
        # Fetch latest FMP DCF
        from etl.sources.fmp import load_api_key
        api_key = load_api_key()
        if api_key:
            try:
                dcf_rows = fetch_fmp_dcf_history(ticker_upper, api_key)
                if dcf_rows:
                    upsert_fmp_dcf_history(conn, dcf_rows)
                    msg = f"✅ 更新成功：{len(ohlcv_rows)} 行OHLCV，{len(dcf_rows)} 行FMP DCF"
                else:
                    msg = f"✅ 更新成功：{len(ohlcv_rows)} 行OHLCV（FMP DCF无新数据）"
            except Exception as e:
                msg = f"✅ 更新成功：{len(ohlcv_rows)} 行OHLCV（FMP DCF获取失败: {str(e)[:30]}）"
        else:
            msg = f"✅ 更新成功：{len(ohlcv_rows)} 行OHLCV（API密钥未配置）"
        
        conn.close()
        return True, msg
        
    except Exception as e:
        import traceback
        error_msg = f"{str(e)[:100]}"
        return False, f"❌ 更新失败：{error_msg}"


@st.cache_data(ttl=3600)
def _fetch_analyst_data_cached(ticker: str) -> dict:
    """Fetch and cache analyst data for 1 hour."""
    return _fmp_analyst_data(ticker)


def _render_analyst_panel_d1(ticker: str, latest_price: float | None) -> None:
    """Render analyst panel using the old-version UI design."""
    try:
        analyst_data = _fetch_analyst_data_cached(ticker)
        current_price = latest_price
        currency_sym = "$"

        if not analyst_data:
            return

        pt = analyst_data.get("price_target") or {}
        pts = analyst_data.get("price_target_summary") or {}
        grades = analyst_data.get("grades") or []
        gc = analyst_data.get("grades_consensus") or {}
        status = analyst_data.get("fmp_analyst_status", "")

        has_data = bool(pt or pts or grades or gc)

        if not has_data:
            st.caption(f"⚠️ {status}" if status else "暂无分析师数据")
            return

        if pt or pts:
            consensus = (pt or {}).get("targetConsensus")
            high = (pt or {}).get("targetHigh")
            low = (pt or {}).get("targetLow")
            median = (pt or {}).get("targetMedian")

            upside = ((consensus - current_price) / current_price * 100) if (consensus and current_price) else None
            up_sign = ("+" if upside >= 0 else "") if upside is not None else ""
            up_clr = "#22c55e" if (upside is not None and upside >= 0) else "#ef4444"
            up_badge = (
                f'<span style="display:inline-block;background:{up_clr}22;color:{up_clr};'
                f'border:1px solid {up_clr}55;border-radius:5px;padding:1px 7px;'
                f'font-size:.75rem;font-weight:700;vertical-align:middle;margin-left:6px;">'
                f'{up_sign}{upside:.1f}%</span>'
            ) if upside is not None else ""

            headline_html = ""
            if consensus:
                headline_html = (
                    f'<div style="display:flex;align-items:baseline;gap:8px;margin-bottom:6px;">'
                    f'<span style="color:#94a3b8;font-size:.85rem;white-space:nowrap;">目标价</span>'
                    f'<span style="color:#e0e7ff;font-size:1.6rem;font-weight:800;'
                    f'letter-spacing:-.5px;font-family:\'Cascadia Mono\',monospace;">'
                    f'{currency_sym}{consensus:,.2f}</span>'
                    f'{up_badge}'
                    f'</div>'
                )

            range_bar_html = ""
            if high and low and high > low:
                def _pct(v):
                    return max(0.0, min(100.0, (v - low) / (high - low) * 100))

                cur_pct = _pct(current_price) if current_price else None
                con_pct = _pct(consensus) if consensus else None
                med_pct = _pct(median) if median else None

                # Decide vertical offsets so 现价 / 共识 labels do not collide horizontally.
                # Both labels live above the bar; if their x-positions are close, stagger.
                cur_label_top = -34
                con_label_top = -34
                if cur_pct is not None and con_pct is not None and abs(cur_pct - con_pct) < 22:
                    # Put 共识 higher, 现价 lower so they do not overlap.
                    con_label_top = -34
                    cur_label_top = -20

                markers_html = ""
                if cur_pct is not None:
                    markers_html += (
                        f'<div style="position:absolute;left:{cur_pct:.1f}%;'
                        f'transform:translateX(-50%);top:-3px;">'
                        f'<div style="width:2px;height:14px;background:#00d4ff;'
                        f'border-radius:1px;box-shadow:0 0 5px #00d4ff88;"></div></div>'
                        f'<div style="position:absolute;left:{cur_pct:.1f}%;'
                        f'transform:translateX(-50%);top:{cur_label_top}px;'
                        f'color:#00d4ff;font-size:.74rem;font-weight:700;white-space:nowrap;">现价</div>'
                    )
                if con_pct is not None:
                    markers_html += (
                        f'<div style="position:absolute;left:{con_pct:.1f}%;'
                        f'transform:translateX(-50%);top:-3px;">'
                        f'<div style="width:2px;height:14px;background:{up_clr};'
                        f'border-radius:1px;box-shadow:0 0 5px {up_clr}88;"></div></div>'
                        f'<div style="position:absolute;left:{con_pct:.1f}%;'
                        f'transform:translateX(-50%);top:{con_label_top}px;'
                        f'color:{up_clr};font-size:.74rem;font-weight:700;white-space:nowrap;">共识</div>'
                    )
                if med_pct is not None:
                    markers_html += (
                        f'<div style="position:absolute;left:{med_pct:.1f}%;'
                        f'transform:translateX(-50%);top:1px;">'
                        f'<div style="width:8px;height:8px;background:#f59e0b;'
                        f'border-radius:50%;box-shadow:0 0 4px #f59e0b88;"></div></div>'
                        f'<div style="position:absolute;left:{med_pct:.1f}%;'
                        f'transform:translateX(-50%);top:14px;'
                        f'color:#f59e0b;font-size:.7rem;font-weight:600;white-space:nowrap;">中位</div>'
                    )

                fill_html = (
                    f'<div style="position:absolute;left:0;width:{cur_pct:.1f}%;'
                    f'height:100%;background:rgba(148,163,184,.2);border-radius:4px 0 0 4px;">'
                    f'</div>'
                ) if cur_pct is not None else ""
                if cur_pct is not None and con_pct is not None:
                    l = min(cur_pct, con_pct)
                    w = abs(con_pct - cur_pct)
                    fill_html += (
                        f'<div style="position:absolute;left:{l:.1f}%;width:{w:.1f}%;'
                        f'height:100%;background:{up_clr}44;border-radius:2px;"></div>'
                    )

                range_bar_html = (
                    f'<div style="margin:42px 4px 34px;">'
                    f'<div style="position:relative;height:8px;background:#1e3a5f;'
                    f'border-radius:4px;overflow:visible;">'
                    f'{fill_html}{markers_html}'
                    f'</div>'
                    f'<div style="display:flex;justify-content:space-between;margin-top:8px;">'
                    f'<span style="color:#64748b;font-size:.74rem;">{currency_sym}{low:,.0f}</span>'
                    f'<span style="color:#64748b;font-size:.74rem;font-style:italic;">目标价区间</span>'
                    f'<span style="color:#64748b;font-size:.74rem;">{currency_sym}{high:,.0f}</span>'
                    f'</div></div>'
                )

            hml_cells = ""
            for v, lbl, clr in [(high, "最高", "#22c55e66"), (median, "中位", "#f59e0b66"), (low, "最低", "#ef444466")]:
                if v:
                    hml_cells += (
                        f'<div style="text-align:center;flex:1;">'
                        f'<div style="color:#64748b;font-size:.78rem;margin-bottom:2px;">{lbl}</div>'
                        f'<div style="color:#e0e7ff;font-size:.95rem;font-weight:700;'
                        f'border-bottom:2px solid {clr};padding-bottom:2px;">'
                        f'{currency_sym}{v:,.2f}</div></div>'
                    )
            hml_html = (
                f'<div style="display:flex;justify-content:space-evenly;'
                f'background:#0a0e17;border-radius:7px;padding:8px 6px;margin-bottom:10px;">'
                f'{hml_cells}</div>'
            ) if hml_cells else ""

            hist_html = ""
            if pts:
                lm_n = pts.get("lastMonthCount", 0)
                lm_avg = pts.get("lastMonthAvgPriceTarget")
                lq_n = pts.get("lastQuarterCount", 0)
                lq_avg = pts.get("lastQuarterAvgPriceTarget")
                ly_n = pts.get("lastYearCount", 0)
                ly_avg = pts.get("lastYearAvgPriceTarget")

                hist_rows = [(lm_avg, "近1月", lm_n), (lq_avg, "近1季", lq_n), (ly_avg, "近1年", ly_n)]
                hist_cells = ""
                for avg, label, n in hist_rows:
                    if avg:
                        delta = ((avg - current_price) / current_price * 100) if current_price else None
                        d_html = ""
                        if delta is not None:
                            d_clr = "#22c55e" if delta >= 0 else "#ef4444"
                            d_sign = "+" if delta >= 0 else ""
                            d_html = f'<div style="color:{d_clr};font-size:.74rem;">{d_sign}{delta:.1f}%</div>'
                        hist_cells += (
                            f'<div style="text-align:center;flex:1;">'
                            f'<div style="color:#475569;font-size:.76rem;">{label} <span style="color:#334155">({n}家)</span></div>'
                            f'<div style="color:#94a3b8;font-size:.9rem;font-weight:600;">{currency_sym}{avg:,.2f}</div>'
                            f'{d_html}'
                            f'</div>'
                        )
                if hist_cells:
                    hist_html = (
                        f'<div style="display:flex;justify-content:space-evenly;'
                        f'border-top:1px solid #1e3a5f;padding-top:8px;margin-top:2px;">'
                        f'{hist_cells}</div>'
                    )

            st.markdown(
                f'<div style="background:linear-gradient(145deg,#0f1d35,#111827);'
                f'border:1px solid #1e3a5f;border-radius:12px;padding:14px 16px 10px;'
                f'margin-bottom:10px;box-shadow:0 4px 20px rgba(0,0,0,.4),'
                f'inset 0 1px 0 rgba(255,255,255,.04);">'
                f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:8px;">'
                f'<span style="display:inline-block;width:3px;height:14px;'
                f'background:linear-gradient(180deg,#00d4ff,#3b82f6);border-radius:2px;"></span>'
                f'<span style="color:#94a3b8;font-size:.85rem;font-weight:700;'
                f'letter-spacing:.05em;">分析师共识</span>'
                f'</div>'
                f'{headline_html}'
                f'{range_bar_html}'
                f'{hml_html}'
                f'{hist_html}'
                f'</div>',
                unsafe_allow_html=True,
            )

        buys = sells = holds = total = 0

        if gc:
            strong_buy = int(gc.get("strongBuy") or 0)
            buy = int(gc.get("buy") or 0)
            hold = int(gc.get("hold") or 0)
            sell = int(gc.get("sell") or 0)
            strong_sell = int(gc.get("strongSell") or 0)
            buys = strong_buy + buy
            holds = hold
            sells = sell + strong_sell
            total = buys + holds + sells
            period_label = "综合评级共识"
        elif grades:
            from datetime import datetime as _dt, timedelta as _td

            BUY_SET = {
                "Strong Buy", "Buy", "Outperform", "Overweight",
                "Add", "Accumulate", "Positive", "Market Outperform",
            }
            SELL_SET = {
                "Sell", "Strong Sell", "Underperform", "Underweight",
                "Reduce", "Negative",
            }
            cutoff = (_dt.now() - _td(days=90)).strftime("%Y-%m-%d")
            recent = [g for g in grades if (g.get("date") or "") >= cutoff] or grades[:15]
            buys = sum(1 for g in recent if (g.get("newGrade") or "") in BUY_SET)
            sells = sum(1 for g in recent if (g.get("newGrade") or "") in SELL_SET)
            holds = len(recent) - buys - sells
            total = buys + holds + sells
            period_label = "近90日评级"

        if total > 0:
            b_pct = buys / total * 100
            h_pct = holds / total * 100
            s_pct = sells / total * 100
            header_html = ""
            if not gc:
                header_html = (
                    f'<div style="margin:6px 0 4px;">'
                    f'<span style="color:#94a3b8;font-size:.85rem;">'
                    f'{period_label}（买入 {buys} / 持有 {holds} / 卖出 {sells}）'
                    f'</span></div>'
                )
            # 持有 sits at the buy/hold boundary (= b_pct%); clamp away from edges
            # so the label has room to render.
            hold_left = max(6.0, min(94.0, b_pct))
            mix_bar_html = (
                header_html
                + f'<div style="display:flex;height:11px;border-radius:5px;'
                f'overflow:hidden;margin-bottom:5px;">'
                f'<div style="width:{b_pct:.1f}%;background:#22c55e;"></div>'
                f'<div style="width:{h_pct:.1f}%;background:#f59e0b;"></div>'
                f'<div style="width:{s_pct:.1f}%;background:#ef4444;"></div>'
                f'</div>'
                f'<div style="position:relative;height:1.3rem;font-size:.85rem;font-weight:600;margin-bottom:8px;">'
                f'<span style="position:absolute;left:0;color:#22c55e;">买入 {b_pct:.0f}%</span>'
                f'<span style="position:absolute;left:{hold_left:.1f}%;transform:translateX(-50%);'
                f'color:#f59e0b;">持有 {h_pct:.0f}%</span>'
                f'<span style="position:absolute;right:0;color:#ef4444;">卖出 {s_pct:.0f}%</span>'
                f'</div>'
            )
            st.markdown(
                mix_bar_html,
                unsafe_allow_html=True,
            )
            if gc:
                consensus_label = gc.get("consensus") or gc.get("rating") or ""
                if consensus_label:
                    cl_color = "#22c55e" if "buy" in consensus_label.lower() else (
                        "#ef4444" if "sell" in consensus_label.lower() else "#f59e0b"
                    )
                    st.markdown(
                        f'<div style="display:flex;justify-content:space-between;align-items:center;'
                        f'gap:8px;margin:0 0 8px;">'
                        f'<span style="color:#94a3b8;font-size:.82rem;">'
                        f'{period_label}（买入 {buys} / 持有 {holds} / 卖出 {sells}）</span>'
                        f'<span style="background:#1a2035;border:1px solid #1e3a5f;'
                        f'border-radius:6px;padding:2px 10px;color:{cl_color};'
                        f'font-size:.9rem;font-weight:700;white-space:nowrap;">'
                        f'共识: {consensus_label}</span></div>',
                        unsafe_allow_html=True,
                    )

        if grades:
            BUY_SET_COLORS = {
                "Strong Buy", "Buy", "Outperform", "Overweight",
                "Add", "Accumulate", "Positive", "Market Outperform",
            }
            SELL_SET_COLORS = {
                "Sell", "Strong Sell", "Underperform", "Underweight",
                "Reduce", "Negative",
            }

            def _grade_color(g):
                if g in BUY_SET_COLORS:
                    return "#22c55e"
                if g in SELL_SET_COLORS:
                    return "#ef4444"
                return "#94a3b8"

            st.markdown('<div style="color:#94a3b8;font-size:.74rem;margin:2px 0 4px;">最近评级动作</div>', unsafe_allow_html=True)
            action_rows = []
            for g in grades:
                dt = (g.get("date") or "")[:10]
                co = g.get("gradingCompany") or "—"
                new_g = g.get("newGrade") or "—"
                prev_g = g.get("previousGrade") or "—"
                action = (g.get("action") or "").lower()
                action_cn = {
                    "upgrade": "上调",
                    "downgrade": "下调",
                    "init": "首次",
                    "reiterated": "重申",
                    "maintained": "维持",
                }.get(action, action or "—")
                action_rows.append(
                    {
                        "日期": dt,
                        "机构": co,
                        "动作": action_cn,
                        "新评级": new_g,
                        "前评级": prev_g,
                    }
                )
            if action_rows:
                action_df = pd.DataFrame(action_rows)
                st.dataframe(action_df, width="stretch", height=252, hide_index=True)

    except Exception as e:
        st.warning(f"分析师数据加载失败: {str(e)[:50]}")


def _render_price_alert_panel_d1(ticker: str, dcf_metrics: dict | None) -> None:
    """Render compact price alert panel."""
    try:
        # Try to connect to Futu OpenD
        try:
            with FutuClient() as client:
                futu_connected = True
        except Exception:
            futu_connected = False
        
        if not futu_connected:
            st.warning("需要启动 Futu OpenD (127.0.0.1:11111) 来设置价格提醒")
            return

        dcf_14x = dcf_24x = dcf_34x = None
        if dcf_metrics:
            dcf_14x = dcf_metrics.get("dcf_14x")
            dcf_24x = dcf_metrics.get("dcf_24x")
            dcf_34x = dcf_metrics.get("dcf_34x")

        left_quick, right_custom = st.columns([0.9, 2.1], gap="small")
        with left_quick:
            if dcf_14x:
                if st.button(f"14x  ${dcf_14x:.1f}", key=f"alert_dcf14_{ticker}", width="stretch"):
                    with FutuClient() as client:
                        code = FutuClient.build_code(ticker, "US")
                        success, msg = client.set_price_alert(code, dcf_14x, note="DCF 14x target")
                        if success:
                            st.success(f"14x 提醒已设置: ${dcf_14x:.2f}")
                        else:
                            st.error(f"设置失败: {msg}")
            if dcf_24x:
                if st.button(f"24x  ${dcf_24x:.1f}", key=f"alert_dcf24_{ticker}", width="stretch"):
                    with FutuClient() as client:
                        code = FutuClient.build_code(ticker, "US")
                        success, msg = client.set_price_alert(code, dcf_24x, note="DCF 24x target")
                        if success:
                            st.success(f"24x 提醒已设置: ${dcf_24x:.2f}")
                        else:
                            st.error(f"设置失败: {msg}")
            if dcf_34x:
                if st.button(f"34x  ${dcf_34x:.1f}", key=f"alert_dcf34_{ticker}", width="stretch"):
                    with FutuClient() as client:
                        code = FutuClient.build_code(ticker, "US")
                        success, msg = client.set_price_alert(code, dcf_34x, note="DCF 34x target")
                        if success:
                            st.success(f"34x 提醒已设置: ${dcf_34x:.2f}")
                        else:
                            st.error(f"设置失败: {msg}")

        with right_custom:
            c_price, c_type, c_btn = st.columns([1.15, 1.0, 0.75], gap="small")
            with c_price:
                custom_price = st.number_input(
                    "目标价格",
                    value=0.0,
                    step=0.01,
                    key=f"price_{ticker}",
                    label_visibility="collapsed",
                    placeholder="目标价格",
                )
            with c_type:
                reminder_type = st.selectbox(
                    "类型",
                    ["PRICE_DOWN", "PRICE_UP"],
                    key=f"type_{ticker}",
                    label_visibility="collapsed",
                )
            with c_btn:
                submit = st.button("设置", key=f"alert_custom_{ticker}", width="stretch")

            note = st.text_input("备注", value="", placeholder="备注(可选)", key=f"note_{ticker}", label_visibility="collapsed")

        if submit:
            if custom_price <= 0:
                st.error("请输入有效的目标价格")
            else:
                try:
                    with FutuClient() as client:
                        code = FutuClient.build_code(ticker, "US")
                        success, msg = client.set_price_alert(code, custom_price, note=note, reminder_type=reminder_type)
                        if success:
                            st.success(f"提醒已设置: ${custom_price:.2f} ({reminder_type}) - {msg}")
                        else:
                            st.error(f"设置失败: {msg}")
                except Exception as e:
                    st.error(f"连接错误: {str(e)[:50]}")
    
    except Exception as e:
        st.warning(f"价格提醒功能暂不可用: {str(e)[:50]}")


def _render_metrics_panel(
    ticker: str,
    df_ohlcv: pd.DataFrame,
    df_fund: pd.DataFrame,
    df_fmp_dcf: pd.DataFrame,
    company: dict | None,
    currency_symbol: str = "$",
) -> None:
    """Render right-side compact metrics without oversized cards.

    `df_fmp_dcf` and `company` are passed in by the caller (already fetched on
    the shared readonly conn) to keep the panel free of DB I/O.
    """
    with st.container():
        # Latest price
        latest_price = float(df_ohlcv["adj_close"].iloc[-1]) if not df_ohlcv.empty else None
        latest_price_text = f"{currency_symbol}{latest_price:,.2f}" if latest_price else "—"
        
        # Market cap
        market_cap = company.get("market_cap") if company else None
        if market_cap and market_cap > 0:
            mcap_display = f"{currency_symbol}{market_cap:,.0f}M" if market_cap >= 1000 else f"{currency_symbol}{market_cap:,.0f}M"
        else:
            mcap_display = "—"
        market_cap_text = mcap_display
        
        # Latest and average FCF
        if not df_fund.empty:
            df_fund_sorted = df_fund.sort_values("fiscal_year", ascending=False)
            latest_fcf_ps = df_fund_sorted.iloc[0].get("fcf_per_share")
            if latest_fcf_ps is not None:
                latest_fcf_ps = float(latest_fcf_ps)
                latest_fcf_text = f"{currency_symbol}{latest_fcf_ps:,.2f}"
            else:
                latest_fcf_text = "—"
            
            # 3-year average FCF
            if len(df_fund_sorted) >= 3:
                avg_fcf = df_fund_sorted.head(3)["fcf_per_share"].astype(float).mean()
            else:
                avg_fcf = df_fund_sorted["fcf_per_share"].astype(float).mean()
            
            if not pd.isna(avg_fcf) and avg_fcf > 0:
                avg_fcf_text = f"{currency_symbol}{avg_fcf:,.2f}"
            else:
                avg_fcf_text = "—"
        else:
            latest_fcf_text = "—"
            avg_fcf_text = "—"

        # FMP DCF latest
        if df_fmp_dcf is not None and not df_fmp_dcf.empty:
            fmp_latest = df_fmp_dcf.iloc[-1]
            fmp_dcf_val = float(fmp_latest.get("dcf_value", 0))
            fmp_dcf_text = f"{currency_symbol}{fmp_dcf_val:,.2f}" if fmp_dcf_val > 0 else "—"
        else:
            fmp_dcf_text = "—"

        metrics_html = (
            '<div style="display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;'
            'background:#0f1629;border:1px solid #1e3a5f;border-radius:8px;padding:8px 10px;margin:2px 0 8px;">'
            f'<div><div style="color:#64748b;font-size:.64rem;">最新价格</div><div style="color:#e0e7ff;font-size:1.02rem;font-weight:800;line-height:1.2;">{latest_price_text}</div></div>'
            f'<div><div style="color:#64748b;font-size:.64rem;">市值</div><div style="color:#e0e7ff;font-size:1.02rem;font-weight:800;line-height:1.2;">{market_cap_text}</div></div>'
            f'<div><div style="color:#64748b;font-size:.64rem;">3年平均FCF/S</div><div style="color:#e0e7ff;font-size:1.02rem;font-weight:800;line-height:1.2;">{avg_fcf_text}</div></div>'
            f'<div><div style="color:#64748b;font-size:.64rem;">FMP DCF估值</div><div style="color:#e879f9;font-size:1.02rem;font-weight:900;line-height:1.2;">{fmp_dcf_text}</div></div>'
            '</div>'
        )
        st.markdown(metrics_html, unsafe_allow_html=True)


def _d1_row_id(market: str, ticker: str) -> str:
    return f"{(market or 'US').strip().upper()}\t{(ticker or '').strip().upper()}"


def _parse_d1_row_id(row_id: str) -> tuple[str, str]:
    parts = (row_id or "").split("\t", 1)
    if len(parts) != 2:
        return "US", ""
    return parts[0].strip().upper(), parts[1].strip().upper()


def render_d1_stock(market: str = "US", ticker_override: str | None = None) -> str:
    st.subheader("D1: 价格图线")
    market_u = (market or "US").strip().upper()
    features = get_market_features(market_u)
    default_ticker = str(
        ticker_override
        or st.session_state.get(f"d1_{market_u.lower()}_ticker")
        or ("NVDA" if market_u == "US" else "")
    ).strip().upper()

    registry_rows: list[dict] = list(build_symbol_registry())
    if default_ticker and find_registry_row(registry_rows, market_u, default_ticker) is None:
        registry_rows.insert(
            0,
            {
                "ticker": default_ticker,
                "market": market_u,
                "name": "",
                "label": f"{default_ticker} [{market_u}]",
            },
        )

    option_ids = [_d1_row_id(r["market"], r["ticker"]) for r in registry_rows]
    label_for = {_d1_row_id(r["market"], r["ticker"]): r["label"] for r in registry_rows}
    default_id = _d1_row_id(market_u, default_ticker)
    if default_id not in label_for:
        registry_rows.insert(
            0,
            {
                "ticker": default_ticker,
                "market": market_u,
                "name": "",
                "label": f"{default_ticker} [{market_u}]",
            },
        )
        option_ids = [_d1_row_id(r["market"], r["ticker"]) for r in registry_rows]
        label_for = {_d1_row_id(r["market"], r["ticker"]): r["label"] for r in registry_rows}
        default_id = _d1_row_id(market_u, default_ticker)

    parent_target = f"{market_u}|{default_ticker}"
    if st.session_state.get("_d1_parent_target") != parent_target:
        st.session_state["_d1_parent_target"] = parent_target
        st.session_state["d1_unified_ticker_select"] = default_id

    # Main layout: chart ~70%, right panel ~30%
    col_left, col_right = st.columns([7, 3], gap="medium")

    with col_right:
        # Same row: ticker + start_date + refresh, vertically centered
        c_tk, c_dt, c_rf = st.columns([1.4, 1.1, 0.7], gap="small", vertical_alignment="center")
        with c_tk:
            st.selectbox(
                "Ticker",
                options=option_ids,
                key="d1_unified_ticker_select",
                format_func=lambda rid, lf=label_for: lf.get(rid, rid),
                help="全市场代码（US/CN/HK）；输入字符可筛选",
                label_visibility="collapsed",
            )
            sel_id = st.session_state.get("d1_unified_ticker_select", default_id)
            sm, tk = _parse_d1_row_id(str(sel_id))
            if sm and tk and (sm != market_u or tk != default_ticker):
                apply_global_selection(st.session_state, sm, tk)
                st.rerun()
            ticker = tk
        st.session_state[f"d1_{market_u.lower()}_ticker"] = ticker
        with c_dt:
            start_date = st.date_input(
                "起始日期",
                value=pd.Timestamp("2000-01-01"),
                key=f"d1_{market_u.lower()}_start",
                label_visibility="collapsed",
            )
        with c_rf:
            do_refresh = st.button("刷新", key=f"refresh_top_{market_u}_{ticker}", type="secondary", width="stretch")

    if do_refresh:
        success, msg = _refresh_latest_fmp_data(ticker)
        if success:
            st.success(msg)
            st.rerun()
        else:
            st.error(msg)

    # Single readonly connection for every repository read in this render —
    # avoids ~6 separate `duckdb.connect` calls per page rerun.
    with get_conn(readonly=True) as conn:
        df_ohlcv = get_ohlcv(ticker, start_date=str(start_date), conn=conn)
        if df_ohlcv.empty:
            with col_left:
                st.info("本地数据库暂无该 ticker 的行情数据。请先在命令行完成 ETL。")
            return ticker

        df_ohlcv = _ensure_ema_columns(df_ohlcv)
        df_fund = get_fundamentals(ticker, conn=conn)
        df_dcf_hist = get_dcf_history(ticker, conn=conn)
        if df_dcf_hist.empty:
            df_dcf_hist = _build_dcf_history_fallback(df_fund, df_ohlcv, ticker)
        df_fmp_dcf = get_fmp_dcf_history(ticker, conn=conn)
        company = get_company(ticker, conn=conn)
        dcf_metrics = get_dcf_metrics(ticker, conn=conn)

    listing_currency = str((company or {}).get("currency") or features.display_currency or "USD").upper()
    currency_symbol = _currency_symbol(listing_currency)
    df_fund_display = _convert_fund_for_listing(df_fund, listing_currency)
    df_dcf_hist_display = _convert_dcf_history_for_listing(df_dcf_hist, df_fund, listing_currency)
    df_fmp_dcf_display = _convert_fmp_dcf_for_listing(df_fmp_dcf, df_fund, listing_currency)

    fig = _build_chart(
        df_ohlcv,
        df_dcf_hist_display,
        df_fmp_dcf_display,
        ticker=ticker,
        display_currency=listing_currency,
        currency_symbol=currency_symbol,
    )

    with col_left:
        st.plotly_chart(fig, width="stretch", config={"scrollZoom": True})
        _render_notes_panel_d1(ticker)

    with col_right:
        _render_metrics_panel(ticker, df_ohlcv, df_fund_display, df_fmp_dcf_display, company, currency_symbol=currency_symbol)
        if features.supports_analyst_panel:
            latest_price = float(df_ohlcv["adj_close"].iloc[-1]) if not df_ohlcv.empty else None
            _render_analyst_panel_d1(ticker, latest_price)
        if features.supports_price_alert:
            _render_price_alert_panel_d1(ticker, dcf_metrics)

    return ticker


def render_d1_us() -> str:
    """Backward-compatible alias."""
    return render_d1_stock(market="US")
