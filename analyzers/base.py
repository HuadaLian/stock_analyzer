"""Base market analyzer with shared rendering & AI fill logic."""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import re
import time
from datetime import datetime
from data_provider import compute_dcf_lines
from gemini_chat import (
    fill_fcf_table_with_llm, save_fcf_table, load_fcf_table,
    recompute_fcf_per_share,
)


class MarketAnalyzer:
    """Base class for market-specific stock analysis.

    Subclasses must set class attributes and implement ``fetch_data``.
    """

    # ── Override in subclass ─────────────────────────────────────────
    market: str = ""              # "US", "CN", "HK"
    default_currency: str = "USD"
    ticker_input_label: str = "代码"
    ticker_input_default: str = ""
    analyze_button_label: str = "📊 一键分析"
    data_source_desc: str = ""    # shown in the spinner
    supports_filing_download: bool = False  # show "下载年报" button only when True

    # ── Static helpers ───────────────────────────────────────────────

    @staticmethod
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

    @staticmethod
    def _apply_adjusted_fcf(data: dict) -> dict:
        """Recompute per-share FCF using latest shares outstanding.

        Since stock prices are adjusted (前复权), FCF per share must also use
        the latest total shares to be on the same basis as the adjusted price.

        Also converts the FCF table from the reported currency (fmp_currency) to
        USD when needed, so DCF overlay lines align with the USD-denominated price
        chart.  Conversion is done here — not in get_us_data — so it applies
        regardless of how the table was built (FMP, loaded CSV, or AI fill).
        The saved CSV on disk always stores the original reported-currency values;
        conversion is re-applied fresh each time this function runs.
        """
        from data_provider import _convert_fcf_to_usd

        data = dict(data)  # shallow copy to avoid mutating cached data
        fcf_table = data.get("fcf_table")
        if fcf_table is None or fcf_table.empty:
            return data

        # ── Currency conversion (non-USD reporters: CNY / JPY / EUR / etc.) ───
        fmp_currency = (data.get("fmp_currency") or "USD").upper()
        if fmp_currency != "USD":
            try:
                fcf_table, fx_note = _convert_fcf_to_usd(fcf_table, fmp_currency)
                data["fcf_table"] = fcf_table
                # Success: store original currency + mark converted
                data["fmp_original_currency"] = fmp_currency
                data["fmp_currency_converted"] = True
                prev_status = data.get("fmp_status", "")
                data["fmp_status"] = (
                    prev_status.rstrip() + f" → 已换算 USD ({fx_note})"
                ).lstrip()
            except Exception as _cx_err:
                # Failure: keep values in original currency; flag so table shows warning
                data["fmp_original_currency"] = fmp_currency
                data["fmp_currency_converted"] = False
                prev_status = data.get("fmp_status", "")
                data["fmp_status"] = (
                    prev_status.rstrip()
                    + f" ⚠️ 货币换算失败 ({fmp_currency}→USD): {_cx_err}"
                ).lstrip()

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

    @staticmethod
    def _fcf_data_sufficient(data: dict) -> tuple:
        """Check whether the FCF table has sufficient quality to skip AI fill.

        Returns (is_sufficient: bool, reason: str).
        Rules:
          - Table must exist and be non-empty
          - At least one non-null FCF value
          - No more than 50% of FCF rows are null
          - Latest year must not be older than 3 years
        """
        fcf_table = data.get("fcf_table")
        if fcf_table is None or fcf_table.empty:
            return False, "无 FCF 数据"

        if "FCF" not in fcf_table.columns:
            return False, "FCF 列缺失"

        total_rows = len(fcf_table)
        valid_fcf = int(fcf_table["FCF"].notna().sum())

        if valid_fcf == 0:
            return False, "FCF 全为空值"

        missing_ratio = 1.0 - valid_fcf / total_rows
        if missing_ratio > 0.5:
            return False, f"FCF 数据 {missing_ratio:.0%} 缺失（{valid_fcf}/{total_rows} 年有效）"

        # Check recency: latest entry should not be more than 3 years stale
        try:
            latest_year = int(str(fcf_table.iloc[0]["年份"])[:4])
            current_year = pd.Timestamp.now().year
            if latest_year < current_year - 3:
                return False, f"最新数据为 {latest_year} 年，数据可能过旧"
        except Exception:
            pass

        return True, f"{valid_fcf}/{total_rows} 年有效 FCF 数据"

    @staticmethod
    def _build_fcf_table_html(fcf_table, currency, source="", prev_table=None,
                               original_currency=None, currency_converted=True):
        """Build styled HTML for the FCF table. Highlight cells that changed from prev_table.

        *original_currency*: if set (e.g. "CNY"), shows a header badge with the
        reported currency and whether conversion to USD succeeded.
        *currency_converted*: True = conversion succeeded; False = failed (shows warning).
        The "兑USD汇率" column is formatted as a plain decimal (no currency symbol).
        """
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

        def _fmt_fx(x):
            if pd.isna(x):
                return "N/A"
            return f"{float(x):.4f}"

        for col in ["OCF", "CapEx", "FCF"]:
            if col in display_tbl.columns:
                display_tbl[col] = display_tbl[col].apply(_fmt_big)
        for col in ["每股FCF", "3年均每股FCF", "5年均每股FCF", "yf每股FCF"]:
            if col in display_tbl.columns:
                display_tbl[col] = display_tbl[col].apply(_fmt_ps)
        if "兑USD汇率" in display_tbl.columns:
            display_tbl["兑USD汇率"] = display_tbl["兑USD汇率"].apply(_fmt_fx)

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

        currency_note_html = ""
        if original_currency and original_currency.upper() != "USD":
            if currency_converted:
                currency_note_html = (
                    f" <span style='color:#f59e0b;font-size:.78rem;font-weight:600;'>"
                    f"原报告货币: {original_currency.upper()} → 已换算 USD"
                    f"（兑USD汇率列为年末汇率）"
                    f"</span>"
                )
            else:
                currency_note_html = (
                    f" <span style='color:#ef4444;font-size:.78rem;font-weight:600;'>"
                    f"原报告货币: {original_currency.upper()} ⚠️ 换算失败，以原始货币显示"
                    f"</span>"
                )

        return f"""
        <div style="margin-bottom:8px;"><strong style="color:#e0e7ff;">📊 历年自由现金流明细</strong>{source_html}{currency_note_html}</div>
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

    # ── Subclass interface ───────────────────────────────────────────

    def fetch_data(self, ticker):
        """Fetch market data. Override in subclass."""
        raise NotImplementedError

    def normalize_ticker(self, ticker):
        """Normalize ticker input. Override if needed."""
        return ticker.strip()

    def format_label(self, ticker):
        """Format ticker for chart title. Override if needed."""
        return ticker

    def download_filings_ui(self, ticker):
        """Download filings with UI feedback. Override in subclass if applicable."""
        pass

    def render_extra_ui(self, ticker):
        """Render extra UI after chart/price alert. Override in subclass."""
        pass

    def on_analysis_complete(self, ticker, data):
        """Called after a ticker is fully analyzed. Override for hooks like tracking."""
        pass

    # ── Analyst panel ────────────────────────────────────────────────

    @staticmethod
    def _render_analyst_panel(analyst_data: dict, current_price, currency_sym: str):
        """Render FMP analyst panel: price-target consensus/summary, grades distribution,
        and recent analyst actions.

        Data keys (from _fmp_analyst_data):
          price_target         – targetConsensus/High/Low/Median
          price_target_summary – lastMonth/Quarter/Year count + avgPriceTarget
          grades               – list of {date, gradingCompany, previousGrade, newGrade, action}
          grades_consensus     – {strongBuy, buy, hold, sell, strongSell, consensus}
        """
        if not analyst_data:
            return

        pt      = analyst_data.get("price_target") or {}
        pts     = analyst_data.get("price_target_summary") or {}
        grades  = analyst_data.get("grades") or []
        gc      = analyst_data.get("grades_consensus") or {}
        status  = analyst_data.get("fmp_analyst_status", "")

        has_data = bool(pt or pts or grades or gc)

        st.markdown(
            '<div style="background:#111827;border:1px solid #1e3a5f;'
            'border-radius:10px;padding:10px 14px;margin-bottom:10px;">'
            '<span style="color:#00d4ff;font-weight:700;font-size:.9rem;">'
            '📊 分析师共识</span></div>',
            unsafe_allow_html=True,
        )

        if not has_data:
            st.caption(f"⚠️ {status}" if status else "暂无分析师数据")
            return

        # ── 1 + 2. Price target card (consensus + range bar + summary) ──
        if pt or pts:
            consensus = (pt or {}).get("targetConsensus")
            high      = (pt or {}).get("targetHigh")
            low       = (pt or {}).get("targetLow")
            median    = (pt or {}).get("targetMedian")

            # upside / downside vs current price
            upside      = ((consensus - current_price) / current_price * 100) if (consensus and current_price) else None
            up_sign     = ("+" if upside >= 0 else "") if upside is not None else ""
            up_clr      = "#22c55e" if (upside is not None and upside >= 0) else "#ef4444"
            up_badge    = (
                f'<span style="display:inline-block;background:{up_clr}22;color:{up_clr};'
                f'border:1px solid {up_clr}55;border-radius:5px;padding:1px 7px;'
                f'font-size:.75rem;font-weight:700;vertical-align:middle;margin-left:6px;">'
                f'{up_sign}{upside:.1f}%</span>'
            ) if upside is not None else ""

            # ── consensus headline ────────────────────────────────────
            headline_html = ""
            if consensus:
                headline_html = (
                    f'<div style="display:flex;align-items:baseline;gap:6px;margin-bottom:6px;">'
                    f'<span style="color:#94a3b8;font-size:.72rem;white-space:nowrap;">目标价</span>'
                    f'<span style="color:#e0e7ff;font-size:1.45rem;font-weight:800;'
                    f'letter-spacing:-.5px;font-family:\'Cascadia Mono\',monospace;">'
                    f'{currency_sym}{consensus:,.2f}</span>'
                    f'{up_badge}'
                    f'</div>'
                )

            # ── range bar (low → current → consensus → high) ─────────
            range_bar_html = ""
            if high and low and high > low:
                # clamp current and consensus into [low, high]
                def _pct(v): return max(0.0, min(100.0, (v - low) / (high - low) * 100))
                cur_pct = _pct(current_price) if current_price else None
                con_pct = _pct(consensus)     if consensus     else None
                med_pct = _pct(median)        if median        else None

                # markers on the bar
                markers_html = ""
                if cur_pct is not None:
                    markers_html += (
                        f'<div style="position:absolute;left:{cur_pct:.1f}%;'
                        f'transform:translateX(-50%);top:-3px;">'
                        f'<div style="width:2px;height:14px;background:#00d4ff;'
                        f'border-radius:1px;box-shadow:0 0 5px #00d4ff88;"></div></div>'
                        f'<div style="position:absolute;left:{cur_pct:.1f}%;'
                        f'transform:translateX(-50%);top:13px;'
                        f'color:#00d4ff;font-size:.62rem;white-space:nowrap;">现价</div>'
                    )
                if con_pct is not None:
                    markers_html += (
                        f'<div style="position:absolute;left:{con_pct:.1f}%;'
                        f'transform:translateX(-50%);top:-3px;">'
                        f'<div style="width:2px;height:14px;background:{up_clr};'
                        f'border-radius:1px;box-shadow:0 0 5px {up_clr}88;"></div></div>'
                        f'<div style="position:absolute;left:{con_pct:.1f}%;'
                        f'transform:translateX(-50%);top:13px;'
                        f'color:{up_clr};font-size:.62rem;white-space:nowrap;">共识</div>'
                    )
                if med_pct is not None:
                    markers_html += (
                        f'<div style="position:absolute;left:{med_pct:.1f}%;'
                        f'transform:translateX(-50%);top:-1px;">'
                        f'<div style="width:6px;height:6px;background:#f59e0b;'
                        f'border-radius:50%;box-shadow:0 0 4px #f59e0b88;"></div></div>'
                    )

                # fill: low→current grey, current→consensus colored
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
                    f'<div style="margin:14px 4px 26px;">'
                    f'<div style="position:relative;height:8px;background:#1e3a5f;'
                    f'border-radius:4px;overflow:visible;">'
                    f'{fill_html}{markers_html}'
                    f'</div>'
                    f'<div style="display:flex;justify-content:space-between;margin-top:4px;">'
                    f'<span style="color:#64748b;font-size:.65rem;">{currency_sym}{low:,.0f}</span>'
                    f'<span style="color:#64748b;font-size:.65rem;font-style:italic;">目标价区间</span>'
                    f'<span style="color:#64748b;font-size:.65rem;">{currency_sym}{high:,.0f}</span>'
                    f'</div></div>'
                )

            # ── high / median / low row ───────────────────────────────
            hml_cells = ""
            for v, lbl, clr in [(high, "最高", "#22c55e66"), (median, "中位", "#f59e0b66"), (low, "最低", "#ef444466")]:
                if v:
                    hml_cells += (
                        f'<div style="text-align:center;flex:1;">'
                        f'<div style="color:#64748b;font-size:.68rem;margin-bottom:2px;">{lbl}</div>'
                        f'<div style="color:#e0e7ff;font-size:.82rem;font-weight:700;'
                        f'border-bottom:2px solid {clr};padding-bottom:2px;">'
                        f'{currency_sym}{v:,.2f}</div></div>'
                    )
            hml_html = (
                f'<div style="display:flex;justify-content:space-evenly;'
                f'background:#0a0e17;border-radius:7px;padding:8px 6px;margin-bottom:10px;">'
                f'{hml_cells}</div>'
            ) if hml_cells else ""

            # ── historical avg targets from price-target-summary ──────
            hist_html = ""
            if pts:
                lm_n   = pts.get("lastMonthCount", 0)
                lm_avg = pts.get("lastMonthAvgPriceTarget")
                lq_n   = pts.get("lastQuarterCount", 0)
                lq_avg = pts.get("lastQuarterAvgPriceTarget")
                ly_n   = pts.get("lastYearCount", 0)
                ly_avg = pts.get("lastYearAvgPriceTarget")

                hist_rows = [
                    (lm_avg, f"近1月", lm_n),
                    (lq_avg, f"近1季", lq_n),
                    (ly_avg, f"近1年", ly_n),
                ]
                hist_cells = ""
                for avg, label, n in hist_rows:
                    if avg:
                        delta = ((avg - current_price) / current_price * 100) if current_price else None
                        d_html = ""
                        if delta is not None:
                            d_clr  = "#22c55e" if delta >= 0 else "#ef4444"
                            d_sign = "+" if delta >= 0 else ""
                            d_html = f'<div style="color:{d_clr};font-size:.63rem;">{d_sign}{delta:.1f}%</div>'
                        hist_cells += (
                            f'<div style="text-align:center;flex:1;">'
                            f'<div style="color:#475569;font-size:.66rem;">{label} <span style="color:#334155">({n}家)</span></div>'
                            f'<div style="color:#94a3b8;font-size:.78rem;font-weight:600;">{currency_sym}{avg:,.2f}</div>'
                            f'{d_html}'
                            f'</div>'
                        )
                if hist_cells:
                    hist_html = (
                        f'<div style="display:flex;justify-content:space-evenly;'
                        f'border-top:1px solid #1e3a5f;padding-top:8px;margin-top:2px;">'
                        f'{hist_cells}</div>'
                    )

            # ── assemble full card ────────────────────────────────────
            st.markdown(
                f'<div style="background:linear-gradient(145deg,#0f1d35,#111827);'
                f'border:1px solid #1e3a5f;border-radius:12px;padding:14px 16px 10px;'
                f'margin-bottom:10px;box-shadow:0 4px 20px rgba(0,0,0,.4),'
                f'inset 0 1px 0 rgba(255,255,255,.04);">'
                f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:8px;">'
                f'<span style="display:inline-block;width:3px;height:14px;'
                f'background:linear-gradient(180deg,#00d4ff,#3b82f6);border-radius:2px;"></span>'
                f'<span style="color:#94a3b8;font-size:.72rem;font-weight:600;'
                f'letter-spacing:.05em;text-transform:uppercase;">目标价</span>'
                f'</div>'
                f'{headline_html}'
                f'{range_bar_html}'
                f'{hml_html}'
                f'{hist_html}'
                f'</div>',
                unsafe_allow_html=True,
            )

        # ── 3. Rating distribution ────────────────────────────────────
        # Prefer grades-consensus (pre-aggregated), fall back to tallying grades list
        buys = sells = holds = total = 0

        if gc:
            strong_buy  = int(gc.get("strongBuy")  or 0)
            buy         = int(gc.get("buy")         or 0)
            hold        = int(gc.get("hold")        or 0)
            sell        = int(gc.get("sell")        or 0)
            strong_sell = int(gc.get("strongSell")  or 0)
            buys  = strong_buy + buy
            holds = hold
            sells = sell + strong_sell
            total = buys + holds + sells
            period_label = "综合评级共识"
        elif grades:
            from datetime import datetime as _dt, timedelta as _td
            BUY_SET  = {
                "Strong Buy", "Buy", "Outperform", "Overweight",
                "Add", "Accumulate", "Positive", "Market Outperform",
            }
            SELL_SET = {
                "Sell", "Strong Sell", "Underperform", "Underweight",
                "Reduce", "Negative",
            }
            cutoff = (_dt.now() - _td(days=90)).strftime("%Y-%m-%d")
            recent = [g for g in grades if (g.get("date") or "") >= cutoff] or grades[:15]
            buys  = sum(1 for g in recent if (g.get("newGrade") or "") in BUY_SET)
            sells = sum(1 for g in recent if (g.get("newGrade") or "") in SELL_SET)
            holds = len(recent) - buys - sells
            total = buys + holds + sells
            period_label = "近90日评级"

        if total > 0:
            b_pct = buys  / total * 100
            h_pct = holds / total * 100
            s_pct = sells / total * 100
            st.markdown(
                f'<div style="margin:8px 0 2px;">'
                f'<span style="color:#94a3b8;font-size:.74rem;">'
                f'{period_label}（买入 {buys} / 持有 {holds} / 卖出 {sells}）'
                f'</span></div>'
                f'<div style="display:flex;height:9px;border-radius:5px;'
                f'overflow:hidden;margin-bottom:5px;">'
                f'<div style="width:{b_pct:.0f}%;background:#22c55e;"></div>'
                f'<div style="width:{h_pct:.0f}%;background:#f59e0b;"></div>'
                f'<div style="width:{s_pct:.0f}%;background:#ef4444;"></div>'
                f'</div>'
                f'<div style="display:flex;justify-content:space-between;'
                f'font-size:.76rem;margin-bottom:8px;">'
                f'<span style="color:#22c55e;">买入 {buys} ({b_pct:.0f}%)</span>'
                f'<span style="color:#f59e0b;">持有 {holds} ({h_pct:.0f}%)</span>'
                f'<span style="color:#ef4444;">卖出 {sells} ({s_pct:.0f}%)</span>'
                f'</div>',
                unsafe_allow_html=True,
            )
            # grades-consensus label
            if gc:
                consensus_label = gc.get("consensus") or gc.get("rating") or ""
                if consensus_label:
                    cl_color = "#22c55e" if "buy" in consensus_label.lower() else (
                        "#ef4444" if "sell" in consensus_label.lower() else "#f59e0b"
                    )
                    st.markdown(
                        f'<div style="text-align:center;margin-bottom:8px;">'
                        f'<span style="background:#1a2035;border:1px solid #1e3a5f;'
                        f'border-radius:6px;padding:2px 10px;color:{cl_color};'
                        f'font-size:.8rem;font-weight:700;">'
                        f'共识: {consensus_label}</span></div>',
                        unsafe_allow_html=True,
                    )

        # ── 4. Recent analyst actions (grades list) ───────────────────
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

            st.markdown(
                '<div style="color:#94a3b8;font-size:.74rem;margin:4px 0 4px;">'
                '最近评级动作</div>',
                unsafe_allow_html=True,
            )
            rows_html = ""
            for g in grades[:8]:
                date     = (g.get("date") or "")[:10]
                co       = g.get("gradingCompany") or "—"
                if len(co) > 16:
                    co = co[:14] + "…"
                new_g    = g.get("newGrade") or "—"
                prev_g   = g.get("previousGrade") or ""
                action   = g.get("action") or ""
                color    = _grade_color(new_g)
                # Show action badge (upgrade/downgrade/initiated/reiterated)
                action_map = {
                    "upgrade":   ('<span style="color:#22c55e;font-size:.66rem;">↑</span> ', "#22c55e"),
                    "downgrade": ('<span style="color:#ef4444;font-size:.66rem;">↓</span> ', "#ef4444"),
                    "init":      ('<span style="color:#3b82f6;font-size:.66rem;">★</span> ', "#3b82f6"),
                    "reiterated":('', "#94a3b8"),
                    "maintained":('', "#94a3b8"),
                }
                act_key  = action.lower() if action else ""
                act_icon, _ = action_map.get(act_key, ("", "#94a3b8"))
                # prev grade hint
                prev_hint = (
                    f'<span style="color:#475569;font-size:.66rem;"> ← {prev_g}</span>'
                    if prev_g and prev_g != new_g else ""
                )
                rows_html += (
                    f'<tr>'
                    f'<td style="padding:2px 3px;color:#64748b;font-size:.71rem;'
                    f'white-space:nowrap;">{date}</td>'
                    f'<td style="padding:2px 3px;color:#94a3b8;font-size:.71rem;">{co}</td>'
                    f'<td style="padding:2px 3px;color:{color};font-size:.71rem;'
                    f'white-space:nowrap;">{act_icon}{new_g}{prev_hint}</td>'
                    f'</tr>'
                )
            st.markdown(
                f'<table style="width:100%;border-collapse:collapse;">'
                f'{rows_html}</table>',
                unsafe_allow_html=True,
            )

        st.caption("数据来源: FMP")

    # ── Chart ────────────────────────────────────────────────────────

    _DOWNSAMPLE_THRESHOLD = 1500  # above this -> resample older data to weekly

    @staticmethod
    def _downsample_ohlcv(ohlcv, threshold=1500):
        """Keep recent *threshold* daily rows; resample older portion to weekly."""
        n = len(ohlcv)
        if n <= threshold:
            return ohlcv
        recent = ohlcv.iloc[-threshold:]
        older = ohlcv.iloc[: n - threshold].copy()
        older["_week"] = pd.to_datetime(older["Date"]).dt.to_period("W").dt.start_time
        weekly = older.groupby("_week", sort=True).agg({
            "Date": "last", "Open": "first", "High": "max",
            "Low": "min", "Close": "last", "Volume": "sum",
        }).reset_index(drop=True)
        return pd.concat([weekly, recent], ignore_index=True)

    def render_chart(self, data, ticker_label, show_fcf_table: bool = True):
        """Draw an interactive Plotly candlestick chart with EMA + DCF lines."""
        ohlcv = data["ohlcv"]
        currency = data.get("currency", self.default_currency)
        currency_sym = {"USD": "$", "CNY": "¥", "HKD": "HK$"}.get(currency, currency)
        last_price = data.get("last_price")
        if last_price is None and ohlcv is not None and not ohlcv.empty:
            last_price = float(ohlcv["Close"].iloc[-1])

        if ohlcv is None or ohlcv.empty:
            st.error("无可用数据。")
            return

        ohlcv = self._downsample_ohlcv(ohlcv.copy())
        ohlcv["EMA10"] = ohlcv["Close"].ewm(span=10, adjust=False).mean()
        ohlcv["EMA250"] = ohlcv["Close"].ewm(span=250, adjust=False).mean()

        dcf_df = compute_dcf_lines(data.get("fcf_per_share_by_year", {}))
        if not dcf_df.empty:
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

        # Candlestick
        fig.add_trace(go.Candlestick(
            x=ohlcv["Date"], open=ohlcv["Open"], high=ohlcv["High"],
            low=ohlcv["Low"], close=ohlcv["Close"],
            name="K线",
            increasing_line_color="#ef5350", increasing_fillcolor="#ef5350",
            decreasing_line_color="#26a69a", decreasing_fillcolor="#26a69a",
        ))

        # EMA (WebGL for speed)
        fig.add_trace(go.Scattergl(
            x=ohlcv["Date"], y=ohlcv["EMA10"], name="EMA 10",
            line=dict(color="#f94144", width=1), mode="lines",
            hoverinfo="skip",
        ))
        fig.add_trace(go.Scattergl(
            x=ohlcv["Date"], y=ohlcv["EMA250"], name="EMA 250",
            line=dict(color="#7209b7", width=2), mode="lines",
            hoverinfo="skip",
        ))

        # DCF valuation lines
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

        # FMP DCF intrinsic value
        fmp_dcf_value = data.get("fmp_dcf_value")
        fmp_dcf_df = data.get("fmp_dcf_df")
        _fmp_dcf_has_history = (
            fmp_dcf_df is not None
            and not fmp_dcf_df.empty
            and "date" in fmp_dcf_df.columns
            and "dcf" in fmp_dcf_df.columns
        )
        if _fmp_dcf_has_history:
            # Extend the last value to today so the step-line reaches the right edge
            _dcf_dates = list(fmp_dcf_df["date"])
            _dcf_vals  = list(fmp_dcf_df["dcf"])
            _dcf_dates.append(pd.Timestamp(datetime.now().date()))
            _dcf_vals.append(_dcf_vals[-1])
            fig.add_trace(go.Scatter(
                x=_dcf_dates, y=_dcf_vals,
                name="FMP DCF",
                line=dict(color="#e879f9", width=2, dash="dot"),
                mode="lines+markers",
                marker=dict(size=7, symbol="circle", color="#e879f9"),
                connectgaps=True,
            ))
        elif fmp_dcf_value and fmp_dcf_value > 0:
            # Fallback: realtime-only → horizontal reference line
            fig.add_hline(
                y=fmp_dcf_value,
                line=dict(color="#e879f9", width=1.5, dash="dot"),
                annotation_text=f"FMP DCF  {currency_sym}{fmp_dcf_value:,.2f}",
                annotation_position="top right",
                annotation_font=dict(color="#e879f9", size=11),
            )

        # Latest price annotation
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

        # Axis ranges
        price_max = ohlcv["High"].max()
        price_min = ohlcv["Low"].min()
        y_top = price_max * 1.12
        y_bottom = max(0, price_min * 0.88)

        date_min = ohlcv["Date"].min()
        date_max = ohlcv["Date"].max()
        date_span = (date_max - date_min)
        x_right = date_max + date_span * 0.20

        fig.update_layout(
            title=dict(text=f"{ticker_label} 日K线", font=dict(color="#e0e7ff")),
            yaxis_title=f"价格 ({currency})",
            xaxis_rangeslider_visible=True,
            xaxis_rangeslider_thickness=0.06,
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

        # FCF report table (suppressed in reviewed/browse mode)
        fcf_table = data.get("fcf_table")
        if show_fcf_table:
            if fcf_table is not None and not fcf_table.empty:
                source = data.get("source", "")
                original_currency    = data.get("fmp_original_currency")
                currency_converted  = data.get("fmp_currency_converted", True)
                st.markdown(
                    self._build_fcf_table_html(
                        fcf_table, currency, source=source,
                        original_currency=original_currency,
                        currency_converted=currency_converted,
                    ),
                    unsafe_allow_html=True,
                )

        # Chart + optional analyst panel (right column)
        analyst_data = data.get("analyst_data")
        _has_analyst = bool(
            analyst_data and (analyst_data.get("price_target") or analyst_data.get("recommendations"))
        )
        if _has_analyst:
            _chart_col, _analyst_col = st.columns([3, 1])
            with _chart_col:
                st.plotly_chart(fig, use_container_width=True, config={"scrollZoom": True})
            with _analyst_col:
                self._render_analyst_panel(analyst_data, last_price, currency_sym)
        else:
            st.plotly_chart(fig, use_container_width=True, config={"scrollZoom": True})

        # Metric cards
        market_cap = data.get("market_cap")

        latest_fcf = None
        latest_fcf_year = None
        avg_fcf_3y = None
        fcf_ps_latest = None
        if fcf_table is not None and not fcf_table.empty:
            for _, row in fcf_table.iterrows():
                if pd.notna(row.get("FCF")):
                    latest_fcf = row["FCF"]
                    latest_fcf_year = str(row["年份"])[:4]
                    break
            fcf_vals = []
            for _, row in fcf_table.iterrows():
                if pd.notna(row.get("FCF")) and len(fcf_vals) < 3:
                    fcf_vals.append(row["FCF"])
            if fcf_vals:
                avg_fcf_3y = sum(fcf_vals) / len(fcf_vals)
            for _, row in fcf_table.iterrows():
                if pd.notna(row.get("每股FCF")):
                    fcf_ps_latest = row["每股FCF"]
                    break

        p_fcf = None
        if last_price and fcf_ps_latest and fcf_ps_latest > 0:
            p_fcf = last_price / fcf_ps_latest

        fmp_dcf_value = data.get("fmp_dcf_value")
        fcf_label = f"最近年度 FCF ({latest_fcf_year})" if latest_fcf_year else "最近年度 FCF"
        n_metrics = (5 if last_price else 4) + (1 if fmp_dcf_value else 0)
        cols = st.columns(n_metrics)
        idx = 0
        if last_price:
            cols[idx].metric("最新价", f"{currency_sym}{last_price:,.2f}")
            idx += 1
        cols[idx].metric("市值", self.fmt_val(market_cap, currency))
        cols[idx + 1].metric(fcf_label, self.fmt_val(latest_fcf, currency))
        cols[idx + 2].metric("3年平均 FCF", self.fmt_val(avg_fcf_3y, currency))
        cols[idx + 3].metric("P/FCF (基于最近年度)", f"{p_fcf:.1f}x" if p_fcf else "N/A")
        if fmp_dcf_value:
            delta = None
            if last_price and last_price > 0:
                delta = f"{(fmp_dcf_value - last_price) / last_price * 100:+.1f}%"
            cols[idx + 4].metric(
                "FMP DCF 估值",
                f"{currency_sym}{fmp_dcf_value:,.2f}",
                delta=delta,
                help="Financial Modeling Prep 内置 DCF 模型实时估值（非历史序列）",
            )

        # Note: P/FCF uses the latest single-year per-share FCF; DCF 曲线使用 3 年滚动均值
        st.caption("提示：图上的 DCF 曲线基于每股 FCF 的 3 年滚动均值；P/FCF 指标使用最新一年的每股 FCF，与曲线值不同。")

    # ── Price alert ──────────────────────────────────────────────────

    def render_price_alert(self, ticker, data=None, key_suffix=""):
        """Render moomoo OpenD price alert subscription UI with DCF quick-subscribe."""
        key_prefix = self.market.lower() + (f"_{key_suffix}" if key_suffix else "")
        st.divider()
        st.subheader("📢 价格提醒订阅 (moomoo OpenD)")

        # DCF Quick-Subscribe
        dcf_df = None
        fcf_ps_latest = None
        fcf_ps_date = None
        currency = data.get("currency", self.default_currency) if data else self.default_currency
        currency_sym = {"USD": "$", "CNY": "¥", "HKD": "HK$"}.get(currency, currency)

        if data:
            dcf_df = compute_dcf_lines(data.get("fcf_per_share_by_year", {}))
            if dcf_df is not None and not dcf_df.empty:
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
                            code = FutuClient.build_code(ticker, self.market)
                            results = []
                            for mult, price in [("14x", p14), ("24x", p24), ("34x", p34)]:
                                note = f"DCF{mult} FCF{currency_sym}{fcf_ps_latest:.2f}"
                                with FutuClient() as fc:
                                    ok, msg = fc.set_price_alert(
                                        code, price, note, reminder_type="PRICE_DOWN",
                                    )
                                results.append((mult, price, ok, msg))
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

        # Manual alert
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
                    code = FutuClient.build_code(ticker, self.market)
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

    # ── AI fill flow ─────────────────────────────────────────────────

    def _run_ai_fill(self, data, ticker):
        """Run AI fill + validate flow. Returns st.empty placeholder or None."""
        gemini_api_key = st.session_state.get("gemini_api_key", "")
        gemini_model = st.session_state.get("gemini_model_name", "")
        enabled_models = st.session_state.get("enabled_models")

        if not isinstance(data, dict) or not gemini_api_key:
            return None
        fmp_currency = data.get("fmp_currency", "USD")
        fcf_tbl = data.get("fcf_table")
        if fcf_tbl is None or fcf_tbl.empty:
            # yfinance 无法提供表格（如非 USD 财报的中国 ADR），创建空骨架
            # fill_fcf_table_with_llm 会自动发现年报年份并逐行填入
            fcf_tbl = pd.DataFrame(columns=["年份", "OCF", "CapEx", "FCF", "每股FCF"])
            data["fcf_table"] = fcf_tbl

        st.divider()
        st.markdown("#### 🤖 AI 自动读取年报填表 + 验证中...")

        currency = data.get("currency", self.default_currency)
        tbl_placeholder = st.empty()
        prev_tbl = [fcf_tbl.copy()]
        if not fcf_tbl.empty:
            tbl_placeholder.markdown(
                self._build_fcf_table_html(fcf_tbl, currency),
                unsafe_allow_html=True,
            )

        def _table_callback(updated_tbl):
            tbl_placeholder.markdown(
                self._build_fcf_table_html(updated_tbl, currency, prev_table=prev_tbl[0]),
                unsafe_allow_html=True,
            )
            prev_tbl[0] = updated_tbl.copy()

        progress_bar = st.progress(0, text="准备中...")
        log_area = st.empty()
        all_logs = []

        prog = {"start_ts": time.time(), "pct": 0.0, "tokens": 0, "est_total": 0}

        def _fmt_counter(p):
            elapsed = int(max(0, time.time() - p["start_ts"]))
            mm, ss = divmod(elapsed, 60)
            est = p["est_total"]
            est_str = f" / 估算总计 ~{est:,}" if est else ""
            return f"已用时 {mm:02d}:{ss:02d} | 已消耗 ~{p['tokens']:,}{est_str} tokens"

        def _progress(msg=None, step=None, total=None):
            if msg:
                all_logs.append(msg)
                log_area.text_area("📋 处理日志", "\n".join(reversed(all_logs)), height=300)
                if "正在等待回复" in msg and "tokens" in msg:
                    m = re.search(r"~\s*([\d,]+)\s*tokens", msg)
                    if m:
                        prog["tokens"] += int(m.group(1).replace(",", ""))
                elif "合计 ~" in msg:
                    m = re.search(r"合计 ~([\d,]+)\s*tokens", msg)
                    if m:
                        prog["est_total"] += int(m.group(1).replace(",", ""))
            if step is not None and total and total > 0:
                prog["pct"] = min(step / total, 1.0)
            progress_bar.progress(prog["pct"], text=_fmt_counter(prog))

        try:
            filled, logs, prompt_info = fill_fcf_table_with_llm(
                api_key=gemini_api_key,
                model_name=gemini_model,
                fcf_table=fcf_tbl.copy(),
                ticker=ticker,
                market=self.market,
                fmp_currency=fmp_currency,
                progress_callback=_progress,
                table_update_callback=_table_callback,
                enabled_models=enabled_models,
            )
            progress_bar.progress(1.0, text=f"完成 | {_fmt_counter(prog)}")
            # Show prompt used
            with st.expander("📜 查看发送给 Gemini 的 Prompt", expanded=False):
                st.markdown(f"**System Prompt:**\n```\n{prompt_info['system_prompt']}\n```")
                st.markdown(f"**规则文件:** `{prompt_info['rules_path']}`")
                st.markdown(f"**规则内容:**\n```\n{prompt_info['rules']}\n```")
                for i, bp in enumerate(prompt_info.get("batch_prompts", [])):
                    st.markdown(f"**批次 {i+1} Prompt:**\n```\n{bp[:2000]}{'...(截断)' if len(bp) > 2000 else ''}\n```")
            # Update data with filled table
            latest_shares = data.get("shares_outstanding")
            if latest_shares and latest_shares > 0:
                filled = recompute_fcf_per_share(filled, latest_shares)
            data["fcf_table"] = filled
            _table_callback(filled)
            # Save
            try:
                saved_path = save_fcf_table(filled, ticker, self.market)
                st.caption(f"📁 表格已保存: {saved_path}")
            except Exception:
                pass
            st.success("AI 年报验证完成!")
        except Exception as e:
            st.error(f"AI 补全失败: {e}")

        return tbl_placeholder

    # ── Main orchestrator ────────────────────────────────────────────

    def run(self):
        """Run the full analysis flow within a Streamlit tab."""
        key = self.market.lower()
        ticker_raw = st.text_input(
            self.ticker_input_label,
            value=st.session_state.get(f"{key}_ticker", self.ticker_input_default),
            key=f"{key}_ticker",
        )

        # Button row: analyze always visible; download-filings only when supported
        if self.supports_filing_download:
            _btn_col, _dl_col = st.columns([5, 2])
            with _btn_col:
                analyze_clicked = st.button(
                    self.analyze_button_label, key=f"{key}_chart",
                    use_container_width=True,
                )
            with _dl_col:
                dl_filings_clicked = st.button(
                    "📥 下载年报", key=f"{key}_dl_filings",
                    use_container_width=True,
                    help="单独下载 SEC 年报，供需要 AI 补全时使用",
                )
        else:
            analyze_clicked = st.button(
                self.analyze_button_label, key=f"{key}_chart",
                use_container_width=True,
            )
            dl_filings_clicked = False

        # ── Download-only flow ────────────────────────────────────────
        if dl_filings_clicked:
            if not ticker_raw:
                st.warning("请输入有效的代码。")
            else:
                self.download_filings_ui(self.normalize_ticker(ticker_raw))

        # ── Analyze flow ──────────────────────────────────────────────
        if analyze_clicked:
            if not ticker_raw:
                st.warning("请输入有效的代码。")
            else:
                ticker = self.normalize_ticker(ticker_raw)

                # Step 1: Fetch data (FMP primary for US)
                data = None
                spinner_msg = (
                    f"正在获取 {ticker} 数据 ({self.data_source_desc})..."
                    if self.data_source_desc
                    else f"正在获取 {ticker} 数据..."
                )
                with st.spinner(spinner_msg):
                    try:
                        data = self.fetch_data(ticker)
                        # Load previously saved FCF table if available
                        saved_tbl = load_fcf_table(ticker, self.market)
                        if saved_tbl is not None and not saved_tbl.empty:
                            data = dict(data)
                            data["fcf_table"] = saved_tbl
                    except Exception as e:
                        st.error(f"数据获取出错: {e}")

                # Show FMP fetch status
                if data is not None and data.get("fmp_status"):
                    fmp_ok = "获取失败" not in data["fmp_status"] and "未返回" not in data["fmp_status"]
                    if fmp_ok:
                        st.caption(f"📊 {data['fmp_status']}")
                    else:
                        st.caption(f"⚠️ {data['fmp_status']}")

                # Step 2 (conditional): download + AI fill only if FCF data is insufficient
                tbl_placeholder = None
                if data is not None:
                    is_sufficient, quality_reason = self._fcf_data_sufficient(data)

                    if is_sufficient:
                        st.success(f"✅ FCF 数据充足（{quality_reason}），跳过年报下载和 AI 补全。")
                    else:
                        st.warning(
                            f"⚠️ FCF 数据不足（{quality_reason}），"
                            "将下载年报并使用 AI 补全..."
                        )
                        self.download_filings_ui(ticker)
                        tbl_placeholder = self._run_ai_fill(data, ticker)

                    # Step 3: Apply adjusted FCF + store in session state
                    data = self._apply_adjusted_fcf(data)
                    st.session_state[f"{key}_chart_data"] = data
                    st.session_state[f"{key}_chart_label"] = self.format_label(ticker)
                    if tbl_placeholder:
                        tbl_placeholder.empty()

                    # Step 4: Notify subclass of completion
                    self.on_analysis_complete(ticker, data)

        # Always render chart if data is available
        if f"{key}_chart_data" in st.session_state:
            self.render_chart(
                st.session_state[f"{key}_chart_data"],
                st.session_state[f"{key}_chart_label"],
            )

        self.render_price_alert(
            ticker_raw,
            data=st.session_state.get(f"{key}_chart_data"),
        )

        self.render_extra_ui(ticker_raw)
