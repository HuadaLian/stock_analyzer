"""Streamlit UI for the cross-market Magic Formula screen."""

from __future__ import annotations

import re

import pandas as pd

from magic_formula.engine import (
    apply_ebit_method,
    rank_financials,
    rank_market,
    rank_utilities,
)
from magic_formula.sources import refresh_fundamentals, refresh_quotes
from magic_formula.store import (
    backfill_cn_currency_metadata,
    latest_fundamentals_update,
    latest_quote_update,
    load_annual_financials,
    load_market,
)

_MARKETS = (
    ("US", "美股"),
    ("HK", "港股"),
    ("CN", "A股"),
)
_MILLION = 1_000_000
_RANK_LIMIT = 100
_PROGRESS_COUNTS = re.compile(r"(\d[\d,]*)/(\d[\d,]*)")


def _market_cap_from_millions(value: float) -> float:
    """Convert the UI's currency-in-millions input to the data's full units."""
    return float(value) * _MILLION


def _update_progress_fraction(
    message: str,
    market: str,
    refresh_market_data: bool,
) -> float | None:
    match = _PROGRESS_COUNTS.search(message)
    if not match:
        return None

    current, total = (int(value.replace(",", "")) for value in match.groups())
    if total == 0:
        return None
    ratio = min(current / total, 1.0)

    if "Yahoo Finance 股票池" in message:
        return ratio * (0.1 if refresh_market_data else 0.25)
    if "正在检查财报日期" in message:
        return ratio * 0.9
    if "正在检查财报更新" in message:
        return ratio * 0.2
    if "正在获取报价和财报" in message:
        return 0.1 + ratio * 0.9
    if "正在拉取近 3 个月日行情" in message:
        return ratio * (0.2 if refresh_market_data else 0.25)
    if "正在更新候选公司财报" in message:
        return 0.2 + ratio * 0.8
    if "正在更新报价" in message:
        if market == "CN" and refresh_market_data:
            return ratio * 0.2
        return 0.25 + ratio * 0.75
    return None


def _format_results(frame: pd.DataFrame, currency: str) -> pd.DataFrame:
    columns = {
        "ticker": "代码",
        "name": "公司",
        "industry": "行业",
        "price": "最新价",
        "currency": "货币",
        "market_cap": f"市值（{currency}）",
        "avg_daily_turnover": f"近3月日均成交额（{currency}）",
        "ebit": f"所选 EBIT（{currency}）",
        "roc": "ROC",
        "roc_rank_basis": "ROC排名口径",
        "earnings_yield": "EBIT/EV",
        "rank_roc": "ROC排名",
        "rank_ey": "EY排名",
        "total_score": "综合分",
        "price_date": "报价日期",
        "report_date": "财报日期",
        "source": "财报数据来源",
        "currency_assumption": "币种假设",
        "audit_label": "审计状态",
    }
    available = [column for column in columns if column in frame.columns]
    result = frame[available].rename(columns=columns).copy()
    for column in ("ROC", "EBIT/EV"):
        if column in result:
            result[column] = result[column].map(
                lambda value: f"{value:.2%}" if pd.notna(value) else "—"
            )
    for column in (
        f"市值（{currency}）",
        f"近3月日均成交额（{currency}）",
        f"所选 EBIT（{currency}）",
    ):
        if column in result:
            result[column] = result[column].map(
                lambda value: f"{value:,.0f}" if pd.notna(value) else "—"
            )
    return result


def _format_sector_results(
    frame: pd.DataFrame,
    *,
    sector: str,
) -> pd.DataFrame:
    if sector == "financial":
        columns = {
            "ticker": "代码",
            "name": "公司",
            "industry": "行业",
            "currency": "货币",
            "roe": "ROE",
            "valuation_metric": "估值口径",
            "valuation_multiple": "P/TBV 或 P/B",
            "rank_roe": "ROE排名",
            "rank_pb": "估值排名",
            "total_score": "综合分",
            "cet1_ratio": "CET1",
            "npl_ratio": "不良贷款率",
            "provision_coverage": "拨备覆盖率",
        }
        percentages = ("ROE", "CET1", "不良贷款率", "拨备覆盖率")
        multiple = "P/TBV 或 P/B"
    else:
        columns = {
            "ticker": "代码",
            "name": "公司",
            "industry": "行业",
            "currency": "货币",
            "eps_growth": "EPS增长代理",
            "dividend_yield": "股息率",
            "payout_ratio": "派息率",
            "ffo_to_debt": "FFO/债务",
            "rank_growth": "增长排名",
            "rank_dividend_yield": "股息率排名",
            "total_score": "综合分",
        }
        percentages = ("EPS增长代理", "股息率", "派息率", "FFO/债务")
        multiple = None

    available = [column for column in columns if column in frame.columns]
    result = frame[available].rename(columns=columns).copy()
    for column in percentages:
        if column in result:
            result[column] = result[column].map(
                lambda value: f"{value:.2%}" if pd.notna(value) else "—"
            )
    if multiple and multiple in result:
        result[multiple] = result[multiple].map(
            lambda value: f"{value:.2f}x" if pd.notna(value) else "—"
        )
    return result


def _currency_normalized_rows(records: pd.DataFrame, market: str) -> pd.DataFrame:
    """Keep records in listing currency, assuming CNY for missing A-share metadata."""
    required = {
        "currency",
        "quote_currency",
        "quote_fx_to_listing",
        "financial_currency",
        "financial_fx_to_listing",
    }
    if records.empty:
        return records.iloc[0:0].copy()
    market = market.upper()
    normalized = records.copy()
    if market == "CN":
        defaults = {
            "currency": "CNY",
            "quote_currency": "CNY",
            "quote_fx_to_listing": 1.0,
            "financial_currency": "CNY",
            "financial_fx_to_listing": 1.0,
        }
        missing_metadata = pd.Series(False, index=normalized.index)
        for column, default in defaults.items():
            if column not in normalized:
                normalized[column] = default
                missing_metadata |= True
                continue
            missing = normalized[column].isna()
            if isinstance(default, str):
                missing |= normalized[column].astype("string").str.strip().eq("").fillna(False)
            normalized.loc[missing, column] = default
            missing_metadata |= missing
        if missing_metadata.any():
            if "currency_assumption" not in normalized:
                normalized["currency_assumption"] = ""
            assumption = normalized["currency_assumption"].fillna("").astype(str)
            assumption.loc[missing_metadata & assumption.eq("")] = (
                "A股缺失币种/汇率→CNY/1"
            )
            normalized["currency_assumption"] = assumption
    if not required.issubset(normalized.columns):
        return normalized.iloc[0:0].copy()
    target = {"US": "USD", "HK": "HKD", "CN": "CNY"}[market]
    quote_fx = pd.to_numeric(normalized["quote_fx_to_listing"], errors="coerce")
    financial_fx = pd.to_numeric(
        normalized["financial_fx_to_listing"], errors="coerce"
    )
    valid = (
        normalized["currency"].astype(str).str.upper().eq(target)
        & normalized["quote_currency"].notna()
        & normalized["financial_currency"].notna()
        & quote_fx.notna()
        & quote_fx.gt(0)
        & financial_fx.notna()
        & financial_fx.gt(0)
    )
    return normalized.loc[valid].copy()


def render_magic_formula(st_mod) -> None:
    st_mod.subheader("✨ Magic Formula")
    st_mod.caption(
        "ROC = EBIT ÷（净营运资本 + 固定资产；不含商誉和无形资产）；"
        "正 EBIT、投入资本 C≤0 且总债务/市值≤10% 时，ROC 质量排名标准化为并列第一；"
        "再与 EBIT/EV 分别排名并求和，分数越低越靠前。"
        "此筛选器仅供研究，不构成投资建议。"
    )
    fundamentals_col, quotes_col = st_mod.columns(2)
    update_fundamentals = fundamentals_col.button(
        "检查并更新三个市场的新财报",
        key="magic_refresh_all_fundamentals",
        type="primary",
        help="只检查本地已缓存股票；发现比本地更新的报告期时才下载该股票的详细财报。",
    )
    update_quotes = quotes_col.button(
        "更新三个市场报价",
        key="magic_refresh_all_quotes",
        help="刷新美股、港股和 A 股已缓存股票的行情，不请求财报。",
    )
    if update_fundamentals or update_quotes:
        action = "新财报" if update_fundamentals else "报价"
        progress_bar = st_mod.progress(0.0, text=f"准备更新三个市场{action}…")
        status = st_mod.empty()
        market_failures = {}
        market_results = []
        for index, (market, label) in enumerate(_MARKETS):
            base_progress = index / len(_MARKETS)
            progress_bar.progress(base_progress, text=f"开始更新{label}{action}…")

            def report_progress(message: str) -> None:
                status.caption(message)
                if "失败" in message and market not in market_failures:
                    market_failures[market] = message
                fraction = _update_progress_fraction(
                    message, market, update_fundamentals
                )
                if fraction is None:
                    fraction = 0.0
                progress_bar.progress(
                    min(base_progress + fraction / len(_MARKETS), 1.0),
                    text=message,
                )

            try:
                if update_fundamentals:
                    count = refresh_fundamentals(market, report_progress)
                else:
                    count = refresh_quotes(
                        market, load_market(market), 0, report_progress
                    )
                market_results.append(f"{label} {count:,} 家")
            except Exception as exc:
                market_failures[market] = f"{label}更新失败：{exc}"
            progress_bar.progress(
                (index + 1) / len(_MARKETS),
                text=f"{label}{action}处理完成。",
            )
        if market_failures:
            st_mod.warning(
                f"已处理：{'；'.join(market_results) or '无成功更新'}。"
                f"有失败或部分失败：{next(iter(market_failures.values()))}"
            )
            for failure in list(market_failures.values())[1:]:
                st_mod.error(failure)
        else:
            st_mod.success(
                f"三个市场{action}处理完成：{'；'.join(market_results)}。"
            )

    tabs = st_mod.tabs([label for _, label in _MARKETS])
    for (market, label), tab in zip(_MARKETS, tabs):
        with tab:
            currency_metadata_backfilled = (
                backfill_cn_currency_metadata() if market == "CN" else 0
            )
            cached = load_market(market)
            annual_financials = load_annual_financials(market)
            quote_updated = latest_quote_update(market)
            fundamentals_updated = latest_fundamentals_update(market)
            currency = {"US": "USD", "HK": "HKD", "CN": "CNY"}[market]
            min_market_cap_millions = st_mod.number_input(
                f"最低市值（{currency} 百万）",
                min_value=0,
                value=50,
                step=100,
                format="%d",
                key=f"magic_min_market_cap_{market}",
                help=(
                    f"单位为百万 {currency}。例如 5,000 表示 50 亿 {currency}；"
                    "默认 50 百万，换算后同时用于股票池抓取和排名。"
                ),
            )
            min_market_cap = _market_cap_from_millions(min_market_cap_millions)
            ebit_method = st_mod.selectbox(
                "EBIT 口径",
                options=("TTM", "3Y 平均", "4Y 平均", "5Y 平均"),
                key=f"magic_ebit_method_{market}",
                help="TTM 汇总最近四个季度；年度平均需有完整 3/4/5 年历史财报。",
            )
            st_mod.caption(
                f"市场：{label} · 已缓存 {len(cached):,} 家 · "
                f"报价更新：{quote_updated or '尚未更新'} · "
                f"财报更新：{fundamentals_updated or '尚未更新'}"
            )
            if currency_metadata_backfilled:
                st_mod.caption(
                    f"已修补 {currency_metadata_backfilled:,} 家 A 股缺失的币种/汇率元数据，"
                    "按 CNY/1 处理；未重新抓取财报。"
                )
            if market == "CN":
                st_mod.caption(
                    "A股：主榜剔除 ST；金融和公用事业分别进入下方专属排名；上市满 1 年，"
                    "以 CNY 市值控件筛选、日均成交额排除最低 10%。财报来自 Tushare。"
                )
            elif market == "HK":
                st_mod.caption(
                    "港股：以 HKD 市值控件筛选；近 3 个月日均成交额至少 1,000 万 HKD、"
                    "主榜近 2 年有分红；金融和公用事业分别进入下方专属排名。"
                    "Yahoo Finance 财报未经审计。"
                )
            else:
                st_mod.caption(
                    "美股：股票池与财报均来自 Yahoo Finance，不调用 FMP；以 USD 市值控件筛选，"
                    "保留最多 5 年年度财报用于平滑 EBIT；金融和公用事业另行排名，排除 OTC。"
                )

            refresh_col, calculate_col = st_mod.columns(2)
            refresh_clicked = refresh_col.button(
                f"检查并更新{label}新财报",
                key=f"magic_refresh_{market}",
                type="primary",
                help="只更新本地没有的较新报告期；不会重抓未变化股票的财报。",
            )
            calculate_clicked = calculate_col.button(
                "刷新报价并重新排名",
                key=f"magic_calculate_{market}",
                disabled=cached.empty,
                help="仅更新行情和报价时间戳，不重新请求财报。",
            )

            if refresh_clicked or calculate_clicked:
                action = "新财报" if refresh_clicked else "报价"
                progress_bar = st_mod.progress(0.0, text=f"正在更新{label}{action}…")
                progress_value = 0.0
                partial_failures = []

                def report_progress(message: str) -> None:
                    nonlocal progress_value
                    fraction = _update_progress_fraction(
                        message, market, refresh_clicked
                    )
                    if fraction is not None:
                        progress_value = max(progress_value, fraction)
                    progress_bar.progress(progress_value, text=message)
                    if "失败" in message:
                        partial_failures.append(message)

                try:
                    progress = report_progress
                    if refresh_clicked:
                        count = refresh_fundamentals(market, progress)
                    else:
                        count = refresh_quotes(
                            market, cached, min_market_cap, progress
                        )
                    progress_bar.progress(
                        1.0, text=f"{action}更新完成：{count:,} 家。"
                    )
                    cached = load_market(market)
                    annual_financials = load_annual_financials(market)
                    quote_updated = latest_quote_update(market)
                    fundamentals_updated = latest_fundamentals_update(market)
                    if partial_failures:
                        st_mod.warning(
                            f"{label}{action}有部分请求失败，未更新的数据保留旧值；"
                            f"详情见上方状态日志：{partial_failures[0]}"
                        )
                except Exception as exc:
                    progress_bar.progress(
                        progress_value, text=f"{label}数据更新失败"
                    )
                    st_mod.error(f"{label}数据更新失败：{exc}")

            normalized = _currency_normalized_rows(cached, market)
            if len(normalized) < len(cached):
                st_mod.warning(
                    f"{len(cached) - len(normalized):,} 家缓存数据缺少币种换算记录，"
                    "未参与排名。请更新股票池与财报以统一为上市地币种。"
                )
            prepared = apply_ebit_method(
                normalized, annual_financials, ebit_method
            )
            ranked = rank_market(
                prepared,
                market,
                top_n=_RANK_LIMIT,
                min_market_cap=min_market_cap,
                cn_min_market_cap=min_market_cap,
                hk_min_market_cap=min_market_cap,
            )
            if ranked.empty:
                st_mod.info(
                    "普通企业主榜当前没有满足条件的候选；可查看下方行业专属排名，"
                    "或更新数据/调整最低市值。选择 3Y/5Y 平均时需有完整年度 EBIT。"
                )
            else:
                st_mod.caption(
                    f"普通企业主榜前 {len(ranked)} 只 · "
                    f"等权组合参考：每只约 {100 / len(ranked):.2f}%"
                )
                st_mod.dataframe(
                    _format_results(ranked, currency),
                    height=520,
                    use_container_width=True,
                    hide_index=True,
                )
                st_mod.download_button(
                    "下载当前排名 CSV",
                    data=ranked.to_csv(index=False).encode("utf-8-sig"),
                    file_name=f"magic_formula_{market.lower()}.csv",
                    mime="text/csv",
                    key=f"magic_download_{market}",
                )

            financials = rank_financials(
                prepared,
                annual_financials,
                market,
                top_n=_RANK_LIMIT,
                min_market_cap=min_market_cap,
                cn_min_market_cap=min_market_cap,
                hk_min_market_cap=min_market_cap,
            )
            st_mod.subheader("金融企业：ROE + P/TBV（无形资产缺失时 P/B）")
            st_mod.caption(
                "优先用最多 5 年年度净利润/权益均值衡量 ROE，缺历史时回退数据源 ROE；"
                "CET1 < 11% 或高于同市场金融股中位数的不良率仅在有数据时过滤。"
                "未提供的风险指标会保留候选，不作虚假推断。"
            )
            if financials.empty:
                st_mod.info("没有满足条件且具备 ROE 与账面估值数据的金融企业。")
            else:
                st_mod.dataframe(
                    _format_sector_results(financials, sector="financial"),
                    height=300,
                    use_container_width=True,
                    hide_index=True,
                )

            utilities = rank_utilities(
                prepared,
                annual_financials,
                market,
                top_n=_RANK_LIMIT,
                min_market_cap=min_market_cap,
                cn_min_market_cap=min_market_cap,
                hk_min_market_cap=min_market_cap,
            )
            st_mod.subheader("公用事业：EPS 增长 + 股息率")
            st_mod.caption(
                "质量优先使用最多 4 年 EPS CAGR，缺历史时采用数据源 EPS 增长；"
                "派息率仅在可得时要求 50%–80%，FFO/债务仅在数据源提供时要求 ≥13%。"
                "当前公开数据源通常不提供 Rate Base、FFO、派息率等完整指标，缺失时不套用该项过滤。"
            )
            if utilities.empty:
                st_mod.info("没有满足条件且具备 EPS 增长与正股息率的公用事业企业。")
            else:
                st_mod.dataframe(
                    _format_sector_results(utilities, sector="utility"),
                    height=300,
                    use_container_width=True,
                    hide_index=True,
                )
