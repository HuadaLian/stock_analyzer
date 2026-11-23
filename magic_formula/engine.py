"""Eligibility and ranking logic for cross-market Magic Formula screens."""

from __future__ import annotations

import numpy as np
import pandas as pd

_FINANCIAL_INDUSTRIES = (
    "bank", "insurance", "insurer", "broker", "capital markets",
    "finance",
    "financial services", "credit services", "consumer finance",
    "investment banking", "asset management", "银行", "保险", "券商",
    "证券", "金融", "信托", "租赁", "资本市场",
)
_UTILITY_INDUSTRIES = (
    "utility", "utilities", "electric utility", "gas utility",
    "water utility", "regulated electric", "regulated gas", "公用事业",
    "电力", "燃气", "水务", "供水", "水的生产和供应", "热力生产",
    "电力、热力生产",
)
_LOW_DEBT_TO_MARKET_CAP = 0.10


def industry_group(industry: object) -> str:
    """Classify a provider's industry label into a dedicated model, if any."""
    value = (
        ""
        if industry is None or pd.isna(industry)
        else str(industry).strip().lower()
    )
    if any(term in value for term in _FINANCIAL_INDUSTRIES):
        return "financial"
    if any(term in value for term in _UTILITY_INDUSTRIES):
        return "utility"
    return "operating"


def apply_ebit_method(
    records: pd.DataFrame,
    annual_financials: pd.DataFrame,
    method: str,
) -> pd.DataFrame:
    """Set EBIT to TTM or the trailing three/five reported annual average."""
    method = method.upper()
    result = records.copy()
    if method == "TTM":
        return result
    periods = {"3Y 平均": 3, "4Y 平均": 4, "5Y 平均": 5}.get(method)
    if periods is None:
        raise ValueError(f"不支持的 EBIT 平滑方法: {method}")
    if annual_financials.empty or not {"ticker", "fiscal_date", "ebit"} <= set(annual_financials):
        result["ebit"] = np.nan
        return result

    history = annual_financials.copy()
    history["fiscal_date"] = pd.to_datetime(history["fiscal_date"], errors="coerce")
    history["ebit"] = pd.to_numeric(history["ebit"], errors="coerce")
    history = history.dropna(subset=["ticker", "fiscal_date", "ebit"])
    history = history.sort_values("fiscal_date", ascending=False)
    history = history.groupby("ticker", sort=False).head(periods)
    averages = history.groupby("ticker")["ebit"].agg(["mean", "count"])
    averages = averages.loc[averages["count"] >= periods, "mean"]
    result["ebit"] = result["ticker"].map(averages)
    return result


def _eligible_records(
    records: pd.DataFrame,
    market: str,
    *,
    sector: str,
    cn_min_market_cap: float,
    hk_min_market_cap: float,
    hk_min_daily_turnover: float,
    hk_require_dividends: bool,
    hk_require_clean_audit: bool,
    min_market_cap: float | None,
) -> pd.DataFrame:
    if records.empty:
        return records.copy()
    required = {"ticker", "industry", "list_date", "report_date", "market_cap", "price"}
    if not required.issubset(records.columns):
        return records.iloc[0:0].copy()

    df = records.copy()
    for column in (
        "market_cap", "price", "avg_daily_turnover", "dividend_years",
    ):
        if column in df:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    if min_market_cap is not None:
        df = df.loc[df["market_cap"] >= min_market_cap]

    now = pd.Timestamp.now().normalize()
    listed = pd.to_datetime(df["list_date"], errors="coerce")
    report_date = pd.to_datetime(df["report_date"], errors="coerce")
    df = df.loc[
        listed.notna()
        & ((now - listed).dt.days >= 365)
        & report_date.notna()
        & ((now - report_date).dt.days <= 550)
    ].copy()

    if market == "CN":
        name = df.get("name", pd.Series("", index=df.index)).fillna("").astype(str)
        df = df.loc[~name.str.contains(r"(?:^|\W)(?:\*?ST)(?:\W|$)", case=False, regex=True)]
        df = df.loc[df["market_cap"] >= cn_min_market_cap]
        if "avg_daily_turnover" in df and not df.empty:
            turnover_cutoff = df["avg_daily_turnover"].quantile(0.10)
            df = df.loc[df["avg_daily_turnover"] >= turnover_cutoff]
    elif market == "HK":
        df = df.loc[df["market_cap"] >= hk_min_market_cap]
        if "avg_daily_turnover" in df:
            df = df.loc[df["avg_daily_turnover"] >= hk_min_daily_turnover]
        if hk_require_dividends:
            dividends = pd.to_numeric(
                df.get("dividend_years", pd.Series(index=df.index, dtype=float)),
                errors="coerce",
            )
            df = df.loc[dividends >= 2]
        if hk_require_clean_audit:
            audit = df.get("audit_opinion", pd.Series("", index=df.index))
            df = df.loc[audit.fillna("").astype(str).str.lower().isin(
                ("unqualified", "unmodified", "clean")
            )]

    groups = df["industry"].map(industry_group)
    return df.loc[groups.eq(sector)].copy()


def _rank_pair(
    frame: pd.DataFrame,
    quality: str,
    value: str,
    *,
    quality_ascending: bool,
    value_ascending: bool,
    rank_quality: str,
    rank_value: str,
    top_n: int,
) -> pd.DataFrame:
    df = frame.replace([np.inf, -np.inf], np.nan).dropna(subset=[quality, value]).copy()
    if df.empty:
        return df
    df[rank_quality] = df[quality].rank(
        ascending=quality_ascending, method="min"
    ).astype(int)
    df[rank_value] = df[value].rank(
        ascending=value_ascending, method="min"
    ).astype(int)
    df["total_score"] = df[rank_quality] + df[rank_value]
    return df.sort_values(
        ["total_score", rank_quality, rank_value, "ticker"]
    ).head(top_n).reset_index(drop=True)


def rank_market(
    records: pd.DataFrame,
    market: str,
    *,
    top_n: int = 100,
    cn_min_market_cap: float = 5_000_000_000,
    hk_min_market_cap: float = 5_000_000_000,
    hk_min_daily_turnover: float = 10_000_000,
    hk_require_dividends: bool = True,
    hk_require_clean_audit: bool = False,
    min_market_cap: float | None = None,
) -> pd.DataFrame:
    """Rank non-financial, non-utility operating companies by ROC and EBIT/EV."""
    market = market.upper()
    df = _eligible_records(
        records, market, sector="operating",
        cn_min_market_cap=cn_min_market_cap,
        hk_min_market_cap=hk_min_market_cap,
        hk_min_daily_turnover=hk_min_daily_turnover,
        hk_require_dividends=hk_require_dividends,
        hk_require_clean_audit=hk_require_clean_audit,
        min_market_cap=min_market_cap,
    )
    for column in (
        "ebit", "current_assets", "cash", "current_liabilities",
        "current_debt", "fixed_assets", "total_debt",
    ):
        if column in df:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    required = (
        "ebit", "current_assets", "cash", "current_liabilities", "current_debt",
        "fixed_assets", "total_debt",
    )
    if not set(required).issubset(df.columns):
        return df.iloc[0:0].copy()
    df = df.dropna(subset=list(required))
    if df.empty:
        return df.assign(
            net_working_capital=pd.Series(dtype=float),
            invested_capital=pd.Series(dtype=float),
            roc=pd.Series(dtype=float),
            enterprise_value=pd.Series(dtype=float),
            earnings_yield=pd.Series(dtype=float),
            rank_roc=pd.Series(dtype="int64"),
            rank_ey=pd.Series(dtype="int64"),
            total_score=pd.Series(dtype="int64"),
        )

    non_interest_current_liabilities = (
        df["current_liabilities"] - df["current_debt"]
    ).clip(lower=0)
    df["net_working_capital"] = (
        df["current_assets"] - df["cash"] - non_interest_current_liabilities
    )
    df["invested_capital"] = df["net_working_capital"] + df["fixed_assets"]
    df["roc"] = df["ebit"] / df["invested_capital"].where(
        df["invested_capital"] != 0
    )
    df["enterprise_value"] = df["market_cap"] + df["total_debt"] - df["cash"]
    df["earnings_yield"] = df["ebit"] / df["enterprise_value"].where(
        df["enterprise_value"] > 0
    )
    df = df.replace([np.inf, -np.inf], np.nan).dropna(
        subset=["earnings_yield"]
    )
    low_debt_nonpositive_capital = (
        df["ebit"].gt(0)
        & df["invested_capital"].le(0)
        & df["market_cap"].gt(0)
        & df["total_debt"].ge(0)
        & (df["total_debt"] / df["market_cap"]).le(_LOW_DEBT_TO_MARKET_CAP)
    )
    positive_capital_roc = df["invested_capital"].gt(0) & df["roc"].gt(0)
    df = df.loc[
        (df["ebit"] > 0)
        & (df["earnings_yield"] > 0)
        & (positive_capital_roc | low_debt_nonpositive_capital)
    ].copy()
    if df.empty:
        return df.assign(
            rank_roc=pd.Series(dtype="int64"),
            rank_ey=pd.Series(dtype="int64"),
            total_score=pd.Series(dtype="int64"),
        )

    low_debt_nonpositive_capital = (
        df["invested_capital"].le(0)
        & df["total_debt"].ge(0)
        & (df["total_debt"] / df["market_cap"]).le(_LOW_DEBT_TO_MARKET_CAP)
    )
    standard_count = int(low_debt_nonpositive_capital.sum())
    df["roc_rank_basis"] = np.where(
        low_debt_nonpositive_capital,
        "标准化（低负债、C≤0）",
        "原始 ROC",
    )
    df["_roc_rank_key"] = (
        df.loc[~low_debt_nonpositive_capital, "roc"]
        .rank(ascending=False, method="min")
        .add(standard_count)
    )
    df.loc[low_debt_nonpositive_capital, "_roc_rank_key"] = 1
    ranked = _rank_pair(
        df, "_roc_rank_key", "earnings_yield", quality_ascending=True,
        value_ascending=False, rank_quality="rank_roc", rank_value="rank_ey",
        top_n=top_n,
    )
    return ranked.drop(columns=["_roc_rank_key"])


def _financial_quality(
    records: pd.DataFrame,
    annual_financials: pd.DataFrame,
) -> pd.Series:
    roe = pd.to_numeric(
        records.get("roe", pd.Series(np.nan, index=records.index)), errors="coerce"
    )
    if annual_financials.empty or not {
        "ticker", "fiscal_date", "net_income", "equity"
    }.issubset(annual_financials.columns):
        return roe
    history = annual_financials.copy()
    for column in ("net_income", "equity"):
        history[column] = pd.to_numeric(history[column], errors="coerce")
    history["fiscal_date"] = pd.to_datetime(history["fiscal_date"], errors="coerce")
    history = history.dropna(subset=["ticker", "fiscal_date", "net_income", "equity"])
    history = history.loc[history["equity"] > 0].sort_values("fiscal_date", ascending=False)
    history = history.groupby("ticker", sort=False).head(5)
    history["annual_roe"] = history["net_income"] / history["equity"]
    smoothed = history.groupby("ticker")["annual_roe"].mean()
    return records["ticker"].map(smoothed).fillna(roe)


def rank_financials(
    records: pd.DataFrame,
    annual_financials: pd.DataFrame,
    market: str,
    *,
    top_n: int = 100,
    cn_min_market_cap: float = 0,
    hk_min_market_cap: float = 0,
    hk_min_daily_turnover: float = 10_000_000,
    min_market_cap: float | None = None,
) -> pd.DataFrame:
    """Rank financial companies by smoothed ROE and P/TBV (P/B fallback)."""
    market = market.upper()
    df = _eligible_records(
        records, market, sector="financial",
        cn_min_market_cap=cn_min_market_cap,
        hk_min_market_cap=hk_min_market_cap,
        hk_min_daily_turnover=hk_min_daily_turnover,
        hk_require_dividends=False, hk_require_clean_audit=False,
        min_market_cap=min_market_cap,
    )
    if df.empty:
        return df
    df["roe"] = _financial_quality(df, annual_financials)
    equity = pd.to_numeric(
        df.get("equity", pd.Series(np.nan, index=df.index)), errors="coerce"
    )
    goodwill = pd.to_numeric(
        df.get("goodwill", pd.Series(np.nan, index=df.index)), errors="coerce"
    )
    goodwill_reported = goodwill.notna()
    goodwill = goodwill.fillna(0)
    tangible_equity = equity - goodwill
    market_cap = pd.to_numeric(df["market_cap"], errors="coerce")
    computed_ptbv = market_cap / tangible_equity.where(
        goodwill_reported & tangible_equity.gt(0)
    )
    computed_pb = market_cap / equity.where(equity > 0)
    reported_pb = pd.to_numeric(
        df.get("price_to_book", pd.Series(np.nan, index=df.index)),
        errors="coerce",
    )
    df["valuation_multiple"] = computed_ptbv.fillna(computed_pb).fillna(reported_pb)
    df["valuation_metric"] = np.where(
        computed_ptbv.notna(), "P/TBV", "P/B"
    )

    if "cet1_ratio" in df:
        cet1 = pd.to_numeric(df["cet1_ratio"], errors="coerce")
        df = df.loc[cet1.isna() | cet1.ge(0.11)]
    if "npl_ratio" in df:
        npl = pd.to_numeric(df["npl_ratio"], errors="coerce")
        available = npl.dropna()
        if not available.empty:
            df = df.loc[npl.isna() | npl.le(available.median())]
    df = df.loc[(df["roe"] > 0) & (df["valuation_multiple"] > 0)]
    return _rank_pair(
        df, "roe", "valuation_multiple", quality_ascending=False,
        value_ascending=True, rank_quality="rank_roe", rank_value="rank_pb",
        top_n=top_n,
    )


def _eps_growth_from_history(
    records: pd.DataFrame,
    annual_financials: pd.DataFrame,
) -> pd.Series:
    provided = pd.to_numeric(
        records.get("eps_growth", pd.Series(np.nan, index=records.index)),
        errors="coerce",
    )
    if annual_financials.empty or not {
        "ticker", "fiscal_date", "eps"
    }.issubset(annual_financials.columns):
        return provided
    history = annual_financials.copy()
    history["eps"] = pd.to_numeric(history["eps"], errors="coerce")
    history["fiscal_date"] = pd.to_datetime(history["fiscal_date"], errors="coerce")
    history = history.dropna(subset=["ticker", "fiscal_date", "eps"])
    history = history.sort_values("fiscal_date", ascending=False)
    history = history.groupby("ticker", sort=False).head(4)
    growth = {}
    for ticker, group in history.groupby("ticker"):
        values = group.sort_values("fiscal_date")["eps"]
        first, last = values.iloc[0], values.iloc[-1]
        years = len(values) - 1
        if years > 0 and first > 0 and last > 0:
            growth[ticker] = (last / first) ** (1 / years) - 1
    return records["ticker"].map(growth).fillna(provided)


def rank_utilities(
    records: pd.DataFrame,
    annual_financials: pd.DataFrame,
    market: str,
    *,
    top_n: int = 100,
    cn_min_market_cap: float = 0,
    hk_min_market_cap: float = 0,
    hk_min_daily_turnover: float = 10_000_000,
    min_market_cap: float | None = None,
) -> pd.DataFrame:
    """Rank utilities by EPS growth and dividend yield, filtering available safety data."""
    market = market.upper()
    df = _eligible_records(
        records, market, sector="utility",
        cn_min_market_cap=cn_min_market_cap,
        hk_min_market_cap=hk_min_market_cap,
        hk_min_daily_turnover=hk_min_daily_turnover,
        hk_require_dividends=False, hk_require_clean_audit=False,
        min_market_cap=min_market_cap,
    )
    if df.empty:
        return df
    df["eps_growth"] = _eps_growth_from_history(df, annual_financials)
    df["dividend_yield"] = pd.to_numeric(
        df.get("dividend_yield", pd.Series(np.nan, index=df.index)),
        errors="coerce",
    )
    if "payout_ratio" in df:
        payout = pd.to_numeric(df["payout_ratio"], errors="coerce")
        df = df.loc[payout.isna() | payout.between(0.50, 0.80)]
    if "ffo_to_debt" in df:
        ffo_to_debt = pd.to_numeric(df["ffo_to_debt"], errors="coerce")
        df = df.loc[ffo_to_debt.isna() | ffo_to_debt.ge(0.13)]
    df = df.loc[df["dividend_yield"] > 0]
    return _rank_pair(
        df, "eps_growth", "dividend_yield", quality_ascending=False,
        value_ascending=False, rank_quality="rank_growth",
        rank_value="rank_dividend_yield", top_n=top_n,
    )
