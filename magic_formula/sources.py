"""Market data collectors for the Magic Formula database.

Collectors are deliberately separate from the existing chart/DCF ETL. They
write only to ``magic_formula/magic_formula.db``.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from typing import Any

import pandas as pd

from magic_formula.engine import industry_group
from magic_formula.store import upsert_rows

_US_OTC_EXCHANGES = {
    "GREY", "OID", "OQB", "OQX", "OTC", "OTCBB", "OTCMKTS",
    "OTCQB", "OTCQX", "PINX", "PNK",
}
_MARKET_CURRENCIES = {"US": "USD", "HK": "HKD", "CN": "CNY"}
_MAX_SUPPORTED_EPOCH = 4_102_444_800
_CN_FORMULA_FINANCIAL_FIELDS = (
    "report_date", "ebit", "current_assets", "cash", "current_liabilities",
    "current_debt", "fixed_assets", "total_debt",
)


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if pd.notna(result) else None


def _epoch_date(value: Any, *, milliseconds: bool = False) -> str | None:
    """Convert provider timestamps safely, ignoring malformed/out-of-range dates."""
    timestamp = _number(value)
    if timestamp is None:
        return None
    if milliseconds:
        timestamp /= 1000
    if not 0 <= timestamp <= _MAX_SUPPORTED_EPOCH:
        return None
    try:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).date().isoformat()
    except (OSError, OverflowError, ValueError):
        return None


def _ratio(value: Any) -> float | None:
    """Normalize ratio fields returned either as fractions or percentages."""
    result = _number(value)
    if result is not None and 1 < abs(result) <= 100:
        return result / 100
    return result


def _currency_unit(currency: str) -> tuple[str, float]:
    """Map Yahoo subunit quote currencies to their ISO currency and unit size."""
    raw = currency.strip()
    subunits = {
        "GBp": ("GBP", 0.01),
        "GBX": ("GBP", 0.01),
        "ZAc": ("ZAR", 0.01),
        "ILA": ("ILS", 0.01),
    }
    if raw in subunits:
        return subunits[raw]
    return raw.upper(), 1.0


def _currency_rate_to_listing(source_currency: str, listing_currency: str) -> float:
    """Return a recent Yahoo FX rate converting source currency to listing currency."""
    source, source_unit = _currency_unit(source_currency)
    target, target_unit = _currency_unit(listing_currency)
    if not source or not target:
        raise ValueError("财报或报价缺少币种，无法统一到上市地币种。")
    if source == target:
        return source_unit / target_unit
    rate = _cached_currency_rate_to_listing(
        source, target, date.today().isoformat()
    )
    return rate * source_unit / target_unit


@lru_cache(maxsize=64)
def _cached_currency_rate_to_listing(
    source: str,
    target: str,
    cache_day: str,
) -> float:
    import yfinance as yf

    def direct_rate(base: str, quote: str) -> float | None:
        for pair, invert in ((f"{base}{quote}=X", False), (f"{quote}{base}=X", True)):
            try:
                history = yf.Ticker(pair).history(period="5d", auto_adjust=False)
            except Exception:
                continue
            if history is None or history.empty or "Close" not in history:
                continue
            rates = pd.to_numeric(history["Close"], errors="coerce").dropna()
            if rates.empty or float(rates.iloc[-1]) <= 0:
                continue
            rate = float(rates.iloc[-1])
            return 1.0 / rate if invert else rate
        return None

    rate = direct_rate(source, target)
    if rate is not None:
        return rate
    if source != "USD" and target != "USD":
        source_usd = direct_rate(source, "USD")
        usd_target = direct_rate("USD", target)
        if source_usd is not None and usd_target is not None:
            return source_usd * usd_target
    raise RuntimeError(
        f"无法获取 {source}/{target} 汇率；拒绝使用未换算金额。"
    )


def _row_value(frame: pd.DataFrame, *names: str) -> float | None:
    if frame is None or frame.empty:
        return None
    normalized = {str(index).strip().lower(): index for index in frame.index}
    for name in names:
        key = name.lower()
        index = normalized.get(key)
        if index is None:
            index = next(
                (actual for label, actual in normalized.items() if key in label),
                None,
            )
        if index is not None:
            values = pd.to_numeric(frame.loc[index], errors="coerce").dropna()
            if not values.empty:
                return float(values.iloc[0])
    return None


def _annual_ebit(ticker: Any) -> tuple[float | None, str | None, list[dict]]:
    annual_history = []
    annual = ticker.income_stmt
    if annual is not None and not annual.empty:
        index = {str(value).strip().lower(): value for value in annual.index}
        operating_row = index.get("operating income") or index.get("ebit")
        net_income_row = next(
            (index[name] for name in (
                "net income common stockholders", "net income",
                "net income from continuing operations",
            ) if name in index),
            None,
        )
        eps_row = next(
            (index[name] for name in ("basic eps", "diluted eps") if name in index),
            None,
        )
        for fiscal_date in annual.columns:
            ebit = (
                _number(annual.loc[operating_row, fiscal_date])
                if operating_row is not None else None
            )
            net_income = (
                _number(annual.loc[net_income_row, fiscal_date])
                if net_income_row is not None else None
            )
            eps = (
                _number(annual.loc[eps_row, fiscal_date])
                if eps_row is not None else None
            )
            if any(value is not None for value in (ebit, net_income, eps)):
                annual_history.append({
                    "fiscal_date": str(pd.Timestamp(fiscal_date).date()),
                    "ebit": ebit,
                    "net_income": net_income,
                    "eps": eps,
                })
        annual_history.sort(key=lambda item: item["fiscal_date"], reverse=True)

    income = ticker.quarterly_income_stmt
    if income is not None and not income.empty:
        index = {str(value).strip().lower(): value for value in income.index}
        operating_row = index.get("operating income") or index.get("ebit")
        if operating_row is not None:
            values = pd.to_numeric(income.loc[operating_row], errors="coerce").dropna()
            if len(values) >= 4:
                values.index = pd.to_datetime(values.index, errors="coerce")
                values = values.loc[values.index.notna()].sort_index(ascending=False)
            if len(values) >= 4:
                return (
                    float(values.iloc[:4].sum()),
                    str(pd.Timestamp(values.index[0]).date()),
                    annual_history[:5],
                )
    latest_annual = annual_history[0] if annual_history else None
    return (
        None,
        latest_annual["fiscal_date"] if latest_annual else None,
        annual_history[:5],
    )


def _balance_value(balance: pd.DataFrame, *names: str) -> float | None:
    return _row_value(balance, *names)


def _exact_balance_value(balance: pd.DataFrame, *names: str) -> float | None:
    if balance is None or balance.empty:
        return None
    normalized = {str(index).strip().lower(): index for index in balance.index}
    for name in names:
        index = normalized.get(name.lower())
        if index is None:
            continue
        values = pd.to_numeric(balance.loc[index], errors="coerce").dropna()
        if not values.empty:
            return float(values.iloc[0])
    return None


def _goodwill_value(balance: pd.DataFrame) -> float | None:
    return _exact_balance_value(balance, "Goodwill")


def _intangible_assets_value(balance: pd.DataFrame) -> float | None:
    return _exact_balance_value(
        balance, "Other Intangible Assets", "Intangible Assets"
    )


def _history_price_and_turnover(ticker: Any) -> tuple[float | None, str | None, float | None]:
    history = ticker.history(period="3mo", auto_adjust=False)
    if history is None or history.empty:
        return None, None, None
    close_col = next((col for col in history.columns if str(col).lower() == "close"), None)
    volume_col = next((col for col in history.columns if str(col).lower() == "volume"), None)
    if close_col is None or volume_col is None:
        return None, None, None
    close = pd.to_numeric(history[close_col], errors="coerce").dropna()
    value = pd.to_numeric(history[close_col], errors="coerce") * pd.to_numeric(
        history[volume_col], errors="coerce"
    )
    value = value.dropna()
    if close.empty:
        return None, None, None
    return (
        float(close.iloc[-1]),
        str(close.index[-1])[:10],
        float(value.tail(63).mean()) if not value.empty else None,
    )


def _yf_record(
    market: str,
    quote: dict,
    *,
    stock: Any | None = None,
    info: dict | None = None,
) -> tuple[dict, list[dict]] | None:
    import yfinance as yf

    ticker_symbol = str(quote.get("symbol") or "").strip().upper()
    if not ticker_symbol:
        return None
    stock = stock or yf.Ticker(ticker_symbol)
    if str(quote.get("quoteType", "")).upper() != "EQUITY":
        return None
    info = info if info is not None else stock.info or {}
    market = market.upper()
    listing_currency = _MARKET_CURRENCIES[market]
    assumed_currencies = []
    quote_currency_value = str(quote.get("currency") or "").strip()
    if not quote_currency_value and market == "US":
        quote_currency_value = "USD"
        assumed_currencies.append("报价币种缺失→USD")
    elif not quote_currency_value:
        raise RuntimeError(
            f"{ticker_symbol}: Yahoo Finance 未提供报价币种，无法统一到上市地币种。"
        )
    quote_currency = quote_currency_value
    financial_currency_value = str(info.get("financialCurrency") or "").strip()
    if not financial_currency_value and market == "US":
        financial_currency_value = "USD"
        assumed_currencies.append("财报币种缺失→USD")
    elif not financial_currency_value:
        raise RuntimeError(
            f"{ticker_symbol}: Yahoo Finance 未提供 financialCurrency，"
            "无法确认财报币种。"
        )
    financial_currency = financial_currency_value
    quote_fx = _currency_rate_to_listing(quote_currency, listing_currency)
    financial_fx = _currency_rate_to_listing(financial_currency, listing_currency)
    price = _number(quote.get("regularMarketPrice"))
    price_date = _epoch_date(quote.get("regularMarketTime"))
    avg_turnover = None
    if market == "HK":
        _, _, avg_turnover = _history_price_and_turnover(stock)

    balance = stock.quarterly_balance_sheet
    annual_balance = stock.balance_sheet
    if balance is None or balance.empty:
        balance = annual_balance
    income, report_date, annual_history = _annual_ebit(stock)
    current_assets = _balance_value(balance, "Current Assets")
    cash = _balance_value(
        balance, "Cash And Cash Equivalents", "Cash Cash Equivalents And Short Term Investments",
        "Cash And Short Term Investments",
    )
    current_liabilities = _balance_value(balance, "Current Liabilities")
    current_debt = _balance_value(balance, "Current Debt", "Short Long Term Debt")
    fixed_assets = _balance_value(
        balance, "Net PPE", "Property Plant Equipment Net", "Net Property Plant And Equipment",
    )
    total_debt = _balance_value(balance, "Total Debt", "Long Term Debt And Capital Lease Obligation")
    equity = _balance_value(
        balance, "Stockholders Equity", "Total Stockholder Equity",
        "Total Equity Gross Minority Interest",
    )
    goodwill = _goodwill_value(balance)
    intangible_assets = _intangible_assets_value(balance)
    if total_debt is None:
        total_debt = current_debt or 0
    annual_equity_by_date = {}
    if annual_balance is not None and not annual_balance.empty:
        equity_row = next(
            (label for label in annual_balance.index if str(label).strip().lower() in (
                "stockholders equity", "total stockholder equity",
                "total equity gross minority interest",
            )),
            None,
        )
        if equity_row is not None:
            for fiscal_date, value in annual_balance.loc[equity_row].items():
                if pd.notna(value):
                    annual_equity_by_date[str(pd.Timestamp(fiscal_date).date())] = float(value)
    for item in annual_history:
        item["equity"] = annual_equity_by_date.get(item["fiscal_date"])
    income = income * financial_fx if income is not None else None
    current_assets = current_assets * financial_fx if current_assets is not None else None
    cash = cash * financial_fx if cash is not None else None
    current_liabilities = (
        current_liabilities * financial_fx if current_liabilities is not None else None
    )
    current_debt = current_debt * financial_fx if current_debt is not None else None
    fixed_assets = fixed_assets * financial_fx if fixed_assets is not None else None
    total_debt = total_debt * financial_fx if total_debt is not None else None
    equity = equity * financial_fx if equity is not None else None
    goodwill = goodwill * financial_fx if goodwill is not None else None
    intangible_assets = (
        intangible_assets * financial_fx
        if intangible_assets is not None else None
    )
    for item in annual_history:
        if item["ebit"] is not None:
            item["ebit"] *= financial_fx
        if item["net_income"] is not None:
            item["net_income"] *= financial_fx
        if item["equity"] is not None:
            item["equity"] *= financial_fx

    market_cap = _number(quote.get("marketCap"))
    if market_cap is None:
        shares = _number(quote.get("sharesOutstanding"))
        market_cap = price * shares if price is not None and shares is not None else None
    price = price * quote_fx if price is not None else None
    market_cap = market_cap * quote_fx if market_cap is not None else None
    if market == "HK":
        dividend_history = stock.dividends
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=730)
        dividend_years = (
            int(pd.to_datetime(dividend_history.index, utc=True)[
                pd.to_datetime(dividend_history.index, utc=True) >= cutoff
            ].year.nunique())
            if dividend_history is not None and not dividend_history.empty else 0
        )
    else:
        dividend_years = None

    list_date = _epoch_date(info.get("firstTradeDateEpochUtc"))
    if list_date is None:
        list_date = _epoch_date(
            quote.get("firstTradeDateMilliseconds"), milliseconds=True
        )
    eps_values = [
        item["eps"] for item in annual_history
        if item.get("eps") is not None and item["eps"] > 0
    ]
    eps_years = min(3, len(eps_values) - 1)
    eps_growth = (
        (eps_values[0] / eps_values[eps_years]) ** (1 / eps_years) - 1
        if eps_years > 0 else _ratio(info.get("earningsGrowth"))
    )
    dividend_yield = _ratio(info.get("dividendYield"))
    if dividend_yield is None:
        dividend_yield = _ratio(info.get("trailingAnnualDividendYield"))
    updated_at = datetime.now().isoformat(timespec="seconds")
    record = {
        "market": market,
        "ticker": ticker_symbol,
        "name": quote.get("shortName") or quote.get("longName") or ticker_symbol,
        "industry": info.get("industry") or info.get("sector"),
        "list_date": list_date,
        "report_date": report_date,
        "ebit": income,
        "current_assets": current_assets,
        "cash": cash,
        "current_liabilities": current_liabilities,
        "current_debt": current_debt or 0,
        "fixed_assets": fixed_assets,
        "total_debt": total_debt,
        "equity": equity,
        "goodwill": goodwill,
        "intangible_assets": intangible_assets,
        "roe": _ratio(info.get("returnOnEquity")),
        "price_to_book": _number(info.get("priceToBook")),
        "dividend_yield": dividend_yield,
        "payout_ratio": _ratio(info.get("payoutRatio")),
        "eps_growth": eps_growth,
        "cet1_ratio": _ratio(info.get("cet1Ratio")),
        "npl_ratio": _ratio(info.get("nplRatio", info.get("nonPerformingLoansRatio"))),
        "provision_coverage": _ratio(info.get("provisionCoverageRatio")),
        "ffo_to_debt": _ratio(info.get("fundsFromOperationsToTotalDebt")),
        "market_cap": market_cap,
        "price": price,
        "price_date": price_date,
        "avg_daily_turnover": avg_turnover * quote_fx if avg_turnover is not None else None,
        "currency": listing_currency,
        "quote_currency": quote_currency,
        "quote_fx_to_listing": quote_fx,
        "financial_currency": financial_currency,
        "financial_fx_to_listing": financial_fx,
        "currency_assumption": "；".join(assumed_currencies) or None,
        "dividend_years": dividend_years,
        "audit_opinion": "yfinance 未审计" if market == "HK" else None,
        "audit_report_date": None,
        "source": "yfinance",
        "updated_at": updated_at,
        "quote_updated_at": updated_at,
        "fundamentals_updated_at": updated_at,
    }
    annual_records = [
        {
            "market": market,
            "ticker": ticker_symbol,
            **item,
            "source": "yfinance",
            "updated_at": record["updated_at"],
        }
        for item in annual_history
    ]
    return record, annual_records


def _yahoo_universe(market: str, min_market_cap: float, progress_callback=None) -> list[dict]:
    import yfinance as yf
    from yfinance import EquityQuery

    region = {"US": "us", "HK": "hk"}[market]
    query = EquityQuery("and", [
        EquityQuery("eq", ["region", region]),
        EquityQuery("gte", ["intradaymarketcap", min_market_cap]),
    ])
    results = []
    offset = 0
    page_size = 250
    while True:
        response = yf.screen(
            query,
            offset=offset,
            size=page_size,
            sortField="intradaymarketcap",
            sortAsc=False,
        )
        quotes = response.get("quotes") or []
        if not quotes:
            break
        results.extend(
            quote for quote in quotes
            if str(quote.get("quoteType", "")).upper() == "EQUITY"
            and (market != "US" or not _is_us_otc_quote(quote))
            and (market != "HK" or _is_hk_exchange_quote(quote))
        )
        if progress_callback:
            progress_callback(
                f"{market}: Yahoo Finance 股票池 {len(results):,}/{response.get('total', '?')}"
            )
        offset += len(quotes)
        if offset >= int(response.get("total") or 0) or len(quotes) < page_size:
            break
    return results


def _is_us_otc_quote(quote: dict) -> bool:
    """Identify OTC listings from Yahoo's exchange code and display metadata."""
    exchange = str(quote.get("exchange", "")).strip().upper()
    exchange_names = " ".join(
        str(quote.get(field, ""))
        for field in ("exchange", "exchangeDisp", "fullExchangeName")
    ).upper()
    return exchange in _US_OTC_EXCHANGES or "OTC" in exchange_names


def _is_hk_exchange_quote(quote: dict) -> bool:
    """Keep securities Yahoo identifies as listed on the Hong Kong exchange."""
    exchange = str(quote.get("exchange", "")).strip().upper()
    symbol = str(quote.get("symbol", "")).strip().upper()
    return exchange in {"HKG", "HKSE"} and symbol.endswith(".HK")


def _fetch_yahoo_market(
    market: str,
    min_market_cap: float,
    progress_callback=None,
) -> int:
    from magic_formula.store import upsert_annual_rows

    quotes = _yahoo_universe(market, min_market_cap, progress_callback)
    total = len(quotes)
    rows = []
    annual_rows = []
    failures = []
    successful = 0
    for index, quote in enumerate(quotes, 1):
        symbol = str(quote.get("symbol") or "")
        try:
            fetched = _yf_record(market, quote)
            if fetched:
                record, annual = fetched
                rows.append(record)
                annual_rows.extend(annual)
                successful += 1
        except Exception as exc:
            failures.append(f"{symbol}: {exc}")
        if len(rows) >= 50:
            upsert_rows(rows)
            upsert_annual_rows(annual_rows)
            rows.clear()
            annual_rows.clear()
        if progress_callback and (index == total or index % 20 == 0):
            progress_callback(f"{market}: 正在获取报价和财报 {index:,}/{total:,}")
        time.sleep(0.15)
    upsert_rows(rows)
    upsert_annual_rows(annual_rows)
    if total and successful == 0:
        sample = f" 首个错误：{failures[0]}" if failures else ""
        raise RuntimeError(f"未能获取任何 Yahoo Finance 数据。{sample}")
    if progress_callback and failures:
        progress_callback(f"{market}: {len(failures):,} 只股票抓取失败，例如 {failures[0]}")
    return successful


def _refresh_yahoo_fundamentals(
    market: str,
    progress_callback=None,
) -> int:
    from magic_formula.store import (
        load_market,
        upsert_annual_rows,
    )

    import yfinance as yf

    market = market.upper()
    cached = load_market(market)
    if cached.empty:
        if progress_callback:
            progress_callback(f"{market}: 尚无缓存股票，跳过财报检查。")
        return 0

    annual_rows = []
    updated_rows = []
    failures = []
    total = len(cached)
    checked = 0
    updated = 0
    financial_columns = (
        "report_date", "ebit", "current_assets", "cash",
        "current_liabilities", "current_debt", "fixed_assets", "total_debt",
        "equity", "goodwill", "intangible_assets", "roe", "price_to_book",
        "dividend_yield", "payout_ratio", "eps_growth", "cet1_ratio",
        "npl_ratio", "provision_coverage", "ffo_to_debt", "financial_currency",
        "financial_fx_to_listing", "currency_assumption",
        "fundamentals_updated_at", "source",
    )
    for _, cached_row in cached.iterrows():
        symbol = str(cached_row["ticker"]).strip().upper()
        try:
            stock = yf.Ticker(symbol)
            info = stock.info or {}
            latest_report = _yahoo_report_date(info)
            if not _report_is_newer(latest_report, cached_row.get("report_date")):
                checked += 1
                if progress_callback and (checked % 50 == 0 or checked == total):
                    progress_callback(
                        f"{market}: 正在检查财报日期 {checked:,}/{total:,}；"
                        f"发现新财报 {updated:,} 家"
                    )
                time.sleep(0.05)
                continue

            quote = {
                "symbol": symbol,
                "quoteType": "EQUITY",
                "currency": cached_row.get("quote_currency"),
                "regularMarketPrice": cached_row.get("price"),
                "marketCap": cached_row.get("market_cap"),
            }
            fetched = _yf_record(market, quote, stock=stock, info=info)
            checked += 1
            if fetched is not None:
                record, annual = fetched
                if _report_is_newer(
                    _normalized_report_date(record.get("report_date")),
                    cached_row.get("report_date"),
                ):
                    merged = cached_row.to_dict()
                    for column in financial_columns:
                        if column in record:
                            merged[column] = record[column]
                    merged["updated_at"] = record["updated_at"]
                    updated_rows.append(merged)
                    annual_rows.extend(annual)
                    updated += 1
            if progress_callback:
                progress_callback(
                    f"{market}: 正在检查财报日期 {checked:,}/{total:,}；"
                    f"发现新财报 {updated:,} 家"
                )
        except Exception as exc:
            checked += 1
            failures.append(f"{symbol}: {exc}")
            if progress_callback:
                progress_callback(
                    f"{market}: {symbol} 财报检查/更新失败，保留旧数据并继续：{exc}"
                )
        if len(updated_rows) >= 50:
            upsert_rows(updated_rows)
            upsert_annual_rows(annual_rows)
            updated_rows.clear()
            annual_rows.clear()
        time.sleep(0.15)

    upsert_rows(updated_rows)
    upsert_annual_rows(annual_rows)
    if progress_callback:
        progress_callback(
            f"{market}: 财报检查完成；检查 {checked:,} 家，"
            f"有新财报并更新 {updated:,} 家。"
        )
        if failures:
            progress_callback(
                f"{market}: {len(failures):,} 家财报检查/更新失败，例如 {failures[0]}"
            )
    return updated


def _tushare_client():
    from etl.sources.tushare_cn import load_tushare_token

    token = load_tushare_token()
    if not token:
        raise RuntimeError(
            "缺少 TUSHARE_TOKEN/TUSHARE_API_KEY，请在 .env 或环境变量中配置。"
        )
    import tushare as ts
    return ts.pro_api(token)


def _tushare_call_with_retry(call, attempts: int = 2):
    """Retry transient per-company Tushare failures once before skipping the ticker."""
    for attempt in range(attempts):
        try:
            return call()
        except Exception:
            if attempt + 1 == attempts:
                raise
            time.sleep(1)


def _ttm_ebit(
    annual_operating_profit: Any,
    annual_finance_expense: Any,
    current_operating_profit: Any,
    current_finance_expense: Any,
    prior_operating_profit: Any,
    prior_finance_expense: Any,
) -> float | None:
    annual = _number(annual_operating_profit)
    if annual is None:
        return None
    annual += _number(annual_finance_expense) or 0
    current = _number(current_operating_profit)
    prior = _number(prior_operating_profit)
    if current is None or prior is None:
        return None
    current += _number(current_finance_expense) or 0
    prior += _number(prior_finance_expense) or 0
    return annual + current - prior


def _ttm_fina_indicator_ebit(
    history: pd.DataFrame,
    current_period: date,
) -> float | None:
    if history is None or history.empty or not {"end_date", "ebit"} <= set(history):
        return None
    data = history.copy()
    data["period"] = pd.to_datetime(data["end_date"], errors="coerce").dt.date
    data["ebit"] = pd.to_numeric(data["ebit"], errors="coerce")
    data = data.dropna(subset=["period", "ebit"])
    if "ann_date" in data:
        data = data.sort_values("ann_date")
    data = data.drop_duplicates("period", keep="last").set_index("period")

    annual_period = date(current_period.year - 1, 12, 31)
    prior_period = current_period.replace(year=current_period.year - 1)
    periods = (annual_period, current_period, prior_period)
    if any(period not in data.index for period in periods):
        return None
    return float(
        data.loc[annual_period, "ebit"]
        + data.loc[current_period, "ebit"]
        - data.loc[prior_period, "ebit"]
    )


def _latest_cn_report_period(today: date) -> date:
    """Most recent interim period whose statutory filing deadline has passed."""
    if today.month >= 11:
        return date(today.year, 9, 30)
    if today.month >= 9:
        return date(today.year, 6, 30)
    if today.month >= 5:
        return date(today.year, 3, 31)
    return date(today.year - 1, 9, 30)


def _normalized_report_date(value: Any) -> date | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    parsed = pd.to_datetime(text, errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _yahoo_report_date(info: dict) -> date | None:
    value = info.get("mostRecentQuarter")
    timestamp = _number(value)
    if timestamp is not None:
        normalized = _epoch_date(timestamp)
        return _normalized_report_date(normalized)
    return _normalized_report_date(value)


def _report_is_newer(latest_report: date | None, stored_report: Any) -> bool:
    stored = _normalized_report_date(stored_report)
    return latest_report is not None and (stored is None or latest_report > stored)


def _cn_ts_code(symbol: Any) -> str:
    ticker = str(symbol).strip().split(".")[0].zfill(6)
    if ticker.startswith(("4", "8", "92")):
        exchange = "BJ"
    elif ticker.startswith(("6", "9")):
        exchange = "SH"
    else:
        exchange = "SZ"
    return f"{ticker}.{exchange}"


def _cn_missing_fundamental_records(records: pd.DataFrame) -> pd.DataFrame:
    if records.empty or not set(_CN_FORMULA_FINANCIAL_FIELDS).issubset(records.columns):
        return records.iloc[0:0].copy()
    operating = records["industry"].map(industry_group).eq("operating")
    missing = records[list(_CN_FORMULA_FINANCIAL_FIELDS)].isna().any(axis=1)
    return records.loc[operating & missing].copy()


def _cn_latest_balance_value(balance_rows: pd.DataFrame, column: str) -> Any:
    if column not in balance_rows:
        return None
    values = balance_rows.loc[balance_rows[column].notna(), column]
    return values.iloc[-1] if not values.empty else None


def _cn_new_report_candidates(
    pro,
    companies: pd.DataFrame,
    existing_records: pd.DataFrame,
    progress_callback=None,
) -> pd.DataFrame:
    """Find cached A-shares with a newer report using small per-ticker queries."""
    if companies.empty:
        return companies.copy()
    existing_dates = {}
    if not existing_records.empty and {"ticker", "report_date"} <= set(existing_records):
        existing_dates = dict(zip(
            existing_records["ticker"].astype(str),
            existing_records["report_date"],
        ))

    ticker_column = "symbol" if "symbol" in companies else "ticker"
    candidates = []
    total = len(companies)
    for index, (_, company) in enumerate(companies.iterrows(), 1):
        ticker = str(company[ticker_column]).zfill(6)
        ts_code = str(company.get("ts_code") or "")
        if not ts_code:
            ts_code = _cn_ts_code(ticker)
        force_update = bool(company.get("_force_update", False))
        stored_report = _normalized_report_date(existing_dates.get(ticker))
        if force_update and stored_report is not None and not bool(
            company.get("_needs_report_lookup", False)
        ):
            latest_report = stored_report
        else:
            reports = _tushare_call_with_retry(
                lambda: pro.income(
                    ts_code=ts_code,
                    fields="ts_code,ann_date,end_date",
                    limit=20,
                )
            )
            if reports is not None and not reports.empty and "end_date" in reports:
                report_dates = pd.to_datetime(
                    reports["end_date"], errors="coerce"
                ).dropna()
                latest_report = (
                    report_dates.max().date() if not report_dates.empty else None
                )
            else:
                latest_report = None
        if latest_report is not None and (
            _report_is_newer(latest_report, stored_report) or force_update
        ):
            candidate = company.copy()
            candidate["_latest_report_date"] = latest_report
            candidates.append(candidate)
        if progress_callback and (index % 50 == 0 or index == total):
            progress_callback(
                f"A股: 正在检查财报更新 {index:,}/{total:,}，"
                f"发现新报告期 {len(candidates):,} 家"
            )
    return pd.DataFrame(
        candidates,
        columns=[*companies.columns, "_latest_report_date"],
    )


def _cn_report_row(frame: pd.DataFrame, period: str) -> pd.Series | None:
    if frame is None or frame.empty or "end_date" not in frame:
        return None
    matches = frame.loc[frame["end_date"].astype(str) == period]
    if matches.empty:
        return None
    if "ann_date" in matches:
        matches = matches.sort_values("ann_date")
    return matches.iloc[-1]


def _fetch_cn_quote_data(
    pro,
    min_market_cap: float,
    progress_callback=None,
    *,
    include_turnover_history: bool = True,
):
    basic = pro.stock_basic(
        exchange="", list_status="L",
        fields="ts_code,symbol,name,industry,list_date",
    )
    if basic is None or basic.empty:
        raise RuntimeError("Tushare stock_basic 未返回 A 股列表。")
    basic = basic.loc[
        ~basic["name"].fillna("").str.contains(
            r"(?:^|\W)\*?ST(?:\W|$)", case=False, regex=True
        )
    ].copy()

    end = date.today()
    start = end - timedelta(days=100)
    calendar = pro.trade_cal(
        exchange="SSE", start_date=start.strftime("%Y%m%d"),
        end_date=end.strftime("%Y%m%d"), is_open="1",
        fields="cal_date",
    )
    trade_dates = sorted(calendar["cal_date"].astype(str).tolist())[-63:]
    if not include_turnover_history:
        trade_dates = trade_dates[-1:]
    if not trade_dates:
        raise RuntimeError("Tushare 未返回近期 A 股交易日。")
    daily_frames = []
    for index, trade_date in enumerate(trade_dates, 1):
        day = pro.daily(
            trade_date=trade_date,
            fields="ts_code,trade_date,close,amount",
        )
        if day is not None and not day.empty:
            daily_frames.append(day)
        if progress_callback and (index % 10 == 0 or index == len(trade_dates)):
            progress_callback(f"A股: 正在拉取近 3 个月日行情 {index}/{len(trade_dates)}")
    daily = pd.concat(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame()
    if daily.empty:
        raise RuntimeError("Tushare daily 未返回行情数据。")
    daily = daily.sort_values("trade_date")
    price = daily.groupby("ts_code").tail(1).set_index("ts_code")
    turnover = daily.assign(turnover=pd.to_numeric(daily["amount"], errors="coerce") * 1000)
    turnover = turnover.groupby("ts_code")["turnover"].mean()

    # Tushare daily_basic reports total_mv in 10,000 RMB.
    daily_basic = pro.daily_basic(
        trade_date=str(price["trade_date"].max()),
        fields="ts_code,total_mv,pb,dv_ttm,pe_ttm",
    )
    if daily_basic is None or daily_basic.empty:
        raise RuntimeError("Tushare daily_basic 未返回最新市值。")
    candidates = basic.merge(
        price[["close", "trade_date"]], left_on="ts_code", right_index=True, how="inner"
    ).merge(
        daily_basic, on="ts_code", how="inner"
    )
    candidates["total_mv"] = pd.to_numeric(candidates["total_mv"], errors="coerce")
    candidates = candidates.loc[candidates["total_mv"] * 10000 >= min_market_cap]
    return candidates, turnover, str(price["trade_date"].max())


def _fetch_cn(
    min_market_cap: float,
    progress_callback=None,
    *,
    financials_only: bool = False,
) -> int:
    from magic_formula.store import load_market, upsert_annual_rows

    pro = _tushare_client()
    existing_records = load_market("CN")
    quote_rows = []
    if financials_only:
        companies = _cn_missing_fundamental_records(existing_records).rename(
            columns={"ticker": "symbol"}
        ).copy()
        companies["ts_code"] = companies["symbol"].map(_cn_ts_code)
        companies["_force_update"] = True
        companies["_needs_income"] = (
            companies["report_date"].isna() | companies["ebit"].isna()
        )
        companies["_needs_report_lookup"] = companies["report_date"].isna()
        quote_rows_by_ticker = {
            str(row["ticker"]): row
            for row in existing_records.to_dict(orient="records")
        }
    else:
        companies, turnover, _ = _fetch_cn_quote_data(
            pro, min_market_cap, progress_callback
        )
        existing = existing_records.set_index("ticker", drop=False)
        quote_time = datetime.now().isoformat(timespec="seconds")
        total = len(companies)
        for index, (_, company) in enumerate(companies.iterrows(), 1):
            ticker = str(company["symbol"]).zfill(6)
            if ticker in existing.index:
                row = existing.loc[ticker].to_dict()
            else:
                row = {
                    "market": "CN",
                    "ticker": ticker,
                    "currency": "CNY",
                    "quote_currency": "CNY",
                    "quote_fx_to_listing": 1.0,
                    "source": "tushare",
                }
            row.update({
                "name": company.get("name"),
                "industry": company.get("industry"),
                "list_date": str(company.get("list_date") or ""),
                "market_cap": (_number(company.get("total_mv")) or 0) * 10000,
                "price": _number(company.get("close")),
                "price_to_book": _number(company.get("pb")),
                "dividend_yield": _ratio(company.get("dv_ttm")),
                "payout_ratio": (
                    (_ratio(company.get("dv_ttm")) or 0) * (_number(company.get("pe_ttm")) or 0)
                    if _number(company.get("dv_ttm")) is not None
                    and _number(company.get("pe_ttm")) is not None
                    else None
                ),
                "price_date": str(company.get("trade_date") or ""),
                "avg_daily_turnover": _number(turnover.get(str(company["ts_code"]))),
                "currency": "CNY",
                "source": "tushare",
                "updated_at": quote_time,
                "quote_updated_at": quote_time,
            })
            quote_rows.append(row)
            if progress_callback and (index % 20 == 0 or index == total):
                progress_callback(f"A股: 正在更新报价 {index}/{total}")
        upsert_rows(quote_rows)
        quote_rows_by_ticker = {row["ticker"]: row for row in quote_rows}

    candidates = _cn_new_report_candidates(
        pro,
        companies,
        existing_records,
        progress_callback,
    )
    total = len(candidates)
    if not total and progress_callback:
        progress_callback("A股: 没有发现比本地记录更新的财报，跳过逐股财报下载。")

    income_fields = (
        "ts_code,ann_date,end_date,operate_profit,fin_exp,n_income_attr_p,basic_eps"
    )
    balance_fields = (
        "ts_code,ann_date,end_date,total_cur_assets,money_cap,total_cur_liab,"
        "st_borr,lt_borr,bonds_payable,fix_assets,total_hldr_eqy_exc_min_int,"
        "goodwill,intan_assets"
    )
    rows = []
    annual_history_rows = []
    failures = []
    for index, (_, company) in enumerate(candidates.iterrows(), 1):
        ts_code = str(company["ts_code"])
        current_date = _normalized_report_date(company["_latest_report_date"])
        if current_date is None:
            continue
        annual_period = f"{current_date.year - 1}1231"
        current_period = current_date.strftime("%Y%m%d")
        prior_period = current_date.replace(
            year=current_date.year - 1
        ).strftime("%Y%m%d")
        stored_date = _normalized_report_date(company.get("report_date"))
        needs_income = (
            bool(company.get("_needs_income", False))
            or stored_date is None
            or current_date > stored_date
        )
        try:
            income = (
                _tushare_call_with_retry(
                    lambda: pro.income(ts_code=ts_code, fields=income_fields)
                )
                if needs_income
                else pd.DataFrame()
            )
            balance = _tushare_call_with_retry(
                lambda: pro.balancesheet(ts_code=ts_code, fields=balance_fields)
            )
        except Exception as exc:
            failures.append(f"{ts_code}: {exc}")
            if progress_callback:
                progress_callback(f"A股: {ts_code} 财报请求失败，保留旧数据并继续：{exc}")
            continue

        if income is None:
            income = pd.DataFrame()
        if balance is None or balance.empty:
            continue
        annual = _cn_report_row(income, annual_period)
        current = _cn_report_row(income, current_period)
        prior = _cn_report_row(income, prior_period)
        ebit = _ttm_ebit(
            annual.get("operate_profit") if annual is not None else None,
            annual.get("fin_exp") if annual is not None else None,
            current.get("operate_profit") if current is not None else None,
            current.get("fin_exp") if current is not None else None,
            prior.get("operate_profit") if prior is not None else None,
            prior.get("fin_exp") if prior is not None else None,
        )
        if ebit is None and needs_income:
            try:
                indicator_history = _tushare_call_with_retry(
                    lambda: pro.fina_indicator(
                        ts_code=ts_code,
                        fields="ts_code,ann_date,end_date,ebit",
                        limit=30,
                    )
                )
            except Exception as exc:
                failures.append(f"{ts_code} EBIT 指标请求失败: {exc}")
                if progress_callback:
                    progress_callback(
                        f"A股: {ts_code} EBIT 指标请求失败，保留空值并继续：{exc}"
                    )
            else:
                ebit = _ttm_fina_indicator_ebit(
                    indicator_history, current_date
                )
                if ebit is None and progress_callback:
                    progress_callback(
                        f"A股: {ts_code} 缺少可比年度/同期 EBIT，无法计算 TTM。"
                    )
        balance_rows = balance.loc[
            balance["end_date"].astype(str) <= current_period
        ].copy()
        if balance_rows.empty:
            continue
        balance_rows = balance_rows.sort_values(["end_date", "ann_date"])
        latest_balance = balance_rows.iloc[-1]

        if income is not None and not income.empty:
            annual_reports = income.loc[
                income["end_date"].astype(str).str.endswith("1231")
            ]
            for period, group in annual_reports.groupby("end_date"):
                annual_row = group.sort_values("ann_date").iloc[-1]
                operating_profit = _number(annual_row.get("operate_profit"))
                annual_ebit = (
                    operating_profit + (_number(annual_row.get("fin_exp")) or 0)
                    if operating_profit is not None else None
                )
                annual_balance = _cn_report_row(balance, str(period))
                annual_equity = (
                    _number(annual_balance.get("total_hldr_eqy_exc_min_int"))
                    if annual_balance is not None else None
                )
                net_income = _number(annual_row.get("n_income_attr_p"))
                eps = _number(annual_row.get("basic_eps"))
                if any(value is not None for value in (
                    annual_ebit, net_income, annual_equity, eps
                )):
                    annual_history_rows.append({
                        "market": "CN",
                        "ticker": str(company["symbol"]).zfill(6),
                        "fiscal_date": str(period),
                        "ebit": annual_ebit,
                        "net_income": net_income,
                        "equity": annual_equity,
                        "eps": eps,
                        "source": "tushare",
                        "updated_at": datetime.now().isoformat(timespec="seconds"),
                    })

        short_debt = _number(_cn_latest_balance_value(balance_rows, "st_borr")) or 0
        total_debt = sum(
            _number(_cn_latest_balance_value(balance_rows, col)) or 0
            for col in ("st_borr", "lt_borr", "bonds_payable")
        )
        ticker = str(company["symbol"]).zfill(6)
        updated_at = datetime.now().isoformat(timespec="seconds")
        row = dict(quote_rows_by_ticker[ticker])
        financial_updates = {
            "market": "CN",
            "name": company.get("name"),
            "industry": company.get("industry"),
            "list_date": str(company.get("list_date") or ""),
            "report_date": str(latest_balance.get("end_date") or ""),
            "ebit": ebit,
            "current_assets": _number(
                _cn_latest_balance_value(balance_rows, "total_cur_assets")
            ),
            "cash": _number(_cn_latest_balance_value(balance_rows, "money_cap")),
            "current_liabilities": _number(
                _cn_latest_balance_value(balance_rows, "total_cur_liab")
            ),
            "current_debt": short_debt,
            "fixed_assets": _number(
                _cn_latest_balance_value(balance_rows, "fix_assets")
            ),
            "total_debt": total_debt,
            "equity": _number(
                _cn_latest_balance_value(
                    balance_rows, "total_hldr_eqy_exc_min_int"
                )
            ),
            "goodwill": _number(_cn_latest_balance_value(balance_rows, "goodwill")),
            "intangible_assets": _number(
                _cn_latest_balance_value(balance_rows, "intan_assets")
            ),
            "roe": (
                _number(annual.get("n_income_attr_p"))
                / _number(latest_balance.get("total_hldr_eqy_exc_min_int"))
                if annual is not None
                and _number(annual.get("n_income_attr_p")) is not None
                and _number(latest_balance.get("total_hldr_eqy_exc_min_int")) not in (None, 0)
                else None
            ),
            "source": "tushare",
            "financial_currency": "CNY",
            "financial_fx_to_listing": 1.0,
        }
        for column, value in financial_updates.items():
            if value is not None and not pd.isna(value):
                if not financials_only or pd.isna(row.get(column)):
                    row[column] = value
        row["updated_at"] = updated_at
        row["fundamentals_updated_at"] = updated_at
        rows.append(row)
        if progress_callback and index % 20 == 0:
            progress_callback(f"A股: 正在更新候选公司财报 {index}/{len(candidates)}")
    count = upsert_rows(rows)
    upsert_annual_rows(annual_history_rows)
    if progress_callback:
        if financials_only:
            progress_callback(f"A股: 新财报更新完成 {count:,} 家。")
        else:
            progress_callback(
                f"A股: 报价已更新 {len(quote_rows):,} 家，"
                f"财报成功更新 {count:,} 家。"
            )
        if failures:
            progress_callback(
                f"A股: {len(failures):,} 家财报更新失败并已跳过，例如 {failures[0]}"
            )
    return count if financials_only else len(quote_rows)


def refresh_fundamentals(market: str, progress_callback=None) -> int:
    """Update only cached companies with a newer published financial report."""
    market = market.upper()
    if market == "CN":
        return _fetch_cn(0, progress_callback, financials_only=True)
    if market in ("US", "HK"):
        return _refresh_yahoo_fundamentals(market, progress_callback)
    raise ValueError(f"不支持的市场: {market}")


def refresh_market(
    market: str,
    min_market_cap: float = 0,
    progress_callback=None,
) -> int:
    """Fetch one market and persist its current screening inputs."""
    market = market.upper()
    if market == "CN":
        return _fetch_cn(min_market_cap, progress_callback)
    if market in ("US", "HK"):
        return _fetch_yahoo_market(market, min_market_cap, progress_callback)
    raise ValueError(f"不支持的市场: {market}")


def _refresh_cn_quotes(
    records: pd.DataFrame,
    progress_callback=None,
) -> int:
    pro = _tushare_client()
    candidates, _, _ = _fetch_cn_quote_data(
        pro, 0, progress_callback, include_turnover_history=False
    )
    existing = records.set_index("ticker", drop=False)
    updated_at = datetime.now().isoformat(timespec="seconds")
    rows = []
    total = len(candidates)
    for index, (_, company) in enumerate(candidates.iterrows(), 1):
        ticker = str(company["symbol"]).zfill(6)
        if ticker not in existing.index:
            if progress_callback and (index % 20 == 0 or index == total):
                progress_callback(f"A股: 正在更新报价 {index}/{total}")
            continue
        row = existing.loc[ticker].to_dict()
        row.update({
            "name": company.get("name") or row.get("name"),
            "industry": company.get("industry") or row.get("industry"),
            "list_date": str(company.get("list_date") or row.get("list_date") or ""),
            "market_cap": (_number(company.get("total_mv")) or 0) * 10000,
            "price": _number(company.get("close")),
            "price_to_book": _number(company.get("pb")),
            "dividend_yield": _ratio(company.get("dv_ttm")),
            "payout_ratio": (
                (_ratio(company.get("dv_ttm")) or 0) * (_number(company.get("pe_ttm")) or 0)
                if _number(company.get("dv_ttm")) is not None
                and _number(company.get("pe_ttm")) is not None
                else None
            ),
            "price_date": str(company.get("trade_date") or ""),
            "currency": "CNY",
            "quote_currency": "CNY",
            "quote_fx_to_listing": 1.0,
            "updated_at": updated_at,
            "quote_updated_at": updated_at,
        })
        rows.append(row)
        if progress_callback and (index % 20 == 0 or index == total):
            progress_callback(f"A股: 正在更新报价 {index}/{total}")
    count = upsert_rows(rows)
    if progress_callback:
        progress_callback(f"A股: 已更新 {count:,} 家报价，未请求财报。")
    return count


def refresh_quotes(
    market: str,
    records: pd.DataFrame,
    min_market_cap: float = 0,
    progress_callback=None,
) -> int:
    """Refresh current quotes only, leaving cached financial statements untouched."""
    if records.empty:
        return 0
    market = market.upper()
    if market == "CN":
        return _refresh_cn_quotes(records, progress_callback)
    if market not in ("US", "HK"):
        raise ValueError(f"不支持的市场: {market}")
    quotes = _yahoo_universe(market, min_market_cap, progress_callback)
    quotes_by_symbol = {str(quote.get("symbol", "")).upper(): quote for quote in quotes}
    listing_currency = _MARKET_CURRENCIES[market]
    rows = []
    failures = []
    updated_at = datetime.now().isoformat(timespec="seconds")
    total = len(records)
    for index, (_, row) in enumerate(records.iterrows(), 1):
        symbol = str(row["ticker"]).upper()
        quote = quotes_by_symbol.get(symbol)
        if quote:
            try:
                raw_price = _number(quote.get("regularMarketPrice"))
                if raw_price is not None:
                    quote_currency_value = str(quote.get("currency") or "").strip()
                    if not quote_currency_value and market == "US":
                        quote_currency_value = "USD"
                        assumed_quote_currency = True
                    elif not quote_currency_value:
                        raise RuntimeError("Yahoo Finance 未提供报价币种。")
                    else:
                        assumed_quote_currency = False
                    quote_currency = quote_currency_value
                    quote_fx = _currency_rate_to_listing(quote_currency, listing_currency)
                    price = raw_price * quote_fx
                    price_date = (
                        _epoch_date(quote.get("regularMarketTime")) or row.get("price_date")
                    )
                    updated = row.to_dict()
                    assumptions = [
                        assumption for assumption in
                        str(updated.get("currency_assumption") or "").split("；")
                        if assumption and assumption != "报价币种缺失→USD"
                    ]
                    if assumed_quote_currency:
                        assumptions.append("报价币种缺失→USD")
                    market_cap = _number(quote.get("marketCap"))
                    updated.update({
                        "price": price,
                        "price_date": price_date,
                        "market_cap": (
                            market_cap * quote_fx if market_cap is not None
                            else row.get("market_cap")
                        ),
                        "currency": listing_currency,
                        "quote_currency": quote_currency,
                        "quote_fx_to_listing": quote_fx,
                        "currency_assumption": "；".join(assumptions) or None,
                        "updated_at": updated_at,
                        "quote_updated_at": updated_at,
                    })
                    rows.append(updated)
            except Exception as exc:
                failures.append(f"{symbol}: {exc}")
        if progress_callback and (index % 20 == 0 or index == total):
            progress_callback(f"{market}: 正在更新报价 {index:,}/{total:,}")
    count = upsert_rows(rows)
    if progress_callback and failures:
        progress_callback(
            f"{market}: {len(failures):,} 只报价/汇率更新失败并已跳过，例如 {failures[0]}"
        )
    return count
