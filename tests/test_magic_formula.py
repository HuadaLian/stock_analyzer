import sqlite3

import pandas as pd

from magic_formula.engine import (
    apply_ebit_method,
    industry_group,
    rank_financials,
    rank_market,
    rank_utilities,
)
from magic_formula import store
from magic_formula.dashboard import (
    _currency_normalized_rows,
    _market_cap_from_millions,
    _update_progress_fraction,
)
from magic_formula import sources
from magic_formula.sources import (
    _annual_ebit,
    _goodwill_value,
    _intangible_assets_value,
    _ttm_ebit,
)
from etl.sources.tushare_cn import load_tushare_token


def _record(ticker, ebit=100, **overrides):
    row = {
        "ticker": ticker,
        "name": ticker,
        "industry": "Software",
        "list_date": "2010-01-01",
        "report_date": "2025-12-31",
        "ebit": ebit,
        "current_assets": 500,
        "cash": 100,
        "current_liabilities": 200,
        "current_debt": 50,
        "fixed_assets": 200,
        "total_debt": 100,
        "market_cap": 1000,
        "price": 10,
        "avg_daily_turnover": 20_000_000,
        "currency": "USD",
        "dividend_years": 2,
        "audit_opinion": "unqualified",
    }
    row.update(overrides)
    return row


def test_ranks_roc_and_earnings_yield_independently_then_sums():
    rows = pd.DataFrame([
        _record("A", ebit=120, market_cap=1500),
        _record("B", ebit=100, market_cap=500),
        _record("C", ebit=70, market_cap=800),
    ])

    ranked = rank_market(rows, "US", top_n=3)

    assert set(ranked["ticker"]) == {"A", "B", "C"}
    assert ranked.iloc[0]["ticker"] == "B"
    assert ranked.iloc[0]["rank_roc"] == 2
    assert ranked.iloc[0]["rank_ey"] == 1
    assert ranked.iloc[0]["total_score"] == 3


def test_roc_excludes_goodwill_and_intangible_assets():
    rows = pd.DataFrame([_record(
        "ADBE",
        goodwill=14_993_000_000,
        intangible_assets=300,
    )])

    ranked = rank_market(rows, "US")

    assert ranked["ticker"].tolist() == ["ADBE"]
    assert ranked.iloc[0]["invested_capital"] == 450


def test_low_debt_company_with_nonpositive_capital_gets_standardized_roc_rank():
    rows = pd.DataFrame([
        _record(
            "LOW_DEBT",
            current_assets=100,
            cash=0,
            current_liabilities=500,
            current_debt=0,
            fixed_assets=50,
            total_debt=40,
            market_cap=500,
        ),
        _record(
            "HIGH_DEBT",
            current_assets=100,
            cash=0,
            current_liabilities=500,
            current_debt=0,
            fixed_assets=50,
            total_debt=100,
            market_cap=500,
        ),
        _record("POSITIVE_CAPITAL", market_cap=800),
    ])

    ranked = rank_market(rows, "US", top_n=3)

    assert "LOW_DEBT" in ranked["ticker"].tolist()
    normalized = ranked.set_index("ticker").loc["LOW_DEBT"]
    assert normalized["invested_capital"] < 0
    assert normalized["roc"] < 0
    assert normalized["roc_rank_basis"] == "标准化（低负债、C≤0）"
    assert normalized["rank_roc"] == 1
    assert "HIGH_DEBT" not in ranked["ticker"].tolist()


def test_yahoo_goodwill_and_intangible_lines_are_not_conflated():
    balance = pd.DataFrame(
        [[14_993], [14_000], [993]],
        index=[
            "Goodwill And Other Intangible Assets",
            "Goodwill",
            "Other Intangible Assets",
        ],
        columns=["2025-12-31"],
    )

    assert _goodwill_value(balance) == 14_000
    assert _intangible_assets_value(balance) == 993


def test_combined_yahoo_goodwill_intangible_line_is_not_counted_as_intangible():
    balance = pd.DataFrame(
        [[14_993]],
        index=["Goodwill And Other Intangible Assets"],
        columns=["2025-12-31"],
    )

    assert _goodwill_value(balance) is None
    assert _intangible_assets_value(balance) is None


def test_default_rank_limit_returns_up_to_100_rows():
    rows = pd.DataFrame([_record(f"TICKER{i:03}") for i in range(105)])

    ranked = rank_market(rows, "US")

    assert len(ranked) == 100


def test_default_utility_rank_limit_returns_up_to_100_rows():
    rows = pd.DataFrame([
        _record(
            f"UTILITY{i:03}",
            industry="Utilities",
            eps_growth=0.1,
            dividend_yield=0.02,
        )
        for i in range(105)
    ])

    ranked = rank_utilities(rows, pd.DataFrame(), "US")

    assert len(ranked) == 100


def test_excludes_nonpositive_ev_financials_recent_listings_and_bad_hk_audits():
    rows = pd.DataFrame([
        _record("GOOD"),
        _record("BANK", industry="Bank"),
        _record("NEW", list_date=pd.Timestamp.now().strftime("%Y-%m-%d")),
        _record("NETCASH", market_cap=100, cash=200, total_debt=0),
    ])

    ranked = rank_market(rows, "US")

    assert ranked["ticker"].tolist() == ["GOOD"]

    hk_rows = pd.DataFrame([
        _record("PASS", market_cap=8_000_000_000, audit_opinion="yfinance 未审计"),
        _record("UNKNOWN", market_cap=8_000_000_000, audit_opinion=None),
        _record("ILLIQUID", market_cap=8_000_000_000, avg_daily_turnover=1_000_000),
    ])
    hk_ranked = rank_market(hk_rows, "HK")
    assert set(hk_ranked["ticker"]) == {"PASS", "UNKNOWN"}


def test_cn_market_cap_threshold_and_st_exclusion():
    rows = pd.DataFrame([
        _record("600001", market_cap=6_000_000_000, name="Ordinary Co"),
        _record("600002", market_cap=6_000_000_000, name="*ST Risk"),
        _record("600003", market_cap=4_000_000_000, name="Small Co"),
    ])

    ranked = rank_market(rows, "CN")

    assert ranked["ticker"].tolist() == ["600001"]


def test_dedicated_store_upserts_and_reads_market_rows(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "magic_formula.db")
    row = _record("AAPL")
    row.update({
        "market": "US",
        "source": "test",
        "updated_at": "2026-01-01T00:00:00",
        "quote_updated_at": "2026-01-01T00:00:00",
        "fundamentals_updated_at": "2026-01-01T00:00:00",
    })

    assert store.upsert_rows([row]) == 1
    result = store.load_market("US")

    assert result["ticker"].tolist() == ["AAPL"]
    assert store.latest_update("US") == "2026-01-01T00:00:00"
    assert store.latest_quote_update("US") == "2026-01-01T00:00:00"
    assert store.latest_fundamentals_update("US") == "2026-01-01T00:00:00"


def test_quote_only_record_keeps_fundamentals_timestamp_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "magic_formula.db")
    store.upsert_rows([{
        **_record("AAPL"),
        "market": "US",
        "source": "test",
        "updated_at": "2026-01-02T00:00:00",
        "quote_updated_at": "2026-01-02T00:00:00",
        "fundamentals_updated_at": None,
    }])

    record = store.load_market("US").iloc[0]

    assert pd.isna(record["fundamentals_updated_at"])
    assert store.latest_fundamentals_update("US") is None


def test_store_migrates_legacy_update_timestamp_columns(tmp_path, monkeypatch):
    database = tmp_path / "magic_formula.db"
    with sqlite3.connect(database) as conn:
        conn.execute(
            """
            CREATE TABLE fundamentals (
                market TEXT NOT NULL,
                ticker TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (market, ticker)
            )
            """
        )
        conn.execute(
            "INSERT INTO fundamentals VALUES ('US', 'AAPL', '2026-01-03T00:00:00')"
        )
    monkeypatch.setattr(store, "DB_PATH", database)

    assert store.latest_quote_update("US") == "2026-01-03T00:00:00"
    assert store.latest_fundamentals_update("US") == "2026-01-03T00:00:00"


def test_tushare_ttm_uses_last_annual_plus_current_ytd_less_prior_ytd():
    assert _ttm_ebit(100, 10, 90, 8, 70, 6) == 132
    assert _ttm_ebit(100, 10, None, None, 70, 6) is None


def test_tushare_indicator_ttm_ebit_uses_annual_current_and_prior_periods():
    history = pd.DataFrame([
        {"end_date": "20251231", "ebit": 350},
        {"end_date": "20260930", "ebit": 286},
        {"end_date": "20250930", "ebit": 248},
    ])

    assert sources._ttm_fina_indicator_ebit(
        history, pd.Timestamp("2026-09-30").date()
    ) == 388


def test_tushare_indicator_ttm_ebit_requires_prior_comparable_period():
    history = pd.DataFrame([
        {"end_date": "20251231", "ebit": 350},
        {"end_date": "20260930", "ebit": 286},
    ])

    assert sources._ttm_fina_indicator_ebit(
        history, pd.Timestamp("2026-09-30").date()
    ) is None


def test_cn_refresh_queries_financial_statements_with_required_ts_code(tmp_path, monkeypatch):
    report_period = sources._latest_cn_report_period(pd.Timestamp.now().date())
    annual_period = f"{report_period.year - 1}1231"
    current_period = report_period.strftime("%Y%m%d")
    prior_period = report_period.replace(year=report_period.year - 1).strftime("%Y%m%d")
    trade_date = pd.Timestamp.now().strftime("%Y%m%d")

    class FakeTushare:
        def __init__(self):
            self.financial_queries = []

        def stock_basic(self, **kwargs):
            return pd.DataFrame([{
                "ts_code": "600001.SH", "symbol": "600001", "name": "Company",
                "industry": "Industry", "list_date": "20100101",
            }])

        def trade_cal(self, **kwargs):
            return pd.DataFrame([{"cal_date": trade_date}])

        def daily(self, **kwargs):
            return pd.DataFrame([{
                "ts_code": "600001.SH", "trade_date": trade_date,
                "close": 10, "amount": 10000,
            }])

        def daily_basic(self, **kwargs):
            return pd.DataFrame([{"ts_code": "600001.SH", "total_mv": 600000}])

        def income(self, *, ts_code, fields, limit=None):
            if fields == "ts_code,ann_date,end_date":
                return pd.DataFrame([{
                    "ts_code": ts_code,
                    "ann_date": current_period,
                    "end_date": current_period,
                }])
            self.financial_queries.append(("income", ts_code))
            return pd.DataFrame([
                {"ts_code": ts_code, "ann_date": annual_period,
                 "end_date": annual_period, "operate_profit": 100, "fin_exp": 10},
                {"ts_code": ts_code, "ann_date": current_period,
                 "end_date": current_period, "operate_profit": 90, "fin_exp": 8},
                {"ts_code": ts_code, "ann_date": prior_period,
                 "end_date": prior_period, "operate_profit": 70, "fin_exp": 6},
            ])

        def balancesheet(self, *, ts_code, fields):
            self.financial_queries.append(("balancesheet", ts_code))
            return pd.DataFrame([{
                "ts_code": ts_code, "ann_date": current_period,
                "end_date": current_period, "total_cur_assets": 500,
                "money_cap": 100, "total_cur_liab": 200, "st_borr": 50,
                "lt_borr": 30, "bonds_payable": 20, "fix_assets": 200,
            }])

    fake = FakeTushare()
    monkeypatch.setattr(sources, "_tushare_client", lambda: fake)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "magic_formula.db")

    assert sources._fetch_cn(5_000_000_000) == 1
    assert fake.financial_queries == [
        ("income", "600001.SH"), ("balancesheet", "600001.SH")
    ]
    record = store.load_market("CN").iloc[0]
    assert record["ebit"] == 132


def test_cn_quote_refresh_does_not_request_financial_statements(tmp_path, monkeypatch):
    trade_date = pd.Timestamp.now().strftime("%Y%m%d")

    class FakeTushare:
        def __init__(self):
            self.daily_dates = []

        def stock_basic(self, **kwargs):
            return pd.DataFrame([{
                "ts_code": "600001.SH", "symbol": "600001", "name": "Company",
                "industry": "Industry", "list_date": "20100101",
            }])

        def trade_cal(self, **kwargs):
            return pd.DataFrame([{"cal_date": trade_date}])

        def daily(self, **kwargs):
            self.daily_dates.append(kwargs["trade_date"])
            return pd.DataFrame([{
                "ts_code": "600001.SH", "trade_date": trade_date,
                "close": 12, "amount": 11000,
            }])

        def daily_basic(self, **kwargs):
            return pd.DataFrame([{"ts_code": "600001.SH", "total_mv": 700000}])

        def income(self, **kwargs):
            raise AssertionError("quote refresh must not fetch income statements")

        def balancesheet(self, **kwargs):
            raise AssertionError("quote refresh must not fetch balance sheets")

    fake = FakeTushare()
    monkeypatch.setattr(sources, "_tushare_client", lambda: fake)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "magic_formula.db")
    store.upsert_rows([{
        **_record("600001", ebit=132),
        "market": "CN",
        "source": "tushare",
        "updated_at": "2026-01-01T00:00:00",
        "quote_updated_at": "2026-01-01T00:00:00",
        "fundamentals_updated_at": "2026-01-01T00:00:00",
    }])

    count = sources.refresh_quotes("CN", store.load_market("CN"))

    refreshed = store.load_market("CN").iloc[0]
    assert count == 1
    assert refreshed["price"] == 12
    assert refreshed["ebit"] == 132
    assert refreshed["fundamentals_updated_at"] == "2026-01-01T00:00:00"
    assert refreshed["quote_updated_at"] > "2026-01-01T00:00:00"
    assert len(fake.daily_dates) == 1


def test_cn_fundamentals_timeout_retries_then_keeps_old_data_and_continues(
    tmp_path, monkeypatch
):
    report_period = sources._latest_cn_report_period(pd.Timestamp.now().date())
    annual_period = f"{report_period.year - 1}1231"
    current_period = report_period.strftime("%Y%m%d")
    prior_period = report_period.replace(year=report_period.year - 1).strftime("%Y%m%d")
    trade_date = pd.Timestamp.now().strftime("%Y%m%d")

    class FakeTushare:
        def __init__(self):
            self.failed_income_calls = 0

        def stock_basic(self, **kwargs):
            return pd.DataFrame([
                {"ts_code": f"{code}.SH", "symbol": code, "name": "Company",
                 "industry": "Industry", "list_date": "20100101"}
                for code in ("600001", "600002")
            ])

        def trade_cal(self, **kwargs):
            return pd.DataFrame([{"cal_date": trade_date}])

        def daily(self, **kwargs):
            return pd.DataFrame([
                {"ts_code": f"{code}.SH", "trade_date": trade_date,
                 "close": 10, "amount": 10000}
                for code in ("600001", "600002")
            ])

        def daily_basic(self, **kwargs):
            return pd.DataFrame([
                {"ts_code": f"{code}.SH", "total_mv": 600000}
                for code in ("600001", "600002")
            ])

        def income(self, *, ts_code, fields, limit=None):
            if fields == "ts_code,ann_date,end_date":
                return pd.DataFrame([{
                    "ts_code": ts_code,
                    "ann_date": current_period,
                    "end_date": current_period,
                }])
            if ts_code == "600001.SH":
                self.failed_income_calls += 1
                raise TimeoutError("simulated Tushare timeout")
            return pd.DataFrame([
                {"ts_code": ts_code, "ann_date": annual_period,
                 "end_date": annual_period, "operate_profit": 100, "fin_exp": 10},
                {"ts_code": ts_code, "ann_date": current_period,
                 "end_date": current_period, "operate_profit": 90, "fin_exp": 8},
                {"ts_code": ts_code, "ann_date": prior_period,
                 "end_date": prior_period, "operate_profit": 70, "fin_exp": 6},
            ])

        def balancesheet(self, *, ts_code, fields):
            return pd.DataFrame([{
                "ts_code": ts_code, "ann_date": current_period,
                "end_date": current_period, "total_cur_assets": 500,
                "money_cap": 100, "total_cur_liab": 200, "st_borr": 50,
                "lt_borr": 30, "bonds_payable": 20, "fix_assets": 200,
            }])

    fake = FakeTushare()
    progress = []
    monkeypatch.setattr(sources, "_tushare_client", lambda: fake)
    monkeypatch.setattr(sources.time, "sleep", lambda _: None)
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "magic_formula.db")
    store.upsert_rows([{
        **_record("600001", ebit=777),
        "market": "CN",
        "source": "tushare",
        "updated_at": "2026-01-01T00:00:00",
        "quote_updated_at": "2026-01-01T00:00:00",
        "fundamentals_updated_at": "2026-01-01T00:00:00",
    }])

    count = sources.refresh_market("CN", 5_000_000_000, progress.append)

    refreshed = store.load_market("CN").set_index("ticker")
    assert count == 2
    assert fake.failed_income_calls == 2
    assert refreshed.loc["600001", "ebit"] == 777
    assert refreshed.loc["600001", "fundamentals_updated_at"] == "2026-01-01T00:00:00"
    assert refreshed.loc["600002", "ebit"] == 132
    assert any("保留旧数据并继续" in message for message in progress)


def test_cn_new_report_candidates_only_selects_newer_published_periods():
    class FakeTushare:
        def __init__(self):
            self.checked = []

        def income(self, *, ts_code, fields, limit):
            self.checked.append(ts_code)
            if ts_code == "600001.SH":
                return pd.DataFrame([{
                    "ts_code": ts_code,
                    "ann_date": "20261028",
                    "end_date": "20260930",
                }])
            return pd.DataFrame([{
                "ts_code": ts_code,
                "ann_date": "20261025",
                "end_date": "20260930",
            }])

    companies = pd.DataFrame([
        {"symbol": "600001", "ts_code": "600001.SH"},
        {"symbol": "600002", "ts_code": "600002.SH"},
    ])
    existing = pd.DataFrame([
        {"ticker": "600001", "report_date": "2026-06-30"},
        {"ticker": "600002", "report_date": "2026-09-30"},
    ])
    fake = FakeTushare()

    candidates = sources._cn_new_report_candidates(fake, companies, existing)

    assert candidates["symbol"].tolist() == ["600001"]
    assert candidates.iloc[0]["_latest_report_date"] == pd.Timestamp("2026-09-30").date()
    assert fake.checked == ["600001.SH", "600002.SH"]


def test_cn_new_report_candidates_can_refresh_missing_fundamentals():
    class FakeTushare:
        def income(self, *, ts_code, fields, limit):
            return pd.DataFrame([{
                "ts_code": ts_code,
                "ann_date": "20261025",
                "end_date": "20260930",
            }])

    companies = pd.DataFrame([{
        "symbol": "920000",
        "ts_code": "920000.BJ",
        "_force_update": True,
    }])
    existing = pd.DataFrame([{
        "ticker": "920000",
        "report_date": "2026-09-30",
    }])

    candidates = sources._cn_new_report_candidates(
        FakeTushare(), companies, existing
    )

    assert candidates["symbol"].tolist() == ["920000"]


def test_cn_backfill_reuses_report_date_for_balance_only_records():
    class FakeTushare:
        def income(self, **kwargs):
            raise AssertionError("existing report date should avoid income lookup")

    companies = pd.DataFrame([{
        "symbol": "920000",
        "ts_code": "920000.BJ",
        "_force_update": True,
        "_needs_income": False,
    }])
    existing = pd.DataFrame([{
        "ticker": "920000",
        "report_date": "2026-09-30",
    }])

    candidates = sources._cn_new_report_candidates(
        FakeTushare(), companies, existing
    )

    assert candidates["symbol"].tolist() == ["920000"]


def test_cn_tushare_codes_cover_beijing_and_existing_exchanges():
    assert sources._cn_ts_code("920001") == "920001.BJ"
    assert sources._cn_ts_code("830799") == "830799.BJ"
    assert sources._cn_ts_code("600000") == "600000.SH"
    assert sources._cn_ts_code("000001") == "000001.SZ"
    assert sources._cn_ts_code("900901") == "900901.SH"


def test_cn_missing_fundamentals_only_selects_operating_companies():
    rows = pd.DataFrame([
        _record("COMPLETE"),
        _record("INCOMPLETE", fixed_assets=None),
        _record("BANK", industry="Banks", fixed_assets=None),
        _record("UTILITY", industry="Utilities", fixed_assets=None),
    ])

    missing = sources._cn_missing_fundamental_records(rows)

    assert missing["ticker"].tolist() == ["INCOMPLETE"]


def test_cn_balance_value_uses_latest_non_null_reported_value():
    balance = pd.DataFrame([
        {"end_date": "20260630", "fix_assets": 123.0},
        {"end_date": "20260930", "fix_assets": float("nan")},
    ])

    assert sources._cn_latest_balance_value(balance, "fix_assets") == 123.0
    assert sources._cn_latest_balance_value(balance, "missing_field") is None


def test_tushare_loader_prefers_updated_repo_env_over_stale_process_env(
    tmp_path, monkeypatch
):
    (tmp_path / ".env").write_text(
        "TUSHARE_TOKEN=new-token\nTUSHARE_API_KEY=legacy-token\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("TUSHARE_TOKEN", "stale-token")

    assert load_tushare_token(tmp_path) == "new-token"


def test_tushare_loader_falls_back_to_process_env_when_repo_env_lacks_token(
    tmp_path, monkeypatch
):
    (tmp_path / ".env").write_text("OTHER_SETTING=value\n", encoding="utf-8")
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.setenv("TUSHARE_API_KEY", "environment-token")

    assert load_tushare_token(tmp_path) == "environment-token"


def test_yahoo_fundamentals_refresh_fetches_details_only_for_new_reports(
    tmp_path, monkeypatch
):
    import yfinance as yf

    monkeypatch.setattr(store, "DB_PATH", tmp_path / "magic_formula.db")
    monkeypatch.setattr(sources.time, "sleep", lambda _: None)
    latest = pd.Timestamp("2026-09-30", tz="UTC").timestamp()
    monkeypatch.setattr(
        yf,
        "Ticker",
        lambda symbol: type("FakeTicker", (), {
            "info": {"mostRecentQuarter": latest}
        })(),
    )
    store.upsert_rows([
        {
            **_record("SAME", ebit=100, report_date="2026-09-30"),
            "market": "US",
            "quote_currency": "USD",
            "financial_currency": "USD",
            "quote_updated_at": "2026-11-01T00:00:00",
            "fundamentals_updated_at": "2026-11-01T00:00:00",
            "updated_at": "2026-11-01T00:00:00",
        },
        {
            **_record("NEW", ebit=100, report_date="2026-06-30"),
            "market": "US",
            "quote_currency": "USD",
            "financial_currency": "USD",
            "quote_updated_at": "2026-11-02T00:00:00",
            "fundamentals_updated_at": "2026-11-02T00:00:00",
            "updated_at": "2026-11-02T00:00:00",
        },
    ])
    fetched_symbols = []

    def fake_yf_record(market, quote, *, stock, info):
        fetched_symbols.append(quote["symbol"])
        return (
            {
                "report_date": "2026-09-30",
                "ebit": 222,
                "fundamentals_updated_at": "2026-11-13T00:00:00",
                "updated_at": "2026-11-13T00:00:00",
            },
            [],
        )

    monkeypatch.setattr(sources, "_yf_record", fake_yf_record)

    updated = sources.refresh_fundamentals("US")

    records = store.load_market("US").set_index("ticker")
    assert updated == 1
    assert fetched_symbols == ["NEW"]
    assert records.loc["SAME", "ebit"] == 100
    assert records.loc["NEW", "ebit"] == 222
    assert records.loc["NEW", "quote_updated_at"] == "2026-11-02T00:00:00"


def test_minimum_market_cap_is_applied_in_market_currency_units():
    rows = pd.DataFrame([
        _record("A", market_cap=4_000_000_000),
        _record("B", market_cap=6_000_000_000),
    ])

    ranked = rank_market(rows, "US", min_market_cap=5_000_000_000)

    assert ranked["ticker"].tolist() == ["B"]


def test_market_cap_filter_input_is_currency_millions():
    assert _market_cap_from_millions(5_000) == 5_000_000_000
    assert _market_cap_from_millions(250) == 250_000_000


def test_magic_formula_progress_maps_market_update_phases():
    assert _update_progress_fraction(
        "US: Yahoo Finance 股票池 5/10", "US", True
    ) == 0.05
    assert _update_progress_fraction(
        "US: 正在获取报价和财报 10/20", "US", True
    ) == 0.55
    assert _update_progress_fraction(
        "US: 正在检查财报日期 5/10", "US", True
    ) == 0.45
    assert _update_progress_fraction(
        "A股: 正在检查财报更新 1/4", "CN", True
    ) == 0.05
    assert _update_progress_fraction(
        "A股: 正在拉取近 3 个月日行情 63/63", "CN", False
    ) == 0.25
    assert _update_progress_fraction(
        "A股: 正在更新报价 5/10", "CN", True
    ) == 0.1
    assert _update_progress_fraction(
        "A股: 正在更新报价 5/10", "CN", False
    ) == 0.625
    assert round(_update_progress_fraction(
        "A股: 正在更新候选公司财报 5/10", "CN", True
    ), 2) == 0.6


def test_ebit_smoothing_uses_exact_three_or_five_annual_observations():
    records = pd.DataFrame([_record("A", ebit=999)])
    annual = pd.DataFrame([
        {"ticker": "A", "fiscal_date": f"{year}-12-31", "ebit": value}
        for year, value in ((2025, 50), (2024, 40), (2023, 30), (2022, 20), (2021, 10))
    ])

    assert apply_ebit_method(records, annual, "TTM").iloc[0]["ebit"] == 999
    assert apply_ebit_method(records, annual, "3Y 平均").iloc[0]["ebit"] == 40
    assert apply_ebit_method(records, annual, "4Y 平均").iloc[0]["ebit"] == 35
    assert apply_ebit_method(records, annual, "5Y 平均").iloc[0]["ebit"] == 30

    short_history = annual.iloc[:2]
    assert pd.isna(
        apply_ebit_method(records, short_history, "3Y 平均").iloc[0]["ebit"]
    )


def test_yfinance_ebit_uses_latest_four_quarters_and_keeps_five_annual_values():
    class FakeTicker:
        income_stmt = pd.DataFrame(
            [[50, 40, 30, 20, 10]],
            index=["EBIT"],
            columns=pd.to_datetime([
                "2025-12-31", "2024-12-31", "2023-12-31",
                "2022-12-31", "2021-12-31",
            ]),
        )
        quarterly_income_stmt = pd.DataFrame(
            [[15, 14, 13, 12, 11]],
            index=["EBIT"],
            columns=pd.to_datetime([
                "2026-06-30", "2026-03-31", "2025-12-31",
                "2025-09-30", "2025-06-30",
            ]),
        )

    ttm, report_date, annual = _annual_ebit(FakeTicker())

    assert ttm == 54
    assert report_date == "2026-06-30"
    assert len(annual) == 5


def test_yfinance_keeps_financial_history_when_income_statement_has_no_ebit():
    class FakeTicker:
        income_stmt = pd.DataFrame(
            [[150, 140, 130], [1.5, 1.4, 1.3]],
            index=["Net Income Common Stockholders", "Basic EPS"],
            columns=pd.to_datetime(["2025-12-31", "2024-12-31", "2023-12-31"]),
        )
        quarterly_income_stmt = pd.DataFrame()

    ttm, report_date, annual = _annual_ebit(FakeTicker())

    assert ttm is None
    assert report_date == "2025-12-31"
    assert annual[0]["net_income"] == 150
    assert annual[0]["eps"] == 1.5
    assert annual[0]["ebit"] is None


def test_yahoo_record_converts_financials_into_listing_currency(monkeypatch):
    import yfinance as yf

    class FakeTicker:
        info = {
            "financialCurrency": "CNY",
            "firstTradeDateEpochUtc": 1_262_304_000,
        }
        income_stmt = pd.DataFrame(
            [[100, 80]], index=["EBIT"],
            columns=pd.to_datetime(["2025-12-31", "2024-12-31"]),
        )
        quarterly_income_stmt = pd.DataFrame(
            [[10, 10, 10, 10]], index=["EBIT"],
            columns=pd.to_datetime([
                "2026-06-30", "2026-03-31", "2025-12-31", "2025-09-30",
            ]),
        )
        quarterly_balance_sheet = pd.DataFrame(
            [[500], [100], [200], [50], [200], [100]],
            index=[
                "Current Assets", "Cash And Cash Equivalents",
                "Current Liabilities", "Current Debt", "Net PPE", "Total Debt",
            ],
        )
        balance_sheet = pd.DataFrame()
        dividends = pd.Series(dtype=float)

    monkeypatch.setattr(yf, "Ticker", lambda _: FakeTicker())
    monkeypatch.setattr(
        sources, "_currency_rate_to_listing",
        lambda source, target: {
            ("CNY", "USD"): 0.14,
            ("HKD", "USD"): 0.13,
        }.get((source, target), 1.0),
    )

    record, annual = sources._yf_record("US", {
        "symbol": "ADR",
        "quoteType": "EQUITY",
        "currency": "HKD",
        "regularMarketPrice": 10,
        "marketCap": 1_000,
        "regularMarketTime": 1_767_000_000,
    })

    assert record["currency"] == "USD"
    assert record["financial_currency"] == "CNY"
    assert record["financial_fx_to_listing"] == 0.14
    assert record["quote_currency"] == "HKD"
    assert record["quote_fx_to_listing"] == 0.13
    assert abs(record["ebit"] - 5.6) < 1e-9
    assert abs(record["current_assets"] - 70) < 1e-9
    assert abs(record["price"] - 1.3) < 1e-9
    assert abs(record["market_cap"] - 130) < 1e-9
    assert abs(annual[0]["ebit"] - 14) < 1e-9


def test_us_yahoo_record_assumes_usd_when_quote_and_financial_currencies_are_missing(
    monkeypatch,
):
    import yfinance as yf

    class FakeTicker:
        info = {}
        income_stmt = pd.DataFrame()
        quarterly_income_stmt = pd.DataFrame()
        quarterly_balance_sheet = pd.DataFrame()
        balance_sheet = pd.DataFrame()
        dividends = pd.Series(dtype=float)

    monkeypatch.setattr(yf, "Ticker", lambda _: FakeTicker())
    record, _ = sources._yf_record("US", {
        "symbol": "FISV",
        "quoteType": "EQUITY",
        "regularMarketPrice": 100,
        "marketCap": 1_000,
    })

    assert record["currency"] == "USD"
    assert record["quote_currency"] == "USD"
    assert record["quote_fx_to_listing"] == 1
    assert record["financial_currency"] == "USD"
    assert record["financial_fx_to_listing"] == 1
    assert record["currency_assumption"] == "报价币种缺失→USD；财报币种缺失→USD"


def test_non_us_yahoo_record_still_requires_financial_currency(monkeypatch):
    import yfinance as yf

    class FakeTicker:
        info = {}

    monkeypatch.setattr(yf, "Ticker", lambda _: FakeTicker())

    try:
        sources._yf_record("HK", {
            "symbol": "0005.HK",
            "quoteType": "EQUITY",
            "currency": "HKD",
        })
    except RuntimeError as exc:
        assert "未提供 financialCurrency" in str(exc)
    else:
        raise AssertionError("HK records must not assume USD")


def test_currency_rate_uses_inverse_yahoo_fx_pair_when_needed(monkeypatch):
    import yfinance as yf

    requested_pairs = []

    class FakeTicker:
        def __init__(self, symbol):
            requested_pairs.append(symbol)

        def history(self, **kwargs):
            if requested_pairs[-1] == "CNYUSD=X":
                return pd.DataFrame()
            return pd.DataFrame({"Close": [7.0]})

    monkeypatch.setattr(yf, "Ticker", FakeTicker)
    sources._cached_currency_rate_to_listing.cache_clear()

    rate = sources._currency_rate_to_listing("CNY", "USD")

    assert rate == 1 / 7
    assert requested_pairs == ["CNYUSD=X", "USDCNY=X"]
    sources._cached_currency_rate_to_listing.cache_clear()


def test_currency_normalization_excludes_records_without_verified_conversion():
    rows = pd.DataFrame([
        {
            "ticker": "NORMALIZED",
            "currency": "USD",
            "quote_currency": "USD",
            "quote_fx_to_listing": 1.0,
            "financial_currency": "CNY",
            "financial_fx_to_listing": 0.14,
        },
        {"ticker": "LEGACY", "currency": "USD"},
    ])

    normalized = _currency_normalized_rows(rows, "US")

    assert normalized["ticker"].tolist() == ["NORMALIZED"]


def test_currency_normalization_assumes_cny_for_missing_cn_metadata():
    rows = pd.DataFrame([
        {
            "ticker": "CN_LEGACY",
            "currency": "CNY",
            "quote_currency": "CNY",
            "quote_fx_to_listing": 1.0,
        },
    ])

    normalized = _currency_normalized_rows(rows, "CN")

    assert normalized["ticker"].tolist() == ["CN_LEGACY"]
    assert normalized.iloc[0]["financial_currency"] == "CNY"
    assert normalized.iloc[0]["financial_fx_to_listing"] == 1.0
    assert normalized.iloc[0]["currency_assumption"] == "A股缺失币种/汇率→CNY/1"


def test_backfill_cn_currency_metadata_only_updates_missing_values(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "magic_formula.db")
    store.upsert_rows([
        {
            **_record("MISSING"),
            "market": "CN",
            "source": "test",
            "updated_at": "2026-01-01T00:00:00",
            "currency": "CNY",
            "quote_currency": "CNY",
            "quote_fx_to_listing": 1.0,
        },
        {
            **_record("PRESENT"),
            "market": "CN",
            "source": "test",
            "updated_at": "2026-01-01T00:00:00",
            "currency": "CNY",
            "quote_currency": "CNY",
            "quote_fx_to_listing": 1.0,
            "financial_currency": "CNY",
            "financial_fx_to_listing": 1.0,
        },
        {
            **_record("US_ROW"),
            "market": "US",
            "source": "test",
            "updated_at": "2026-01-01T00:00:00",
        },
    ])

    assert store.backfill_cn_currency_metadata() == 1
    rows = store.load_market("CN").set_index("ticker")
    assert rows.loc["MISSING", "financial_currency"] == "CNY"
    assert rows.loc["MISSING", "financial_fx_to_listing"] == 1.0
    assert rows.loc["MISSING", "currency_assumption"] == (
        "A股缺失币种/汇率→CNY/1"
    )
    assert pd.isna(rows.loc["PRESENT", "currency_assumption"])
    assert store.load_market("US").iloc[0]["financial_currency"] is None


def test_yahoo_quote_refresh_converts_amounts_and_preserves_financial_data(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "magic_formula.db")
    store.upsert_rows([{
        **_record("ADR", ebit=20, price=10, market_cap=100),
        "market": "US",
        "source": "yfinance",
        "updated_at": "2026-01-01T00:00:00",
        "quote_updated_at": "2026-01-01T00:00:00",
        "fundamentals_updated_at": "2026-01-01T00:00:00",
        "quote_currency": "USD",
        "quote_fx_to_listing": 1.0,
        "financial_currency": "USD",
        "financial_fx_to_listing": 1.0,
    }])
    monkeypatch.setattr(
        sources, "_yahoo_universe",
        lambda *args: [{
            "symbol": "ADR",
            "currency": "HKD",
            "regularMarketPrice": 20,
            "marketCap": 200,
            "regularMarketTime": 1_767_000_000,
        }],
    )
    monkeypatch.setattr(
        sources, "_currency_rate_to_listing", lambda source, target: 0.13
    )

    count = sources.refresh_quotes("US", store.load_market("US"))

    refreshed = store.load_market("US").iloc[0]
    assert count == 1
    assert refreshed["price"] == 2.6
    assert refreshed["market_cap"] == 26
    assert refreshed["currency"] == "USD"
    assert refreshed["quote_currency"] == "HKD"
    assert refreshed["quote_fx_to_listing"] == 0.13
    assert refreshed["ebit"] == 20
    assert refreshed["financial_fx_to_listing"] == 1.0


def test_us_quote_refresh_assumes_usd_when_quote_currency_is_missing(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(store, "DB_PATH", tmp_path / "magic_formula.db")
    store.upsert_rows([{
        **_record("FISV"),
        "market": "US",
        "source": "yfinance",
        "updated_at": "2026-01-01T00:00:00",
        "quote_updated_at": "2026-01-01T00:00:00",
        "fundamentals_updated_at": "2026-01-01T00:00:00",
        "quote_currency": "USD",
        "quote_fx_to_listing": 1.0,
        "financial_currency": "USD",
        "financial_fx_to_listing": 1.0,
    }])
    monkeypatch.setattr(
        sources, "_yahoo_universe",
        lambda *args: [{
            "symbol": "FISV",
            "regularMarketPrice": 20,
            "marketCap": 2_000,
        }],
    )
    monkeypatch.setattr(
        sources, "_currency_rate_to_listing", lambda source, target: 1.0
    )

    assert sources.refresh_quotes("US", store.load_market("US")) == 1

    refreshed = store.load_market("US").iloc[0]
    assert refreshed["price"] == 20
    assert refreshed["quote_currency"] == "USD"
    assert refreshed["currency_assumption"] == "报价币种缺失→USD"


def test_yahoo_universe_uses_region_and_minimum_market_cap(monkeypatch):
    import yfinance as yf

    requests = []

    def fake_screen(_query, **kwargs):
        assert _query is not None
        requests.append(kwargs)
        if kwargs["offset"] == 0:
            return {
                "quotes": [
                    {
                        "symbol": f"T{i}",
                        "quoteType": "EQUITY",
                        "exchange": "PNK" if i == 0 else "NMS",
                    }
                    for i in range(250)
                ],
                "total": 252,
            }
        return {
            "quotes": [
                {"symbol": "BABA", "quoteType": "EQUITY", "exchange": "NYQ"},
                {"symbol": "LAST", "quoteType": "EQUITY", "exchange": "NYQ"},
            ],
            "total": 252,
        }

    monkeypatch.setattr(yf, "screen", fake_screen)

    from magic_formula.sources import _yahoo_universe
    quotes = _yahoo_universe("US", 5_000_000_000)

    assert len(quotes) == 251
    assert all(quote["exchange"] != "PNK" for quote in quotes)
    assert any(quote["symbol"] == "BABA" for quote in quotes)
    assert [request["offset"] for request in requests] == [0, 250]


def test_us_otc_quote_filter_uses_yahoo_exchange_metadata():
    from magic_formula.sources import _is_us_otc_quote

    assert _is_us_otc_quote({"exchange": "PNK"})
    assert _is_us_otc_quote({"exchange": "OQB"})
    assert _is_us_otc_quote({"fullExchangeName": "OTC Markets OTCQX"})
    assert not _is_us_otc_quote({"exchange": "NMS"})
    assert not _is_us_otc_quote({"exchange": "NYQ"})


def test_classifies_financial_and_utility_industries_in_multiple_languages():
    assert industry_group("Banks—Regional") == "financial"
    assert industry_group("资本市场服务") == "financial"
    assert industry_group("Utilities—Regulated Electric") == "utility"
    assert industry_group("水的生产和供应业") == "utility"
    assert industry_group("Software—Infrastructure") == "operating"


def test_operating_formula_excludes_financials_and_utilities_for_special_tables():
    rows = pd.DataFrame([
        _record("OPERATING"),
        _record("BANK", industry="Banks—Regional"),
        _record("UTILITY", industry="Utilities—Regulated Electric"),
    ])

    assert rank_market(rows, "US")["ticker"].tolist() == ["OPERATING"]


def test_financial_screen_ranks_roe_and_tangible_book_value():
    rows = pd.DataFrame([
        _record(
            "BANK_A", industry="Banks—Regional", market_cap=800,
            equity=1_000, goodwill=200, roe=0.18, price_to_book=0.9,
        ),
        _record(
            "BANK_B", industry="Insurance—Life", market_cap=600,
            equity=1_000, goodwill=None, roe=0.12, price_to_book=0.6,
        ),
    ])
    annual = pd.DataFrame([
        {"ticker": "BANK_A", "fiscal_date": "2025-12-31", "net_income": 150, "equity": 1_000},
        {"ticker": "BANK_A", "fiscal_date": "2024-12-31", "net_income": 140, "equity": 1_000},
        {"ticker": "BANK_B", "fiscal_date": "2025-12-31", "net_income": 100, "equity": 1_000},
    ])

    ranked = rank_financials(rows, annual, "US")

    assert ranked["ticker"].tolist() == ["BANK_A", "BANK_B"]
    assert abs(ranked.iloc[0]["roe"] - 0.145) < 1e-12
    assert ranked.iloc[0]["valuation_metric"] == "P/TBV"
    assert ranked.iloc[0]["valuation_multiple"] == 1
    assert ranked.iloc[1]["valuation_metric"] == "P/B"
    assert ranked.iloc[1]["valuation_multiple"] == 0.6


def test_financial_screen_applies_available_cet1_and_relative_npl_gates():
    rows = pd.DataFrame([
        _record(
            "PASS", industry="Bank", equity=1_000, roe=0.12,
            cet1_ratio=0.12, npl_ratio=0.01,
        ),
        _record(
            "LOW_CET1", industry="Bank", equity=1_000, roe=0.20,
            cet1_ratio=0.10, npl_ratio=0.005,
        ),
        _record(
            "HIGH_NPL", industry="Bank", equity=1_000, roe=0.18,
            cet1_ratio=0.12, npl_ratio=0.04,
        ),
    ])

    assert rank_financials(rows, pd.DataFrame(), "US")["ticker"].tolist() == ["PASS"]


def test_utility_screen_uses_eps_cagr_and_dividend_yield_and_available_gates():
    rows = pd.DataFrame([
        _record(
            "UTILITY_A", industry="Utilities—Regulated Electric",
            eps_growth=0.01, dividend_yield=0.04, payout_ratio=0.65,
        ),
        _record(
            "UTILITY_B", industry="Gas Utility",
            eps_growth=0.02, dividend_yield=0.03, payout_ratio=0.90,
        ),
        _record(
            "UTILITY_C", industry="Water Utility",
            eps_growth=0.05, dividend_yield=0.02,
        ),
        _record(
            "UTILITY_D", industry="Water Utility",
            eps_growth=0.05, dividend_yield=0.06, ffo_to_debt=0.10,
        ),
    ])
    annual = pd.DataFrame([
        {"ticker": "UTILITY_A", "fiscal_date": f"{year}-12-31", "eps": eps}
        for year, eps in ((2025, 0.8), (2024, 0.7), (2023, 0.6), (2022, 0.5))
    ])

    ranked = rank_utilities(rows, annual, "US")

    assert ranked["ticker"].tolist() == ["UTILITY_A", "UTILITY_C"]
    assert abs(ranked.loc[ranked["ticker"] == "UTILITY_A", "eps_growth"].iloc[0] - 0.1696) < 0.001


def test_provider_timestamps_outside_windows_safe_to_ignore():
    assert sources._epoch_date(1e30) is None
    assert sources._epoch_date(1_262_304_000) == "2010-01-01"
    assert sources._epoch_date(1_262_304_000_000, milliseconds=True) == "2010-01-01"


def test_subunit_currency_quotes_are_converted_before_fx(monkeypatch):
    monkeypatch.setattr(
        sources, "_cached_currency_rate_to_listing", lambda source, target, day: 0.8
    )

    assert sources._currency_rate_to_listing("GBp", "USD") == 0.008


def test_hong_kong_universe_requires_a_hk_exchange_listing():
    assert sources._is_hk_exchange_quote({"exchange": "HKG", "symbol": "0700.HK"})
    assert sources._is_hk_exchange_quote({"exchange": "HKSE", "symbol": "0005.HK"})
    assert not sources._is_hk_exchange_quote({"exchange": "NMS", "symbol": "BABA"})
    assert not sources._is_hk_exchange_quote({"exchange": "HKG", "symbol": "0700"})
