"""run_us_ticker respects USRunOptions.skip_optional (mocked FMP)."""


def test_run_us_ticker_skip_optional_skips_steps_9_to_12(in_memory_db, monkeypatch):
    from etl.pipeline import USRunOptions, run_us_ticker

    calls: list[str] = []

    profile = {
        "ticker": "ZZZ",
        "market": "US",
        "name": "Z Co",
        "exchange": "NASDAQ",
        "exchange_full_name": None,
        "country": "US",
        "sector": "Tech",
        "industry": "Software",
        "currency": "USD",
        "description": "x",
        "shares_out": 1.0,
        "_shares_out_raw": 1e6,
        "_market_cap": 1e9,
        "_is_etf": False,
        "_is_fund": False,
    }

    monkeypatch.setattr("etl.pipeline.fetch_profile", lambda t: profile)
    monkeypatch.setattr("etl.pipeline.upsert_company", lambda c, d: calls.append("upsert_company"))
    monkeypatch.setattr("etl.pipeline.fetch_fcf_annual", lambda *a, **k: [])
    monkeypatch.setattr("etl.pipeline.upsert_fundamentals_annual", lambda *a, **k: calls.append("fcf"))
    monkeypatch.setattr("etl.pipeline.fetch_income_statement_annual", lambda t: [])
    monkeypatch.setattr("etl.pipeline.upsert_income_statement_annual", lambda *a, **k: calls.append("income"))
    monkeypatch.setattr("etl.pipeline.fetch_ohlcv", lambda *a, **k: [])
    monkeypatch.setattr("etl.pipeline.upsert_ohlcv_daily", lambda *a, **k: calls.append("ohlcv"))
    monkeypatch.setattr("etl.pipeline.load_fmp_dcf_history", lambda *a, **k: 0)
    monkeypatch.setattr("etl.pipeline.compute_ema", lambda *a, **k: 0)
    monkeypatch.setattr("etl.pipeline.compute_dcf_history", lambda *a, **k: 0)
    monkeypatch.setattr("etl.pipeline.compute_dcf_lines", lambda *a, **k: None)

    def _no_mgmt(*_a, **_k):
        raise AssertionError("fetch_management should not run when skip_optional")

    monkeypatch.setattr("etl.pipeline.fetch_management", _no_mgmt)

    run_us_ticker(in_memory_db, "ZZZ", USRunOptions(skip_optional=True, verbose=False))

    assert calls == ["upsert_company", "fcf", "income", "ohlcv"]
