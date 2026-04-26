"""get_industry_peers_revenue 返回列与最新财年逻辑。"""

from db.repository import get_industry_peers_revenue


def test_get_industry_peers_revenue_columns_and_latest_year(in_memory_db, monkeypatch):
    monkeypatch.setattr("db.repository.get_conn", lambda readonly=True: in_memory_db)
    in_memory_db.execute(
        """
        INSERT INTO companies (
            ticker, market, name, exchange, exchange_full_name, country,
            sector, industry, currency, description, shares_out, updated_at
        ) VALUES
        ('NVDA', 'US', 'NVIDIA', 'NASDAQ', 'NASDAQ', 'US', 'Technology', 'Semiconductors', 'USD', 'x', 100.0, NULL),
        ('AMD', 'US', 'AMD', 'NASDAQ', 'NASDAQ', 'US', 'Technology', 'Semiconductors', 'USD', 'x', 50.0, NULL)
        """
    )
    for t, fy, rev in [
        ("NVDA", 2022, 10.0),
        ("NVDA", 2024, 130.0),
        ("AMD", 2023, 20.0),
        ("AMD", 2024, 25.0),
    ]:
        in_memory_db.execute(
            """
            INSERT INTO fundamentals_annual (
                ticker, fiscal_year, fiscal_end_date, filing_date, currency,
                reporting_currency, fx_to_usd, revenue, fcf, fcf_per_share,
                shares_out, source
            ) VALUES (?, ?, '2024-12-31', '2025-01-15', 'USD', 'USD', 1.0,
                      ?, 1.0, 1.0, 100.0, 'fmp')
            """,
            [t, fy, rev],
        )

    in_memory_db.execute(
        """
        INSERT INTO ohlcv_daily (ticker, date, open, high, low, close, volume, adj_close, market_cap)
        VALUES
        ('NVDA', '2025-01-10', 100, 101, 99, 100, 1000, 100, 3000.0),
        ('AMD', '2025-01-10', 50, 51, 49, 50, 1000, 50, 500.0)
        """
    )

    df = get_industry_peers_revenue("NVDA")
    assert set(df.columns) >= {
        "ticker",
        "name",
        "sector",
        "industry",
        "currency",
        "fiscal_year",
        "revenue",
        "market_cap",
        "fund_currency",
        "reporting_currency",
    }
    nv = df[df["ticker"] == "NVDA"].iloc[0]
    assert int(nv["fiscal_year"]) == 2024
    assert float(nv["revenue"]) == 130.0
    amd = df[df["ticker"] == "AMD"].iloc[0]
    assert float(amd["revenue"]) == 25.0
    assert float(nv["market_cap"]) == 3000.0
    assert float(amd["market_cap"]) == 500.0
