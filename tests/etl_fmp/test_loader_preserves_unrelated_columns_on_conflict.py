"""
测试目标：upsert_fundamentals_annual 在主键冲突时只更新 FCF 相关列，
不会清空已被后续阶段填充的字段 (revenue / roic / eps 等)。

意义：项目按阶段填充 fundamentals_annual ——
  阶段 2 写 FCF → 阶段 3 写损益表 → 阶段 4 写 ROIC 等。
  如果阶段 2 重跑时把 revenue 覆盖成 NULL，会破坏后续分析。
  loader.py 的 ON CONFLICT DO UPDATE 子句严格只列 FCF 列，本测试守住这条边界。
"""


def test_repeated_fcf_upsert_preserves_revenue_and_roic(in_memory_db):
    # 1. 先模拟阶段 3 / 阶段 4 已经填好的一行
    in_memory_db.execute("""
        INSERT INTO fundamentals_annual
            (ticker, fiscal_year, fiscal_end_date, filing_date, currency,
             revenue, gross_profit, net_income, eps, roic,
             fcf, fcf_per_share, shares_out, source)
        VALUES
            ('NVDA', 2024, '2024-01-26', '2024-03-05', 'USD',
             60_922, 50_000, 30_000, 1.20, 0.45,
             58_000, 2.36, 24_400, 'fmp')
    """)

    # 2. 阶段 2 重跑：只更新 FCF 列
    from etl.loader import upsert_fundamentals_annual
    base = {col: None for col in [
        "revenue", "revenue_per_share", "gross_profit", "gross_margin",
        "operating_income", "operating_margin", "net_income", "profit_margin",
        "eps", "depreciation", "effective_tax_rate", "dividend_per_share",
        "total_equity", "long_term_debt", "working_capital",
        "book_value_per_share", "tangible_bv_per_share",
        "roic", "return_on_capital", "return_on_equity",
    ]}
    base.update({
        "ticker": "NVDA", "fiscal_year": 2024,
        "fiscal_end_date": "2024-01-26", "filing_date": "2024-03-05",
        "currency": "USD",
        "reporting_currency": "USD", "fx_to_usd": 1.0,
        "fcf": 60_922.0,                 # 新值
        "fcf_per_share": 2.48,           # 新值
        "shares_out": 24_500.0,          # 新值
        "source": "fmp",
    })
    upsert_fundamentals_annual(in_memory_db, [base])

    # 3. 断言：FCF 已更新；revenue / roic / eps 等仍为原值，未被 NULL 覆盖
    row = in_memory_db.execute("""
        SELECT fcf, fcf_per_share, shares_out,
               revenue, gross_profit, net_income, eps, roic
        FROM fundamentals_annual WHERE ticker='NVDA' AND fiscal_year=2024
    """).fetchone()

    fcf, fcf_ps, shares, revenue, gp, ni, eps, roic = row
    # FCF 列被刷新
    assert fcf == 60_922.0
    assert fcf_ps == 2.48
    assert shares == 24_500.0
    # 其他列保留 (绝不能被覆盖成 None)
    assert revenue == 60_922.0
    assert gp      == 50_000.0
    assert ni      == 30_000.0
    assert eps     == 1.20
    assert roic    == 0.45
