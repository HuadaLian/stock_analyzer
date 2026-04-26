# Database Design

DuckDB 单文件数据库 `stock.db`，存储所有行情、基本面、因子、用户数据。

---

## 设计原则

- **原始数据与派生数据严格分离**：财报数字不可变；因子和估值是计算结果，随时可清空重算
- **时间点准确（Point-in-Time）**：每条基本面数据记录实际公开日期（`filing_date`），防止回测前视偏差
- **按访问模式设计表**：Streamlit UI 只读；ETL 写入市场数据；笔记和提醒由 UI 直接写入

---

## 现状确认（2026-04-25）

当前数据库结构与代码库实现的对应关系如下：

- **已落地并在主流程使用（D1 / 旧版分析）**：`companies`、`ohlcv_daily`、`fundamentals_annual`、`dcf_history`、`dcf_metrics`、`fmp_dcf_history`、`notes`
- **已建表，当前仅部分路径使用**：`price_alerts`、`fundamentals_quarterly`、`estimates`、`ohlcv_minute`
- **已建表，等待新版看板接入**：`revenue_by_segment`、`revenue_by_geography`、`management`、`factor_scores`
- **已建表，等待回测模块重构后统一接入**：`backtest_runs`

补充说明：

- D1 的投研笔记当前采用“**每个 ticker 一条主记录**”的写入策略：`id = d1_note_{TICKER}`。
- `raw_text` 采用追加写入（append-only 语义），`markdown` 存储最近一次 LLM 整理结果。
- 这与表结构并不冲突（`id` 仍是通用 VARCHAR 主键），后续可按需要扩展为多条笔记版本。

---

## 表结构总览

```
┌─────────────────────────────────────────────────────────────────┐
│                        原始数据层                                │
│           （历史事实，ETL 写入，UI 只读，不可修改）               │
├──────────────────────┬──────────────────────────────────────────┤
│ companies            │ 公司元数据，每家公司一行                   │
│                      │ 主键：ticker                              │
├──────────────────────┼──────────────────────────────────────────┤
│ fundamentals_annual  │ 年报数据（10-K），每家公司每年一行          │
│                      │ 主键：ticker + fiscal_year               │
│                      │ 关键额外字段：filing_date（实际公开日）    │
├──────────────────────┼──────────────────────────────────────────┤
│ fundamentals_        │ 季报数据（10-Q），每家公司每季一行          │
│ quarterly            │ 主键：ticker + fiscal_year + quarter     │
│                      │ 关键额外字段：filing_date                 │
├──────────────────────┼──────────────────────────────────────────┤
│ estimates            │ 前向预测，分析师共识或自填                  │
│                      │ 主键：ticker + fiscal_year + quarter     │
│                      │       + source + published_at            │
│                      │ source：'consensus' | 'self'             │
├──────────────────────┼──────────────────────────────────────────┤
│ revenue_by_segment   │ 收入按业务板块拆分（Dashboard 2）          │
│                      │ 主键：ticker + fiscal_year + segment     │
├──────────────────────┼──────────────────────────────────────────┤
│ revenue_by_geography │ 收入按地区拆分（Dashboard 2）              │
│                      │ 主键：ticker + fiscal_year + region      │
├──────────────────────┼──────────────────────────────────────────┤
│ management           │ 现任管理层（Dashboard 3）                  │
│                      │ 主键：ticker + title                     │
├──────────────────────┼──────────────────────────────────────────┤
│ ohlcv_daily          │ 日K线（前复权），每家公司每天一行            │
│                      │ 主键：ticker + date                      │
│                      │ 含 market_cap + ema10/ema250（D1）       │
├──────────────────────┼──────────────────────────────────────────┤
│ ohlcv_minute         │ 分钟K（1m/5m/15m/30m/1h），按需拉取        │
│                      │ 主键：ticker + ts + resolution           │
└──────────────────────┴──────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                        Tick 数据层                               │
│               （Parquet 文件，DuckDB 视图查询）                   │
├──────────────────────┬──────────────────────────────────────────┤
│ ticks（视图）         │ 指向 ticks/NVDA/2026-04-23.parquet       │
│                      │ 按需拉取，用完可删，不常驻 DuckDB           │
└──────────────────────┴──────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                        派生数据层                                │
│         （计算结果，ETL 结束后写入，可随时清空重算）               │
├──────────────────────┬──────────────────────────────────────────┤
│ factor_scores        │ 每个再平衡日的因子快照，回测回放用           │
│                      │ 主键：ticker + as_of_date                │
│                      │ 含：invest_score、short_score、           │
│                      │     roic_percentile、fcf_growth 等        │
├──────────────────────┼──────────────────────────────────────────┤
│ dcf_metrics          │ DCF 估值线（14x/24x/34x），最新一次        │
│                      │ 主键：ticker                              │
├──────────────────────┼──────────────────────────────────────────┤
│ dcf_history          │ 每财年一行的 DCF 阶梯线历史（D1）          │
│                      │ 主键：ticker + fiscal_year               │
├──────────────────────┼──────────────────────────────────────────┤
│ fmp_dcf_history      │ FMP 每日内在价值历史线（D1 参照）          │
│                      │ 主键：ticker + date                      │
└──────────────────────┴──────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                        用户数据层                                │
│              （用户生成，UI 直接写入，不经 ETL）                  │
├──────────────────────┬──────────────────────────────────────────┤
│ notes                │ 投资笔记，raw_text + LLM 整理后 markdown  │
│                      │ 主键：id（VARCHAR；D1 当前用 d1_note_{TICKER}） │
│                      │ ticker 可为 NULL（全局笔记）               │
├──────────────────────┼──────────────────────────────────────────┤
│ price_alerts         │ 价格提醒阈值，通过 moomoo OpenD 推送       │
│                      │ 主键：id（UUID）                          │
└──────────────────────┴──────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                        回测结果层                                │
├──────────────────────┬──────────────────────────────────────────┤
│ backtest_runs        │ 每次回测的参数和指标存档                    │
│                      │ 主键：run_id（UUID）                      │
│                      │ params 和 metrics 存为 JSON               │
└──────────────────────┴──────────────────────────────────────────┘
```

---

## 各表字段详情

### companies

| 字段 | 类型 | 说明 | FMP 来源 |
|------|------|------|----------|
| `ticker` | VARCHAR | 主键，股票代码 | `/stock/list` |
| `market` | VARCHAR | 'US' \| 'CN' \| 'HK' | 手动标注 |
| `name` | VARCHAR | 公司全名 | `/profile` |
| `exchange` | VARCHAR | 交易所，如 'NASDAQ' \| 'NYSE' | `/profile` |
| `sector` | VARCHAR | 板块，如 'Technology' | `/profile` |
| `industry` | VARCHAR | 行业，如 'Semiconductors' | `/profile` |
| `currency` | VARCHAR | 报告货币，如 'USD' \| 'CNY' | `/profile` |
| `description` | TEXT | 公司业务描述（英文长文） | `/profile` |
| `shares_out` | DOUBLE | 最新流通股数（百万） | `/profile` |
| `updated_at` | TIMESTAMP | 最后更新时间 | 本地写入 |

---

### fundamentals_annual

每家公司每个财年一行，对应 10-K 年报。字段来自 FMP 三个接口合并：`/income-statement`、`/cash-flow-statement`、`/key-metrics`（均加 `?period=annual`）。

| 字段 | 类型 | ROIC.ai 对应名称 | FMP 字段名 |
|------|------|-----------------|------------|
| `ticker` | VARCHAR | 主键 | — |
| `fiscal_year` | INTEGER | 主键，如 2024 | — |
| `fiscal_end_date` | DATE | 财年结束日 | `date` |
| `filing_date` | DATE | **实际公开日（防前视偏差）** | `fillingDate` |
| `currency` | VARCHAR | 报告货币 | `reportedCurrency` |
| `revenue` | DOUBLE | Sales/Revenue/Turnover（百万） | `revenue` |
| `revenue_per_share` | DOUBLE | Revenue per Share | `revenuePerShare` |
| `gross_profit` | DOUBLE | — | `grossProfit` |
| `gross_margin` | DOUBLE | Gross Margin %（0-1） | `grossProfitRatio` |
| `operating_income` | DOUBLE | — | `operatingIncome` |
| `operating_margin` | DOUBLE | Operating Margin %（0-1） | `operatingIncomeRatio` |
| `net_income` | DOUBLE | Net Income, GAAP（百万） | `netIncome` |
| `profit_margin` | DOUBLE | Profit Margin %（0-1） | `netIncomeRatio` |
| `eps` | DOUBLE | Basic EPS, GAAP | `eps` |
| `depreciation` | DOUBLE | Depreciation Expense（百万） | `depreciationAndAmortization` |
| `effective_tax_rate` | DOUBLE | Effective Tax Rate %（0-1） | `effectiveTaxRate` |
| `fcf` | DOUBLE | Free Cash Flow（百万） | `freeCashFlow` |
| `fcf_per_share` | DOUBLE | Free Cash Flow per Share | `freeCashFlowPerShare` |
| `dividend_per_share` | DOUBLE | Dividend per Share | `dividendPerShare` |
| `shares_out` | DOUBLE | Basic Weighted Avg Shares（百万） | `weightedAverageShsOut` |
| `book_value_per_share` | DOUBLE | Book Value per Share | `bookValuePerShare` |
| `tangible_bv_per_share` | DOUBLE | Tangible Book Value per Share | `tangibleBookValuePerShare` |
| `total_equity` | DOUBLE | Total Equity（百万） | `totalStockholdersEquity` |
| `long_term_debt` | DOUBLE | LT Debt（百万） | `longTermDebt` |
| `working_capital` | DOUBLE | Working Capital（百万） | `totalCurrentAssets - totalCurrentLiabilities` |
| `roic` | DOUBLE | Return on Invested Capital %（0-1） | `roic` |
| `return_on_capital` | DOUBLE | Return on Capital %（0-1） | `returnOnCapitalEmployed` |
| `return_on_equity` | DOUBLE | Return on Common Equity %（0-1） | `roe` |
| `source` | VARCHAR | 数据来源 | 'fmp' \| 'llm' |

---

### fundamentals_quarterly

结构与 `fundamentals_annual` 基本一致，增加 `quarter` 字段，用于 TTM 计算和最新季度展示。FMP 接口加 `?period=quarter`。

| 字段 | 类型 | 说明 |
|------|------|------|
| `ticker` | VARCHAR | 主键 |
| `fiscal_year` | INTEGER | 主键 |
| `quarter` | INTEGER | 主键，1-4 |
| `period_end` | DATE | 季度结束日 |
| `filing_date` | DATE | 实际公开日 |
| `currency` | VARCHAR | — |
| `revenue` | DOUBLE | — |
| `gross_profit` | DOUBLE | — |
| `gross_margin` | DOUBLE | — |
| `operating_income` | DOUBLE | — |
| `net_income` | DOUBLE | — |
| `eps` | DOUBLE | — |
| `fcf` | DOUBLE | — |
| `shares_out` | DOUBLE | — |

> TTM = 最近四个季度 `revenue` / `fcf` / `net_income` 加总，在 `repository.py` 实时计算，不单独存储。

---

### estimates

分析师共识预测或自填判断，同一季度允许多个时间版本共存。

| 字段 | 类型 | 说明 |
|------|------|------|
| `ticker` | VARCHAR | 主键 |
| `fiscal_year` | INTEGER | 主键 |
| `quarter` | INTEGER | 主键，NULL 表示全年预测 |
| `source` | VARCHAR | 主键，'consensus' \| 'self' |
| `published_at` | DATE | 主键，预测发布日期 |
| `revenue` | DOUBLE | 预测收入（百万） |
| `eps` | DOUBLE | 预测 EPS |
| `fcf` | DOUBLE | 预测 FCF（百万） |
| `net_income` | DOUBLE | 预测净利润（百万） |
| `note` | TEXT | 备注（自填时使用） |

---

### ohlcv_daily

| 字段 | 类型 | 说明 |
|------|------|------|
| `ticker` | VARCHAR | 主键 |
| `date` | DATE | 主键 |
| `open` | DOUBLE | 开盘价 |
| `high` | DOUBLE | 最高价 |
| `low` | DOUBLE | 最低价 |
| `close` | DOUBLE | 收盘价（未复权） |
| `adj_close` | DOUBLE | 前复权收盘价（DCF 估值线用此列） |
| `volume` | BIGINT | 成交量 |
| `market_cap` | DOUBLE | 当日市值（百万），= adj_close × shares_out，ETL 预算 |
| `ema10` | DOUBLE | 基于 `adj_close` 的 10 日 EMA（`adjust=False`） |
| `ema250` | DOUBLE | 基于 `adj_close` 的 250 日 EMA（`adjust=False`） |

---

### ohlcv_minute

| 字段 | 类型 | 说明 |
|------|------|------|
| `ticker` | VARCHAR | 主键 |
| `ts` | TIMESTAMP | 主键，精确到分钟 |
| `resolution` | VARCHAR | 主键，'1m' \| '5m' \| '15m' \| '30m' \| '1h' |
| `open` | DOUBLE | — |
| `high` | DOUBLE | — |
| `low` | DOUBLE | — |
| `close` | DOUBLE | — |
| `volume` | BIGINT | — |

---

### revenue_by_segment / revenue_by_geography

| 字段 | 类型 | 说明 |
|------|------|------|
| `ticker` | VARCHAR | 主键 |
| `fiscal_year` | INTEGER | 主键 |
| `segment` / `region` | VARCHAR | 主键，如 'Data Center' / 'United States' |
| `revenue` | DOUBLE | 该项收入（百万） |
| `pct` | DOUBLE | 占总收入比例（0-1） |

---

### management

| 字段 | 类型 | 说明 |
|------|------|------|
| `ticker` | VARCHAR | 主键 |
| `title` | VARCHAR | 主键，'CEO' \| 'CFO' \| 'COO' \| 'CTO' |
| `name` | VARCHAR | 姓名 |
| `updated_at` | DATE | 数据更新日期 |

---

### factor_scores

每个再平衡日的因子快照，主键含 `as_of_date`，支持回测历史回放。

| 字段 | 类型 | 说明 |
|------|------|------|
| `ticker` | VARCHAR | 主键 |
| `as_of_date` | DATE | 主键，计算日期（通常为再平衡日） |
| `invest_score` | DOUBLE | 投资潜力综合分（0-100） |
| `short_score` | DOUBLE | 做空潜力综合分（0-100） |
| `roic_percentile` | DOUBLE | ROIC 在同行业的百分位（0-1） |
| `fcf_growth_3yr` | DOUBLE | FCF 三年复合增长率 |
| `revenue_growth_3yr` | DOUBLE | 收入三年复合增长率 |
| `gross_margin_percentile` | DOUBLE | 毛利率在同行业的百分位（0-1） |
| `pfcf_vs_history` | DOUBLE | 当前 P/FCF 相对历史均值的偏离倍数 |
| `computed_at` | TIMESTAMP | 实际计算时间 |

---

### dcf_metrics

| 字段 | 类型 | 说明 |
|------|------|------|
| `ticker` | VARCHAR | 主键 |
| `fcf_per_share_avg3yr` | DOUBLE | 最近 3 年每股 FCF 滚动均值 |
| `dcf_14x` | DOUBLE | 估值线下轨（保守） |
| `dcf_24x` | DOUBLE | 估值线中轨 |
| `dcf_34x` | DOUBLE | 估值线上轨（乐观） |
| `computed_at` | TIMESTAMP | 计算时间 |

---

### dcf_history

| 字段 | 类型 | 说明 |
|------|------|------|
| `ticker` | VARCHAR | 主键 |
| `fiscal_year` | INTEGER | 主键 |
| `anchor_date` | DATE | `filing_date` 对齐到最近交易日（用于图上 x 坐标） |
| `fcf_ps_avg3yr` | DOUBLE | 3 年滚动每股 FCF；若 <=0 用 5 年窗口 fallback |
| `dcf_14x` | DOUBLE | 14x 阶梯线 |
| `dcf_24x` | DOUBLE | 24x 阶梯线 |
| `dcf_34x` | DOUBLE | 34x 阶梯线 |

---

### fmp_dcf_history

| 字段 | 类型 | 说明 |
|------|------|------|
| `ticker` | VARCHAR | 主键 |
| `date` | DATE | 主键 |
| `dcf_value` | DOUBLE | FMP 内在价值 |
| `stock_price` | DOUBLE | FMP 同日股价（审计用） |

---

### notes

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | VARCHAR | 主键；当前 D1 约定为 `d1_note_{TICKER}` |
| `ticker` | VARCHAR | 关联股票，NULL 表示全局笔记 |
| `raw_text` | TEXT | 用户原始输入，先于 LLM 调用写入 |
| `markdown` | TEXT | LLM 整理后的 Markdown，可为 NULL（LLM 失败时） |
| `created_at` | TIMESTAMP | — |
| `updated_at` | TIMESTAMP | LLM 回写时更新 |

---

### price_alerts

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | VARCHAR | 主键，UUID |
| `ticker` | VARCHAR | — |
| `direction` | VARCHAR | 'above' \| 'below' |
| `price` | DOUBLE | 触发价格 |
| `note` | VARCHAR | 备注 |
| `active` | BOOLEAN | 是否启用 |
| `created_at` | TIMESTAMP | — |

---

### backtest_runs

| 字段 | 类型 | 说明 |
|------|------|------|
| `run_id` | VARCHAR | 主键，UUID |
| `strategy` | VARCHAR | 策略名称 |
| `tickers` | VARCHAR[] | 回测股票池 |
| `resolution` | VARCHAR | '1m' \| '1d' 等 |
| `start_date` | DATE | — |
| `end_date` | DATE | — |
| `params` | JSON | 策略参数，如窗口期、阈值 |
| `metrics` | JSON | 回测结果：sharpe、max_drawdown、total_return、win_rate 等 |
| `created_at` | TIMESTAMP | — |

---

## 关键设计决策

### filing_date 防止前视偏差

`fundamentals_annual` 和 `fundamentals_quarterly` 都记录 `filing_date`（报告实际公开日期，不是财年结束日期）。回测查询基本面数据时必须加条件 `filing_date <= 回测日期`，确保只用当时市场上真实能看到的数字。

```
时间线示例（NVDA 2024 年度数据）：
  fiscal_end_date = 2025-01-26   ← 财年结束
  filing_date     = 2025-03-05   ← 10-K 实际提交 SEC，这天之后才能用
```

### factor_scores 保留历史快照

因子分数不只存最新一行，主键包含 `as_of_date`。每次再平衡日计算完存一行，回测时可完整还原每个时间点选了哪些股票及当时的因子排名。

### TTM 不单独存

过去十二个月合计（TTM）由最近四个季报加总得到，在 `repository.py` 里实时计算。原始季报数据是事实，TTM 是派生结果，不存入数据库。

### 预测数据独立成表

`estimates` 表和 `fundamentals` 严格分开：
- 同一季度在不同时间点会有不同预测版本，主键里包含 `published_at`
- 来源区分：分析师共识（`source='consensus'`）或自填（`source='self'`）
- 季报实际公布后进入 `fundamentals_quarterly`，`estimates` 对应行保留，用于事后复盘预测准确度

### Tick 数据不入主库

Tick 数据体积过大（活跃股每天数万条），且只在回测特定策略时才需要。按需拉取存为 Parquet 文件，DuckDB 通过视图透明查询，用完可删。

```
ticks/
  NVDA/
    2026-04-22.parquet
    2026-04-23.parquet
  AAPL/
    2026-04-23.parquet
```

```sql
-- 定义一次，之后当普通表查
CREATE VIEW ticks AS
  SELECT * FROM read_parquet('ticks/*/*.parquet');
```

### market_cap 预算进 ohlcv_daily

市值 = 收盘价 × 流通股数，ETL 写入日K时顺手计算存入。回测做市值过滤时直接 `WHERE market_cap > X`，不用每次实时计算。

---

## 数据写入权限

| 写入方 | 可写的表 |
|--------|----------|
| ETL 管道 | `companies` · `fundamentals_annual` · `fundamentals_quarterly` · `estimates`（consensus）· `revenue_by_segment` · `revenue_by_geography` · `management` · `ohlcv_daily` · `ohlcv_minute` · `factor_scores` · `dcf_metrics` · `dcf_history` · `fmp_dcf_history` |
| Streamlit UI | `notes` · `price_alerts` · `estimates`（self） |
| 回测引擎 | `backtest_runs` |

---

## 文件

| 文件 | 作用 |
|------|------|
| `schema.py` | 所有建表 DDL + DuckDB 连接工厂 |
| `repository.py` | 所有只读查询函数，UI 层唯一数据入口 |
| `checks.py` | 数据完整度检查（单票/全量），用于 D1 上线前核对 |

---

## 完整度检查

命令行检查单票（建议在启动 UI 前执行）：

```bash
python -m db.checks --ticker NVDA
```

检查库内所有 ticker：

```bash
python -m db.checks --all
```

输出 JSON：

```bash
python -m db.checks --ticker NVDA --json
```
