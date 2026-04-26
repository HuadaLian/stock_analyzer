# Database Design

DuckDB 单文件数据库 `stock.db`，存储所有行情、基本面、因子、用户数据。

---

## 设计原则

- **原始数据与派生数据严格分离**：财报数字不可变；因子和估值是计算结果，随时可清空重算
- **时间点准确（Point-in-Time）**：每条基本面数据记录实际公开日期（`filing_date`），防止回测前视偏差
- **按访问模式设计表**：Streamlit UI 只读；ETL 写入市场数据；笔记和提醒由 UI 直接写入

### 读写分离（Windows：bulk 与 Streamlit 并行）

DuckDB 在 Windows 上对同一 `stock.db` 常为**单进程独占**；`etl.us_bulk_run` 长时间持有一条写连接时，其它进程用只读打开主库容易报「另一程序正在使用此文件」。

#### 1) bulk 短连接 + 可选原子快照（推荐）

`python -m etl.us_bulk_run` 支持在跑批时**周期性关闭写连接并 sleep 一小段**，留出窗口供其它进程只读打开 `stock.db`，或把主库复制为副本。实现见 `etl/us_bulk_run.py`（`_iter_batches` / `_open_conn_with_retry`）与 `etl/snapshot.py`（`.tmp` + `os.replace`，含 WAL 处理）。

| 参数 | 含义 |
|------|------|
| `--reconnect-every N` | 每处理 N 个 ticker 关闭写连接；`0`（默认）= 整条跑完才释放，与旧行为一致。日常全量建议 **50～100**；N 越小，重连开销越大。 |
| `--reconnect-pause-ms M` | 批次之间在**已关连接**后 sleep **M** 毫秒（默认 `1000`），给快照/只读查询抢锁时间。Windows 若仍抢不到锁可适当加大。 |
| `--snapshot-every K` | 每 **K 个批次**在释放窗口内把 `stock.db` 复制到 `--snapshot-path`；**必须**同时 `--reconnect-every>0`，否则该选项会被忽略。`0` = 不快照。 |
| `--snapshot-path` | 快照目标路径；**默认**为与 `stock.db` 同目录下的 `stock_read.db`。 |

示例（跑批间隙更新副本，便于 Streamlit 读快照）：

```bash
conda activate stock_analyzer
python -m etl.us_bulk_run --reconnect-every 50 --snapshot-every 1 --rate-limit-ms 400 --skip-optional
```

压力更小的本地验证（少票、短 pause）：

```bash
python -m etl.us_bulk_run --reconnect-every 5 --reconnect-pause-ms 2000 --limit 30 --skip-optional
```

#### 2) Streamlit 默认读副本（`STOCK_ANALYZER_READ_DB`）

- **`dashboards/db_status.py` 的 `bootstrap_read_replica`** 在 **`app.py` 启动早期**调用：若环境变量 **`STOCK_ANALYZER_READ_DB` 未设置**，且与 `stock.db` 同目录下已存在 **`stock_read.db`**，则自动把该变量设为该快照的绝对路径。之后所有 `get_conn(readonly=True)` 走副本。
- **用户已在 shell / 系统里设置的 `STOCK_ANALYZER_READ_DB` 永远优先**：bootstrap **不会覆盖**已有值。
- 故意只读主库时：不设变量且删掉/改名 `stock_read.db`，或显式把变量指到 `stock.db` 的绝对路径（状态条会显示「主库」语义）。

#### 3) 顶部状态条颜色（`compute_db_status`）

| 颜色 | 含义 |
|------|------|
| 绿 | 副本 `mtime` 未满 1 小时 |
| 黄 | 已满 1 小时且未满 24 小时 |
| 红 | 已满 24 小时，或副本文件不存在 |
| 灰 | 正在读 **主库**（与 bulk 共用；bulk 跑时只读可能失败） |

#### 4) 手工复制（无 bulk 时的兜底）

在 bulk **完全未占用**主库时，仍可手动：

1. `Copy-Item .\stock.db .\stock_read.db -Force`（或任意路径）。
2. `$env:STOCK_ANALYZER_READ_DB = "…\stock_read.db"` 后再 `streamlit run app.py`。

仅想复制一次、不跟 bulk 挂钩时，也可在项目里执行：`from etl.snapshot import snapshot_db; snapshot_db("stock.db", "stock_read.db")`（路径按实际 `DB_PATH` 调整）。

#### 5) 限制说明

D1 里「保存笔记」「刷新最新数据」等仍使用 **`get_conn()` 写主库 `stock.db`**；bulk 占用主库时这些操作仍可能失败。镜像 + 短连接最适合**只读看板**；要边跑 bulk 边记笔记，需后续 **Phase 3**（如笔记独立库）或停 bulk / 接受偶发失败。

#### 6) Windows 上建议自测「批次间隙能否只读」

1. 终端 A：`python -m etl.us_bulk_run --reconnect-every 5 --reconnect-pause-ms 2000 --limit 30 --skip-optional`（需 `FMP_API_KEY`）。
2. 终端 B（在 A 运行期间反复执行）：  
   `python -c "from db.schema import get_conn; print(get_conn(readonly=True).execute('SELECT COUNT(*) FROM companies').fetchone())"`  
   若在 batch 之间的窗口期能稳定成功，说明短连接 + pause 在本机有效；若始终失败，可尝试加大 `--reconnect-pause-ms`，或在关连后增加 `CHECKPOINT` 等策略（见总计划 Phase 2 备注）。

---

## 现状确认（2026-04-26）

当前数据库结构与代码库实现的对应关系如下：

- **已落地并在主流程使用（D1 / D2 / D3 / 旧版分析）**：`companies`、`ohlcv_daily`、`fundamentals_annual`、`dcf_history`、`dcf_metrics`、`fmp_dcf_history`、`notes`；D2 读取 `revenue_by_segment`、`revenue_by_geography`、`management`；D3 通过 `repository.get_industry_peers_revenue` 联查 `companies` + `fundamentals_annual`（输出含 `currency`）。
- **已建表，当前仅部分路径使用**：`price_alerts`、`fundamentals_quarterly`、`estimates`、`ohlcv_minute`
- **已建表，主要服务横截面/回测规划**：`factor_scores`
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
│                      │ 主键：ticker；含 exchange / country 等   │
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
│ management           │ 现任管理层（D2）                           │
│                      │ 主键：(ticker, title)；姓名字段 name       │
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

┌─────────────────────────────────────────────────────────────────┐
│  ETL 运维（美股全量批跑）                                         │
├──────────────────────┬──────────────────────────────────────────┤
│ etl_us_bulk_state    │ 美股 bulk 进度：ticker / status / step   │
│                      │ status: done \| failed \| skipped \| …   │
└──────────────────────┴──────────────────────────────────────────┘
```

---

## 各表字段详情

### companies

| 字段 | 类型 | 说明 | FMP 来源 |
|------|------|------|----------|
| `ticker` | VARCHAR | 主键，股票代码 | `/profile` |
| `market` | VARCHAR | 'US' \| 'CN' \| 'HK' | 手动标注 |
| `name` | VARCHAR | 公司全名 | `/profile` |
| `exchange` | VARCHAR | 交易所短码，如 'NASDAQ'、'NYSE' | `exchangeShortName` / `exchange` |
| `exchange_full_name` | VARCHAR | 交易所全称（可空） | `exchangeFullName` / `fullExchangeName` |
| `country` | VARCHAR | 公司所属国家/地区（可空） | `country` |
| `sector` | VARCHAR | 板块，如 'Technology' | `/profile` |
| `industry` | VARCHAR | 行业，如 'Semiconductors' | `/profile` |
| `currency` | VARCHAR | 与日线/OHLCV 一致的报价货币（项目约定：与 K 线货币对齐），如 'USD'、'HKD'；来自 `/profile` 的 `currency` 字段并大写归一 |
| `description` | TEXT | 公司业务描述（英文长文） | `/profile` |
| `shares_out` | DOUBLE | 最新流通股数（百万） | `/profile` |
| `updated_at` | TIMESTAMP | 最后更新时间 | 本地写入 |

---

### fundamentals_annual

每家公司每个财年一行，对应 10-K 年报。字段来自 FMP 多个接口合并：`/income-statement`、`/cash-flow-statement`、`/balance-sheet-statement`、`/key-metrics`（均加 `?period=annual`）。ETL 将**金额类字段**按 `reportedCurrency` 与财年日 FX 换算后以 **USD、百万** 写入 `revenue`、`fcf` 等；原始报告货币与汇率保留在 `reporting_currency`、`fx_to_usd`。

| 字段 | 类型 | ROIC.ai 对应名称 | FMP / 说明 |
|------|------|-----------------|------------|
| `ticker` | VARCHAR | 主键 | — |
| `fiscal_year` | INTEGER | 主键，如 2024 | — |
| `fiscal_end_date` | DATE | 财年结束日 | `date` |
| `filing_date` | DATE | **实际公开日（防前视偏差）** | `fillingDate` |
| `currency` | VARCHAR | 归一后存储货币，当前为 **USD**（与「百万美元」口径一致） | 写入常量 / 归一结果 |
| `reporting_currency` | VARCHAR | FMP 原始 `reportedCurrency`（如 ADR/多地上市为 CNY 等） | `reportedCurrency` |
| `fx_to_usd` | DOUBLE | 财年结束日附近 **1 reporting_currency = N USD**（已为 USD 则为 1.0） | 由 `fetch_fx_to_usd` 计算 |
| `interest_expense` | DOUBLE | 利息费用（百万 USD，与上同口径） | `/income-statement`（若 ETL 拉取） |
| `revenue` | DOUBLE | Sales/Revenue/Turnover（百万 **USD**） | `revenue` × 换算 |
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
| `ticker` | VARCHAR | 与 `title` 组成主键 |
| `name` | VARCHAR | 姓名 |
| `title` | VARCHAR | 职位，如 'CEO'、'CFO' |
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
| `latest_price` | DOUBLE | 最新 `adj_close`（与日线一致） |
| `latest_price_date` | DATE | 上述价格的交易日 |
| `short_potential` | DOUBLE | 相对 DCF 34x 的上行空间指标（派生） |
| `invest_potential` | DOUBLE | 相对 DCF 区间的投资潜力指标（派生） |
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
| ETL 管道 | `companies` · `fundamentals_annual` · `fundamentals_quarterly` · `estimates`（consensus）· `revenue_by_segment` · `revenue_by_geography` · `management` · `ohlcv_daily` · `ohlcv_minute` · `factor_scores` · `dcf_metrics` · `dcf_history` · `fmp_dcf_history` · `etl_us_bulk_state`（美股 bulk 进度） |
| Streamlit UI | `notes` · `price_alerts` · `estimates`（self） |
| 回测引擎 | `backtest_runs` |

---

## 文件

| 文件 | 作用 |
|------|------|
| `schema.py` | 所有建表 DDL + DuckDB 连接工厂 |
| `repository.py` | 所有只读查询函数，UI 层唯一数据入口；含 `get_industry_peers_revenue(ticker)`：返回同业 `sector`+`industry` 的 `ticker, name, sector, industry, currency`（挂牌）、`fiscal_year, revenue, fund_currency, reporting_currency, market_cap`。D3 散点图：**收入**按 `fund_currency`→USD（库内多为已归一 USD）；**市值**按挂牌 `currency`→USD，并做 ISO 币种与 FMP 汇率可用性检查。 |
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
