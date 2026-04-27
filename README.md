# Stock Analyzer

基于 Streamlit + DuckDB 的本地化投研分析平台。代码库当前以**美股、数据库入库、预计算后再展示**为主线；A 股 / 港股与「旧版三市场分析流」仍并行保留，便于迁移期对照与回退。

**核心执行方式（推荐以此理解仓库）**：围绕**数据库**、**预计算**、**前端 UI** 三块协作——原始与派生数据进库，ETL/计算任务写库，Streamlit 经 `db/repository` 等只读取数渲染。

---

## 核心架构：数据库、预计算、前端 UI

| 支柱 | 职责 | 主要落点 |
|------|------|----------|
| **数据库** | DuckDB 单文件事实库；分层（原始 / 派生 / 用户 / 回测与运维表）；读写路径、副本与锁策略 | `db/schema.py`（建表与连接）、`db/repository.py`（UI 侧只读查询入口）、`db/checks.py`；主库 `stock.db`、可选只读副本 `stock_read.db`、环境变量 `STOCK_ANALYZER_READ_DB`。设计细节见 [db/README.md](db/README.md) |
| **预计算** | K 线衍生（如 EMA）、DCF 阶梯与指标、FMP DCF 历史等**入库**后再给看板，避免每次全量现算 | `etl/compute.py`、`etl/loader.py`、`etl/pipeline.py`、`etl_run.py`；美股全量 `etl/us_bulk_run.py`、`etl/us_bulk_watch.py`；快照与短连接 `etl/snapshot.py`；FMP 拉取 `etl/sources/fmp.py`、`fmp_dcf.py`；辅助 `etl/us_ticker_bundle.py`、`etl/us_run_options.py`、`etl/dotenv_local.py` |
| **前端 UI** | Streamlit：D1/D2/D3、库连接状态、数据库质量页 | `app.py`；`dashboards/d1_fcf_multiple.py`、`d2_business.py`、`d3_industry.py`；`dashboards/db_status.py`（副本引导与状态条）、`dashboards/db_quality.py`（读审计缓存）；`dashboards/cache.py`；可选启动器 `launch_app.py`（见下） |

**数据质量与可观测性**（支撑数据库与运维，而非业务看板本身）：

- 维度定义（单一事实来源）：`db/data_quality_spec.py`
- 一键跑全量并写缓存：`python -m reports.run_db_quality_audit` → `reports/db_quality_cache/`（如 `report.json`、`state.json`、`us_audit.md`）；Streamlit「数据库质量」页只读该缓存
- 仅生成 Markdown 的审计：`python -m db.us_data_audit` → `reports/us_etl_audit_*.md`（若仍使用）
- 美股 bulk 进度表：`etl_us_bulk_state`；日志 `logs/us_bulk_*.log`；小时自检 `python -m etl.us_bulk_watch` → `reports/us_etl_watch.log`

---

## 阶段性总结（截至 2026-04-26）

- 已完成新版 D1 主图看板：日 K、EMA10/250、14x/24x/34x DCF 阶梯线、FMP DCF 单条最新值横线。
- 已完成新版 D1 右侧信息区：核心指标、分析师共识、最近评级动作、价格提醒。
- 已接入 D1 投研笔记：原始笔记追加写入 + LLM 结构化 Markdown 回写数据库。
- **D2**（`dashboards/d2_business.py`）：公司简介、收入按业务/地区、EBITDA 覆盖度、管理层链接等只读展示。
- **D3**（`dashboards/d3_industry.py`）：同行业最新总收入 CCDF 分布（USD 口径）、同业列表含「货币」列。
- 年报入库：`fundamentals_annual` 含 `reporting_currency` / `fx_to_usd`，收入等金额在 ETL 中归一为 USD（百万）存储。
- 已确认数据库分层：原始层、派生层、用户层、回测层均有建表与文档说明（详见 db/README）。
- 旧版三市场分析入口继续保留，作为迁移期基线；剥离前见下文 **Legacy** 与 **清理候选** 清单。
- **自动化测试**：`python -m pytest tests -q`（含 D1 图线结构、D3 换算与 Plotly 轴、`get_industry_peers_revenue` 等）。

---

## 当前支持功能（按代码库现状）

### 1) 三市场分析入口（旧版 / Legacy）

- 美股 / A 股 / 港股分析流程可运行（位于 `analyzers/`）。
- 下载、AI 填表、图线渲染、已分析缓存等旧流程仍可用。计划剥离时以本文 **Legacy 功能与对应文件** 为准逐项拆除。

### 2) 美股新版 D1（重点）

- 日 K 主图：K 线 + EMA10/250 + DCF 阶梯线 + FMP DCF。
- 右侧信息：最新价、市值、3 年平均 FCF/S、FMP DCF 估值。
- 分析师模块：目标价、综合评级、最近评级动作表。
- 提醒模块：14x/24x/34x 快捷提醒 + 自定义提醒。
- 轻量刷新：可拉取最新 OHLCV 与 FMP DCF 并回写数据库。

### 3) 投研笔记（当前位于 D1 下方）

- 保存时追加写入 `raw_text`（不覆盖历史内容）。
- 保存时触发 LLM，生成结构化 Markdown 并写入 `markdown` 字段。
- 展示区可折叠，仅在点击后渲染内容。

### 4) ETL 与数据库（命令摘要）

- **单票 / 少量**：`python etl_run.py --tickers NVDA ...`（可选 `--skip-optional`、`--init`）。
- **美股全量（FMP 活跃普通股口径，含 ADR/OTC；排除 ETF/基金/优先股/债券类）**：`python -m etl.us_bulk_run --init-db` 后 `python -m etl.us_bulk_run --rate-limit-ms 400 --skip-optional`；`--retry-failed` 只重跑失败；`--force` 重跑含已 done。FMP `isEtf`/`isFund` 的标的会记为 `skipped` 不入主流程。短连接、快照与只读副本参数见 [db/README.md](db/README.md)。
- **每小时自检（本机）**：`python -m etl.us_bulk_watch` → 追加 `reports/us_etl_watch.log`。Windows 任务计划程序示例：`schtasks /Create /TN StockUSWatch /SC HOURLY /TR "conda run -n stock_analyzer python -m etl.us_bulk_watch" /F`（请把 `conda` 与项目路径改成你的环境）。
- 核心数据源：FMP（profile / ohlcv / annual fcf / dcf history）。
- 主数据库：`stock.db`（DuckDB 单文件）。

---

## 快速开始

### 1) 环境

```bash
conda activate stock_analyzer
pip install streamlit duckdb yfinance akshare plotly pandas numpy requests beautifulsoup4 google-genai futu-api pytest
```

### 2) 配置 .env（项目根目录）

```env
GEMINI_API_KEY=...    # 笔记整理 / AI 聊天（旧版填表等亦可能使用）
FMP_API_KEY=...       # 基本面 + OHLCV + FMP DCF
TUSHARE_TOKEN=...     # 可选（A 股补充）
```

### 3) 初始化并更新数据

```bash
# 首次可加 --init
python etl_run.py --init --tickers NVDA

# 增量更新多只股票
python etl_run.py --tickers NVDA AAPL MSFT

# 更快：跳过管理层 / 分部收入 / 地区收入 / 利息费用
python etl_run.py --tickers NVDA --skip-optional
```

### 4) 启动应用

**方式 A（常用）**：直接启动 Streamlit。

```bash
streamlit run app.py
```

访问 http://localhost:8501

**方式 B（可选）**：先后台启动数据库质量审查子进程，再以前台启动 Streamlit（`launch_app.py`）。

```bash
python launch_app.py
```

子进程跑 `python -m reports.run_db_quality_audit`，标准输出会追加到 `reports/db_quality_cache/audit_subprocess.log`。关闭 Streamlit 后子进程仍可能继续运行，直至自行结束；需要时在任务管理器中结束对应 Python 进程。

### 5) 运行单元测试

```bash
python -m pytest tests -q
```

---

## 当前项目结构（与仓库实际对齐，便于导航）

```text
stock_analyzer/
├── app.py                      # Streamlit 主入口
├── launch_app.py               # 可选：后台审计 + 启动 Streamlit
├── etl_run.py
├── analyzers/                  # Legacy：三市场旧版分析
├── dashboards/
│   ├── d1_fcf_multiple.py
│   ├── d2_business.py
│   ├── d3_industry.py
│   ├── db_status.py            # 只读副本引导、顶部状态条
│   ├── db_quality.py           # 数据库质量页（读缓存）
│   └── cache.py
├── db/
│   ├── schema.py
│   ├── repository.py
│   ├── checks.py
│   ├── data_quality_spec.py    # 质量维度定义
│   ├── us_data_audit.py
│   └── README.md
├── etl/
│   ├── compute.py
│   ├── loader.py
│   ├── pipeline.py
│   ├── snapshot.py
│   ├── dotenv_local.py
│   ├── us_bulk_run.py
│   ├── us_bulk_watch.py
│   ├── us_ticker_bundle.py
│   ├── us_run_options.py
│   └── sources/
│       ├── fmp.py
│       └── fmp_dcf.py
├── reports/
│   ├── run_db_quality_audit.py
│   └── db_quality_cache/         # 审计生成物（是否提交见 .gitignore 策略）
├── logs/
├── plans/                      # 内部计划文档，非运行时代码
├── prompts/                    # LLM 规则等
├── tests/
├── data_provider.py            # Legacy
├── gemini_chat.py              # Legacy + D1 笔记等
├── downloader.py               # Legacy
├── filing_store.py             # Legacy
├── chart_store.py              # Legacy
├── analysis_tracker.py         # Legacy
├── background_worker.py        # Legacy
├── futu_client.py              # Legacy（港股等）
├── us_universe.py / cn_universe.py  # Legacy
├── stock.db / stock_read.db    # 本地数据文件（通常不提交）
└── …                         # 见下文「清理候选」中的其它目录
```

---

## Legacy（旧版）功能与对应文件

剥离旧功能时建议按表核对依赖与数据目录，避免误删 D1/D2/D3 仍需要的模块（例如 `gemini_chat` 若仍服务于 D1 笔记，需拆分后再删旧路径）。

| Legacy 能力 | 主要代码 / 目录 | 典型数据或产物（清理时一并考虑） |
|-------------|-----------------|----------------------------------|
| 三市场旧版分析（下载财报 → 拉数 → AI 填表 → 图） | `analyzers/base.py`、`us.py`、`cn.py`、`hk.py`；`app.py` 内与旧版 Tab 相关的编排 | `SEC_Filings/`、`CN_Filings/`；若存在 `HK_Filings/` 同理 |
| 非 DB 首选的多源行情 / 财报拉取 | `data_provider.py` | 与旧流绑定的运行时行为 |
| LLM 填 FCF 表、旧版 AI 聊天、模型轮换等 | `gemini_chat.py`；`prompts/fcf_extraction_rules.txt`；`saved_tables/model_status.json`（若仍在用） | `saved_tables/<TICKER>_<MARKET>/` 下 CSV 等 |
| 财报下载与去重 | `downloader.py`、`filing_store.py` | 同上 filings 目录 |
| 「已分析」Pickle 缓存与浏览 | `chart_store.py` | `saved_charts/` |
| US 分析进度 / 指标 JSON、批量后台分析 | `analysis_tracker.py`、`background_worker.py`、`us_universe.py` | `saved_tables/us_tracker.json` 等 |
| A 股 universe / 批处理辅助 | `cn_universe.py` | 视是否保留 CN 功能 |
| 港股富途通道 | `futu_client.py`（及 HK 分析路径） | 依赖本机 OpenD 等 |

与 Legacy 强相关、但可能被其它文档引用的根目录示例：`saved_charts/`、`saved_tables/`、`SEC_Filings/`、`CN_Filings/`。`CLAUDE.md` 中有旧架构数据流说明，可与上表交叉核对。

---

## 清理与剥离准备：多余目录与文件（候选清单）

以下**不修改 .gitignore、不自动删文件**；仅供你准备仓库瘦身时逐项确认是否仍被脚本或习惯依赖。

### A. 通常应忽略或勿提交版本库

- `.pytest_cache/`、各包下 `__pycache__/`
- 本地数据库：`stock.db`、`stock_read.db`（若策略为「库不进仓」）
- `logs/*.log`（可只保留目录与 `logs/.gitkeep` 一类约定）
- `reports/db_quality_cache/` 下生成物（`report.json`、`state.json`、`us_audit.md`、`audit_subprocess.log` 等）— 是否与 CI/制品策略一致再决定是否跟踪

### B. 高度可疑：非应用源码或编辑器残留（优先核对）

- **`.vscode-tunnel/`**：多为 Cursor/VS Code Remote 隧道本地状态，**不应作为产品代码**；若已被 Git 跟踪，建议移出版本库并加入 `.gitignore`。

### C. 构建产物、孤儿配置、实验文件

- `build/`、`dist/`、`main.spec`：`main.spec` 指向的 `main.py` 在仓库根目录常不存在，易为**残留 PyInstaller 配置**；删除前确认是否仍有打包流程。
- `test.ipynb`、`fmp_api_demo.ipynb`：探索性笔记本，可标为非核心或移出主仓。
- `tests/results/*.txt`：测试运行产物。

### D. 需在仓库内确认用途后再标记删除或保留

- `.claude/`：工具配置目录，是否版本化由团队决定。
- `plans/`：内部计划，非运行时代码；可迁出主仓或保留仅作记录。
- `cache/`、`scripts/`、`SEC_Data/` 等：请在仓库内搜索引用后再决定；可能与历史脚本或数据实验相关。

---

## 与 db/README.md 的同步建议（文档维护）

在后续编辑 [db/README.md](db/README.md) 时建议补全或对齐下列叙述（当前以 README 为「产品总览」、db/README 为「库与表权威说明」）：

- 文首用一小节写清数据流：**ETL 写原始层 → 预计算写派生层 → UI 经 `repository` 只读**；副本与 `STOCK_ANALYZER_READ_DB` 保持现有说明即可。
- **`db` 包内模块表**：除 `schema.py`、`repository.py`、`checks.py` 外，显式列出 `data_quality_spec.py`、`us_data_audit.py`，并写清与 `reports/run_db_quality_audit.py`、`dashboards/db_quality.py` 的分工（谁执行重查询、谁只读缓存）。
- **表分层**：区分 D1/D2/D3 **热路径表**与低频 / 规划表（回测、因子、分钟线等），便于定义「剥离旧 UI 后的数据库最小集」。
- **ticks / Parquet 视图**：若 `schema.py` 或仓库中无实际 `ticks/` 与建视图 DDL，建议在 db/README 中标为「设计草案」或指向真实定义处，避免文档与代码不一致。

---

## 迁移进度追踪（用于项目管理）

### 已完成

- [x] 数据库 schema 与连接工厂（`db/schema.py`）
- [x] D1 所需核心查询（`db/repository.py`）
- [x] FMP 基本 ETL（profile / annual fcf / ohlcv / fmp dcf）
- [x] D1 图线计算（EMA / DCF history / DCF metrics）
- [x] 新版 D1 页面（与旧版并行）
- [x] D1 笔记入库 + LLM markdown 回写
- [x] 新版 D2：业务描述、板块/行业、收入拆分、EBITDA 覆盖、管理层（数据依赖 ETL 是否拉全）
- [x] 新版 D3：同板块同行业总收入 CCDF + 排名 + 货币列

### 原计划偏差（已作废或延期）

- ~~独立建设 `dashboards/d3_management.py`、`dashboards/d4_competitors.py` 等再串联~~ → 业务与管理层已并入 D2；同业对比以 D3 为主。
- ~~先做完整 screener/backtesting 新架构，再回到看板细化~~ → 先 D1 再 D2/D3，减少上下文切换。
- ~~迁移完成后立即删除 `analyzers/`、`data_provider.py`、`gemini_chat.py` 等~~ → 在剥离计划明确前保留旧链路；删除前对照本文 Legacy 表与对 `app.py` 的依赖梳理。
- ~~scheduler 每日自动任务作为默认路径~~ → 现状以手动 `etl_run.py` / `us_bulk_run` 为主；小时自检见 `us_bulk_watch`。

---

## 数据库现状（简版）

完整说明见 [db/README.md](db/README.md)。

当前重点表：

- `companies`：公司元数据（含 `currency`，与日线报价货币一致，供 D3 等同业对比换算）
- `ohlcv_daily`：日行情 + ema10/ema250 + market_cap
- `fundamentals_annual`：年度基本面（`currency` 为归一后 USD 百万口径；`reporting_currency` + `fx_to_usd` 保留原始报告货币与汇率审计）
- `dcf_history` / `dcf_metrics`：D1 DCF 阶梯线与最新估值（`dcf_metrics` 含 latest_price、多空潜力等派生字段）
- `fmp_dcf_history`：FMP DCF 日历史
- `revenue_by_segment` / `revenue_by_geography` / `management`：D2 数据源
- `notes`：raw_text + markdown
- `price_alerts`：提醒记录（当前提醒流程仍以 OpenD 调用为主，与 Legacy 提醒链路相关）

---

## 看板能力摘要（D2 / D3 已实现）

以下目标已在当前代码中落地；细节以各 `dashboards/d*.py` 为准。

**D2（笔记输入区下方）**：收入来源（业务/区域）、行业与板块、`companies.description` 业务叙事、管理层信息（CEO/CFO/COO 等）、EBITDA 覆盖等只读展示。

**D3（类 Value Line 板块对比）**：板块内总收入排名与 CCDF、分位点（如 Top 10%、Top 25%、中位数）、与板块中位数/龙头的差距感知；列表含挂牌货币与换算说明。

---

## 类 ROIC.ai + Value Line 的下一步可做能力

基于 FMP 数据库与已入库字段，可扩展横截面分析（与是否删除 Legacy 无冲突）：

1. **板块收入层级地图**：每个板块收入分层（S/A/B/C），识别龙头与尾部。
2. **收入质量双维度散点**：横轴收入规模或分位，纵轴收入增速（3Y CAGR），气泡大小为市值。
3. **业务描述结构化标签**：对 `description` 做关键词分类（硬件/软件/订阅/平台/周期品等），支持筛选。
4. **板块相对估值温度计**：Revenue Multiple（P/S）或 FCF Multiple（P/FCF）板块内 z-score。
5. **Value Line 风格综合评分卡**：规模、增长、盈利（FCF margin / ROIC）、稳定性（毛利率波动等）。
6. **技术位置 + 基本面位置交叉矩阵**：价格相对 EMA/DCF 与收入分位等 2×2 象限观察名单。

---

## 备注

- 本 README 作为**产品总览、架构说明与剥离清单**；表结构与锁策略以 [db/README.md](db/README.md) 为准。
- 后续若正式宣告「核心仅美股 DB 看板 + ETL + 质量工具」，请同步更新文首表述，并按 **Legacy** 与 **清理候选** 两节执行 `app.py` 与 `.gitignore` 的收敛。
- `CLAUDE.md` 中若与本文或测试命令不一致（例如是否配置 pytest），以仓库实际脚本为准并在整理时择一修正，避免新人误读。
