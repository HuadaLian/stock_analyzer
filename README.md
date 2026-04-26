# Stock Analyzer

基于 Streamlit + DuckDB 的本地化投研分析平台，支持美股 / A 股 / 港股。当前采用“旧版分析流程 + 新版 D1 看板并行”的迭代策略，确保每次改版都能对照验证。

---

## 阶段性总结（截至 2026-04-25）

- 已完成新版 D1 主图看板：日 K、EMA10/250、14x/24x/34x DCF 阶梯线、FMP DCF 参照线。
- 已完成新版 D1 右侧信息区：核心指标、分析师共识、最近评级动作、价格提醒。
- 已接入 D1 投研笔记：原始笔记追加写入 + LLM 结构化 Markdown 回写数据库。
- 已确认数据库分层：原始层、派生层、用户层、回测层均有建表与文档说明。
- 旧版三市场分析入口继续保留，作为迁移期稳定基线。

---

## 当前支持功能（按代码库现状）

### 1) 三市场分析入口（旧版）

- 美股 / A 股 / 港股分析流程可运行（位于 analyzers）。
- 下载、AI 填表、图线渲染、已分析缓存等旧流程仍可用。

### 2) 美股新版 D1（重点）

- 日 K 主图：K 线 + EMA10/250 + DCF 阶梯线 + FMP DCF。
- 右侧信息：最新价、市值、3 年平均 FCF/S、FMP DCF 估值。
- 分析师模块：目标价、综合评级、最近评级动作表。
- 提醒模块：14x/24x/34x 快捷提醒 + 自定义提醒。
- 轻量刷新：可拉取最新 OHLCV 与 FMP DCF 并回写数据库。

### 3) 投研笔记（当前位于 D1 下方）

- 保存时追加写入 raw_text（不覆盖历史内容）。
- 保存时触发 LLM，生成结构化 Markdown 并写入 markdown 字段。
- 展示区可折叠，仅在点击后渲染内容。

### 4) ETL 与数据库

- ETL 入口：etl_run.py（--tickers 必填，--init 可选）。
- 核心数据源：FMP（profile / ohlcv / annual fcf / dcf history）。
- 主数据库：stock.db（DuckDB 单文件）。

---

## 快速开始

### 1) 环境

```bash
conda activate stock_analyzer
pip install streamlit duckdb yfinance akshare plotly pandas numpy requests beautifulsoup4 google-genai futu-api
```

### 2) 配置 .env（项目根目录）

```env
GEMINI_API_KEY=...    # 笔记整理 / AI 聊天
FMP_API_KEY=...       # 基本面 + OHLCV + FMP DCF
TUSHARE_TOKEN=...     # 可选（A 股补充）
```

### 3) 初始化并更新数据

```bash
# 首次可加 --init
python etl_run.py --init --tickers NVDA

# 增量更新多只股票
python etl_run.py --tickers NVDA AAPL MSFT
```

### 4) 启动应用

```bash
streamlit run app.py
```

访问 http://localhost:8501

---

## 当前项目结构（实际存在）

```text
stock_analyzer/
├── app.py
├── analyzers/
│   ├── base.py
│   ├── us.py
│   ├── cn.py
│   └── hk.py
├── dashboards/
│   └── d1_fcf_multiple.py
├── db/
│   ├── schema.py
│   ├── repository.py
│   ├── checks.py
│   └── README.md
├── etl/
│   ├── compute.py
│   ├── loader.py
│   └── sources/
│       ├── fmp.py
│       └── fmp_dcf.py
├── etl_run.py
├── data_provider.py
├── gemini_chat.py
├── downloader.py
├── futu_client.py
├── analysis_tracker.py
├── chart_store.py
├── background_worker.py
├── stock.db
└── tests/
```

---

## 迁移进度追踪（用于项目管理）

### 已完成

- [x] 数据库 schema 与连接工厂（db/schema.py）
- [x] D1 所需核心查询（db/repository.py）
- [x] FMP 基本 ETL（profile / annual fcf / ohlcv / fmp dcf）
- [x] D1 图线计算（EMA / DCF history / DCF metrics）
- [x] 新版 D1 页面（与旧版并行）
- [x] D1 笔记入库 + LLM markdown 回写

### 进行中

- [ ] 新版 D2（位于笔记输入下方）：
  - 收入来源饼图
  - 行业 / 板块名称
  - 类 ROIC.ai 的 business 描述
  - 管理层信息（从原 D3 需求并入）
- [ ] 新版 D3（类 Value Line 板块比较）：
  - 仅用总收入做板块内相对水平排名

### 原计划偏差（已作废或延期）

- ~~独立建设 dashboards/d2_business.py、dashboards/d3_management.py、dashboards/d4_competitors.py 后再串联~~
  - 改为：先把“业务结构 + 管理层”并入新版 D2，一次完成信息叙事。
- ~~先做完整 screener/backtesting 新架构，再回到看板细化~~
  - 改为：先把 D1 做实，再按 D2/D3 递进，减少上下文切换。
- ~~迁移完成后立即删除 analyzers/、data_provider.py、gemini_chat.py 等旧模块~~
  - 改为：在 D2/D3 稳定前保留旧链路，避免三市场功能回退。
- ~~scheduler.py 每日自动任务作为默认路径~~
  - 现状：当前以手动执行 etl_run.py 为主。

---

## 数据库现状（简版）

完整说明见 [db/README.md](db/README.md)

当前重点表：

- companies：公司元数据
- ohlcv_daily：日行情 + ema10/ema250 + market_cap
- fundamentals_annual：年度基本面
- dcf_history / dcf_metrics：D1 DCF 阶梯线与最新估值
- fmp_dcf_history：FMP DCF 日历史
- notes：raw_text + markdown
- price_alerts：提醒记录（当前提醒流程仍以 OpenD 调用为主）

---

## 下一阶段看板定义（与你当前设计一致）

### D2（在笔记输入区下方）

目标：把“公司业务画像”一次看清。

- 收入来源饼图（按业务 / 区域）
- 行业、板块名称
- 类 ROIC.ai 的 business 描述
- 管理层信息（CEO/CFO/COO 等）

### D3（类 Value Line 板块对比）

目标：只用总收入快速判断公司在板块中的位置。

- 板块内总收入排名
- 分位点（Top 10%、Top 25%、中位数）
- 与板块中位数 / 龙头的差距

---

## 类 ROIC.ai + Value Line 的下一步可做能力

基于 FMP 数据库，不再需要逐页看个股，可直接做横截面分析：

1. 板块收入层级地图
- 每个板块生成收入分层（S/A/B/C），快速识别龙头与尾部。

2. 收入质量双维度散点
- 横轴：收入规模（或分位）
- 纵轴：收入增速（3Y CAGR）
- 气泡大小：市值

3. 业务描述结构化标签
- 对 company description 做关键词分类（硬件/软件/订阅/平台/周期品），支持快速筛选。

4. 板块相对估值温度计
- 用 Revenue Multiple（P/S）或 FCF Multiple（P/FCF）做板块内 z-score，识别高估/低估区间。

5. Value Line 风格综合评分卡
- 规模（Revenue percentile）
- 增长（Revenue CAGR）
- 盈利（FCF margin / ROIC）
- 稳定性（毛利率波动）

6. 板块内“技术位置 + 基本面位置”交叉矩阵
- 技术位置：价格相对 EMA / DCF 线
- 基本面位置：收入分位
- 用 2x2 象限做策略观察名单。

---

## 备注

本 README 作为项目进度追踪文档。后续每完成 D2 / D3 一个里程碑，直接更新“迁移进度追踪”与“下一阶段看板定义”。
