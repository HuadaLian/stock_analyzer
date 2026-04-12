# Stock Analyzer

一个面向价值投资场景的本地工具，支持：

- 美股：下载 SEC 报告（10-K / 10-Q / 20-F / 6-K），并绘制长期趋势图。
- A 股：从巨潮资讯批量下载定期报告（PDF）。
- 图表分析：基于 15 年日线数据计算 EMA10 与 EMA250，辅助判断短中长期趋势。

## 核心功能

### 1. 美股分析中心（SEC + 图表）
入口在 `app.py` 的“美股分析中心”页签，包含两个动作：

- 生成专业价格图表
  - 数据源：Yahoo Finance（`yfinance`）
  - 区间：过去 15 年，日线（1d）
  - 指标：
    - EMA10：短期动量
    - EMA250：长线牛熊分界
  - 图表元素：收盘价、双 EMA、成交量面积图
  - 页面会自动给出简要趋势解读（价格相对 EMA10/EMA250 的位置）

- 下载 SEC 报告
  - 通过 ticker 自动查 CIK（也可输入 SEC URL 辅助精准识别）
  - 拉取 `data.sec.gov/submissions/CIKxxxx.json` 索引
  - 下载表单类型：`10-K`、`10-Q`、`20-F`、`6-K`
  - 对 `6-K` 会额外尝试抓取附件（exhibit / press release 等链接）

### 2. A 股分析中心（巨潮资讯）
入口在 `app.py` 的“A股分析中心”页签：

- 输入 6 位股票代码，自动匹配 orgId
- 按近 10 年时间范围抓取公告
- 支持关键字筛选
- 默认下载定期报告类别（年报、半年报、一季报、三季报）
- 自动过滤“摘要”“提示性”类文件

## 项目结构说明

- `app.py`
  - Streamlit Web 应用入口
  - 负责页面交互、图表展示、日志输出
- `downloader.py`
  - `SmartSECDownloader`：美股 SEC 报告下载逻辑
  - `CninfoDownloader`：A 股巨潮公告检索与 PDF 下载
- `SEC_Filings/`
  - 美股报告保存目录，按 ticker / 表单类型分层
- `CN_Filings/`
  - A 股报告保存目录，按股票代码分层
- `main.spec`
  - PyInstaller 打包配置（用于生成可执行文件）

## 安装依赖

建议使用虚拟环境：

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -U pip
pip install streamlit yfinance matplotlib requests beautifulsoup4
```

## 运行方式

在项目根目录执行：

```powershell
streamlit run app.py
```

浏览器会自动打开本地页面（通常是 `http://localhost:8501`）。

## 下载结果目录示例

- 美股：`SEC_Filings/<TICKER>/<FORM>/...`
  - 例如：`SEC_Filings/BILI/20-F/...`
- A 股：`CN_Filings/<CODE>/...`
  - 例如：`CN_Filings/002352/...`

## 关键实现细节

- SEC 下载
  - 先获取公司提交索引，再批量下载主文件
  - 时间过滤：默认仅保留 2005-01-01 之后的报告
  - 已存在文件不会重复下载

- 巨潮下载
  - 分页拉取公告
  - 默认每页 30 条
  - 通过 `hasMore` 控制翻页结束
  - 已存在文件不会重复下载

## 使用注意事项

- SEC 接口对 `User-Agent` 有要求，代码里已设置；建议替换为你自己的邮箱标识。
- 网络抖动或站点限流时，可能出现个别文件下载失败，可重试。
- 股票代码、公告标题等用于文件名时，个别字符可能受系统文件命名规则影响。
- 若某公司上市时间较短，可能无法满足 EMA250 计算所需数据量。

## 可选：打包为桌面程序

仓库包含 `main.spec`，可用 PyInstaller 打包（需先安装 `pyinstaller`）：

```powershell
pip install pyinstaller
pyinstaller main.spec
```

> 注意：当前项目入口文件为 `app.py`，而 `main.spec` 中脚本名写的是 `main.py`。打包前请先确认并统一入口文件名。

## 适用场景

- 价值投资中的长期趋势观察
- 跟踪企业定期报告与临时披露
- 构建本地化、可复用的投研资料库
