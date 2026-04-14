# Stock Analyzer

本仓库提供一套本地化的投研工具，结合 SEC / 巨潮的数据抓取与 Streamlit 可视化，适合价值投资者构建长期研究资料库。

核心能力：
- 自动下载并管理美股 SEC 报告（10-K / 10-Q / 20-F / 6-K）
- 批量抓取 A 股公告并下载 PDF（来自巨潮资讯）
- 用 Streamlit 展示价格图与长期技术指标（如 EMA10 / EMA250）

## 快速开始

1. 克隆仓库并进入目录：

```powershell
git clone <repo-url>
cd stock_analyzer
```

2. 建议使用虚拟环境并安装必要依赖：

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -U pip
pip install -r requirements.txt
```

如果没有 `requirements.txt`，可安装常用依赖：

```powershell
pip install streamlit yfinance matplotlib requests beautifulsoup4
```

3. 启动 Web 界面：

```powershell
streamlit run app.py
```

打开浏览器访问 `http://localhost:8501`。

## 代码示例（可复制运行）

- 通过代码下载美股报告（示例使用 `SmartSECDownloader.smart_download_us`）：

```python
from downloader import SmartSECDownloader

def log(msg):
    print(msg)

dl = SmartSECDownloader(email="you@example.com")
# 直接传入 ticker，方法会自动解析 CIK 并下载（返回 FilingStore 实例）
store = dl.smart_download_us("AAPL", log)
print(store.summary())
```

- 通过代码下载 A 股报告（巨潮）：

```python
from downloader import CninfoDownloader

dl = CninfoDownloader()
# 参数 order: code, keyword, log_func
count = dl.download_cn_reports("002352", "", print)
print(f"下载完成: {count} 个文件")
```

- 使用 `analysis_tracker` 跟踪已分析的 ticker：

```python
from analysis_tracker import get_analyzed_tickers, mark_analyzed, remove_analyzed

print(get_analyzed_tickers())
mark_analyzed("AAPL")
remove_analyzed("AAPL")
```

## 项目结构（简要）

- `app.py`：Streamlit 应用入口，包含美股 / A 股 / 港股分析标签页与 AI 年报问答。
- `downloader.py`：包含 `SmartSECDownloader` 与 `CninfoDownloader`，实现网络抓取与文件保存逻辑。
- `analysis_tracker.py`：持久化记录已分析的 tickers（保存在 `saved_tables/us_tracker.json`）。
- `filing_store.py`：封装下载索引与已下载状态（用于 dedup 与统计）。
- `SEC_Filings/`、`CN_Filings/`：下载结果目录。

## 配置与说明

- Gemini（AI 年报问答）需要 API Key，可在根目录下创建 `.env` 并写入：

```
GEMINI_API_KEY=your_api_key_here
```

- SEC 请求要求自定义 `User-Agent`，`SmartSECDownloader` 构造时需传入 email（会写入 User-Agent）。

## 常见问题

- Q: 下载大量文件失败或被限流？
- A: 可通过降低并发、延长请求间隔或使用代理解决；代码中已设置小间隔以降低限流概率。

## 接下来的建议

- 我可以：
  - 把当前依赖写入 `requirements.txt`（若你同意，我可以自动生成）
  - 添加更详细的 API 文档或示例脚本（例如批量下载脚本）

如需我现在把 `requirements.txt` 或示例脚本生成并加入仓库，请告诉我。
