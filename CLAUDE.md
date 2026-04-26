# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Python environment locates here 
'C:\Users\huada\anaconda3\envs\stock_analyzer'

## Running the App

```bash
conda activate stock_analyzer
streamlit run app.py
```

Access at `http://localhost:8501`. There are no tests or linting scripts configured.

## Installing Packages

No `requirements.txt` exists. Install into the conda environment:

```bash
pip install streamlit yfinance akshare plotly pandas numpy requests beautifulsoup4 google-genai futu-api
```

## API Keys & External Services

All secrets go in `.env` at the repo root:

```
GEMINI_API_KEY=...    # required for AI FCF fill
FMP_API_KEY=...       # required for FMP FCF (primary US source) and OHLCV fallback
TUSHARE_TOKEN=...     # optional; without it CN universe falls back to local CN_Filings/ scan
```

**Futu OpenD** — required only for HK stocks and price alerts. Must be running locally on `127.0.0.1:11111`.

## Architecture Overview

`app.py` is a thin orchestrator: sets up Streamlit tabs, delegates each market to a `MarketAnalyzer` subclass, and hosts Settings UI. In US tab, it now nests `st.tabs(["旧版", "D1"])` so legacy flow and D1 DB-backed price chart can be compared side-by-side.

### Analysis Flow

```
User clicks "分析" → MarketAnalyzer.run()
  → download_filings_ui()        (US only: downloads SEC filings via SmartSECDownloader)
  → fetch_data(ticker)           → data_provider.get_*_data()
  → load_fcf_table()             (loads previously AI-filled CSV if it exists)
  → _run_ai_fill()               → gemini_chat.fill_fcf_table_with_llm()
  → _apply_adjusted_fcf()        (recomputes per-share FCF using latest share count)
  → render_chart()               (Plotly candlestick + EMA10/250 + DCF step-lines)

ETL path for D1 (`etl_run.py` / `dashboards/d1_fcf_multiple.py`):

User clicks "刷新并重算" (D1)
  → fetch_profile()              → companies
  → fetch_ohlcv()                → ohlcv_daily
  → load_fmp_dcf_history()       → fmp_dcf_history
  → fetch_fcf_annual()           → fundamentals_annual
  → compute_ema()                → ohlcv_daily.ema10 / ema250
  → compute_dcf_history()        → dcf_history
  → compute_dcf_lines()          → dcf_metrics
```

### Key Modules

- **`analyzers/base.py` — `MarketAnalyzer`**: Base class with all shared UI: candlestick chart, FCF table HTML renderer, price alert UI (moomoo OpenD), and the `_run_ai_fill` flow. Subclasses override `fetch_data`, `normalize_ticker`, `download_filings_ui`, `render_extra_ui`, and `on_analysis_complete`.

- **`analyzers/us.py`**: US subclass. Overrides `run()` to prepend the analysis-tracker UI. `on_analysis_complete` writes DCF metrics to the tracker and saves a chart pickle. Includes a background batch-analysis worker (`background_worker.py`) polled via `@st.fragment(run_every=2)`.

- **`analyzers/cn.py`**, **`analyzers/hk.py`**: A-share and HK subclasses. CN uses akshare; HK uses Futu OpenD for kline + yfinance for financials.

- **`data_provider.py`**: Per-market data fetching.
  - `get_us_data`: OHLCV + FCF from FMP (US path is now FMP-only). Returns `fmp_currency` (the `reportedCurrency` from FMP, e.g. `"USD"` or `"CNY"` for Chinese ADRs).
  - `get_cn_data`: OHLCV from 新浪财经 → 腾讯财经 → Tushare (cascade). FCF from akshare 新浪报表.
  - `get_hk_data`: Kline from Futu OpenD; FCF from yfinance.
  - `compute_dcf_lines`: builds 14x/24x/34x step-lines from the `fcf_per_share_by_year` dict.
  - FMP helpers: `_load_fmp_api_key`, `_fmp_ohlcv`, `_fmp_fcf_data`, `_extend_with_fmp_fcf`.

- **`etl/sources/fmp_dcf.py`**: Fetches FMP `/historical-discounted-cash-flow-statement/{ticker}` and upserts into `fmp_dcf_history`.

- **`dashboards/d1_fcf_multiple.py`**: D1 chart-only dashboard (candles + EMA10/250 + 14x/24x/34x step-lines + FMP DCF line + latest-price annotation), backed by DB/repository.

- **`gemini_chat.py`**: Large module (~2500 lines).
  - Model rotation with per-model rate-limit tracking persisted to `saved_tables/model_status.json`.
  - `fill_fcf_table_with_llm`: batches filing texts token-aware, sends to Gemini, merges results. Accepts `fmp_currency` and injects it into the batch prompt so the LLM targets the right currency table in the filing.
  - `_build_batch_fcf_prompt` / `_build_batch_fcf_prompt_cn`: prompt builders for US/HK vs CN filings.
  - FCF extraction rules loaded from `prompts/fcf_extraction_rules.txt` (editable in Settings tab, hot-reloaded each run).
  - `init_chat` / `send_message`: Gemini chat session over selected filing texts (AI chat tab).

- **`downloader.py`**: `SmartSECDownloader` — downloads 10-K/20-F/10-Q/6-K from SEC EDGAR. `CninfoDownloader` — downloads A-share annual reports from 巨潮. Both deduplicate via `filing_store.py`.

- **`background_worker.py`**: Thread-based worker for batch US analysis. State shared via a global dict, polled by a Streamlit fragment. Calls `USAnalyzer._run_single_auto_analysis` per ticker.

- **`analysis_tracker.py`**: Persists analysis state to `saved_tables/us_tracker.json`. Stores market cap, last price, DCF 14x/34x per ticker.

- **`chart_store.py`**: Saves/loads full analyzed data dicts as pickles in `saved_charts/`. Used for the "已分析股票" browse tab (no re-fetch needed).

- **`us_universe.py`**, **`cn_universe.py`**: Universe/watchlist management for batch workflows.

### FCF Data Source Priority (US)

1. **FMP** (`_fmp_fcf_data`) — primary source, up to 30 years of history; `reportedCurrency` stored as `fmp_currency` and passed to LLM prompt
2. **LLM** (`fill_fcf_table_with_llm`) — fills remaining N/A cells by reading downloaded filing text

> **Historical note**: XBRL (`xbrl_parser.py`) and yfinance cross-check were previously steps 1–2 but removed in favor of FMP-only.

### DCF Valuation Logic

Overlays three lines at 14x, 24x, 34x of the 3-year rolling average per-share FCF. Since OHLCV uses forward-adjusted prices (前复权), `_apply_adjusted_fcf` recomputes all per-share FCF values using the **latest** total share count so DCF lines are on the same price basis.

### Persistent Storage

| Path | Contents |
|------|----------|
| `SEC_Filings/<TICKER>/` | Downloaded SEC filings (10-K, 20-F, etc.) |
| `CN_Filings/<CODE>/` | Downloaded 巨潮 A-share filings |
| `HK_Filings/<CODE>/` | Downloaded HK annual reports |
| `stock.db` (`ohlcv_daily.ema10/ema250`) | Precomputed EMA lines for D1 |
| `stock.db` (`dcf_history`) | Per-fiscal-year DCF step-line history |
| `stock.db` (`fmp_dcf_history`) | FMP daily intrinsic value history |
| `saved_tables/<TICKER>_<MARKET>/` | AI-filled FCF tables (CSV) |
| `saved_tables/us_tracker.json` | US analysis tracker (DCF metrics per ticker) |
| `saved_tables/model_status.json` | Gemini model rate-limit state |
| `saved_charts/<MARKET>_<TICKER>.pkl` | Full chart data cache (pickle) |
| `prompts/fcf_extraction_rules.txt` | LLM FCF extraction rules (editable in UI) |
