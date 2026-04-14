# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the App

```bash
streamlit run app.py
```

Access at `http://localhost:8501`. There are no tests or linting scripts configured.

## Environment Setup

Use the conda environment `stock_analyzer`:

```bash
conda activate stock_analyzer
```

No `requirements.txt` exists. To install missing packages into the environment:

```bash
pip install streamlit yfinance akshare plotly pandas numpy requests beautifulsoup4 google-genai futu-api
```

**Gemini API Key** — required for the AI FCF fill feature. Place in `.env` at the repo root:
```
GEMINI_API_KEY=your_key_here
TUSHARE_TOKEN=your_token_here
```

**Tushare Token** — optional but recommended. Without it, the A-share universe tracker falls back to scanning local `CN_Filings/` folders only, giving an incomplete stock list. Get a free token at tushare.pro.

**Futu OpenD** — required only for HK stocks and price alerts. Must be running locally on `127.0.0.1:11111`.

## Architecture Overview

`app.py` is a thin orchestrator: it sets up Streamlit tabs and delegates each market to a `MarketAnalyzer` subclass. It also contains the AI chat tab and settings UI for Gemini model/key management.

### Data Flow

```
User clicks "分析" → MarketAnalyzer.run()
  → download_filings_ui()    (optional, only US downloads SEC filings)
  → fetch_data(ticker)       → data_provider.get_*_data()
  → load_fcf_table()         (loads previously saved AI-filled table if any)
  → _run_ai_fill()           → gemini_chat.fill_fcf_table_with_llm()
  → _apply_adjusted_fcf()    (normalizes FCF/share to latest share count)
  → render_chart()           (Plotly candlestick + EMA10/250 + DCF lines)
```

### Key Modules

- **`analyzers/base.py` — `MarketAnalyzer`**: Base class containing all shared UI rendering: candlestick chart with EMA + DCF step-lines, FCF table HTML, price alert UI (moomoo OpenD), and the AI fill flow. Subclasses override `fetch_data`, `normalize_ticker`, `download_filings_ui`, and `render_extra_ui`.

- **`analyzers/us.py`, `cn.py`, `hk.py`**: Market-specific subclasses. US uses SEC EDGAR + yfinance, CN uses akshare, HK uses Futu OpenD for kline + yfinance for financials.

- **`data_provider.py`**: Fetches OHLCV and FCF data per market. `get_us_data` uses SEC XBRL as primary FCF source with yfinance as cross-check. `get_cn_data` uses akshare 新浪报表. `get_hk_data` uses Futu + yfinance. `compute_dcf_lines` calculates 14x/24x/34x DCF valuation lines using 3-year rolling average per-share FCF.

- **`xbrl_parser.py`**: Pulls FCF directly from the SEC EDGAR Company Facts API (structured JSON, no HTML parsing). Given a CIK it returns a clean FCF table. Caches JSON in `SEC_Data/`.

- **`downloader.py`**: `SmartSECDownloader` downloads SEC 10-K/10-Q/20-F/6-K filings. `CninfoDownloader` downloads A-share filings from 巨潮. Files are stored in `SEC_Filings/<TICKER>/` and `CN_Filings/<CODE>/`.

- **`gemini_chat.py`**: Manages Gemini model rotation with rate-limit tracking, AI chat sessions over filing text, and `fill_fcf_table_with_llm` which sends filing text to LLM to fill missing FCF rows. FCF extraction rules are in `prompts/fcf_extraction_rules.txt` (editable from the Settings tab).

- **`filing_store.py`**: Tracks which filings have been downloaded (dedup index). `FilingStore` is returned by `SmartSECDownloader.smart_download_us`.

- **`futu_client.py`**: Thin context-manager wrapper around `futu.OpenQuoteContext`. Used for HK kline data and setting moomoo price alerts.

- **`analysis_tracker.py`**: Persists which US tickers have been analyzed to `saved_tables/us_tracker.json`. Called automatically by `USAnalyzer.on_analysis_complete`.

- **`chart_store.py`**: Saves/loads the full analyzed data dict (OHLCV + FCF + metadata) as pickle files in `saved_charts/`. Allows re-rendering a previously fetched ticker without re-fetching.

- **`us_universe.py`**, **`cn_universe.py`**: Universe/watchlist management for batch analysis workflows.

### Persistent Storage

| Path | Contents |
|------|----------|
| `SEC_Filings/<TICKER>/` | Downloaded SEC filings |
| `CN_Filings/<CODE>/` | Downloaded 巨潮 filings |
| `SEC_Data/` | XBRL Company Facts JSON cache |
| `saved_tables/<TICKER>/` | AI-filled FCF tables (CSV) |
| `saved_tables/us_tracker.json` | Analysis tracker |
| `saved_charts/<MARKET>_<TICKER>.pkl` | Full chart data cache (pickle) |
| `prompts/fcf_extraction_rules.txt` | LLM prompt rules for FCF extraction |

### DCF Valuation Logic

The chart overlays three DCF valuation lines at 14x, 24x, and 34x multiples of the 3-year rolling average per-share FCF. Since OHLCV uses forward-adjusted prices (前复权), `_apply_adjusted_fcf` recomputes all per-share FCF figures using the *latest* total share count so DCF lines are on the same basis as the price.
