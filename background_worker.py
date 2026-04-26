"""Background analysis worker.

Runs download → fetch → AI-fill → save in a daemon thread so the Streamlit
UI thread stays free.  State is stored in a module-level dict keyed by
session_id; the @st.fragment polls this dict every 2 seconds.
"""

import threading
import datetime

# session_id → {"status": "idle"|"running"|"done",
#                "queue": [...], "idx": int, "logs": [str]}
_states: dict = {}
_lock = threading.Lock()


# ── Public API ────────────────────────────────────────────────────────

def get_state(session_id: str) -> dict | None:
    return _states.get(session_id)


def is_running(session_id: str) -> bool:
    s = _states.get(session_id)
    return s is not None and s["status"] == "running"


def clear(session_id: str):
    with _lock:
        _states.pop(session_id, None)


def start(session_id: str, market: str, queue: list, gemini_config: dict):
    """Start a background analysis thread for *queue* tickers."""
    with _lock:
        _states[session_id] = {
            "status": "running",
            "queue": list(queue),
            "idx": 0,
            "logs": [],
        }

    state = _states[session_id]

    def _log(msg=None, step=None, total=None):
        """Accept the same signature as progress_callback in gemini_chat.py."""
        if msg is not None:
            ts = datetime.datetime.now().strftime("%H:%M:%S")
            state["logs"].append(f"[{ts}] {msg}")

    def _worker():
        try:
            if market == "US":
                _run_us(state, gemini_config, _log)
            elif market == "CN":
                _run_cn(state, gemini_config, _log)
        except Exception as e:
            _log(f"💥 Worker 崩溃: {e}")
        finally:
            state["status"] = "done"
            _log(f"🎉 批量完成，共 {len(state['queue'])} 只。")

    threading.Thread(target=_worker, daemon=True).start()


# ── Market workers ────────────────────────────────────────────────────

def _run_us(state: dict, cfg: dict, log):
    from downloader import SmartSECDownloader
    from data_provider import get_us_data
    from gemini_chat import (
        load_fcf_table, fill_fcf_table_with_llm,
        save_fcf_table, recompute_fcf_per_share,
    )
    from analysis_tracker import mark_analyzed
    from chart_store import save_chart
    from analyzers.base import MarketAnalyzer

    queue = state["queue"]
    for i, ticker in enumerate(queue):
        state["idx"] = i
        ticker = ticker.strip().upper()
        log(f"▶ [{i + 1}/{len(queue)}] {ticker}")

        # 1. Fetch OHLCV + FCF (FMP primary)
        try:
            data = get_us_data(ticker)
            saved_tbl = load_fcf_table(ticker, "US")
            if saved_tbl is not None and not saved_tbl.empty:
                data = dict(data)
                data["fcf_table"] = saved_tbl
        except Exception as e:
            log(f"❌ 数据获取失败: {e}")
            mark_analyzed(ticker, status="error")
            continue

        # 2. Check FCF quality — only download + AI fill if insufficient
        is_sufficient, quality_reason = MarketAnalyzer._fcf_data_sufficient(data)
        if is_sufficient:
            log(f"✅ FCF 数据充足（{quality_reason}），跳过年报下载和 AI 补全")
        else:
            log(f"⚠️ FCF 数据不足（{quality_reason}），开始下载年报...")
            try:
                dl = SmartSECDownloader(email="lianhdff@gmail.com")
                dl.smart_download_us(ticker, log)
            except Exception as e:
                log(f"⚠️ SEC 下载: {e}")
            data = _ai_fill(data, ticker, "US", cfg, log, state,
                            fmp_currency=data.get("fmp_currency", "USD"))

        # 3. Apply adjusted FCF + persist (also handles USD conversion for non-USD reporters)
        data = MarketAnalyzer._apply_adjusted_fcf(data)
        mcap = data.get("market_cap")
        mark_analyzed(ticker, metadata={"market_cap": mcap, "market": "US"} if mcap else {"market": "US"})
        try:
            save_chart(ticker, "US", data)
        except Exception as e:
            log(f"⚠️ 保存图表失败: {e}")

        log(f"✅ {ticker} 完成")


def _run_cn(state: dict, cfg: dict, log):
    from downloader import CninfoDownloader
    from data_provider import get_cn_data
    from gemini_chat import (
        load_fcf_table, fill_fcf_table_with_llm,
        save_fcf_table, recompute_fcf_per_share,
    )
    from analysis_tracker import mark_analyzed
    from chart_store import save_chart
    from analyzers.base import MarketAnalyzer

    queue = state["queue"]
    for i, ticker in enumerate(queue):
        state["idx"] = i
        t = ticker.strip()
        if t.isdigit():
            t = t.zfill(6)
        log(f"▶ [{i + 1}/{len(queue)}] {t}")

        # 1. Fetch OHLCV + FCF (akshare primary)
        try:
            data = get_cn_data(t)
            saved_tbl = load_fcf_table(t, "CN")
            if saved_tbl is not None and not saved_tbl.empty:
                data = dict(data)
                data["fcf_table"] = saved_tbl
        except Exception as e:
            log(f"❌ 数据获取失败: {e}")
            mark_analyzed(t, status="error")
            continue

        # 2. Check FCF quality — only download + AI fill if insufficient
        is_sufficient, quality_reason = MarketAnalyzer._fcf_data_sufficient(data)
        if is_sufficient:
            log(f"✅ FCF 数据充足（{quality_reason}），跳过年报下载和 AI 补全")
        else:
            log(f"⚠️ FCF 数据不足（{quality_reason}），开始下载年报...")
            try:
                dl = CninfoDownloader()
                dl.download_cn_reports(t, "年度报告", log)
            except Exception as e:
                log(f"⚠️ 巨潮下载: {e}")
            data = _ai_fill(data, t, "CN", cfg, log, state,
                            fmp_currency=data.get("fmp_currency", "USD"))

        # 3. Apply adjusted FCF + persist
        data = MarketAnalyzer._apply_adjusted_fcf(data)
        mcap = data.get("market_cap")
        mark_analyzed(t, metadata={"market_cap": mcap, "market": "CN"} if mcap else {"market": "CN"})
        try:
            save_chart(t, "CN", data)
        except Exception as e:
            log(f"⚠️ 保存图表失败: {e}")

        log(f"✅ {t} 完成")


def _ai_fill(data: dict, ticker: str, market: str, cfg: dict, log, state: dict = None,
             fmp_currency: str = "USD") -> dict:
    """Run fill_fcf_table_with_llm headlessly (no Streamlit calls).

    *fmp_currency* is passed through to the LLM prompt so it targets the right
    currency table in the filing (e.g. CNY for Chinese ADRs filing 20-F).
    Actual USD conversion is handled later by _apply_adjusted_fcf.
    """
    from gemini_chat import fill_fcf_table_with_llm, save_fcf_table, recompute_fcf_per_share

    fcf_tbl = data.get("fcf_table") if isinstance(data, dict) else None
    if fcf_tbl is None or fcf_tbl.empty or not cfg.get("api_key"):
        return data

    def _table_cb(updated_tbl):
        """Push each intermediate table update into worker state for the fragment."""
        if state is not None:
            state["last_fcf_table"] = updated_tbl
            state["last_fcf_ticker"] = ticker
            state["last_fcf_market"] = market

    try:
        filled, _, _ = fill_fcf_table_with_llm(
            api_key=cfg["api_key"],
            model_name=cfg.get("model_name", ""),
            fcf_table=fcf_tbl.copy(),
            ticker=ticker,
            market=market,
            fmp_currency=fmp_currency,
            progress_callback=log,
            table_update_callback=_table_cb,
            enabled_models=cfg.get("enabled_models"),
        )
        latest_shares = data.get("shares_outstanding")
        if latest_shares and latest_shares > 0:
            filled = recompute_fcf_per_share(filled, latest_shares)
        data = dict(data)
        data["fcf_table"] = filled
        # Final update with share-adjusted table
        if state is not None:
            state["last_fcf_table"] = filled
            state["last_fcf_ticker"] = ticker
            state["last_fcf_market"] = market
        n_filled = filled["FCF"].notna().sum() if "FCF" in filled.columns else 0
        log(f"✅ AI 填充完成: {ticker} 共 {n_filled} 行 FCF 数据")
        try:
            save_fcf_table(filled, ticker, market)
        except Exception as e:
            log(f"⚠️ 保存 FCF 表格失败: {e}")
    except Exception as e:
        log(f"⚠️ AI 填充出错: {e}")

    return data
