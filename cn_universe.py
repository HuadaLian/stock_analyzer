"""CN universe builder — Tushare primary, local scan fallback.

Fetches all A-share listed companies via Tushare ``stock_basic`` API,
including company name, industry, area, and market segment.
Caches to ``saved_tables/cn_universe.json`` for 7 days.

Fallback (no token): scans CN_Filings/ and saved_tables/ folders.
"""

import os
import json
import time
from collections import OrderedDict
from datetime import datetime
from core.instrument_policy import get_policy

BASE_DIR = os.path.dirname(__file__)
CACHE_DIR = os.path.join(BASE_DIR, "saved_tables")
UNIVERSE_FILE = os.path.join(CACHE_DIR, "cn_universe.json")
CN_FILINGS_DIR = os.path.join(BASE_DIR, "CN_Filings")
_CACHE_MAX_AGE_DAYS = 7


def _is_a_share_common(market_text: str) -> bool:
    """True for A-share ordinary boards; excludes B-share labels."""
    m = (market_text or "").strip().upper()
    if not m:
        return True
    # Tushare market examples: 主板/创业板/科创板/CDR/B股...
    return "B" not in m


def _load_tushare_token() -> str:
    """Try TUSHARE_TOKEN from env, then .env file."""
    token = os.environ.get("TUSHARE_TOKEN", "")
    if token:
        return token
    env_path = os.path.join(BASE_DIR, ".env")
    if os.path.exists(env_path):
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("TUSHARE_TOKEN="):
                        token = line.split("=", 1)[1].strip()
                        if token:
                            return token
        except OSError:
            pass
    return ""


def _fetch_from_tushare(token: str, progress_callback=None, *, filter_mode: str | None = None) -> OrderedDict:
    """Fetch all currently listed A-share stocks from Tushare stock_basic."""
    import tushare as ts

    if progress_callback:
        progress_callback("📡 正在从 Tushare 获取 A 股上市公司列表...")

    pro = ts.pro_api(token)
    policy = get_policy(filter_mode)
    df = pro.stock_basic(
        exchange="",
        list_status="L",
        fields="ts_code,symbol,name,area,industry,market,list_date",
    )

    if df is None or df.empty:
        raise ValueError("Tushare stock_basic 返回空数据")

    result = OrderedDict()
    for _, row in df.iterrows():
        code = str(row.get("symbol", "")).strip().zfill(6)
        if not code or len(code) != 6:
            continue
        market = str(row.get("market", "") or "")
        if policy.cn_exclude_b_share and not _is_a_share_common(market):
            continue
        result[code] = {
            "name":       str(row.get("name",       "") or ""),
            "industry":   str(row.get("industry",   "") or ""),
            "area":       str(row.get("area",       "") or ""),
            "market":     market,
            "list_date":  str(row.get("list_date",  "") or ""),
            "source":     "tushare",
        }

    if progress_callback:
        progress_callback(f"✅ 已获取 {len(result):,} 只 A 股上市公司 (来自 Tushare)")

    return result


def _fetch_from_local(progress_callback=None) -> OrderedDict:
    """Fallback: collect codes from CN_Filings/ and saved_tables/ folders."""
    result = OrderedDict()

    try:
        if os.path.exists(CN_FILINGS_DIR):
            for name in sorted(os.listdir(CN_FILINGS_DIR)):
                if os.path.isdir(os.path.join(CN_FILINGS_DIR, name)):
                    code = name.strip()
                    if code.isdigit():
                        result[code.zfill(6)] = {
                            "name": "", "industry": "", "source": "cn_filings",
                        }
    except Exception:
        pass

    try:
        if os.path.exists(CACHE_DIR):
            for entry in sorted(os.listdir(CACHE_DIR)):
                # saved FCF tables land in folders like "600519_CN"
                parts = entry.split("_")
                if len(parts) == 2 and parts[0].isdigit() and parts[1] == "CN":
                    code = parts[0].zfill(6)
                    if code not in result:
                        result[code] = {
                            "name": "", "industry": "", "source": "saved_tables",
                        }
    except Exception:
        pass

    if progress_callback:
        progress_callback(
            f"⚠️ Tushare 不可用，从本地文件扫描到 {len(result)} 只 A 股代码"
        )
    return result


def fetch_cn_universe(force_refresh: bool = False, progress_callback=None, filter_mode: str | None = None) -> OrderedDict:
    """Return OrderedDict {code: {"name", "industry", "area", "market", "list_date", "source"}}.

    Tries Tushare first (requires TUSHARE_TOKEN in env or .env).
    Falls back to local folder scan when no token is available or on error.
    Result is cached for up to 7 days.
    """
    os.makedirs(CACHE_DIR, exist_ok=True)

    # ── Check disk cache ─────────────────────────────────────────────
    if not force_refresh and os.path.exists(UNIVERSE_FILE):
        try:
            mtime = os.path.getmtime(UNIVERSE_FILE)
            age_days = (time.time() - mtime) / 86400
            if age_days < _CACHE_MAX_AGE_DAYS:
                with open(UNIVERSE_FILE, "r", encoding="utf-8") as f:
                    cached = json.load(f)
                stocks = cached.get("stocks")
                if stocks:
                    result = OrderedDict((k, v) for k, v in stocks.items())
                    if progress_callback:
                        src = cached.get("source", "?")
                        progress_callback(
                            f"✅ 从缓存加载 A 股列表 ({len(result):,} 只, "
                            f"更新于 {cached.get('updated', '?')}, 来源: {src})"
                        )
                    return result
        except (json.JSONDecodeError, OSError, KeyError):
            pass

    # ── Fetch ────────────────────────────────────────────────────────
    token = _load_tushare_token()
    source_label = "local"
    result: OrderedDict = OrderedDict()

    if token:
        try:
            result = _fetch_from_tushare(token, progress_callback, filter_mode=filter_mode)
            source_label = "tushare"
        except Exception as e:
            if progress_callback:
                progress_callback(f"⚠️ Tushare 获取失败: {e}，切换到本地扫描")
            result = _fetch_from_local(progress_callback)
    else:
        if progress_callback:
            progress_callback(
                "⚠️ 未找到 TUSHARE_TOKEN，使用本地文件扫描。"
                " 在 .env 中设置 TUSHARE_TOKEN 可获取完整 A 股列表。"
            )
        result = _fetch_from_local(progress_callback)

    if not result:
        return result

    # ── Save cache ───────────────────────────────────────────────────
    cache_payload = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "count": len(result),
        "source": source_label,
        "stocks": dict(result),
    }
    try:
        with open(UNIVERSE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache_payload, f, ensure_ascii=False)
    except OSError:
        pass

    return result
