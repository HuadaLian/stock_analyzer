"""Fetch and cache the US-listed operating-company universe from SEC EDGAR.

Data source: ``company_tickers.json`` — maintained by SEC and **pre-sorted by
market-cap (descending)**.  We preserve that insertion order so that scanning
proceeds from the largest companies first.

Filtering pipeline:
1. CIK-based dedup  (one ticker per legal entity)
2. Ticker-pattern exclusion  (preferred, warrants, units, rights, notes …)
3. Company-name exclusion  (ETFs, commodity trusts, closed-end funds …)
"""

import json
import os
import re
import time
import urllib.request
from collections import OrderedDict
from datetime import datetime

BASE_DIR = os.path.dirname(__file__)
CACHE_DIR = os.path.join(BASE_DIR, "saved_tables")
UNIVERSE_FILE = os.path.join(CACHE_DIR, "us_universe.json")
_CACHE_MAX_AGE_DAYS = 7

# SEC EDGAR endpoint — pre-sorted by market cap
_SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
_USER_AGENT = "Huada Lian lianhdff@gmail.com"

# ── Ticker patterns for non-common-stock securities ──────────────────
_JUNK_TICKER = re.compile(
    r"[-. /](P[A-Z]?|PR[A-Z]?"       # preferred
    r"|W[A-Z]?|WS[A-Z]?|WT[A-Z]?"    # warrants
    r"|R|RT|RTS"                        # rights
    r"|U|UN|UNT"                        # units
    r"|CL|EC|EP|ES|ET"                  # convertible / when-issued
    r"|NTS?|DB|SB"                      # notes / debentures
    r")$"
    r"|[/]"                             # slash in ticker (debt instruments)
    r"|^\d+$",                          # purely numeric
    re.IGNORECASE,
)

# ── Company-name patterns for non-operating entities ─────────────────
_EXCLUDE_NAME = re.compile(
    r"\bETF\b"
    r"|\bETN\b"
    r"|\bSPDR\b"
    r"|\biSHARES\b"
    r"|\bPROSHARES\b"
    r"|\bVANGUARD\b"
    r"|\bDIREXION\b"
    r"|\bWISDOM\s*TREE\b"
    r"|\bINVESCO\s+(QQQ|DB|BULL|BEAR)\b"
    r"|\bGRAYSCALE\b"
    r"|\bSPROTT\s+PHYSICAL\b"
    r"|\bTEUCRIUM\s+COMMODITY\b"
    r"|\bAMPLIFY\s+COMMODITY\b"
    r"|\bUNITED\s+STATES\s+(OIL|COMMODITY|NATURAL\s+GAS)\b"
    r"|\bINDEX\s+FUND\b"
    r"|\bMUTUAL\s+FUND\b"
    r"|\bCLOSED.?END\b"
    r"|\bUNIT\s+INVESTMENT\b"
    r"|\bTRUST,?\s+SERIES\b"              # "QQQ Trust, Series 1" style
    r"|\bPHYSICAL\s+(GOLD|SILVER|PLATINUM|COPPER|PALLADIUM|EUROPEAN)\b"
    r"|\bCOMMODITY\s+(TRUST|INDEX)\b"
    r"|\b(BITCOIN|CRYPTO|ETHER)\s+(FUND|TRUST)\b"
    r"|\bNOTES?\s+DUE\b"
    # Catch "XYZ Fund, Inc." / "XYZ Fund LP" but NOT "Netflix Inc"
    r"|(?<![A-Za-z])FUND(?:,?\s+(?:INC|LP|LLC|LTD|TRUST|OF))\b",
    re.IGNORECASE,
)


def _is_valid_ticker(ticker: str) -> bool:
    """Return True if the ticker looks like a common stock."""
    t = ticker.strip().upper()
    if not t or len(t) > 8:
        return False
    if _JUNK_TICKER.search(t):
        return False
    return True


def fetch_us_universe(force_refresh=False, progress_callback=None):
    """Return OrderedDict {ticker: {"name": …, "cik": …}} sorted by market cap.

    Uses SEC EDGAR ``company_tickers.json`` which is pre-sorted by market cap.
    CIK-based dedup keeps one ticker per legal entity.
    Caches to ``saved_tables/us_universe.json`` for 7 days.
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
                tickers = cached.get("tickers")
                if tickers:
                    # Restore insertion order
                    result = OrderedDict(
                        (k, v) for k, v in tickers.items()
                    )
                    if progress_callback:
                        progress_callback(
                            f"✅ 从缓存加载美股列表 ({len(result):,} 只, "
                            f"更新于 {cached.get('updated', '?')})"
                        )
                    return result
        except (json.JSONDecodeError, OSError, KeyError):
            pass

    # ── Fetch from SEC ───────────────────────────────────────────────
    if progress_callback:
        progress_callback("📡 正在从 SEC EDGAR 下载美股上市公司列表...")

    req = urllib.request.Request(_SEC_TICKERS_URL, headers={
        "User-Agent": _USER_AGENT,
        "Accept-Encoding": "gzip",
    })
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
        if resp.headers.get("Content-Encoding") == "gzip":
            import gzip
            raw = gzip.decompress(raw)
        data = json.loads(raw.decode("utf-8"))

    # data = {"0": {"cik_str": 1045810, "ticker": "NVDA", "title": "..."}, ...}
    # Keys are stringified indices ordered by market cap (0 = largest).

    universe: OrderedDict[str, dict] = OrderedDict()
    seen_cik: set[int] = set()

    for idx_key in sorted(data.keys(), key=lambda k: int(k)):
        entry = data[idx_key]
        try:
            ticker = str(entry["ticker"]).strip().upper()
            name = str(entry["title"]).strip()
            cik = int(entry["cik_str"])
        except (KeyError, ValueError, TypeError):
            continue

        # 1) CIK dedup — keep first (= higher market cap) ticker per entity
        if cik in seen_cik:
            continue

        # 2) Ticker pattern filter
        if not _is_valid_ticker(ticker):
            continue

        # 3) Company name filter
        if _EXCLUDE_NAME.search(name):
            continue

        seen_cik.add(cik)
        universe[ticker] = {"name": name, "cik": cik}

    if progress_callback:
        progress_callback(f"✅ 已获取 {len(universe):,} 只美股上市公司 (按市值排序)")

    # ── Save cache ───────────────────────────────────────────────────
    cache_payload = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "count": len(universe),
        "tickers": dict(universe),  # plain dict for JSON; order preserved in 3.7+
    }
    try:
        with open(UNIVERSE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache_payload, f, ensure_ascii=False)
    except OSError:
        pass

    return universe
