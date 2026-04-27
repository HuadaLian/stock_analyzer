"""Fetch and cache active operating-company symbols from FMP.

Primary universe endpoint ``available-traded/list`` is now legacy-gated for
many plans. This module therefore builds the universe via
``/stable/search-symbol`` prefix crawl and caches the result locally.

Filtering pipeline:
1. Symbol de-dup
2. Exchange / name-based non-company exclusion (ETF/fund/crypto/fx/etc)
3. Keep active symbols for downstream profile enrichment (country is filled in ETL)
"""

import json
import os
import re
import string
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime

from etl.sources.fmp import search_symbols
from core.instrument_policy import get_policy

BASE_DIR = os.path.dirname(__file__)
CACHE_DIR = os.path.join(BASE_DIR, "saved_tables")
UNIVERSE_FILE = os.path.join(CACHE_DIR, "global_active_universe.json")
_CACHE_MAX_AGE_DAYS = 7
_POLICY_VERSION = 2

_SEARCH_LIMIT = 500
_PREFIX_CHARS = string.ascii_uppercase + string.digits
_MAX_PREFIX_DEPTH = 3

# ── Ticker patterns for non-common-stock securities ──────────────────
_JUNK_TICKER = re.compile(r"[=/]", re.IGNORECASE)
_FOREIGN_MARKET_SUFFIX = re.compile(r"\.(SZ|SS|SH|HK|BJ)$", re.IGNORECASE)

_EXCLUDE_EXCHANGE = {
    "CRYPTO",
    "FOREX",
    "FX",
    "CCC",
}

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

# Prefer ordinary/common stock-like instruments. Keep ADR/ADS/OTC ordinary shares.
_EXCLUDE_PREFERRED_BY_SYMBOL = re.compile(
    # Typical preferred-share tickers: BRK-B, BAC-PRK, XYZ.PA
    r"-(?:PR|P|PS|PFD)[A-Z0-9]*$|\.P[A-Z0-9]*$",
    re.IGNORECASE,
)

_EXCLUDE_NON_COMMON_NAME = re.compile(
    r"\bPREFERRED\b"
    r"|\bPREFERENCE\b"
    r"|\bPREF\b"
    r"|\bCONVERTIBLE\b"
    r"|\bDEBENTURE\b"
    r"|\bSENIOR\s+NOTES?\b"
    r"|\bNOTES?\b"
    r"|\bBOND\b"
    r"|\bWARRANTS?\b"
    r"|\bRIGHTS?\b"
    r"|\bUNITS?\b"
    r"|\bTRUST\s+PREFERRED\b"
    r"|\bPERPETUAL\b",
    re.IGNORECASE,
)

_ADR_HINT = re.compile(r"\bADR\b|\bADS\b|\bAMERICAN\s+DEPOSITARY\b", re.IGNORECASE)


@dataclass(frozen=True)
class UniverseFilterPolicy:
    include_adr_otc: bool = True
    exclude_funds_etfs: bool = True
    exclude_preferred_bond_convertible: bool = True


def _filter_policy_from_mode(filter_mode: str | None) -> UniverseFilterPolicy:
    mode = (filter_mode or "ordinary_common_stock").strip().lower()
    p = get_policy(mode)
    return UniverseFilterPolicy(
        include_adr_otc=p.include_adr_otc,
        exclude_funds_etfs=p.exclude_funds_etfs,
        exclude_preferred_bond_convertible=p.exclude_preferred_bond_convertible,
    )


def _is_valid_ticker(ticker: str) -> bool:
    """Return True if the ticker looks like a common stock."""
    t = ticker.strip().upper()
    if not t or len(t) > 16:
        return False
    if _JUNK_TICKER.search(t):
        return False
    if _FOREIGN_MARKET_SUFFIX.search(t):
        return False
    return True


def _is_operating_company(item: dict, policy: UniverseFilterPolicy) -> bool:
    ex = str(item.get("exchange") or "").strip().upper()
    if policy.exclude_funds_etfs and ex in _EXCLUDE_EXCHANGE:
        return False

    name = str(item.get("name") or "").strip()
    if not name:
        return False
    if policy.exclude_funds_etfs and _EXCLUDE_NAME.search(name):
        return False
    if policy.exclude_preferred_bond_convertible:
        sym = str(item.get("symbol") or "").strip().upper()
        # Keep ADR/ADS explicitly even if name contains the hint.
        if not (policy.include_adr_otc and _ADR_HINT.search(name)):
            if _EXCLUDE_PREFERRED_BY_SYMBOL.search(sym):
                return False
            if _EXCLUDE_NON_COMMON_NAME.search(name):
                return False
    return True


def _crawl_symbol_prefixes(progress_callback=None) -> list[dict]:
    """Crawl symbols via search prefix expansion to avoid result truncation."""
    queue: list[str] = list(_PREFIX_CHARS)
    out: list[dict] = []
    seen_symbol: set[str] = set()
    calls = 0

    while queue:
        prefix = queue.pop(0)
        rows = search_symbols(prefix, limit=_SEARCH_LIMIT)
        calls += 1

        # If bucket is full, expand prefix depth-first until it stops truncating.
        if len(rows) >= _SEARCH_LIMIT and len(prefix) < _MAX_PREFIX_DEPTH:
            for ch in _PREFIX_CHARS:
                queue.append(prefix + ch)
            continue

        for r in rows:
            sym = str(r.get("symbol") or "").strip().upper()
            if not _is_valid_ticker(sym):
                continue
            if sym in seen_symbol:
                continue
            seen_symbol.add(sym)
            out.append({
                "symbol": sym,
                "name": str(r.get("name") or "").strip(),
                "exchange": str(r.get("exchange") or "").strip(),
                "exchange_full_name": str(r.get("exchangeFullName") or "").strip(),
                "currency": str(r.get("currency") or "").strip().upper() or None,
                # search-symbol payload has no country; ETL profile fills it.
                "country": None,
            })

        if calls % 50 == 0 and progress_callback:
            progress_callback(f"📡 FMP 抓取中：calls={calls}, 去重后符号={len(out):,}")

        # Soft throttling to be polite with API limits.
        time.sleep(0.01)

    return out


def fetch_us_universe(force_refresh=False, progress_callback=None, filter_mode: str | None = None):
    """Return OrderedDict {ticker: {name/exchange/country/...}} from FMP active symbols.

    Name kept for compatibility with existing callers.
    """
    mode = (filter_mode or get_policy().name)
    policy = _filter_policy_from_mode(mode)
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
                cached_mode = str(cached.get("filter_mode") or mode)
                cached_ver = int(cached.get("policy_version") or 0)
                if tickers and cached_mode == mode and cached_ver == _POLICY_VERSION:
                    # Restore insertion order
                    result = OrderedDict(
                        (k, v) for k, v in tickers.items()
                    )
                    if progress_callback:
                        progress_callback(f"✅ 从缓存加载 FMP 活跃公司列表 ({len(result):,} 只)")
                    return result
        except (json.JSONDecodeError, OSError, KeyError):
            pass

    # ── Fetch from FMP stable/search-symbol ──────────────────────────
    if progress_callback:
        progress_callback("📡 正在通过 FMP stable/search-symbol 抓取活跃股票...")

    raw_rows = _crawl_symbol_prefixes(progress_callback=progress_callback)

    universe: OrderedDict[str, dict] = OrderedDict()
    for row in sorted(raw_rows, key=lambda x: x.get("symbol", "")):
        if not _is_operating_company(row, policy):
            continue
        ticker = row["symbol"]
        universe[ticker] = {
            "name": row.get("name") or ticker,
            "exchange": row.get("exchange") or None,
            "exchange_full_name": row.get("exchange_full_name") or None,
            "currency": row.get("currency") or None,
            "country": row.get("country"),
            "source": "fmp_stable_search_symbol",
        }

    if progress_callback:
        progress_callback(f"✅ 已获取 {len(universe):,} 只 FMP 活跃公司股票")

    # ── Save cache ───────────────────────────────────────────────────
    cache_payload = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "source": "stable/search-symbol",
        "filter_mode": mode,
        "policy_version": _POLICY_VERSION,
        "count": len(universe),
        "tickers": dict(universe),  # plain dict for JSON; order preserved in 3.7+
    }
    try:
        with open(UNIVERSE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache_payload, f, ensure_ascii=False)
    except OSError:
        pass

    return universe
