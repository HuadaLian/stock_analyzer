# xbrl_parser.py
"""
Extract FCF components from SEC EDGAR XBRL (Company Facts API).

Priority chain:
  1. SEC XBRL Company Facts API  (structured JSON, no filing download needed)
  2. Fallback: yfinance cashflow  (if XBRL is unavailable)

The module is self-contained: give it a CIK and get back a clean FCF table.
"""

import json
import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime

# ═══════════════════════════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════════════════════════

DATA_DIR = os.path.join(os.path.dirname(__file__), "SEC_Data")

# XBRL tags we care about
_OCF_TAGS = [
    "NetCashProvidedByUsedInOperatingActivities",
    "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
]
_CAPEX_TAGS = [
    "PaymentsToAcquirePropertyPlantAndEquipment",
    "PaymentsToAcquireProductiveAssets",
]
_SHARES_TAGS = [
    "CommonStockSharesOutstanding",
    "WeightedAverageNumberOfShareOutstandingBasicAndDiluted",
    "WeightedAverageNumberOfDilutedSharesOutstanding",
    "EntityCommonStockSharesOutstanding",
]

_SEC_HEADERS = {
    "User-Agent": "ResearchApp lianhdff@gmail.com",
    "Accept": "application/json",
}


# ═══════════════════════════════════════════════════════════════════════
#  Cache helpers
# ═══════════════════════════════════════════════════════════════════════

def _facts_cache_path(cik: str) -> str:
    os.makedirs(DATA_DIR, exist_ok=True)
    return os.path.join(DATA_DIR, f"facts_{cik}.json")


def _load_cached_facts(cik: str):
    path = _facts_cache_path(cik)
    if os.path.exists(path):
        age_hours = (datetime.now().timestamp() - os.path.getmtime(path)) / 3600
        if age_hours < 24 * 7:  # cache 7 days
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    return None


def _save_facts_cache(cik: str, data: dict):
    path = _facts_cache_path(cik)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)


# ═══════════════════════════════════════════════════════════════════════
#  SEC XBRL Company Facts fetch
# ═══════════════════════════════════════════════════════════════════════

def fetch_company_facts(cik: str) -> dict:
    """Fetch the full Company Facts JSON from SEC EDGAR.

    Returns the raw JSON dict (us-gaap + dei facts).
    """
    cik = cik.lstrip("0").zfill(10)

    cached = _load_cached_facts(cik)
    if cached:
        return cached

    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    r = requests.get(url, headers=_SEC_HEADERS, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"SEC Company Facts API returned {r.status_code}")

    data = r.json()
    _save_facts_cache(cik, data)
    return data


# ═══════════════════════════════════════════════════════════════════════
#  Extract annual values from XBRL facts
# ═══════════════════════════════════════════════════════════════════════

def _extract_annual_values(facts: dict, tag_list: list, namespace="us-gaap"):
    """Search for the first matching tag and return {fiscal_year_end: value}.

    Only picks 10-K / 20-F annual filings (full-year, not quarterly).
    """
    ns = facts.get("facts", {}).get(namespace, {})

    for tag in tag_list:
        concept = ns.get(tag)
        if not concept:
            continue
        units = concept.get("units", {})
        # pick USD for money, shares for share counts
        series = units.get("USD") or units.get("shares") or units.get("pure")
        if not series:
            # try first available unit
            series = next(iter(units.values()), None)
        if not series:
            continue

        result = {}
        for item in series:
            form = item.get("form", "")
            if form not in ("10-K", "20-F", "10-KT"):
                continue
            fy = item.get("fy")
            fp = item.get("fp", "")
            if fp not in ("FY", ""):
                continue
            end_date = item.get("end")
            val = item.get("val")
            if val is None or fy is None:
                continue
            # Keep the latest filing for each fiscal year
            key = int(fy)
            if key not in result or end_date > result[key]["end"]:
                result[key] = {"end": end_date, "val": float(val)}

        if result:
            return {k: v["val"] for k, v in result.items()}, tag

    return {}, None


def _extract_quarterly_values(facts: dict, tag_list: list, namespace="us-gaap"):
    """Extract individual quarterly (10-Q) values for the current year."""
    ns = facts.get("facts", {}).get(namespace, {})
    current_year = datetime.now().year

    for tag in tag_list:
        concept = ns.get(tag)
        if not concept:
            continue
        units = concept.get("units", {})
        series = units.get("USD") or units.get("shares") or units.get("pure")
        if not series:
            series = next(iter(units.values()), None)
        if not series:
            continue

        result = {}
        for item in series:
            form = item.get("form", "")
            if form != "10-Q":
                continue
            fy = item.get("fy")
            fp = item.get("fp", "")
            end_date = item.get("end", "")
            val = item.get("val")
            if val is None:
                continue
            # Only latest year's quarters
            if fy and int(fy) >= current_year - 1:
                key = f"{fy}-{fp}"
                if key not in result or end_date > result[key]["end"]:
                    result[key] = {"end": end_date, "val": float(val), "fy": fy, "fp": fp}

        if result:
            return result, tag

    return {}, None


def _extract_shares_annual(facts: dict):
    """Get shares outstanding by fiscal year, trying multiple tags."""
    # Try dei namespace first (EntityCommonStockSharesOutstanding)
    for ns in ["dei", "us-gaap"]:
        tags = _SHARES_TAGS if ns == "us-gaap" else ["EntityCommonStockSharesOutstanding"]
        vals, _ = _extract_annual_values(facts, tags, namespace=ns)
        if vals:
            return vals

    # Also try us-gaap tags in 10-Q forms to get a wider range
    ns_data = facts.get("facts", {}).get("us-gaap", {})
    for tag in _SHARES_TAGS:
        concept = ns_data.get(tag)
        if not concept:
            continue
        units = concept.get("units", {})
        series = units.get("shares")
        if not series:
            continue
        result = {}
        for item in series:
            fy = item.get("fy")
            form = item.get("form", "")
            if form not in ("10-K", "20-F", "10-KT", "10-Q"):
                continue
            end_date = item.get("end", "")
            val = item.get("val")
            if val is None or fy is None:
                continue
            key = int(fy)
            if key not in result or end_date > result[key]["end"]:
                result[key] = {"end": end_date, "val": float(val)}
        if result:
            return {k: v["val"] for k, v in result.items()}

    return {}


# ═══════════════════════════════════════════════════════════════════════
#  Main: build FCF table from XBRL
# ═══════════════════════════════════════════════════════════════════════

def get_fcf_from_xbrl(cik: str):
    """Return a dict compatible with the data_provider format.

    Keys: fcf_table, fcf_per_share_by_year, latest_fcf, avg_fcf_5y,
          quarterly_info (str summary of available 10-Q data).
    """
    result = {
        "fcf_table": pd.DataFrame(),
        "fcf_per_share_by_year": {},
        "latest_fcf": None,
        "avg_fcf_5y": None,
        "source": "XBRL",
    }

    try:
        facts = fetch_company_facts(cik)
    except Exception:
        result["source"] = "XBRL_FAILED"
        return result

    # --- Annual OCF ---------------------------------------------------
    ocf_by_year, ocf_tag = _extract_annual_values(facts, _OCF_TAGS)
    if not ocf_by_year:
        result["source"] = "XBRL_NO_OCF"
        return result

    # --- Annual CapEx -------------------------------------------------
    capex_by_year, capex_tag = _extract_annual_values(facts, _CAPEX_TAGS)

    # --- Shares -------------------------------------------------------
    shares_by_year = _extract_shares_annual(facts)

    # --- Build rows ---------------------------------------------------
    all_years = sorted(set(ocf_by_year.keys()) | set(capex_by_year.keys()), reverse=True)

    rows = []
    for yr in all_years:
        ocf = ocf_by_year.get(yr)
        capex = capex_by_year.get(yr)
        fcf = (ocf - capex) if (ocf is not None and capex is not None) else None
        shares = shares_by_year.get(yr)
        fcf_ps = (fcf / shares) if (fcf is not None and shares and shares > 0) else None

        rows.append({
            "年份": str(yr),
            "OCF": ocf,
            "CapEx": capex,
            "FCF": fcf,
            "总股本": shares,
            "每股FCF": fcf_ps,
        })
        if fcf_ps is not None:
            # Use a Timestamp key for compatibility with compute_dcf_lines
            result["fcf_per_share_by_year"][pd.Timestamp(f"{yr}-12-31")] = fcf_ps

    # --- 3-year & 5-year rolling average per-share FCF -----------------
    # rows are sorted descending, so i:i+N gives current year + older years
    for i, row in enumerate(rows):
        w3 = rows[i: i + 3]
        w5 = rows[i: i + 5]
        ps3 = [r["每股FCF"] for r in w3 if r["每股FCF"] is not None]
        ps5 = [r["每股FCF"] for r in w5 if r["每股FCF"] is not None]
        row["3年均每股FCF"] = np.mean(ps3) if ps3 else None
        row["5年均每股FCF"] = np.mean(ps5) if ps5 else None

    tbl = pd.DataFrame(rows)
    result["fcf_table"] = tbl

    # --- Aggregate metrics --------------------------------------------
    fcf_vals = [r["FCF"] for r in rows if r["FCF"] is not None]
    if fcf_vals:
        result["latest_fcf"] = fcf_vals[0]
        n = min(5, len(fcf_vals))
        result["avg_fcf_5y"] = sum(fcf_vals[:n]) / n

    # --- Quarterly snapshot -------------------------------------------
    q_ocf, _ = _extract_quarterly_values(facts, _OCF_TAGS)
    if q_ocf:
        parts = []
        for k in sorted(q_ocf.keys()):
            v = q_ocf[k]
            parts.append(f"{v['fp']}: OCF={v['val']/1e6:.0f}M")
        result["quarterly_info"] = " | ".join(parts)

    result["source"] = f"XBRL ({ocf_tag})"
    return result
