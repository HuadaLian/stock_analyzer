# gemini_chat.py
"""Gemini-powered chat & LLM FCF filler for SEC / CN filings."""

import os
import re
import json
import pandas as pd
import numpy as np
from google import genai
from google.genai import types
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
import warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

BASE_DIR = os.path.dirname(__file__)
RULES_PATH = os.path.join(BASE_DIR, "prompts", "fcf_extraction_rules.txt")

# Available free-tier models from Google AI Studio (April 2026)
# Organized by category matching the rate-limit dashboard
MODELS = {
    # ── Gemma 4 系列 (主力, unlimited TPM) ──────────────────────────
    "gemma-4-31b-it":        "Gemma 4 31B",
    "gemma-4-26b-it":        "Gemma 4 26B",
    # ── Gemma 3 系列 (仅保留 27B) ───────────────────────────────────
    "gemma-3-27b-it":        "Gemma 3 27B",
    # ── Gemini 系列 (最后备用, 便宜优先) ────────────────────────────
    "gemini-2.0-flash-lite": "Gemini 2 Flash Lite",
    "gemini-2.5-flash-lite": "Gemini 2.5 Flash Lite",
    "gemini-3.1-flash-lite": "Gemini 3.1 Flash Lite",
    "gemini-2.0-flash":      "Gemini 2 Flash",
    "gemini-3-flash":        "Gemini 3 Flash",
    "gemini-2.5-flash":      "Gemini 2.5 Flash",
    "gemini-3.1-pro":        "Gemini 3.1 Pro",
    "gemini-2.5-pro":        "Gemini 2.5 Pro",
}

# Rate limits per model (RPM, TPM, RPD) — paid Google AI Studio tier
# tpm = -1 means unlimited, tpm/rpm/rpd = 0 means no quota available
MODEL_RATE_LIMITS = {
    # ── Gemma 4 (paid: unlimited TPM) ───────────────────────────────
    "gemma-4-31b-it":        {"rpm": 30, "tpm": -1,       "rpd": 14_400, "category": "Gemma4"},
    "gemma-4-26b-it":        {"rpm": 30, "tpm": -1,       "rpd": 14_400, "category": "Gemma4"},
    # ── Gemma 3 27B (paid) ──────────────────────────────────────────
    "gemma-3-27b-it":        {"rpm": 60, "tpm": 30_000,   "rpd": 14_400, "category": "Gemma3"},
    # ── Gemini (paid, cheapest → most expensive) ─────────────────────
    "gemini-2.0-flash-lite": {"rpm": 30, "tpm": 1_000_000, "rpd": 1_500, "category": "Gemini"},
    "gemini-2.5-flash-lite": {"rpm": 30, "tpm": 1_000_000, "rpd": 1_500, "category": "Gemini"},
    "gemini-3.1-flash-lite": {"rpm": 30, "tpm": 1_000_000, "rpd": 1_500, "category": "Gemini"},
    "gemini-2.0-flash":      {"rpm": 30, "tpm": 1_000_000, "rpd": 1_500, "category": "Gemini"},
    "gemini-3-flash":        {"rpm": 15, "tpm": 1_000_000, "rpd": 1_500, "category": "Gemini"},
    "gemini-2.5-flash":      {"rpm": 15, "tpm": 1_000_000, "rpd": 1_500, "category": "Gemini"},
    "gemini-3.1-pro":        {"rpm": 10, "tpm": 1_000_000, "rpd": 1_500, "category": "Gemini"},
    "gemini-2.5-pro":        {"rpm": 10, "tpm": 1_000_000, "rpd": 1_500, "category": "Gemini"},
}

# Default enabled models for rotation
# Priority: Gemma 4 first → Gemma 3 27B → Gemini cheap→expensive
DEFAULT_ENABLED_MODELS = [
    # ── Gemma 4 — 主力 (unlimited TPM) ──────────────────────────────
    "gemma-4-31b-it",        # 主力: Gemma 4 最大
    "gemma-4-26b-it",        # 备用: Gemma 4 较大
    # ── Gemma 3 27B — 先于所有 Gemini ───────────────────────────────
    "gemma-3-27b-it",
    # ── Gemini — 最后备用, 便宜优先 ─────────────────────────────────
    "gemini-2.0-flash-lite", # 最便宜
    "gemini-2.5-flash-lite",
    "gemini-3.1-flash-lite",
    "gemini-2.0-flash",
    "gemini-3-flash",
    "gemini-2.5-flash",
    "gemini-3.1-pro",
    "gemini-2.5-pro",        # 最贵
]

# ── Model call status — persisted to disk ─────────────────────────────
_MODEL_STATUS_FILE = os.path.join(BASE_DIR, "saved_tables", "model_status.json")
_RATE_LIMIT_COOLDOWN_AFTER_N = 2
_RATE_LIMIT_COOLDOWN_SECONDS = 1800     # 30-min hard cooldown after 2 consecutive 429s
_RATE_LIMIT_SOFT_COOLDOWN_SECONDS = 600  # 10-min soft cooldown after first 429


def _reset_model_status_for_new_day(info: dict, today: str):
    """Reset per-day counters/state when calendar day changes."""
    info["calls_today"] = 0
    info["tokens_today"] = 0
    info["consecutive_429"] = 0
    info["cooldown_until"] = 0
    info["date"] = today
    if info.get("status") in ("rate_limited", "cooldown"):
        info["status"] = "unused"
        info["detail"] = "⬜ 新的一天，状态已重置"


def _load_model_status() -> dict:
    """Load persisted model call status from disk."""
    try:
        with open(_MODEL_STATUS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Reset daily counters if the date changed
        today = pd.Timestamp.now().strftime("%Y-%m-%d")
        for mid, info in data.items():
            if not isinstance(info, dict):
                continue
            if info.get("date") != today:
                _reset_model_status_for_new_day(info, today)
            # Backward-compatible defaults for older status files
            info.setdefault("calls_today", 0)
            info.setdefault("tokens_today", 0)
            info.setdefault("consecutive_429", 0)
            info.setdefault("cooldown_until", 0)
        return data
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        return {}


def _save_model_status():
    """Persist model call status to disk."""
    os.makedirs(os.path.dirname(_MODEL_STATUS_FILE), exist_ok=True)
    try:
        today = pd.Timestamp.now().strftime("%Y-%m-%d")
        for _mid, _info in _model_call_status.items():
            if isinstance(_info, dict) and not _info.get("date"):
                _info["date"] = today
        with open(_MODEL_STATUS_FILE, "w", encoding="utf-8") as f:
            json.dump(_model_call_status, f, indent=2, ensure_ascii=False)
    except OSError:
        pass


# Load on module import
_model_call_status: dict = _load_model_status()


def get_model_call_status() -> dict:
    """Return a copy of the per-model call status dict."""
    return dict(_model_call_status)


def reset_model_call_status():
    """Clear all tracked model call status and delete file."""
    _model_call_status.clear()
    try:
        os.remove(_MODEL_STATUS_FILE)
    except OSError:
        pass


def _record_model_ok(model_id: str, elapsed: float, chars: int, est_tokens: int):
    """Record a successful call."""
    import time as _t
    today = pd.Timestamp.now().strftime("%Y-%m-%d")
    prev = _model_call_status.get(model_id, {})
    _model_call_status[model_id] = {
        "status": "ok", "time": _t.time(), "date": today,
        "detail": f"✅ {elapsed:.1f}s · {chars:,} 字符",
        "calls_today": prev.get("calls_today", 0) + 1,
        "tokens_today": prev.get("tokens_today", 0) + est_tokens,
        "consecutive_429": 0,
        "cooldown_until": 0,
    }
    _save_model_status()


def _record_model_rate_limited(model_id: str, elapsed: float, err_msg: str):
    """Record a rate-limit (429) hit."""
    import time as _t
    today = pd.Timestamp.now().strftime("%Y-%m-%d")
    prev = _model_call_status.get(model_id, {})
    consec = int(prev.get("consecutive_429", 0)) + 1
    now_ts = _t.time()
    is_cooldown = consec >= _RATE_LIMIT_COOLDOWN_AFTER_N
    if is_cooldown:
        cooldown_until = int(now_ts + _RATE_LIMIT_COOLDOWN_SECONDS)
        cooldown_ts = pd.to_datetime(cooldown_until, unit="s").strftime("%H:%M:%S")
        detail = f"🧊 连续429={consec}，冷却至 {cooldown_ts}"
        status = "cooldown"
    else:
        # soft cooldown: skip this model for a short window even on first 429
        cooldown_until = int(now_ts + _RATE_LIMIT_SOFT_COOLDOWN_SECONDS)
        detail = f"⚠️ 429 限流 ({elapsed:.1f}s, 连续{consec})，软冷却90s"
        status = "rate_limited"
    _model_call_status[model_id] = {
        "status": status, "time": now_ts, "date": today,
        "detail": detail,
        "calls_today": prev.get("calls_today", 0),
        "tokens_today": prev.get("tokens_today", 0),
        "consecutive_429": consec,
        "cooldown_until": cooldown_until,
    }
    _save_model_status()


def _record_model_error(model_id: str, elapsed: float, err_msg: str):
    """Record a non-rate-limit error."""
    import time as _t
    today = pd.Timestamp.now().strftime("%Y-%m-%d")
    prev = _model_call_status.get(model_id, {})
    # Treat explicit NOT_FOUND / 404 as a permanent unavailability for today
    em = (err_msg or "").lower()
    if "not found" in em or "404" in em or "not_found" in em:
        status = "not_found"
        # avoid retrying the same invalid model for 24h
        cooldown_until = int(_t.time() + 86400)
        detail = f"❌ NOT_FOUND: {err_msg[:120]}"
    else:
        status = "error"
        cooldown_until = 0
        detail = f"❌ {err_msg[:120]}"

    _model_call_status[model_id] = {
        "status": status, "time": _t.time(), "date": today,
        "detail": detail,
        "calls_today": prev.get("calls_today", 0),
        "tokens_today": prev.get("tokens_today", 0),
        "consecutive_429": 0,
        "cooldown_until": cooldown_until,
    }
    _save_model_status()


# ── Model priority & token budget ─────────────────────────────────────

# Explicit rotation rank — lower number = tried first
# Gemma 4 → Gemma 3 27B → Gemini cheap→expensive
_MODEL_CAPABILITY_RANK = {
    # ── Gemma 4 — 主力 ───────────────────────────────────────────────
    "gemma-4-31b-it":        0,
    "gemma-4-26b-it":        1,
    # ── Gemma 3 27B ─────────────────────────────────────────────────
    "gemma-3-27b-it":        2,
    # ── Gemini — 便宜优先 ────────────────────────────────────────────
    "gemini-2.0-flash-lite": 3,
    "gemini-2.5-flash-lite": 4,
    "gemini-3.1-flash-lite": 5,
    "gemini-2.0-flash":      6,
    "gemini-3-flash":        7,
    "gemini-2.5-flash":      8,
    "gemini-3.1-pro":        9,
    "gemini-2.5-pro":        10,
}


def _model_priority_key(model_id: str):
    """Sort key: lower = higher capability. Unknown models go last."""
    return (_MODEL_CAPABILITY_RANK.get(model_id, 99), model_id)


def _sort_models_by_priority(models: list) -> list:
    """Sort models: gemini first, then gemma-4, then gemma-3."""
    return sorted(models, key=_model_priority_key)


def _get_model_token_budget(model_id: str) -> int:
    """Max input tokens per request for a model, based on TPM.

    - Gemini: keep 900K budget for batch extraction
    - Gemma 4: cap at ~240K (API hard limit is around 262K)
    - Gemma 3 (15K TPM): 12K budget (single year, truncated)
    - No quota: 0
    """
    if model_id.startswith("gemma-4"):
        # Allow Gemma-4 to handle larger requests; API hard limit ≈262k
        return 240_000
    if model_id.startswith("gemma-3"):
        # Paid tier: 30K TPM → 24K budget (80%)
        return 24_000

    lim = MODEL_RATE_LIMITS.get(model_id, {})
    tpm = lim.get("tpm", 0)
    rpd = lim.get("rpd", 0)
    if tpm == 0 and rpd == 0:
        return 0
    if tpm == -1 or tpm >= 200_000:
        return _MAX_TOKENS_PER_REQUEST
    return int(tpm * 0.8)


def _normalize_model_id(model_name: str) -> str:
    """Normalize user-provided model names to known model ids.

    Accepts common misspellings or human-friendly names like
    "gemme 4 26b" or "Gemma 4 26B" and returns the canonical id
    defined in `MODELS` (e.g. "gemma-4-26b-it"). If no mapping
    is found, returns the original input.
    """
    if not model_name:
        return model_name
    m = model_name.strip().lower()
    # Fix common typos
    m = m.replace("gemme", "gemma")
    m = m.replace(" ", "-")
    m = m.replace("..", "-")

    # Direct prefix match
    for k in MODELS.keys():
        if k.startswith(m) or k == m:
            return k

    # Pattern-based heuristics
    if "4" in m and ("26" in m or "26b" in m):
        return "gemma-4-26b-it"
    if "4" in m and ("31" in m or "31b" in m):
        return "gemma-4-31b-it"
    if "2.5" in m and "pro" in m:
        return "gemini-2.5-pro"
    if "2.5" in m and "flash" in m:
        return "gemini-2.5-flash"

    return model_name


def _is_model_available(model_id: str) -> bool:
    """Check if model is likely available (not rate-limited today)."""
    import time as _t
    info = _model_call_status.get(model_id)
    if not info:
        return True
    today = pd.Timestamp.now().strftime("%Y-%m-%d")
    if info.get("date") != today:
        _reset_model_status_for_new_day(info, today)
        _save_model_status()
        return True

    cooldown_until = int(info.get("cooldown_until", 0) or 0)
    if cooldown_until > int(_t.time()):
        return False
    if info.get("status") in ("cooldown", "rate_limited") and cooldown_until <= int(_t.time()):
        info["status"] = "unused"
        info["detail"] = "⬜ 冷却结束，可重试"
        info["consecutive_429"] = 0
        info["cooldown_until"] = 0
        _save_model_status()
        return True

    if info.get("status") == "rate_limited":
        # Check if RPD is exhausted
        lim = MODEL_RATE_LIMITS.get(model_id, {})
        rpd = lim.get("rpd", 0)
        if rpd > 0 and info.get("calls_today", 0) >= rpd:
            return False  # daily limit likely exhausted
    return True

SYSTEM_PROMPT = (
    "你是一个专业的财务分析师助手。用户已经提供了一些SEC年报(10-K/20-F)或季报(10-Q/6-K)"
    "以及A股年报的完整内容。请基于这些报告内容准确回答用户的问题。"
    "当引用具体数据时，请注明来源报告。如果报告中没有相关信息，请明确告知用户。"
    "用中文回答。"
)

FCF_SYSTEM_PROMPT = (
    "You are a financial data extraction expert. "
    "You read annual reports (10-K, 20-F, or Chinese 年报) and extract "
    "exact numerical values for cash flow items. "
    "\n\n"
    "★★★ CRITICAL RULE — UNIT DECLARATIONS ★★★\n"
    "Before extracting ANY number from ANY financial table or statement, you MUST FIRST:\n"
    "  1) Look at the TOP of the table/statement for a unit declaration line, e.g.:\n"
    "     'in thousands', 'in millions', 'in billions',\n"
    "     '(Amounts in thousands of RMB)', '单位：千元', '单位：万元', '单位：百万元'\n"
    "  2) Also check the column headers and footnotes for unit hints.\n"
    "  3) MULTIPLY every number you extract by the declared unit to get the ACTUAL value.\n"
    "     e.g. if table says 'in thousands' and you see 1,234,567 → actual = 1,234,567,000.\n"
    "  4) Different tables in the SAME report may use DIFFERENT units! Check each table separately.\n"
    "\n"
    "Always return valid JSON. Never include commentary outside the JSON."
)

# CN-specific system prompt (Chinese, A-share annual reports)
FCF_SYSTEM_PROMPT_CN = (
    "你是一名专业的A股财务数据提取专家，熟悉中国上市公司年度报告（年报）的结构与会计准则。\n\n"

    "【核心任务】\n"
    "从A股上市公司年度报告的【合并现金流量表】中精确提取三项数据：OCF、CapEx、FCF。\n\n"

    "【字段定义】\n"
    "• OCF   = 经营活动产生的现金流量净额\n"
    "• CapEx = 购建固定资产、无形资产和其他长期资产支付的现金（取正值）\n"
    "• FCF   = OCF − CapEx\n\n"

    "【单位换算规则（关键）】\n"
    "提取任何数字前，必须先定位该张报表的单位声明：\n"
    "  单位：元 / 人民币元      → ×1\n"
    "  单位：千元               → ×1,000\n"
    "  单位：万元               → ×10,000\n"
    "  单位：百万元             → ×1,000,000\n"
    "  单位：亿元               → ×100,000,000\n"
    "示例：单位：万元，数值 45,678.9 → 实际 = 456,789,000 元\n"
    "同一份年报中不同报表的单位可能不同，每张表必须单独核查。\n\n"

    "【A股年报结构要点】\n"
    "• 年报财务报表章节通常在第十节/第十一节『财务报告』或『财务报表』中\n"
    "• 若同时存在『合并现金流量表』与『母公司现金流量表』，只取【合并】数据\n"
    "• 括号数值表示负数：(1,234,567) = -1,234,567\n"
    "• CapEx 在现金流量表中本为负号的现金流出，提取时须取绝对值（返回正数）\n"
    "• 只使用年度（全年 12-31）数据，不要使用半年报或季报中间期数据\n\n"

    "【质量控制】\n"
    "• FCF = OCF - CapEx，若报表给出 FCF 请核对是否一致\n"
    "• CapEx 不能为零（所有实体企业均有资本支出）\n"
    "• 返回值必须为换算后的人民币元（CNY）实际金额\n\n"

    "必须返回合法 JSON 数组，JSON 之外不得有任何说明文字或 markdown 代码块标记。"
)


def _is_gemma_model(model_id: str) -> bool:
    """Return True if the model is any Gemma variant."""
    return (model_id or "").startswith("gemma-")

_DEFAULT_RULES = """## 提取字段
1. OCF: "Net cash provided by operating activities" / "经营活动产生的现金流量净额"
2. CapEx: "Purchases of property and equipment" / "购建固定资产..." — 返回正数
3. FCF = OCF - CapEx
"""


def load_fcf_rules() -> str:
    """Load FCF extraction rules from file, or return default."""
    if os.path.exists(RULES_PATH):
        with open(RULES_PATH, "r", encoding="utf-8") as f:
            return f.read()
    return _DEFAULT_RULES


def save_fcf_rules(text: str):
    """Save FCF extraction rules to file."""
    os.makedirs(os.path.dirname(RULES_PATH), exist_ok=True)
    with open(RULES_PATH, "w", encoding="utf-8") as f:
        f.write(text)


# ═══════════════════════════════════════════════════════════════════════
#  Text extraction
# ═══════════════════════════════════════════════════════════════════════

def extract_text(file_path: str) -> str:
    """Extract readable text from HTM/HTML/TXT/PDF."""
    ext = os.path.splitext(file_path)[1].lower()

    if ext in (".htm", ".html"):
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            html = f.read()
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "meta", "link"]):
            tag.decompose()
        text = soup.get_text(separator="\n", strip=True)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text

    if ext == ".pdf":
        import fitz  # pymupdf
        doc = fitz.open(file_path)
        pages = []
        for page in doc:
            pages.append(page.get_text())
        doc.close()
        return "\n".join(pages)

    if ext == ".txt":
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()

    return f"[不支持的文件格式: {ext}]"


def estimate_tokens(text: str) -> int:
    """Rough token estimate (~4 chars/token for mixed EN/CN)."""
    return len(text) // 4


# ═══════════════════════════════════════════════════════════════════════
#  List available filings
# ═══════════════════════════════════════════════════════════════════════

def list_sec_filings(ticker: str) -> list:
    """List all downloaded SEC filings for a US ticker."""
    results = []
    sec_dir = os.path.join(BASE_DIR, "SEC_Filings", ticker.upper())
    if not os.path.isdir(sec_dir):
        return results

    for form_type in sorted(os.listdir(sec_dir)):
        form_dir = os.path.join(sec_dir, form_type)
        if not os.path.isdir(form_dir):
            continue
        for fname in sorted(os.listdir(form_dir), reverse=True):
            fpath = os.path.join(form_dir, fname)
            if not os.path.isfile(fpath):
                continue
            if not fname.endswith((".htm", ".html", ".txt")):
                continue
            # Skip exhibit files and cached extracts
            if "_EX_" in fname or "_financial" in fname:
                continue
            # Filename formats:
            #   new: {report_date}_{filing_date}_{form}_{doc}  (2×YYYY-MM-DD prefix)
            #   old: {report_date}_{form}_{doc}                (1×YYYY-MM-DD prefix)
            date_str = fname[:10] if len(fname) >= 10 else ""
            filing_date = ""
            if (len(fname) > 21 and fname[10] == "_"
                    and re.match(r"^\d{4}-\d{2}-\d{2}$", fname[11:21])):
                filing_date = fname[11:21]
            results.append({
                "label": f"[{form_type}] {date_str}",
                "path": fpath,
                "form": form_type,
                "date": date_str,
                "filing_date": filing_date,
                "is_annual": form_type in ("10-K", "20-F"),
            })
    return results


def list_cn_filings(code: str) -> list:
    """List all downloaded CN filings for an A-share code."""
    results = []
    cn_dir = os.path.join(BASE_DIR, "CN_Filings", code)
    if not os.path.isdir(cn_dir):
        return results

    for fname in sorted(os.listdir(cn_dir), reverse=True):
        fpath = os.path.join(cn_dir, fname)
        if not os.path.isfile(fpath):
            continue
        if not fname.endswith((".pdf", ".htm", ".html", ".txt")):
            continue
        # Skip cached extracts
        if "_financial" in fname:
            continue
        is_annual = "年度报告" in fname or "年报" in fname
        results.append({
            "label": fname,
            "path": fpath,
            "form": "年报" if is_annual else "季报/半年报",
            "is_annual": is_annual,
        })
    return results


def list_hk_filings(code: str) -> list:
    """List all downloaded HK filings for a Hong Kong stock code."""
    results = []
    hk_dir = os.path.join(BASE_DIR, "HK_Filings", code)
    if not os.path.isdir(hk_dir):
        return results

    for fname in sorted(os.listdir(hk_dir), reverse=True):
        fpath = os.path.join(hk_dir, fname)
        if not os.path.isfile(fpath):
            continue
        if not fname.endswith((".pdf", ".htm", ".html", ".txt")):
            continue
        if "_financial" in fname:
            continue
        is_annual = (
            "年度报告" in fname or "年报" in fname
            or "annual" in fname.lower()
        )
        results.append({
            "label": fname,
            "path": fpath,
            "form": "年报" if is_annual else "中期报告",
            "is_annual": is_annual,
        })
    return results


# ═══════════════════════════════════════════════════════════════════════
#  Gemini chat session
# ═══════════════════════════════════════════════════════════════════════

def init_chat(api_key: str, model_name: str, filing_texts: list):
    """Create a Gemini chat session with filing context.

    filing_texts: list of (label, text) tuples.
    Returns: (client, chat) tuple.
    """
    # Normalize model id (tolerate human-friendly names / typos)
    model_name = _normalize_model_id(model_name)
    client = genai.Client(api_key=api_key)

    # Build the context message with all selected filings
    parts = []
    for label, text in filing_texts:
        parts.append(f"{'=' * 60}\n📄 {label}\n{'=' * 60}\n{text}")
    context_msg = "以下是用户选择的报告全文内容:\n\n" + "\n\n".join(parts)

    chat = client.chats.create(
        model=model_name,
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT,
        ),
        history=[
            types.Content(role="user", parts=[types.Part(text=context_msg)]),
            types.Content(role="model", parts=[types.Part(text=(
                f"我已经仔细阅读了你提供的 {len(filing_texts)} 份报告。"
                "请随时向我提问，我会基于报告内容为你详细解答。"
            ))]),
        ],
    )
    return chat


def send_message(chat, message: str) -> str:
    """Send a user message and return the model response."""
    response = chat.send_message(message)
    return response.text


# ═══════════════════════════════════════════════════════════════════════
#  Financial section extraction (token-efficient)
# ═══════════════════════════════════════════════════════════════════════

# Keywords that indicate financial data sections
_FIN_KEYWORDS_EN = [
    "cash flow", "operating activities", "investing activities",
    "financing activities", "capital expenditure", "free cash flow",
    "net cash", "depreciation", "amortization", "balance sheet",
    "total assets", "total liabilities", "shareholders",
    "shares outstanding", "revenue", "net income", "operating income",
    "financial statements", "consolidated statements",
]
_FIN_KEYWORDS_CN = [
    "现金流", "经营活动", "投资活动", "筹资活动",
    "资本支出", "自由现金流", "净额", "折旧", "摊销",
    "资产负债", "总资产", "负债合计", "股东权益",
    "营业收入", "净利润", "合并报表",
    # 注意：不含 "总股本"，避免提取股本数据并触发额外验证请求
]


def extract_financial_sections(file_path: str) -> str:
    """Extract only financial-data-relevant sections from a filing.

    For HTM: finds tables and paragraphs containing financial keywords.
    For PDF:
      - CN filings (path contains CN_Filings) → pdfplumber (better CJK + table extraction)
      - US/HK filings → PyMuPDF (faster, sufficient for English text)
    Returns a much smaller text than the full filing.
    """
    ext = os.path.splitext(file_path)[1].lower()
    all_kw = _FIN_KEYWORDS_EN + _FIN_KEYWORDS_CN

    if ext in (".htm", ".html"):
        return _extract_fin_sections_html(file_path, all_kw)
    elif ext == ".pdf":
        # Detect CN filing by path (CN_Filings directory)
        norm_path = os.path.normpath(file_path).replace("\\", "/")
        _is_cn_filing = "CN_Filings" in norm_path or "cn_filings" in norm_path.lower()
        if _is_cn_filing:
            return _extract_fin_sections_pdf_plumber(file_path, all_kw)
        return _extract_fin_sections_pdf(file_path, all_kw)
    else:
        return extract_text(file_path)


def _extract_fin_sections_html(file_path: str, keywords: list) -> str:
    """Extract financial tables and surrounding context from HTML filings."""
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        html = f.read()
    # lxml is ~10x faster than html.parser for large SEC filings
    try:
        soup = BeautifulSoup(html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "meta", "link"]):
        tag.decompose()

    sections = []

    # Strategy 1: Find all tables (SEC filings put financials in tables)
    for table in soup.find_all("table"):
        table_text = table.get_text(separator=" ", strip=True)
        table_lower = table_text.lower()
        # Check if table has financial content
        hits = sum(1 for kw in keywords if kw in table_lower)
        if hits >= 2 or len(table_text) > 200:
            # Include preceding heading/paragraph for context
            prev = table.find_previous(["h1", "h2", "h3", "h4", "p", "b", "strong"])
            heading = prev.get_text(strip=True) if prev else ""
            sections.append(f"[Table heading: {heading}]\n{table_text}")

    # Strategy 2: If tables don't cover enough, also grab keyword paragraphs
    if len(sections) < 3:
        for p in soup.find_all(["p", "div"]):
            text = p.get_text(strip=True)
            if len(text) < 50:
                continue
            text_lower = text.lower()
            if any(kw in text_lower for kw in keywords):
                sections.append(text)

    result = "\n\n---\n\n".join(sections)
    # If extraction is too small, fall back to full text (truncated)
    if len(result) < 1000:
        full = soup.get_text(separator="\n", strip=True)
        result = full[:500000]  # ~125k tokens max
    return result


def split_html_into_chapters(file_path: str) -> dict:
    """Split an HTML filing into topical chapters based on headings.

    Returns mapping {slug: text}. Slug is short and safe for filenames.
    """
    from pathlib import Path
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        html = f.read()
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "meta", "link"]):
        tag.decompose()

    chapters = []
    # Find headings and capture following nodes until next heading
    headings = soup.find_all(["h1", "h2", "h3", "h4", "h5"])
    if not headings:
        # fallback: one chapter with full text
        return {"full": soup.get_text(separator="\n", strip=True)}

    for i, h in enumerate(headings):
        title = h.get_text(strip=True)
        parts = []
        # include the heading
        parts.append(title)
        node = h.next_sibling
        while node:
            if getattr(node, "name", None) and node.name in ("h1", "h2", "h3", "h4", "h5"):
                break
            text = node.get_text(strip=True) if hasattr(node, "get_text") else str(node)
            if text:
                parts.append(text)
            node = node.next_sibling
        chapters.append((title, "\n\n".join(parts)))

    # Build slugs and return dict
    out = {}
    base = Path(file_path).stem
    for idx, (title, text) in enumerate(chapters, start=1):
        slug = re.sub(r"[^0-9A-Za-z_-]", "_", title.strip())[:60] or f"sec{idx}"
        key = f"{base}_sec{idx}_{slug}"
        out[key] = text
    return out


def _extract_fin_sections_pdf(file_path: str, keywords: list) -> str:
    """Extract pages from PDF that contain financial keywords."""
    import fitz
    doc = fitz.open(file_path)
    relevant_pages = []

    for page_num in range(len(doc)):
        page = doc[page_num]
        text = page.get_text()
        text_lower = text.lower()
        hits = sum(1 for kw in keywords if kw in text_lower)
        if hits >= 2:
            relevant_pages.append(f"[Page {page_num + 1}]\n{text}")

    doc.close()

    if relevant_pages:
        return "\n\n---\n\n".join(relevant_pages)
    # Fallback: return all text (truncated)
    doc = fitz.open(file_path)
    all_text = "\n".join(page.get_text() for page in doc)
    doc.close()
    return all_text[:500000]


def _extract_fin_sections_pdf_plumber(file_path: str, keywords: list) -> str:
    """Extract financial statement pages from a CN annual report PDF using pdfplumber.

    Strategy:
      1. Scan all pages for CN financial-statement keywords.
      2. Collect a window of pages around each hit (±1 page for context).
      3. For hit pages, also run table extraction so structured data
         (rows/columns) survives as pipe-delimited text for the LLM.
      4. Falls back to full-text extraction if no keyword hits found.

    Uses pdfplumber instead of PyMuPDF because pdfplumber:
      - handles CJK character spacing better
      - provides accurate table bounding-box extraction
      - avoids glyph-substitution artifacts common in Chinese PDFs
    """
    import pdfplumber

    # Keywords that strongly signal the cash-flow / financial-statement section
    _priority_kw = [
        "合并现金流量表",
        "现金流量表",
        "经营活动产生的现金流量净额",
        "购建固定资产",
        "合并资产负债表",
        "合并利润表",
        "合并利润及利润分配表",
        "财务报表",
        "财务报告",
    ]
    all_kw_lower = [kw.lower() for kw in keywords + _priority_kw]

    relevant_page_indices = set()

    try:
        with pdfplumber.open(file_path) as pdf:
            total = len(pdf.pages)

            # Pass 1 – find hit pages
            for i, page in enumerate(pdf.pages):
                text = page.extract_text() or ""
                text_l = text.lower()
                priority = sum(1 for kw in _priority_kw if kw.lower() in text_l)
                general  = sum(1 for kw in all_kw_lower if kw in text_l)
                if priority >= 1 or general >= 2:
                    # include ±1 page for table-continuation rows
                    for offset in (-1, 0, 1):
                        idx = i + offset
                        if 0 <= idx < total:
                            relevant_page_indices.add(idx)

            if not relevant_page_indices:
                # Fallback: return all pages as plain text (truncated)
                all_text = "\n".join(
                    page.extract_text() or "" for page in pdf.pages
                )
                return all_text[:500_000]

            # Pass 2 – extract text + tables for selected pages
            parts = []
            for i in sorted(relevant_page_indices):
                page = pdf.pages[i]
                page_text = page.extract_text() or ""

                # Table extraction — pipe-delimited rows for the LLM
                table_text = ""
                try:
                    tables = page.extract_tables() or []
                    for tbl in tables:
                        for row in tbl:
                            if row and any(c for c in row if c):
                                table_text += (
                                    " | ".join(
                                        str(c).strip() if c else "" for c in row
                                    ) + "\n"
                                )
                except Exception:
                    pass

                block = f"[第 {i + 1} 页]\n{page_text}"
                if table_text.strip():
                    block += f"\n\n[表格]\n{table_text}"
                parts.append(block)

    except Exception:
        # pdfplumber failed — fall back to PyMuPDF
        return _extract_fin_sections_pdf(file_path, keywords)

    return "\n\n---\n\n".join(parts)


def extract_and_cache_financial_section(file_path: str) -> tuple:
    """Extract financial section, cache to disk, return (text, token_estimate).

    Cache is stored under <BASE_DIR>/cache/<relative_path>_financial.txt
    so cached files don't pollute the filing directories.
    """
    # Build cache path under cache/ directory
    rel = os.path.relpath(file_path, BASE_DIR)
    cache_name = os.path.splitext(rel)[0] + "_financial.txt"
    cache_path = os.path.join(BASE_DIR, "cache", cache_name)

    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as f:
            text = f.read()
    else:
        text = extract_financial_sections(file_path)
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, "w", encoding="utf-8") as f:
            f.write(text)

    return text, estimate_tokens(text)


def estimate_and_cache_full_tokens(file_path: str) -> int:
    """Estimate full-report tokens and cache the numeric result.

    Uses file-size as a fast heuristic for uncached files (~5 bytes per token),
    avoiding the need to read and parse the entire filing on first call.
    """
    rel = os.path.relpath(file_path, BASE_DIR)
    cache_name = os.path.splitext(rel)[0] + "_full_tokens.json"
    cache_path = os.path.join(BASE_DIR, "cache", cache_name)

    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            return int(payload.get("tokens", 0))
        except (OSError, ValueError, json.JSONDecodeError):
            pass

    # Fast heuristic: file size / 5 ≈ tokens (avoids reading entire file)
    try:
        tokens = os.path.getsize(file_path) // 5
    except OSError:
        tokens = 0

    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump({"tokens": tokens}, f, ensure_ascii=False)
    except OSError:
        pass
    return tokens


def _sanitize_fcf_table_columns(tbl: pd.DataFrame) -> pd.DataFrame:
    """Keep only canonical columns and normalize types/order.

    This prevents legacy saved files (e.g. 总股本/校验) from polluting
    the current workflow.
    """
    if tbl is None or tbl.empty:
        return tbl

    out = tbl.copy()
    canonical_cols = [
        "年份", "OCF", "CapEx", "FCF", "每股FCF", "3年均每股FCF", "5年均每股FCF",
    ]
    optional_cols = ["yf每股FCF", "申报日期"]

    keep_cols = [c for c in canonical_cols + optional_cols if c in out.columns]
    if "年份" not in keep_cols and "年份" in out.columns:
        keep_cols = ["年份"] + keep_cols
    out = out[keep_cols]

    # Normalize 年份 display format and numeric columns
    if "年份" in out.columns:
        out["年份"] = out["年份"].astype(str).str[:10]
    for c in ["OCF", "CapEx", "FCF", "每股FCF", "3年均每股FCF", "5年均每股FCF", "yf每股FCF"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    # Sort descending by year when possible
    if "年份" in out.columns:
        out = out.sort_values("年份", ascending=False).reset_index(drop=True)

    return out


def save_fcf_table(fcf_table: pd.DataFrame, ticker: str, market: str):
    """Save the filled FCF table to disk (canonical + timestamped backup).

    Files are stored under saved_tables/{ticker}_{market}/
    """
    tbl = _sanitize_fcf_table_columns(fcf_table.copy())

    ticker_dir = os.path.join(BASE_DIR, "saved_tables", f"{ticker}_{market}")
    os.makedirs(ticker_dir, exist_ok=True)
    # Canonical file (overwritten each time for quick reload)
    canonical = os.path.join(ticker_dir, "fcf_table.csv")
    tbl.to_csv(canonical, index=False, encoding="utf-8-sig")
    # Timestamped backup
    ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    backup = os.path.join(ticker_dir, f"fcf_table_{ts}.csv")
    tbl.to_csv(backup, index=False, encoding="utf-8-sig")
    return canonical


def load_fcf_table(ticker: str, market: str) -> pd.DataFrame | None:
    """Load the most recent saved FCF table for a ticker, or None."""
    ticker_dir = os.path.join(BASE_DIR, "saved_tables", f"{ticker}_{market}")
    canonical = os.path.join(ticker_dir, "fcf_table.csv")
    # Fallback: old flat layout
    old_canonical = os.path.join(BASE_DIR, "saved_tables", f"{ticker}_{market}.csv")
    path = canonical if os.path.exists(canonical) else (
        old_canonical if os.path.exists(old_canonical) else None
    )
    if path is None:
        return None
    try:
        tbl = pd.read_csv(path, encoding="utf-8-sig")
        return _sanitize_fcf_table_columns(tbl)
    except Exception:
        return None


def recompute_fcf_per_share(tbl: pd.DataFrame, latest_shares: float) -> pd.DataFrame:
    """Recompute 每股FCF using latest shares outstanding, then rolling averages.

    This aligns FCF per share with adjusted (复权) stock prices.
    """
    tbl = tbl.copy()
    if "FCF" in tbl.columns and latest_shares and latest_shares > 0:
        tbl["每股FCF"] = tbl["FCF"].apply(
            lambda x: x / latest_shares if pd.notna(x) else None
        )
    tbl = _recompute_averages(tbl)
    return tbl


# ═══════════════════════════════════════════════════════════════════════
#  LLM-based FCF table filler  (batch + retry)
# ═══════════════════════════════════════════════════════════════════════

# Free tier limits (per model, rough): ~15 RPM, ~1M tokens/min
_MAX_TOKENS_PER_REQUEST = 900_000  # leave margin for prompt + response
_MAX_RETRIES = 19                  # total attempts = 20 (请求 1/20)
_RETRY_DELAYS = [20, 50, 90, 120, 180]  # seconds to wait per retry attempt
_MAX_ANNUAL_FILINGS = 20           # limit annual filings to reduce upload size
_MAX_TOKENS_PER_FILING = 150_000   # per-filing token cap


def _find_filing_for_year(year: int, filings: list) -> dict | None:
    """Find the annual filing whose report date matches a given year."""
    for f in filings:
        if not f.get("is_annual"):
            continue
        date_str = f.get("date", "") or f.get("label", "")
        if str(year) in date_str:
            return f
    return None


def _validate_shares_consistency(tbl: pd.DataFrame) -> list[int]:
    """Check for 100x+ jumps in shares outstanding between adjacent years.

    Returns list of years that need re-examination.
    """
    if "总股本" not in tbl.columns:
        return []

    sorted_tbl = tbl.sort_values("年份", ascending=True).reset_index(drop=True)
    problem_years = []

    for i in range(1, len(sorted_tbl)):
        s_cur = sorted_tbl.at[i, "总股本"]
        s_prev = sorted_tbl.at[i - 1, "总股本"]

        if pd.isna(s_cur) or pd.isna(s_prev) or s_prev == 0 or s_cur == 0:
            continue

        ratio = max(s_cur / s_prev, s_prev / s_cur)
        if ratio > 100:
            year_cur = int(str(sorted_tbl.at[i, "年份"])[:4])
            year_prev = int(str(sorted_tbl.at[i - 1, "年份"])[:4])
            problem_years.extend([year_cur, year_prev])

    return sorted(set(problem_years), reverse=True)


def _build_shares_recheck_prompt(problem_years: list, tbl: pd.DataFrame) -> str:
    """Build a prompt specifically to recheck shares outstanding for problematic years."""
    lines = []
    for year in problem_years:
        row = tbl[tbl["年份"].astype(str).str.startswith(str(year))]
        if not row.empty:
            val = row.iloc[0].get("总股本", "N/A")
            lines.append(f"  - {year}: currently {val:,.0f}" if isinstance(val, (int, float)) else f"  - {year}: currently {val}")

    return f"""⚠️ SHARES OUTSTANDING VALIDATION FAILED!

The following years have 总股本 values that differ by MORE THAN 100x from adjacent years.
This strongly indicates a UNIT ERROR (e.g. "in thousands" not converted):

{chr(10).join(lines)}

Please re-examine EACH of these years' annual reports VERY carefully:
1. Go to the Earnings Per Share (EPS) section
2. Find "weighted-average diluted shares outstanding" (the denominator of diluted EPS)
3. CHECK THE UNIT HEADER of the financial statements!
   - Does it say "in thousands"? → multiply by 1,000
   - Does it say "in millions"? → multiply by 1,000,000
   - "万股" → multiply by 10,000
4. A normal company typically has 100 million ~ 10 billion shares
5. Convert to ACTUAL share count

Return a JSON array with corrected values ONLY:
[{{"year": XXXX, "总股本": actual_number_of_shares}}, ...]

ONLY return the JSON array, no explanation."""


def _local_magnitude_check(tbl: pd.DataFrame) -> list[dict]:
    """Pure-Python check for 100x+ magnitude jumps between adjacent years.

    Returns list of {"year": int, "field": str, "value": float,
    "neighbor_year": int, "neighbor_value": float, "ratio": float}.
    """
    check_cols = ["OCF", "CapEx", "FCF"]
    sorted_tbl = tbl.sort_values("年份", ascending=False).reset_index(drop=True)
    anomalies = []
    for col in check_cols:
        if col not in sorted_tbl.columns:
            continue
        vals = []
        for i, row in sorted_tbl.iterrows():
            try:
                yr = int(str(row["年份"])[:4])
            except (ValueError, TypeError):
                continue
            v = row.get(col)
            if pd.notna(v) and isinstance(v, (int, float)) and v != 0:
                vals.append((yr, float(v), i))
        for j in range(len(vals) - 1):
            yr_a, v_a, _ = vals[j]
            yr_b, v_b, _ = vals[j + 1]
            ratio = abs(v_a / v_b) if v_b != 0 else 0
            if ratio >= 100 or (ratio > 0 and ratio <= 0.01):
                anomalies.append({
                    "year": yr_a if abs(v_a) < abs(v_b) else yr_b,
                    "field": col,
                    "value_a": v_a, "year_a": yr_a,
                    "value_b": v_b, "year_b": yr_b,
                    "ratio": ratio,
                })
    return anomalies


def _local_row_consistency_check(tbl: pd.DataFrame, threshold: float = 0.30) -> list[dict]:
    """Horizontal check: OCF-CapEx should be within 70%-130% of FCF.

    Uses threshold=0.30 by default, i.e. acceptable ratio range is
    [1-threshold, 1+threshold] against FCF baseline.
    """
    needed = {"年份", "OCF", "CapEx", "FCF"}
    if tbl is None or tbl.empty or not needed.issubset(set(tbl.columns)):
        return []

    anomalies = []
    for _, row in tbl.iterrows():
        yr = _to_year(row.get("年份"))
        if yr is None:
            continue
        ocf = row.get("OCF")
        capex = row.get("CapEx")
        fcf = row.get("FCF")
        if not all(isinstance(v, (int, float, np.integer, np.floating)) and pd.notna(v)
                   for v in [ocf, capex, fcf]):
            continue

        expected_fcf = float(ocf) - float(capex)
        fcf_abs = max(abs(float(fcf)), 1e-9)
        ratio_to_fcf = abs(expected_fcf) / fcf_abs
        diff_ratio = abs(float(fcf) - expected_fcf) / fcf_abs

        # CapEx == 0 is almost never correct for a real business
        if abs(float(capex)) < 1.0:
            anomalies.append({
                "year": yr,
                "field": "CapEx",
                "ocf": float(ocf),
                "capex": float(capex),
                "fcf": float(fcf),
                "expected_fcf": expected_fcf,
                "diff_ratio": 1.0,
                "ratio_to_fcf": ratio_to_fcf,
            })
            continue

        if ratio_to_fcf < (1 - threshold) or ratio_to_fcf > (1 + threshold):
            anomalies.append({
                "year": yr,
                "field": "FCF",
                "ocf": float(ocf),
                "capex": float(capex),
                "fcf": float(fcf),
                "expected_fcf": expected_fcf,
                "diff_ratio": diff_ratio,
                "ratio_to_fcf": ratio_to_fcf,
            })

    return anomalies


def _to_year(val) -> int | None:
    """Parse a year from int/float/date-like string, e.g. 2008 or 2008-12-31."""
    if val is None:
        return None
    if isinstance(val, (int, np.integer)):
        return int(val)
    s = str(val).strip()
    m = re.search(r"((?:19|20)\d{2})", s)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _log_anomaly_table(tbl: pd.DataFrame, suspect_years: list | None, log_fn,
                       local_anomalies: list | None = None):
    """Log a visual comparison table showing anomalous values vs neighbors."""
    check_cols = ["OCF", "CapEx", "FCF"]
    sorted_tbl = tbl.sort_values("年份", ascending=False).reset_index(drop=True)

    # Build year→row lookup
    yr_data = {}
    for _, row in sorted_tbl.iterrows():
        try:
            yr = int(str(row["年份"])[:4])
        except (ValueError, TypeError):
            continue
        yr_data[yr] = {c: row.get(c) for c in check_cols if c in sorted_tbl.columns}

    # Collect flagged (year, field) pairs
    flagged = set()
    if suspect_years:
        for s in suspect_years:
            yr = _to_year(s.get("year"))
            if yr is None:
                continue
            for f in s.get("fields", check_cols):
                flagged.add((yr, f))
    if local_anomalies:
        for a in local_anomalies:
            flagged.add((a["year"], a["field"]))

    if not flagged:
        return

    # Determine years to show
    show_years = set()
    for yr, _ in flagged:
        for offset in range(-2, 3):
            if yr + offset in yr_data:
                show_years.add(yr + offset)
    show_years = sorted(show_years, reverse=True)

    # Build text table
    header = f"{'年份':>6}"
    for c in check_cols:
        header += f"  {c:>16}"
    lines = ["📊 异常数据对比表:", header, "-" * len(header)]
    for yr in show_years:
        d = yr_data.get(yr, {})
        row_str = f"{yr:>6}"
        for c in check_cols:
            v = d.get(c)
            if v is not None and pd.notna(v):
                marker = " ⚠️" if (yr, c) in flagged else ""
                row_str += f"  {v:>16,.0f}{marker}"
            else:
                row_str += f"  {'N/A':>16}"
        lines.append(row_str)

    log_fn("\n".join(lines))


def _build_verification_prompt(tbl: pd.DataFrame) -> str:
    """Build a prompt asking Gemini to analyze the table for magnitude anomalies.

    This is a TABLE-ONLY check — no reports attached. Gemini just looks at numbers.
    """
    table_csv = tbl.to_csv(index=False)

    return f"""Analyze the following FCF data table for anomalies.

```
{table_csv}
```

Your task:
1. For EACH column (OCF, CapEx, FCF), compare values across ALL adjacent years.
2. Flag any year where a value differs from its neighbor(s) by 2-3 orders of magnitude (100x ~ 1000x).
   ★ The SMALLER value in such a pair is most likely wrong (unit conversion error).
3. Also flag any obvious sign errors (e.g. negative OCF where all other years are positive).

Return a JSON array listing ONLY the suspicious years and fields:
[{{"year": XXXX, "fields": ["OCF", "CapEx", ...], "reason": "brief explanation"}}]

- If no anomalies are found, return an empty array: []
- Return ONLY the JSON array, no explanation outside it.
"""


def _build_verification_fix_prompt(
    tbl: pd.DataFrame,
    suspect_years: list[dict],
    market: str | None = None,
) -> str:
    """Build a prompt to re-extract data for flagged years using FULL reports."""
    table_csv = tbl.to_csv(index=False)
    details = "\n".join(
        f"  - {s['year']}: fields {s.get('fields', [])} — {s.get('reason', '')}"
        for s in suspect_years
    )

    market_currency_rule = ""
    if (market or "").upper() == "US":
        market_currency_rule = (
            "4. For US annual reports, OCF/CapEx/FCF must be extracted in USD exactly as reported.\n"
            "   Do NOT keep or return CNY values for US filings.\n"
        )

    return f"""The following FCF table has suspected errors in certain years:

```
{table_csv}
```

Suspected anomalies:
{details}

Your task:
1. Re-read the attached FULL annual reports for the flagged years.
2. For each flagged field, find the CORRECT value from the report.
3. ★★★ CRITICAL: Check the UNIT DECLARATION at the top of each financial table! ★★★
   (e.g. "in thousands", "in millions", "单位：万元", "单位：千元")
   Different tables may use different units! Multiply raw numbers by the declared unit.
{market_currency_rule}

Return a JSON array with corrected values:
[{{"year": XXXX, "OCF": corrected_value, "CapEx": corrected_value, "FCF": corrected_value, "reason": "brief explanation"}}]

- Always return numeric values for OCF/CapEx/FCF when available from the report (do not use null for these fields).
- CapEx CANNOT be zero. If no explicit line, estimate as (ending net PP&E − beginning net PP&E + depreciation).
- If a field is already correct, still return the SAME numeric value from the current table/report.
- Return ONLY the JSON array, no explanation outside it.
"""


def _build_batch_fcf_prompt(
    table_csv: str,
    year_list: list[int],
    market: str | None = None,
) -> str:
    """Build a prompt asking Gemini to extract FCF data for MULTIPLE years at once."""
    years_str = ", ".join(str(y) for y in year_list)
    example_obj = ', '.join(
        f'{{"year": {y}, "OCF": ..., "CapEx": ..., "FCF": ...}}'
        for y in year_list[:2]
    )

    rules = load_fcf_rules()
    market_currency_rule = ""
    if (market or "").upper() == "US":
        market_currency_rule = (
            "- US market rule: For 10-K/20-F filings, OCF/CapEx/FCF must be in USD.\n"
            "- Do NOT keep or output CNY values for US filings.\n"
        )

    return f"""Below is a Free Cash Flow table for a company. Some cells contain "N/A".
Your task: Read ALL the attached annual report excerpts and fill in data for fiscal years: {years_str}

Current table (CSV):
```
{table_csv}
```

{rules}

Return a JSON ARRAY with one object per year:
[{example_obj}, ...]

Each object MUST have these exact keys: "year", "OCF", "CapEx", "FCF"

★★★ BEFORE EXTRACTING ANY NUMBER ★★★
For EVERY financial table you read, FIRST locate the UNIT DECLARATION line at the top of that
table or statement section (e.g. "in thousands", "in millions", "单位：万元", "单位：千元").
Different tables within the same report may use different units!
Multiply every raw number by the declared unit to produce the ACTUAL value.

IMPORTANT:
- All monetary values must be the ACTUAL value (after unit conversion), in the original currency.
- For US filings, always use USD values from annual reports.
{market_currency_rule}
- CapEx should be a POSITIVE number (absolute value).
- CapEx CANNOT be zero for any real business. If the cash flow statement does not show a
  separate "Capital Expenditures" line, look for "Purchases of property and equipment",
  "Payments for property, plant and equipment", or similar items.
  If truly not found, ESTIMATE CapEx = (ending net PP&E − beginning net PP&E + depreciation).
  Never return 0 for CapEx without explanation.
- Negative OCF is possible but unusual. Double-check the sign convention.
- If you cannot find a value, use null.
- Return ONLY the JSON array, no markdown fences, no explanation.
"""


def _build_batch_fcf_prompt_cn(
    table_csv: str,
    year_list: list[int],
) -> str:
    """Build a CN-specific prompt for A-share annual reports.

    Uses Chinese field names, CNY unit rules, no 每股FCF (computed locally).
    """
    years_str = "、".join(str(y) for y in year_list)
    example_obj = ', '.join(
        f'{{"year": {y}, "OCF": ..., "CapEx": ..., "FCF": ...}}'
        for y in year_list[:2]
    )

    return f"""以下是一家A股上市公司的自由现金流表格，部分数据为 N/A，需要从附件年报中补充。
请从附件年度报告的【合并现金流量表】中，精确提取以下财年的数据：{years_str}

当前表格（CSV）：
```
{table_csv}
```

━━━━━━━━━━ 字段定义 ━━━━━━━━━━
• OCF   = 经营活动产生的现金流量净额
         （报表行名通常正好是这一行，位于"经营活动现金流量"小节末尾）
• CapEx = 购建固定资产、无形资产和其他长期资产支付的现金
         （A股年报标准行名；取正值，因其在报表中已是负数/括号数）
• FCF   = OCF − CapEx

━━━━━━━━━━ 单位换算（关键！）━━━━━━━━━━
提取任何数字前，必须先找到该张报表表头或表尾的单位声明：
  "单位：元" / "单位：人民币元"  → 乘以 1
  "单位：千元"                  → 乘以 1,000
  "单位：万元"                  → 乘以 10,000
  "单位：百万元" / "单位：百万人民币元" → 乘以 1,000,000
  "单位：亿元"                  → 乘以 100,000,000
同一份报告中不同报表单位可能不同，每张表单独核查。
最终所有金额须为【人民币元（CNY）】实际值。

━━━━━━━━━━ A股年报结构提示 ━━━━━━━━━━
A股年报中，合并现金流量表通常位于"第X节 财务报告"或"财务报表"章节：
  1. 先找"合并现金流量表"标题
  2. 在"经营活动产生的现金流量"小节末尾找 OCF
  3. 在"投资活动产生的现金流量"小节开头找 CapEx（"购建固定资产…支付的现金"）
  4. 注意：括号表示负数，例如 (1,234,567) = -1,234,567；CapEx 取绝对值
  5. 若有"母公司现金流量表"和"合并现金流量表"，优先使用【合并】数据
  6. 年报截止日为 12 月 31 日（即财年与自然年相同）

━━━━━━━━━━ 注意事项 ━━━━━━━━━━
- CapEx 必须为正数（报表中的现金流出取绝对值）。
- CapEx 不能为零。若报表无明确行，估算：
    CapEx ≈ 期末固定资产净值 − 期初固定资产净值 + 本期折旧摊销
- OCF 为负数是合理的（亏损期或快速扩张期），请仔细核对符号。
- 只提取年度（全年）数据，不要使用半年报或季报数据。
- 找不到数据时返回 null，不要猜测。

━━━━━━━━━━ 返回格式 ━━━━━━━━━━
仅返回 JSON 数组，每个财年一个对象，不得有任何说明文字或 markdown 代码块：
[{example_obj}, ...]

每个对象必须且仅包含这些键："year"、"OCF"、"CapEx"、"FCF"
"""


def _render_parsed_llm_for_log(parsed, max_chars: int = 1200) -> str:
    """Render parsed LLM JSON to a human-readable summary."""
    if isinstance(parsed, dict):
        parsed = [parsed]
    if not isinstance(parsed, list):
        raw = json.dumps(parsed, ensure_ascii=False, indent=2)
        return raw[:max_chars] + ("\n...(截断)" if len(raw) > max_chars else "")

    lines = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        yr = _to_year(item.get("year"))

        # Step1 anomaly style: year + fields + reason
        if "fields" in item:
            fields = item.get("fields") or []
            field_txt = "/".join(str(f) for f in fields) if fields else "(未给字段)"
            reason = str(item.get("reason", "")).strip()
            head = f"- {yr}: 可疑字段 {field_txt}" if yr else f"- 可疑字段 {field_txt}"
            lines.append(head + (f" | 原因: {reason}" if reason else ""))
            continue

        # Extraction/correction style: show non-null values clearly
        non_null = []
        for k in ["OCF", "CapEx", "FCF", "每股FCF"]:
            v = item.get(k)
            if v is not None:
                try:
                    non_null.append(f"{k}={float(v):,.2f}")
                except (ValueError, TypeError):
                    non_null.append(f"{k}={v}")

        reason = str(item.get("reason", "")).strip()
        if non_null:
            head = f"- {yr}: " if yr else "- "
            lines.append(head + "; ".join(non_null) + (f" | 说明: {reason}" if reason else ""))
        elif reason:
            head = f"- {yr}: " if yr else "- "
            lines.append(head + f"说明: {reason}")

    if not lines:
        raw = json.dumps(parsed, ensure_ascii=False, indent=2)
        return raw[:max_chars] + ("\n...(截断)" if len(raw) > max_chars else "")

    out = "\n".join(lines)
    if len(out) > max_chars:
        out = out[:max_chars] + "\n...(截断)"
    return out


def _format_llm_output_for_log(text: str, max_chars: int = 1200) -> str:
    """Format LLM output for logs: readable summary first, raw text fallback."""
    parsed = _parse_llm_json(text)
    if parsed is not None:
        return _render_parsed_llm_for_log(parsed, max_chars=max_chars)

    cleaned = re.sub(r"\s+", " ", text or "").strip()
    if len(cleaned) > max_chars:
        return cleaned[:max_chars] + " ...(截断)"
    return cleaned


def _reason_indicates_correct(reason: str) -> bool:
    txt = str(reason or "").lower()
    return any(kw in txt for kw in [
        "already correct", "are correct", "is correct",
        "no correction", "correct based on",
        "一致", "已正确", "无需修正", "保持原值",
    ])


def _normalize_verification_corrections(corrections, tbl: pd.DataFrame) -> list[dict]:
    """Normalize verification JSON to unified numeric format.

    If model says values are already correct but returns nulls, backfill from
    current table values so logs and downstream checks stay consistent.
    """
    if isinstance(corrections, dict):
        corrections = [corrections]
    if not isinstance(corrections, list):
        return []

    out = []
    for item in corrections:
        if not isinstance(item, dict):
            continue
        yr = _to_year(item.get("year"))
        if yr is None:
            continue
        it = dict(item)
        it["year"] = yr

        if _reason_indicates_correct(it.get("reason", "")):
            row = tbl[tbl["年份"].astype(str).str.startswith(str(yr))]
            if not row.empty:
                row0 = row.iloc[0]
                for k in ["OCF", "CapEx", "FCF"]:
                    if it.get(k) is None:
                        v = row0.get(k)
                        if pd.notna(v):
                            it[k] = float(v)

        out.append(it)
    return out


def _log_token_estimate_table(log_fn, title: str, rows: list[dict]):
    """Log token estimates in a compact text table."""
    if not rows:
        return
    hdr = f"{'年份':>6}    {'类型':<8}    {'tokens(估算)':>16}    {'文件':<56}"
    total_tokens = sum(int(r.get("tokens", 0) or 0) for r in rows)
    lines = [
        "",
        f"📊 {title}",
        f"   共 {len(rows)} 条 | 合计 ~{total_tokens:,} tokens",
        hdr,
        "-" * len(hdr),
    ]
    for r in rows:
        year = str(r.get("year", ""))
        kind = str(r.get("kind", ""))
        tokens = int(r.get("tokens", 0) or 0)
        label = str(r.get("label", ""))
        if len(label) > 56:
            label = label[:53] + "..."
        lines.append(f"{year:>6}    {kind:<8}    {tokens:>16,}    {label:<56}")
    lines.append("")
    log_fn("\n".join(lines))


def _parse_llm_json(text: str) -> dict | list | None:
    """Robustly parse JSON from LLM response (single object or array)."""
    text = text.strip()
    # Remove markdown fences if present
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    # Try to find JSON array
    match = re.search(r"\[.*\]", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    # Try to find single JSON object
    match = re.search(r"\{[^{}]*\}", text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    return None


def _gemini_call_with_retry(client, model_name, contents, config,
                            max_retries=_MAX_RETRIES, log_fn=None,
                            enabled_models=None):
    """Call Gemini with smart model rotation + exponential backoff.

    Routing strategy:
    1. Sort enabled models by priority (gemini → gemma-4 → gemma-3)
    2. Skip models whose token budget is too small for this request
    3. Skip models known to be rate-limited today (RPD exhausted)
    4. On 429, mark model exhausted and immediately try the next one
    5. Persist call status to disk after every call for next-session awareness
    """
    import time
    _log = log_fn or (lambda msg: None)

    # Estimate content size for budget checking
    _est_tokens = 0
    for c in contents:
        if hasattr(c, "parts"):
            for p in c.parts:
                if hasattr(p, "text") and p.text:
                    _est_tokens += len(p.text) // 4

    # Build rotation list: primary model first, then others, sorted by priority
    if enabled_models and len(enabled_models) > 1:
        others = _sort_models_by_priority([m for m in enabled_models if m != model_name])
        rotation = [model_name] + others
    else:
        rotation = [model_name]

    # Pre-filter: remove models with no quota or already RPD-exhausted
    rotation = [m for m in rotation
                if _get_model_token_budget(m) > 0 and _is_model_available(m)]
    if not rotation:
        # Fallback: try all enabled models regardless of status
        rotation = [model_name] + (enabled_models or [])
        rotation = list(dict.fromkeys(rotation))  # dedupe, preserve order

    exhausted_models = set()
    last_err = None

    for attempt in range(max_retries + 1):
        available = [m for m in rotation if m not in exhausted_models]
        if not available:
            _log("⚠️ 所有模型均已耗尽配额，重置并等待后重试...")
            exhausted_models.clear()
            available = rotation

        # Find a model whose budget can handle this request
        current_model = None
        for m in available:
            budget = _get_model_token_budget(m)
            if budget > 0 and _est_tokens > budget:
                m_tag = m.split("-", 1)[1] if "-" in m else m
                _log(f"⏭️ [{m_tag}] 内容 ~{_est_tokens:,} tokens > 预算 {budget:,}, 跳过")
                exhausted_models.add(m)
                continue
            current_model = m
            break

        if current_model is None:
            raise RuntimeError(
                f"PAYLOAD_TOO_LARGE_FOR_ALL_MODELS: est_tokens={_est_tokens}"
            )

        try:
            t0 = time.time()
            model_tag = current_model.split("-", 1)[1] if "-" in current_model else current_model
            _log(f"⏳ [{model_tag}] 正在等待回复... (~{_est_tokens:,} tokens, 请求 {attempt+1}/{max_retries+1})")
            response = client.models.generate_content(
                model=current_model,
                contents=contents,
                config=config,
            )
            elapsed = time.time() - t0
            _log(f"✅ [{model_tag}] {elapsed:.1f}秒 回复 ({len(response.text):,} 字符)")
            _record_model_ok(current_model, elapsed, len(response.text), _est_tokens)
            return response.text
        except Exception as e:
            elapsed = time.time() - t0
            last_err = e
            err_str = str(e).lower()
            is_rate_limit = any(kw in err_str for kw in [
                "429", "rate", "resource_exhausted", "quota",
            ])
            is_payload_too_large = ("400" in err_str and "token" in err_str
                                    and ("exceeds" in err_str or "invalid" in err_str))
            is_retryable = is_rate_limit or is_payload_too_large or any(kw in err_str for kw in [
                "500", "503", "unavailable", "overloaded", "busy",
                "deadline", "timeout",
            ])

            # Record status to disk
            if is_rate_limit:
                _record_model_rate_limited(current_model, elapsed, str(e))
            else:
                _record_model_error(current_model, elapsed, str(e))

            if not is_retryable or attempt >= max_retries:
                _log(f"❌ [{model_tag}] 请求失败 ({elapsed:.1f}秒): {e}")
                raise

            if (is_rate_limit or is_payload_too_large) and len(rotation) > 1:
                exhausted_models.add(current_model)
                next_available = [m for m in rotation
                                  if m not in exhausted_models
                                  and _get_model_token_budget(m) > _est_tokens]
                if next_available:
                    next_tag = next_available[0].split("-", 1)[1] if "-" in next_available[0] else next_available[0]
                    _log(f"⚠️ [{model_tag}] 触发限流 ({elapsed:.1f}秒), 切换至 {next_tag}")
                    continue
                else:
                    _log(f"⚠️ [{model_tag}] 触发限流, 所有模型均已耗尽，等待重试...")
                    exhausted_models.clear()

            delay = _RETRY_DELAYS[min(attempt, len(_RETRY_DELAYS) - 1)]
            # Parse server-suggested retry delay (handle ms vs s)
            retry_match = re.search(r'retry.*?(\d+(?:\.\d+)?)\s*(ms|s)', err_str)
            if retry_match:
                server_val = float(retry_match.group(1))
                unit = retry_match.group(2)
                server_delay_s = server_val / 1000.0 if unit == 'ms' else server_val
                delay = max(delay, int(server_delay_s) + 2)
            _log(f"⚠️ [{model_tag}] 失败 ({elapsed:.1f}秒): {e}")
            _log(f"🔄 重试 {attempt+1}/{max_retries}，等待 {delay}秒...")
            time.sleep(delay)
    raise last_err


def fill_fcf_table_with_llm(
    api_key: str,
    model_name: str,
    fcf_table: pd.DataFrame,
    ticker: str,
    market: str,
    progress_callback=None,
    table_update_callback=None,
    enabled_models=None,
):
    """Use Gemini to fill N/A values (and add missing years) by reading annual reports.

    Features:
    - Batches multiple years into one request (token-aware)
    - Retries on rate-limit / transient errors with model rotation
    - Adds new rows for years that have filings but aren't in the table
    - Validates shares consistency (100x check) and retries if needed
    - Limits to most recent _MAX_ANNUAL_FILINGS years

    Returns: (filled_table, log_messages, prompt_info)
        prompt_info: dict with system_prompt, rules, rules_path, batch_prompts
    """
    # Normalize model id to be tolerant of user input
    model_name = _normalize_model_id(model_name)
    client = genai.Client(api_key=api_key)
    fcf_table = _sanitize_fcf_table_columns(fcf_table)
    _enabled = enabled_models or DEFAULT_ENABLED_MODELS
    logs = []
    _is_cn_market = (market or "").upper() == "CN"
    prompt_info = {
        "system_prompt": FCF_SYSTEM_PROMPT_CN if _is_cn_market else FCF_SYSTEM_PROMPT,
        "rules": load_fcf_rules(),
        "rules_path": RULES_PATH,
        "batch_prompts": [],
    }

    def log(msg):
        logs.append(msg)
        if progress_callback:
            progress_callback(msg)

    # ── List available annual filings ─────────────────────────────────
    if market == "US":
        filings = list_sec_filings(ticker)
    elif market == "HK":
        filings = list_hk_filings(ticker)
    else:
        filings = list_cn_filings(ticker)

    annual_filings = [f for f in filings if f.get("is_annual")]
    if not annual_filings:
        log("⚠️ 未找到任何年报文件，无法进行 AI 补全。")
        return fcf_table, logs, prompt_info

    # LLM request budget: 2× annual filings, minimum 4
    max_successful_requests = max(4, 2 * len(annual_filings))
    _successful_llm_calls = 0
    log(f"🔢 LLM请求上限: {max_successful_requests} 次 (年报数量 {len(annual_filings)} × 2，最少4次)")

    # ── Determine filing years & add missing rows ─────────────────────
    # Build year → (report_date, filing_date) from annual filings
    _yr_report_date: dict = {}
    _yr_filing_date: dict = {}
    filing_years = set()
    for f in annual_filings:
        date_str = f.get("date", "") or f.get("label", "")
        for m in re.findall(r"((?:19|20)\d{2})", date_str):
            yr_int = int(m)
            filing_years.add(yr_int)
            if yr_int not in _yr_report_date:
                _yr_report_date[yr_int] = f.get("date", f"{yr_int}-12-31") or f"{yr_int}-12-31"
                _yr_filing_date[yr_int] = f.get("filing_date", "") or ""

    existing_years = set()
    for _, row in fcf_table.iterrows():
        try:
            existing_years.add(int(str(row["年份"])[:4]))
        except (ValueError, TypeError):
            pass

    # Add rows for years with filings but not in table
    new_years = filing_years - existing_years
    filled = fcf_table.copy()
    filled = filled.drop(columns=["总股本"], errors="ignore")

    # Backfill 申报日期 for any existing rows that are missing it
    if "申报日期" not in filled.columns:
        filled["申报日期"] = ""
    for i, row in filled.iterrows():
        if row.get("申报日期"):
            continue
        yr = _to_year(str(row.get("年份", "")))
        if yr and yr in _yr_filing_date and _yr_filing_date[yr]:
            filled.at[i, "申报日期"] = _yr_filing_date[yr]

    if new_years:
        log(f"📝 发现 {len(new_years)} 个有年报但表格中缺失的年份: {sorted(new_years, reverse=True)}")
        for yr in new_years:
            new_row = {
                "年份": _yr_report_date.get(yr, f"{yr}-12-31"),
                "申报日期": _yr_filing_date.get(yr, ""),
            }
            for col in filled.columns:
                if col not in ("年份", "申报日期"):
                    new_row[col] = np.nan
            filled = pd.concat([filled, pd.DataFrame([new_row])], ignore_index=True)
        # Re-sort descending by year
        filled = filled.sort_values("年份", ascending=False).reset_index(drop=True)

    # ── Find ALL years that need filling (have any N/A) ───────────────
    na_cols = ["OCF", "CapEx", "FCF"]
    all_years_to_fill = []
    for _, row in filled.iterrows():
        year_str = str(row["年份"])[:4]
        try:
            year = int(year_str)
        except ValueError:
            continue
        has_na = any(
            pd.isna(row.get(col)) or row.get(col) is None
            for col in na_cols if col in filled.columns
        )
        if has_na:
            all_years_to_fill.append(year)

    all_years_to_fill = sorted(set(all_years_to_fill), reverse=True)

    # Limit to most recent N years (for filling)
    if len(all_years_to_fill) > _MAX_ANNUAL_FILINGS:
        log(f"📝 需处理年份 ({len(all_years_to_fill)}) 超过上限 {_MAX_ANNUAL_FILINGS}，仅处理最近 {_MAX_ANNUAL_FILINGS} 年")
        all_years_to_fill = all_years_to_fill[:_MAX_ANNUAL_FILINGS]

    # ── Determine ALL table years with filings (for verification) ─────
    all_table_years = set()
    for _, row in filled.iterrows():
        try:
            all_table_years.add(int(str(row["年份"])[:4]))
        except (ValueError, TypeError):
            pass
    all_years_with_filings = sorted(
        [y for y in all_table_years if _find_filing_for_year(y, annual_filings)],
        reverse=True,
    )[:_MAX_ANNUAL_FILINGS]

    if all_years_to_fill:
        log(f"📋 找到 {len(annual_filings)} 份年报 | 需填充年份: {all_years_to_fill}")
    else:
        log("✅ 表格中没有 N/A 值需要填充，将直接进行验证。")

    # ── Extract financial sections for fill years + verification years ─
    years_to_extract = sorted(
        set(all_years_to_fill) | set(all_years_with_filings), reverse=True
    )
    table_csv = filled.to_csv(index=False)
    year_filing_text = {}  # year -> (fin_text, tokens, label)
    token_rows = []

    # Collect (year, filing) pairs; log missing immediately
    year_filing_pairs = []
    for year in years_to_extract:
        filing = _find_filing_for_year(year, annual_filings)
        if not filing:
            if year in all_years_to_fill:
                log(f"⚠️ {year}: 未找到对应年报，跳过")
            continue
        year_filing_pairs.append((year, filing))

    def _extract_one(year_filing):
        """Worker: extract financial section + estimate tokens (no Streamlit calls)."""
        year, filing = year_filing
        try:
            fin_text, tokens = extract_and_cache_financial_section(filing["path"])
            full_tokens = estimate_and_cache_full_tokens(filing["path"])
            return year, fin_text, tokens, full_tokens, filing["label"], None
        except Exception as e:
            return year, None, 0, 0, filing["label"], str(e)

    if year_filing_pairs:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        n_workers = min(12, len(year_filing_pairs))
        log(f"⚡ 并行提取 {len(year_filing_pairs)} 份年报财务章节 (线程数: {n_workers})...")
        results_map = {}
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            future_to_year = {
                pool.submit(_extract_one, yf): yf[0]
                for yf in year_filing_pairs
            }
            for fut in as_completed(future_to_year):
                yr, fin_text, tokens, full_tokens, label, err = fut.result()
                results_map[yr] = (fin_text, tokens, full_tokens, label, err)

        # Reassemble in year-descending order (preserve original ordering)
        for year, _ in sorted(year_filing_pairs, key=lambda x: -x[0]):
            fin_text, tokens, full_tokens, label, err = results_map[year]
            if err:
                log(f"❌ {year}: 文件读取失败: {err}")
                continue
            if tokens > _MAX_TOKENS_PER_FILING:
                fin_text = fin_text[:_MAX_TOKENS_PER_FILING * 4]
                tokens = _MAX_TOKENS_PER_FILING
            token_rows.append({"year": year, "kind": "节选", "tokens": tokens, "label": label})
            token_rows.append({"year": year, "kind": "完整", "tokens": full_tokens, "label": label})
            year_filing_text[year] = (fin_text, tokens, label)

    _kind_rank = {"节选": 0, "完整": 1}
    _log_token_estimate_table(
        log,
        "年报 token 估算（节选 + 完整）",
        sorted(token_rows, key=lambda x: (-int(x["year"]), _kind_rank.get(x["kind"], 9))),
    )

    if not year_filing_text:
        log("⚠️ 没有可用的年报文本，无法继续。")
        return filled, logs, prompt_info

    # ── Compute adaptive batch budget from best available model ─────
    _avail_budgets = [_get_model_token_budget(m)
                      for m in _enabled if _is_model_available(m)]
    batch_token_budget = max(_avail_budgets) if _avail_budgets else _MAX_TOKENS_PER_REQUEST
    if batch_token_budget != _MAX_TOKENS_PER_REQUEST:
        log(f"🎯 批量预算: ~{batch_token_budget:,} tokens (基于最佳可用模型)")

    # ── Gemma constraint: 1 filing per request ────────────────────────
    # Gemma models work best (and avoid context issues) with a single
    # annual report per request, regardless of token budget.
    _primary_model = _normalize_model_id(model_name) if model_name else (
        _enabled[0] if _enabled else ""
    )
    _max_years_per_batch = 1 if _is_gemma_model(_primary_model) else _MAX_ANNUAL_FILINGS
    if _max_years_per_batch == 1:
        log(f"📌 Gemma 模型限制: 每次请求仅发送 1 份年报 (模型: {_primary_model})")

    # ── Group years into batches (only years needing fill) ────────────
    _fill_year_set = set(all_years_to_fill)
    batches = []   # each batch: list of (year, fin_text, label)
    current_batch = []
    current_tokens = 0
    prompt_overhead = 2000  # rough estimate for the prompt itself

    for year in sorted(year_filing_text.keys(), reverse=True):
        if year not in _fill_year_set:
            continue
        fin_text, tokens, label = year_filing_text[year]

        if (len(current_batch) >= _max_years_per_batch
                or (current_tokens + tokens + prompt_overhead > batch_token_budget)
                ) and current_batch:
            batches.append(current_batch)
            current_batch = []
            current_tokens = 0

        current_batch.append((year, fin_text, label))
        current_tokens += tokens

    if current_batch:
        batches.append(current_batch)

    total_batches = len(batches)
    total_years = len([y for y in year_filing_text if y in _fill_year_set])
    if total_batches:
        log(f"📦 分为 {total_batches} 批次发送 (共 {total_years} 个年份需填充)")

    # ── Send batches to Gemini ────────────────────────────────────────
    failed_years = []
    processed = 0
    total_years = len(year_filing_text)

    for batch_i, batch in enumerate(batches):
        batch_years = [y for y, _, _ in batch]
        log(f"\n🤖 批次 {batch_i+1}/{total_batches}: 年份 {batch_years}")

        if _successful_llm_calls >= max_successful_requests:
            log(f"⛔ 已达到LLM请求上限 ({max_successful_requests} 次)，跳过剩余批次。")
            break

        if progress_callback:
            progress_callback(None, processed, total_years)

        # Build contents: all filing texts + one prompt
        _is_cn = (market or "").upper() == "CN"
        parts = []
        for year, fin_text, label in batch:
            header = f"═══ 年度报告: {label} (财年 {year}) ═══" if _is_cn else \
                     f"═══ Annual report: {label} (Fiscal Year {year}) ═══"
            parts.append(types.Part(text=f"{header}\n\n{fin_text}"))

        if _is_cn:
            prompt = _build_batch_fcf_prompt_cn(table_csv, batch_years)
            _sys_prompt = FCF_SYSTEM_PROMPT_CN
        else:
            prompt = _build_batch_fcf_prompt(table_csv, batch_years, market=market)
            _sys_prompt = FCF_SYSTEM_PROMPT
        prompt_info["batch_prompts"].append(prompt)
        parts.append(types.Part(text=prompt))

        try:
            reply = _gemini_call_with_retry(
                client, model_name,
                contents=[types.Content(role="user", parts=parts)],
                config=types.GenerateContentConfig(
                    system_instruction=_sys_prompt,
                    temperature=0.1,
                ),
                log_fn=log, enabled_models=_enabled,
            )
            _successful_llm_calls += 1
        except Exception as e:
            log(f"❌ 批次 {batch_i+1} Gemini 请求失败 (已重试 {_MAX_RETRIES} 次): {e}")
            failed_years.extend(batch_years)
            processed += len(batch)
            continue

        log(f"📨 Gemini 批次 {batch_i+1} 回复:\n{_format_llm_output_for_log(reply, 1200)}")

        # Parse response — expect array or single object
        data = _parse_llm_json(reply)
        if data is None:
            log(f"❌ 批次 {batch_i+1}: 无法解析 Gemini 返回:\n{_format_llm_output_for_log(reply, 500)}")
            failed_years.extend(batch_years)
            processed += len(batch)
            continue

        # Normalize to list
        if isinstance(data, dict):
            data = [data]

        # Merge each year's data
        for item in data:
            year = _to_year(item.get("year"))
            if year is None:
                continue

            row_mask = filled["年份"].astype(str).str.startswith(str(year))
            if not row_mask.any():
                log(f"⚠️ {year}: 表格中未找到对应行")
                continue

            idx = filled.index[row_mask][0]
            updated_fields = []
            for col, json_key in [
                ("OCF", "OCF"), ("CapEx", "CapEx"), ("FCF", "FCF"),
                ("每股FCF", "每股FCF"),
            ]:
                if col not in filled.columns:
                    continue
                current = filled.at[idx, col]
                new_val = item.get(json_key)
                if new_val is not None:
                    if col == "每股FCF":
                        continue
                    if pd.isna(current) or current is None:
                        filled.at[idx, col] = float(new_val)
                        updated_fields.append(col)

            if updated_fields:
                log(f"✅ {year}: 填充了 {', '.join(updated_fields)}")
                filled = _recompute_averages(filled)
                if table_update_callback:
                    table_update_callback(filled)
            else:
                log(f"ℹ️ {year}: Gemini 未返回新数据")

        processed += len(batch)

    # ── Retry failed years one by one ─────────────────────────────────
    if failed_years:
        log(f"\n🔄 重试失败的年份: {failed_years}")
        import time
        for year in failed_years:
            if year not in year_filing_text:
                continue
            fin_text, tokens, label = year_filing_text[year]

            if progress_callback:
                progress_callback(None, processed, total_years + len(failed_years))

            if _successful_llm_calls >= max_successful_requests:
                log(f"⛔ 已达到LLM请求上限 ({max_successful_requests} 次)，跳过重试。")
                break
            log(f"🔄 重试 {year}...")
            _is_cn_retry = (market or "").upper() == "CN"
            if _is_cn_retry:
                _retry_prompt = _build_batch_fcf_prompt_cn(table_csv, [year])
                _retry_sys = FCF_SYSTEM_PROMPT_CN
                _retry_header = f"年度报告节选 ({label}):"
            else:
                _retry_prompt = _build_batch_fcf_prompt(table_csv, [year], market=market)
                _retry_sys = FCF_SYSTEM_PROMPT
                _retry_header = f"Annual report excerpt ({label}):"
            parts = [
                types.Part(text=f"{_retry_header}\n\n{fin_text}"),
                types.Part(text=_retry_prompt),
            ]

            try:
                time.sleep(10)  # extra cooldown before retry
                reply = _gemini_call_with_retry(
                    client, model_name,
                    contents=[types.Content(role="user", parts=parts)],
                    config=types.GenerateContentConfig(
                        system_instruction=_retry_sys,
                        temperature=0.1,
                    ),
                    max_retries=2, log_fn=log, enabled_models=_enabled,
                )
                _successful_llm_calls += 1
            except Exception as e:
                log(f"❌ {year}: 重试仍然失败: {e}")
                processed += 1
                continue

            log(f"📨 Gemini 重试 {year} 回复:\n{_format_llm_output_for_log(reply, 1000)}")
            data = _parse_llm_json(reply)
            if isinstance(data, list) and data:
                data = data[0]
            if not isinstance(data, dict) or data is None:
                log(f"❌ {year}: 重试解析失败")
                processed += 1
                continue

            row_mask = filled["年份"].astype(str).str.startswith(str(year))
            if not row_mask.any():
                processed += 1
                continue

            idx = filled.index[row_mask][0]
            updated_fields = []
            for col, json_key in [
                ("OCF", "OCF"), ("CapEx", "CapEx"), ("FCF", "FCF"),
                ("每股FCF", "每股FCF"),
            ]:
                if col not in filled.columns:
                    continue
                current = filled.at[idx, col]
                new_val = data.get(json_key)
                if new_val is not None:
                    if col == "每股FCF":
                        continue
                    if pd.isna(current) or current is None:
                        filled.at[idx, col] = float(new_val)
                        updated_fields.append(col)

            if updated_fields:
                log(f"✅ {year}: 重试成功，填充了 {', '.join(updated_fields)}")
                filled = _recompute_averages(filled)
                if table_update_callback:
                    table_update_callback(filled)
            processed += 1

    # ── Recompute rolling averages ────────────────────────────────────
    filled = filled.drop(columns=["总股本"], errors="ignore")
    filled = _recompute_averages(filled)
    if table_update_callback:
        table_update_callback(filled)

    # Shares-specific validation has been removed to save tokens.

    # ── NA Retry Loop: rescan table for remaining N/A and re-ask ────
    _NA_RETRY_ROUNDS = 2
    _na_key_cols = ["OCF", "CapEx", "FCF"]

    for na_round in range(_NA_RETRY_ROUNDS):
        if _successful_llm_calls >= max_successful_requests:
            log(f"⛔ 已达到LLM请求上限，跳过NA补填。")
            break
        # Find years that still have N/A in key columns and have a filing
        na_years = []
        for _, row in filled.iterrows():
            yr_str = str(row["年份"])[:4]
            try:
                yr = int(yr_str)
            except ValueError:
                continue
            if yr not in year_filing_text:
                continue
            has_na = any(
                pd.isna(row.get(col)) or row.get(col) is None
                for col in _na_key_cols if col in filled.columns
            )
            if has_na:
                na_years.append(yr)

        if not na_years:
            break

        log(f"\n🔄 NA 补填第 {na_round+1} 轮: 发现 {len(na_years)} 个年份仍有 N/A: {na_years}")

        _is_cn_na = (market or "").upper() == "CN"
        for yr in na_years:
            if _successful_llm_calls >= max_successful_requests:
                log(f"⛔ 已达到LLM请求上限，跳过剩余NA年份。")
                break
            fin_text, _tok, label = year_filing_text[yr]
            if _is_cn_na:
                _na_prompt = _build_batch_fcf_prompt_cn(filled.to_csv(index=False), [yr])
                _na_sys = FCF_SYSTEM_PROMPT_CN
                _na_header = f"═══ 年度报告: {label} (财年 {yr}) ═══"
            else:
                _na_prompt = _build_batch_fcf_prompt(filled.to_csv(index=False), [yr], market=market)
                _na_sys = FCF_SYSTEM_PROMPT
                _na_header = f"═══ Annual report: {label} (Fiscal Year {yr}) ═══"
            parts = [
                types.Part(text=f"{_na_header}\n\n{fin_text}"),
                types.Part(text=_na_prompt),
            ]

            try:
                import time
                time.sleep(5)
                reply = _gemini_call_with_retry(
                    client, model_name,
                    contents=[types.Content(role="user", parts=parts)],
                    config=types.GenerateContentConfig(
                        system_instruction=_na_sys,
                        temperature=0.1,
                    ),
                    max_retries=2, log_fn=log, enabled_models=_enabled,
                )
                _successful_llm_calls += 1
            except Exception as e:
                log(f"❌ {yr}: NA 补填失败: {e}")
                continue

            log(f"📨 Gemini NA补填 {yr} 回复:\n{_format_llm_output_for_log(reply, 1000)}")
            data = _parse_llm_json(reply)
            if isinstance(data, list) and data:
                data = data[0]
            if not isinstance(data, dict) or data is None:
                log(f"❌ {yr}: NA 补填解析失败")
                continue

            row_mask = filled["年份"].astype(str).str.startswith(str(yr))
            if not row_mask.any():
                continue
            idx = filled.index[row_mask][0]
            updated = []
            for col, jk in [("OCF", "OCF"), ("CapEx", "CapEx"), ("FCF", "FCF"),
                             ("每股FCF", "每股FCF")]:
                if col not in filled.columns:
                    continue
                cur = filled.at[idx, col]
                nv = data.get(jk)
                if col == "每股FCF":
                    continue
                if nv is not None and (pd.isna(cur) or cur is None):
                    filled.at[idx, col] = float(nv)
                    updated.append(col)
            if updated:
                log(f"✅ {yr}: NA 补填成功 → {', '.join(updated)}")
                filled = _recompute_averages(filled)
                if table_update_callback:
                    table_update_callback(filled)

        filled = _recompute_averages(filled)
        filled = filled.drop(columns=["总股本"], errors="ignore")
        if table_update_callback:
            table_update_callback(filled)

    # ── Iterative Verification Loop (±3 year expansion, max 10 iterations) ─
    _MAX_VERIFY_ITERATIONS = 10
    _EXPAND_RANGE = 3  # check ±3 years around each flagged year

    for v_iter in range(1, _MAX_VERIFY_ITERATIONS + 1):
        if _successful_llm_calls >= max_successful_requests:
            log(f"⛔ 已达到LLM请求上限，跳过验证迭代。")
            break
        log(
            f"\n🔍 验证迭代 {v_iter}/{_MAX_VERIFY_ITERATIONS}: "
            "先横向检测(OCF-CapEx 是否在 FCF 的70%-130%)，再纵向检测(相邻年份数量级 ≥100x)..."
        )

        # Step1-A: horizontal check first (row consistency, threshold 30%)
        local_row_anomalies = _local_row_consistency_check(filled, threshold=0.30)
        # Step1-B: vertical check next (adjacent-year 100x)
        local_anomalies = _local_magnitude_check(filled)
        suspect_years = []
        for a in local_row_anomalies:
            if a.get("field") == "CapEx" and abs(a["capex"]) < 1.0:
                reason = (
                    f"CapEx=0 不合理，必须从年报中找到资本支出数据。"
                    f"如找不到，请用 (期末固定资产净值-期初固定资产净值+本期折旧) 估算"
                )
            else:
                reason = (
                    f"行内一致性异常: FCF({a['fcf']:,.0f}) 与 OCF-CapEx({a['expected_fcf']:,.0f}) "
                    f"偏差 {a['diff_ratio'] * 100:.1f}% (需落在70%-130%范围内)"
                )
            suspect_years.append({
                "year": a["year"],
                "fields": ["OCF", "CapEx", "FCF"],
                "reason": reason,
            })
        for a in local_anomalies:
            suspect_years.append({
                "year": a["year"],
                "fields": [a["field"]],
                "reason": (
                    f"与相邻年份 {a['year_b']} 相差 {a['ratio']:.1f}x "
                    f"({a['value_a']:,.0f} vs {a['value_b']:,.0f})"
                ),
            })
        # Deduplicate: merge fields for same year
        _yr_suspect: dict = {}
        for s in suspect_years:
            yr = s["year"]
            if yr not in _yr_suspect:
                _yr_suspect[yr] = {"year": yr, "fields": list(s["fields"]), "reason": s["reason"]}
            else:
                for f in s["fields"]:
                    if f not in _yr_suspect[yr]["fields"]:
                        _yr_suspect[yr]["fields"].append(f)
        suspect_years = list(_yr_suspect.values())

        if not suspect_years:
            log(f"✅ 验证迭代 {v_iter}: 未发现横向或纵向异常，验证通过!")
            break

        # Log anomaly comparison table
        flagged_year_nums = sorted(set(s["year"] for s in suspect_years), reverse=True)
        _log_anomaly_table(
            filled,
            suspect_years,
            log,
            local_row_anomalies + local_anomalies,
        )
        if local_row_anomalies:
            log("📌 横向异常明细(OCF-CapEx 是否在 FCF 的70%-130%范围):")
            for a in sorted(local_row_anomalies, key=lambda x: x["year"], reverse=True):
                log(
                    f"  - {a['year']}: OCF={a['ocf']:,.0f}, CapEx={a['capex']:,.0f}, "
                    f"FCF={a['fcf']:,.0f}, OCF-CapEx={a['expected_fcf']:,.0f}, "
                    f"偏差={a['diff_ratio'] * 100:.1f}%, 比值={a['ratio_to_fcf']:.2f}"
                )

        # Expand to ±3 years around each flagged year
        expanded_years = set()
        for yr in flagged_year_nums:
            for offset in range(-_EXPAND_RANGE, _EXPAND_RANGE + 1):
                expanded_years.add(yr + offset)
        # Only keep years that actually have filings
        expanded_years = sorted(
            [y for y in expanded_years if _find_filing_for_year(y, annual_filings)],
            reverse=True,
        )
        # Horizontal anomalies also follow excerpt-first, then fallback to full report.
        force_full_years = set()
        log(f"\n⚠️ 发现 {len(flagged_year_nums)} 个可疑年份: {flagged_year_nums}")
        log(f"📎 扩展至 ±{_EXPAND_RANGE} 年进行确认: {expanded_years}")
        if force_full_years:
            log(f"📘 强制完整年报复核年份: {sorted(force_full_years, reverse=True)}")
        log(f"🔍 验证迭代 {v_iter} Step 2: 读取扩展年份的完整年报进行修正...")

        # Build fix prompt with expanded years info
        expanded_suspect = list(suspect_years)
        for yr in expanded_years:
            if yr not in flagged_year_nums:
                expanded_suspect.append({"year": yr, "fields": ["OCF", "CapEx", "FCF"],
                                          "reason": f"±{_EXPAND_RANGE} 年扩展确认"})

        # Step2: verify/correct ONE YEAR PER REQUEST.
        # Excerpt first, then fallback to FULL report for that same year.
        _v_sys_prompt = FCF_SYSTEM_PROMPT_CN if (market or "").upper() == "CN" else FCF_SYSTEM_PROMPT
        any_correction = False
        step2_token_rows = []
        for yr in expanded_years:
            if _successful_llm_calls >= max_successful_requests:
                log(f"⛔ 已达到LLM请求上限，跳过后续验证年份。")
                break
            filing = _find_filing_for_year(yr, annual_filings)
            if not filing:
                continue

            one_suspect = [
                s for s in expanded_suspect if _to_year(s.get("year")) == yr
            ]
            if not one_suspect:
                one_suspect = [{
                    "year": yr,
                    "fields": ["OCF", "CapEx", "FCF"],
                    "reason": "single-year verification",
                }]
            fix_prompt = _build_verification_fix_prompt(filled, one_suspect, market=market)

            # 1) Try excerpt first for all suspect years
            excerpt_text = None
            excerpt_label = filing["label"]
            if yr in year_filing_text:
                excerpt_text, _tok, excerpt_label = year_filing_text[yr]
            else:
                try:
                    excerpt_text, _tok = extract_and_cache_financial_section(filing["path"])
                except Exception:
                    excerpt_text = None

            corrections = None
            if excerpt_text:
                step2_token_rows.append({
                    "year": yr, "kind": "节选", "tokens": estimate_tokens(excerpt_text),
                    "label": excerpt_label,
                })
                prompt_info["batch_prompts"].append(
                    f"[VERIFICATION iter{v_iter} STEP2 year{yr} excerpt]\n{fix_prompt}"
                )
                log(f"📄 Step2 {yr}: 先用节选数据验证")
                try:
                    import time
                    time.sleep(5)
                    reply = _gemini_call_with_retry(
                        client, model_name,
                        contents=[types.Content(role="user", parts=[
                            types.Part(text=f"═══ Annual report EXCERPT: {excerpt_label} (Fiscal Year {yr}) ═══\n\n{excerpt_text}"),
                            types.Part(text=fix_prompt),
                        ])],
                        config=types.GenerateContentConfig(
                            system_instruction=_v_sys_prompt,
                            temperature=0.1,
                        ),
                        log_fn=log, enabled_models=_enabled,
                    )
                    _successful_llm_calls += 1
                    parsed = _parse_llm_json(reply)
                    corrections = _normalize_verification_corrections(parsed, filled)
                    if corrections:
                        log(
                            f"📨 Gemini 验证 iter{v_iter} Step2 year{yr} excerpt 回复:\n"
                            f"{_render_parsed_llm_for_log(corrections, 1000)}"
                        )
                    else:
                        log(f"📨 Gemini 验证 iter{v_iter} Step2 year{yr} excerpt 回复:\n{_format_llm_output_for_log(reply, 1000)}")
                except Exception as e:
                    log(f"⚠️ 验证 iter{v_iter} year{yr} 节选失败: {e}")

            # 2) Fallback to full report for this same year
            need_full_fallback = True
            if isinstance(corrections, list) and corrections:
                for _it in corrections:
                    if all(_it.get(k) is not None for k in ["OCF", "CapEx", "FCF"]):
                        need_full_fallback = False
                        break

                    # If model explicitly confirms current values are correct,
                    # treat excerpt check as sufficient (avoid redundant FULL fallback).
                    if _reason_indicates_correct(_it.get("reason", "")):
                        need_full_fallback = False
                        break

            if need_full_fallback:
                if _successful_llm_calls >= max_successful_requests:
                    log(f"⛔ 已达到LLM请求上限，跳过完整年报验证。")
                    corrections = []
                else:
                    try:
                        full_text = extract_text(filing["path"])
                        max_chars = _MAX_TOKENS_PER_FILING * 4
                        if len(full_text) > max_chars:
                            full_text = full_text[:max_chars]
                        step2_token_rows.append({
                            "year": yr, "kind": "完整", "tokens": estimate_tokens(full_text),
                            "label": filing["label"],
                        })
                        prompt_info["batch_prompts"].append(
                            f"[VERIFICATION iter{v_iter} STEP2 year{yr} full]\n{fix_prompt}"
                        )
                        log(f"📄 Step2 {yr}: 节选不足，回退到完整年报")
                        import time
                        time.sleep(5)
                        reply = _gemini_call_with_retry(
                            client, model_name,
                            contents=[types.Content(role="user", parts=[
                                types.Part(text=f"═══ FULL Annual report: {filing['label']} (Fiscal Year {yr}) ═══\n\n{full_text}"),
                                types.Part(text=fix_prompt),
                            ])],
                            config=types.GenerateContentConfig(
                                system_instruction=_v_sys_prompt,
                                temperature=0.1,
                            ),
                            log_fn=log, enabled_models=_enabled,
                        )
                        _successful_llm_calls += 1
                        parsed = _parse_llm_json(reply)
                        corrections = _normalize_verification_corrections(parsed, filled)
                        if corrections:
                            log(
                                f"📨 Gemini 验证 iter{v_iter} Step2 year{yr} full 回复:\n"
                                f"{_render_parsed_llm_for_log(corrections, 1000)}"
                            )
                        else:
                            log(f"📨 Gemini 验证 iter{v_iter} Step2 year{yr} full 回复:\n{_format_llm_output_for_log(reply, 1000)}")
                    except Exception as e:
                        log(f"⚠️ 验证 iter{v_iter} year{yr} 完整年报失败: {e}")
                        corrections = []

            if isinstance(corrections, list) and corrections:
                for item in corrections:
                    yr2 = item.get("year")
                    yr2 = _to_year(yr2)
                    if yr2 is None:
                        continue
                    row_mask = filled["年份"].astype(str).str.startswith(str(yr2))
                    if not row_mask.any():
                        continue
                    idx = filled.index[row_mask][0]
                    corrected_fields = []
                    checked_fields = []
                    for col, jk in [("OCF", "OCF"), ("CapEx", "CapEx"), ("FCF", "FCF")]:
                        if col not in filled.columns:
                            continue
                        nv = item.get(jk)
                        if nv is not None:
                            old_v = filled.at[idx, col]
                            new_v = float(nv)
                            if isinstance(old_v, (int, float)) and pd.notna(old_v) and np.isclose(float(old_v), new_v, rtol=1e-6, atol=1e-6):
                                checked_fields.append(f"{col}: {new_v:,.0f} ✓")
                            else:
                                filled.at[idx, col] = new_v
                                old_str = f"{old_v:,.0f}" if isinstance(old_v, (int, float)) and pd.notna(old_v) else "N/A"
                                corrected_fields.append(f"{col}: {old_str} → {new_v:,.0f}")
                                any_correction = True
                    # If OCF or CapEx was corrected, force-recompute FCF = OCF - abs(CapEx)
                    ocf_cap_changed = any(
                        f.startswith("OCF:") or f.startswith("CapEx:") for f in corrected_fields
                    )
                    if ocf_cap_changed and all(c in filled.columns for c in ["OCF", "CapEx", "FCF"]):
                        new_ocf = filled.at[idx, "OCF"]
                        new_capex = filled.at[idx, "CapEx"]
                        if pd.notna(new_ocf) and pd.notna(new_capex):
                            auto_fcf = new_ocf - abs(new_capex)
                            old_fcf = filled.at[idx, "FCF"]
                            if not (isinstance(old_fcf, (int, float)) and pd.notna(old_fcf)
                                    and np.isclose(float(old_fcf), auto_fcf, rtol=1e-4)):
                                filled.at[idx, "FCF"] = auto_fcf
                                corrected_fields.append(f"FCF(重算): {auto_fcf:,.0f}")
                                any_correction = True

                    reason = item.get("reason", "")
                    if corrected_fields:
                        log(f"🔧 {yr2}: 验证更正 — {'; '.join(corrected_fields)}"
                            + (f" (原因: {reason})" if reason else ""))
                        filled = _recompute_averages(filled)
                        if table_update_callback:
                            table_update_callback(filled)
                    if checked_fields:
                        log(f"✅ {yr2}: 一致校验 — {'; '.join(checked_fields)}")

        _log_token_estimate_table(
            log,
            f"验证 iter{v_iter} Step2 token 估算（节选/完整）",
            sorted(step2_token_rows, key=lambda x: (x["year"], x["kind"])),
        )

        if any_correction:
            filled = _recompute_averages(filled)
            log(f"✅ 验证迭代 {v_iter}: 更正已应用，已重新计算滚动均值")
            if table_update_callback:
                table_update_callback(filled)
            continue  # re-validate
        else:
            # Mark all non-anomalous years as verified
            log(f"✅ 验证迭代 {v_iter}: Gemini 确认数据无误")
            break
    else:
        log(f"⚠️ 已达到最大验证迭代次数 ({_MAX_VERIFY_ITERATIONS})，停止验证")

    # Local anomaly check for diagnostics only
    local_anomalies = _local_magnitude_check(filled)
    if local_anomalies:
        _log_anomaly_table(filled, None, log, local_anomalies)
        anomaly_years = sorted(set(a["year"] for a in local_anomalies), reverse=True)
        log(f"⚠️ 本地检测到数量级异常年份: {anomaly_years}")

    log(f"\n🎉 完成! 共处理 {total_years} 个年份" +
        (f"，其中 {len(failed_years)} 个需要重试" if failed_years else ""))
    log(f"📊 LLM请求统计: 共使用 {_successful_llm_calls}/{max_successful_requests} 次成功请求")
    if progress_callback:
        progress_callback(None, total_years, total_years)

    return filled, logs, prompt_info


def _recompute_averages(tbl: pd.DataFrame) -> pd.DataFrame:
    """Auto-fill FCF from OCF-CapEx, recompute 每股FCF, and rolling averages."""
    # Ensure sorted descending by year
    tbl = tbl.sort_values("年份", ascending=False).reset_index(drop=True)

    # Auto-compute FCF = OCF - CapEx where FCF is missing but both inputs exist
    if "OCF" in tbl.columns and "CapEx" in tbl.columns and "FCF" in tbl.columns:
        for i in range(len(tbl)):
            if pd.isna(tbl.at[i, "FCF"]):
                ocf = tbl.at[i, "OCF"]
                capex = tbl.at[i, "CapEx"]
                if pd.notna(ocf) and pd.notna(capex):
                    tbl.at[i, "FCF"] = ocf - abs(capex)

    # 每股FCF is aligned to latest shares via recompute_fcf_per_share;
    # do not rely on historical shares column here.

    if "每股FCF" not in tbl.columns:
        return tbl

    avg3 = []
    avg5 = []
    for i in range(len(tbl)):
        w3 = tbl.iloc[i: i + 3]
        w5 = tbl.iloc[i: i + 5]
        ps3 = w3["每股FCF"].dropna().tolist()
        ps5 = w5["每股FCF"].dropna().tolist()
        avg3.append(np.mean(ps3) if ps3 else None)
        avg5.append(np.mean(ps5) if ps5 else None)

    tbl["3年均每股FCF"] = avg3
    tbl["5年均每股FCF"] = avg5
    return tbl
