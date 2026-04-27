"""Global symbol parsing and session-state routing helpers."""

from __future__ import annotations


def parse_global_symbol(raw: str) -> tuple[str, str]:
    """Parse user input into (market, normalized_ticker)."""
    s = (raw or "").strip().upper()
    if not s:
        return "US", ""

    # CN forms: 600519 / 600519.SH / 000001.SZ
    if s.endswith(".SH") or s.endswith(".SZ") or s.endswith(".BJ"):
        core = s.split(".", 1)[0]
        if core.isdigit():
            suffix = s.split(".", 1)[1]
            return "CN", f"{core.zfill(6)}.{suffix}"
    if s.isdigit() and len(s) == 6:
        suffix = "SS" if s.startswith(("6", "9")) else "SZ"
        return "CN", f"{s}.{suffix}"

    # HK forms: 00700 / 700 / HK.00700 / 00700.HK
    if s.startswith("HK."):
        core = s.split(".", 1)[1]
        if core.isdigit():
            return "HK", f"{core.zfill(4)}.HK"
    if s.endswith(".HK"):
        core = s.split(".", 1)[0]
        if core.isdigit():
            return "HK", f"{core.zfill(4)}.HK"
    if s.isdigit() and 1 <= len(s) <= 5:
        return "HK", f"{s.zfill(4)}.HK"

    # Fallback: US/ADR/OTC-like symbols
    return "US", s


def apply_global_selection(session_state, market: str, ticker: str) -> None:
    """Apply global selection into canonical keys and market-specific keys."""
    m = (market or "US").strip().upper()
    t = (ticker or "").strip().upper()
    session_state["global_selected_market"] = m
    session_state["global_selected_ticker"] = t
    session_state["active_market"] = m
    session_state["active_ticker"] = t
    session_state[f"d1_{m.lower()}_ticker"] = t
