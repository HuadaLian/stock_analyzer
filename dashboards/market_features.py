"""Market feature adapters for unified D1 pipeline."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketFeatures:
    market: str
    supports_analyst_panel: bool
    supports_price_alert: bool
    display_currency: str


_MAP = {
    "US": MarketFeatures("US", supports_analyst_panel=True, supports_price_alert=True, display_currency="USD"),
    "CN": MarketFeatures("CN", supports_analyst_panel=False, supports_price_alert=False, display_currency="CNY"),
    "HK": MarketFeatures("HK", supports_analyst_panel=False, supports_price_alert=False, display_currency="HKD"),
}


def get_market_features(market: str) -> MarketFeatures:
    return _MAP.get((market or "US").strip().upper(), _MAP["US"])
