"""Save / load analysed chart data (ohlcv + FCF + metadata) to disk."""

import os
import pickle

BASE_DIR = os.path.dirname(__file__)
CHART_DIR = os.path.join(BASE_DIR, "saved_charts")


def save_chart(ticker: str, market: str, data: dict):
    """Persist the full data dict for one ticker."""
    os.makedirs(CHART_DIR, exist_ok=True)
    path = os.path.join(CHART_DIR, f"{market}_{ticker.upper()}.pkl")
    with open(path, "wb") as f:
        pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)


def load_chart(ticker: str, market: str) -> dict | None:
    """Load previously saved chart data.  Returns None if not found."""
    path = os.path.join(CHART_DIR, f"{market}_{ticker.upper()}.pkl")
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return pickle.load(f)


def list_charts(market: str) -> list[str]:
    """Return sorted list of tickers that have saved charts for a market."""
    if not os.path.exists(CHART_DIR):
        return []
    prefix = f"{market}_"
    suffix = ".pkl"
    return sorted(
        f[len(prefix): -len(suffix)]
        for f in os.listdir(CHART_DIR)
        if f.startswith(prefix) and f.endswith(suffix)
    )
