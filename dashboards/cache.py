"""Streamlit-aware cached wrappers around `db.repository`.

Lives in dashboards/ (not db/) so the data layer stays free of Streamlit imports.
Cached entries survive across reruns of the same session; bulk ETL writes are
not auto-invalidated, so TTLs are kept short and a "刷新列表" UI control
should call `clear_ticker_list_cache` after a known bulk completion.
"""

from __future__ import annotations

import streamlit as st

from db import repository

# Tickers list rarely changes within a session; 10 min TTL strikes a balance
# between dodging the `ohlcv_daily` DISTINCT fallback cost and still picking up
# newly-loaded tickers without restarting the app.
_TICKER_LIST_TTL_S = 600


@st.cache_data(ttl=_TICKER_LIST_TTL_S, show_spinner=False)
def get_all_tickers_cached(market: str | None = None):
    """Cached `repository.get_all_tickers`. TTL 10 min."""
    return repository.get_all_tickers(market=market)


def clear_ticker_list_cache() -> None:
    """Force next `get_all_tickers_cached` call to re-query the DB.

    Call after a bulk run or any operation that changed `companies` rows.
    """
    get_all_tickers_cached.clear()
