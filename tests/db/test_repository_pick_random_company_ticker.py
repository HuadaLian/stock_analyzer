"""pick_random_company_ticker：单次 SQL，无预聚合。"""

from __future__ import annotations

import db.repository as repo


def test_pick_random_company_ticker_returns_one_us_row(in_memory_db):
    in_memory_db.execute(
        "INSERT INTO companies (ticker, market, name, currency) VALUES ('ZZZ', 'US', 'z', 'USD')"
    )
    tk = repo.pick_random_company_ticker("US", conn=in_memory_db)
    assert tk == "ZZZ"


def test_pick_random_company_ticker_empty_market(in_memory_db):
    assert repo.pick_random_company_ticker("US", conn=in_memory_db) is None
