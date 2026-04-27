"""Initial read replica helper used by us_bulk_run for Streamlit + bulk coexistence."""

from __future__ import annotations

import logging

from etl import us_bulk_run


def test_ensure_initial_read_replica_copies_once(tmp_path, monkeypatch):
    log = logging.getLogger("test_replica")
    src = tmp_path / "stock.db"
    src.write_bytes(b"duckdb-placeholder")
    dst = tmp_path / "stock_read.db"

    calls = {"n": 0}

    def fake_snap(s, d):
        calls["n"] += 1
        assert s == src
        assert d == dst
        dst.write_bytes(b"copy")
        return True, 12, "ok"

    monkeypatch.setattr(us_bulk_run, "snapshot_db", fake_snap)
    us_bulk_run._ensure_initial_read_replica(log, src=src, dst=dst)
    assert calls["n"] == 1
    assert dst.read_bytes() == b"copy"

    us_bulk_run._ensure_initial_read_replica(log, src=src, dst=dst)
    assert calls["n"] == 1, "second call must not re-snapshot when dst exists"


def test_ensure_initial_read_replica_skips_when_dst_is_src(tmp_path):
    log = logging.getLogger("test_replica")
    p = tmp_path / "same.db"
    p.write_bytes(b"x")
    us_bulk_run._ensure_initial_read_replica(log, src=p, dst=p)
    assert p.read_bytes() == b"x"
