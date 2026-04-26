"""
测试目标：dashboards.db_status 的两件事
- bootstrap_read_replica：默认走副本但绝不覆盖用户已显式设置的 env
- compute_db_status：返回的 (label / color / age) 在不同新鲜度档位上的分支正确

意义：Phase 2-4 让 Streamlit 默认读 stock_read.db；副本几小时没刷的话用户必须
能一眼看出（颜色编码 + age），否则会拿陈旧数据做决策。bootstrap 的安全性也很
关键 —— 不能"好心"把用户主库 / 测试 DB 的 env 覆盖掉。
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from dashboards import db_status


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv(db_status.READ_DB_ENV, raising=False)
    yield


# ---------------------------------------------------------------------------
# bootstrap_read_replica
# ---------------------------------------------------------------------------

def test_bootstrap_sets_env_when_snapshot_exists_and_env_unset(tmp_path, monkeypatch):
    snap = tmp_path / "stock_read.db"
    snap.write_bytes(b"snap")
    fake_main = tmp_path / "stock.db"
    fake_main.write_bytes(b"main")

    out = db_status.bootstrap_read_replica(db_path=fake_main)
    assert out == str(snap.resolve())
    assert os.environ[db_status.READ_DB_ENV] == str(snap.resolve())


def test_bootstrap_returns_none_when_no_snapshot(tmp_path):
    fake_main = tmp_path / "stock.db"
    fake_main.write_bytes(b"main")
    out = db_status.bootstrap_read_replica(db_path=fake_main)
    assert out is None
    assert db_status.READ_DB_ENV not in os.environ


def test_bootstrap_preserves_explicit_user_env(tmp_path, monkeypatch):
    """关键：如果用户已经手动设了 STOCK_ANALYZER_READ_DB，不能被覆盖
    （比如测试场景指了一个测试 DB，bootstrap 不能把它替换成 stock_read.db）。"""
    snap = tmp_path / "stock_read.db"
    snap.write_bytes(b"snap")
    fake_main = tmp_path / "stock.db"
    fake_main.write_bytes(b"main")
    monkeypatch.setenv(db_status.READ_DB_ENV, "/some/explicit/path.db")

    out = db_status.bootstrap_read_replica(db_path=fake_main)
    assert out == "/some/explicit/path.db"
    assert os.environ[db_status.READ_DB_ENV] == "/some/explicit/path.db"


def test_bootstrap_accepts_explicit_snapshot_path(tmp_path):
    snap = tmp_path / "custom_replica.db"
    snap.write_bytes(b"x")
    out = db_status.bootstrap_read_replica(snapshot_path=snap)
    assert out == str(snap.resolve())


# ---------------------------------------------------------------------------
# compute_db_status — color bands by age
# ---------------------------------------------------------------------------

def _set_replica(monkeypatch, p: Path) -> None:
    monkeypatch.setenv(db_status.READ_DB_ENV, str(p))


def test_status_fresh_when_replica_under_1h(tmp_path, monkeypatch):
    snap = tmp_path / "stock_read.db"
    snap.write_bytes(b"x")
    now = time.time()
    os.utime(snap, (now - 600, now - 600))   # 10 minutes old
    _set_replica(monkeypatch, snap)

    info = db_status.compute_db_status(now=now)
    assert info["is_replica"] is True
    assert info["color"] == "#22c55e"  # green / fresh
    assert "10m" in info["label"]


def test_status_stale_when_replica_between_1h_and_24h(tmp_path, monkeypatch):
    snap = tmp_path / "stock_read.db"
    snap.write_bytes(b"x")
    now = time.time()
    os.utime(snap, (now - 6 * 3600, now - 6 * 3600))   # 6 hours old
    _set_replica(monkeypatch, snap)

    info = db_status.compute_db_status(now=now)
    assert info["color"] == "#f59e0b"   # yellow
    assert "6h" in info["label"]


def test_status_old_when_replica_over_24h(tmp_path, monkeypatch):
    snap = tmp_path / "stock_read.db"
    snap.write_bytes(b"x")
    now = time.time()
    os.utime(snap, (now - 3 * 86400, now - 3 * 86400))   # 3 days old
    _set_replica(monkeypatch, snap)

    info = db_status.compute_db_status(now=now)
    assert info["color"] == "#ef4444"   # red
    assert "3d" in info["label"]


def test_status_reports_main_db_when_env_unset(monkeypatch):
    monkeypatch.delenv(db_status.READ_DB_ENV, raising=False)
    info = db_status.compute_db_status()
    assert info["is_replica"] is False
    assert "主库" in info["label"]


def test_status_reports_missing_replica_when_env_points_at_nonexistent_file(tmp_path, monkeypatch):
    """env 指向一个不存在的文件（用户配错路径）→ UI 必须用红色明显告警，
    而不是悄悄 fallback 到主库。"""
    monkeypatch.setenv(db_status.READ_DB_ENV, str(tmp_path / "nope.db"))
    info = db_status.compute_db_status()
    assert info["color"] == "#ef4444"
    assert "缺失" in info["label"]
