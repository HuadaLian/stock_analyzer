"""
测试目标：etl.snapshot.snapshot_db 的原子文件复制行为。

意义：Phase 2-2 在 bulk 的重连窗口期把 stock.db 复制成 stock_read.db，
让 Streamlit 读副本不抢主库。要点是「原子发布」：要么读到旧快照、要么
新快照，永远不能读到半写文件。

关键不变量：
- 主文件按内容、修改时间、权限完整复制（shutil.copy2 行为）。
- 同名 .wal 文件存在时一并复制；源没 WAL 但目标有旧 WAL 时要清理，否则副本启动会脏读。
- 复制中断（mock raise）不能污染既有目标文件，且要清理 .tmp。
- 目标目录不存在时自动创建，避免调用方关心 mkdir。
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from etl.snapshot import snapshot_db


def _make_file(p: Path, content: bytes = b"hello duckdb") -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(content)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_snapshot_copies_main_file_to_destination(tmp_path):
    src = tmp_path / "stock.db"
    dst = tmp_path / "stock_read.db"
    _make_file(src, b"PRIMARY DB CONTENT")

    ok, n, msg = snapshot_db(src, dst)
    assert ok is True
    assert n == len(b"PRIMARY DB CONTENT")
    assert dst.is_file()
    assert dst.read_bytes() == b"PRIMARY DB CONTENT"
    # tmp file gone after atomic publish
    assert not (tmp_path / "stock_read.db.tmp").exists()


def test_snapshot_creates_destination_directory_if_missing(tmp_path):
    src = tmp_path / "stock.db"
    dst = tmp_path / "subdir" / "stock_read.db"
    _make_file(src)
    ok, _, _ = snapshot_db(src, dst)
    assert ok is True
    assert dst.is_file()


def test_snapshot_copies_wal_alongside_main(tmp_path):
    src = tmp_path / "stock.db"
    src_wal = tmp_path / "stock.db.wal"
    dst = tmp_path / "stock_read.db"
    _make_file(src, b"main")
    _make_file(src_wal, b"wal-content")

    ok, n, _ = snapshot_db(src, dst)
    assert ok is True
    assert (tmp_path / "stock_read.db.wal").read_bytes() == b"wal-content"
    # bytes_total 含 WAL
    assert n == len(b"main") + len(b"wal-content")


def test_snapshot_removes_stale_dst_wal_when_src_has_no_wal(tmp_path):
    """如果上一次快照写过 WAL，但源现在已经把 WAL checkpoint 干净了，
    目标的旧 WAL 必须删，否则副本启动会"重放"陈旧 WAL → 脏读。"""
    src = tmp_path / "stock.db"
    dst = tmp_path / "stock_read.db"
    dst_wal = tmp_path / "stock_read.db.wal"
    _make_file(src, b"new-main")
    _make_file(dst, b"old-main")
    _make_file(dst_wal, b"old-wal-from-prior-snapshot")
    # 源没有 .wal

    ok, _, _ = snapshot_db(src, dst)
    assert ok is True
    assert dst.read_bytes() == b"new-main"
    assert not dst_wal.exists(), "旧 WAL 必须被清理"


# ---------------------------------------------------------------------------
# Failure modes
# ---------------------------------------------------------------------------

def test_snapshot_returns_failure_when_src_missing(tmp_path):
    ok, n, msg = snapshot_db(tmp_path / "nope.db", tmp_path / "out.db")
    assert ok is False
    assert n == 0
    assert "missing" in msg.lower()
    assert not (tmp_path / "out.db").exists()


def test_snapshot_failure_does_not_corrupt_existing_destination(tmp_path):
    """复制中断时目标必须保持原状（既有快照仍可读），且 .tmp 被清理。"""
    src = tmp_path / "stock.db"
    dst = tmp_path / "stock_read.db"
    _make_file(src, b"NEW")
    _make_file(dst, b"OLD-still-good")

    with patch("etl.snapshot.shutil.copy2", side_effect=OSError("disk full")):
        ok, n, msg = snapshot_db(src, dst)

    assert ok is False
    assert n == 0
    assert "disk full" in msg
    # 既有副本完好（关键：不能读到半写文件）
    assert dst.read_bytes() == b"OLD-still-good"
    # 没有 .tmp 残留
    assert not (tmp_path / "stock_read.db.tmp").exists()


def test_snapshot_atomic_publish_uses_replace_not_overwrite(tmp_path):
    """监视 os.replace 被调用 → 验证走的是原子路径，不是直接 copy2 over dst。"""
    src = tmp_path / "stock.db"
    dst = tmp_path / "stock_read.db"
    _make_file(src, b"data")

    with patch("etl.snapshot.os.replace", wraps=__import__("os").replace) as spy:
        ok, _, _ = snapshot_db(src, dst)
    assert ok is True
    assert spy.call_count >= 1, "必须用 os.replace 做原子发布"
