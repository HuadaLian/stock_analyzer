"""
测试目标：us_bulk_run 的两个短连接基础设施
- `_iter_batches`：把 ticker 流切成批，0/负值时退化为单批（行为兼容）
- `_open_conn_with_retry`：重开连接时遇到 Windows 文件锁要指数退避重试

意义：Phase 2-1 的目标是让 bulk 周期性释放 stock.db 的写锁，让只读端
（Streamlit / snapshot / audit）有窗口拿到文件。两个 helper 是这个机制的核心：
切批控制释放频率，重连重试覆盖另一进程抢到锁的竞态。
"""

from __future__ import annotations

import logging

import pytest

from etl.us_bulk_run import _iter_batches, _open_conn_with_retry


# ---------------------------------------------------------------------------
# _iter_batches
# ---------------------------------------------------------------------------

def test_iter_batches_zero_yields_whole_list_once():
    """batch_size=0 必须保留旧行为（单连接跑全表），否则会破坏默认 CLI。"""
    out = list(_iter_batches([1, 2, 3, 4, 5], 0))
    assert out == [[1, 2, 3, 4, 5]]


def test_iter_batches_negative_treated_as_zero():
    out = list(_iter_batches([1, 2, 3], -1))
    assert out == [[1, 2, 3]]


def test_iter_batches_chunks_evenly():
    out = list(_iter_batches([1, 2, 3, 4, 5, 6], 2))
    assert out == [[1, 2], [3, 4], [5, 6]]


def test_iter_batches_last_chunk_can_be_smaller():
    out = list(_iter_batches([1, 2, 3, 4, 5], 2))
    assert out == [[1, 2], [3, 4], [5]]


def test_iter_batches_empty_input_yields_nothing_when_n_positive():
    """N>0 + 空 → 不产任何批，避免下游空批 open 连接。"""
    assert list(_iter_batches([], 3)) == []


def test_iter_batches_empty_input_with_zero_yields_one_empty_batch():
    """N=0 走"单批"路径；保持与"非空 + N=0"一致的形状。"""
    assert list(_iter_batches([], 0)) == [[]]


def test_iter_batches_handles_iterator_input():
    """tickers 可能是 dict_keys 等可迭代，不是 list；helper 不能假设有 len。"""
    out = list(_iter_batches(iter([1, 2, 3, 4]), 3))
    assert out == [[1, 2, 3], [4]]


# ---------------------------------------------------------------------------
# _open_conn_with_retry
# ---------------------------------------------------------------------------

class _FakeConn:
    """Sentinel returned by patched get_conn so we can identify success."""
    def close(self): pass


def test_open_conn_with_retry_succeeds_first_try(monkeypatch, caplog):
    calls = {"n": 0}
    def fake_get_conn():
        calls["n"] += 1
        return _FakeConn()
    monkeypatch.setattr("etl.us_bulk_run.get_conn", fake_get_conn)

    log = logging.getLogger("test")
    conn = _open_conn_with_retry(log)
    assert isinstance(conn, _FakeConn)
    assert calls["n"] == 1


def test_open_conn_with_retry_retries_on_lock_then_succeeds(monkeypatch):
    """模拟 Windows 锁：前 2 次抛"Cannot open file"，第 3 次成功 → 应当重试到底。"""
    attempts = {"n": 0}
    def fake_get_conn():
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise IOError("Cannot open file: stock.db (locked by other process)")
        return _FakeConn()
    monkeypatch.setattr("etl.us_bulk_run.get_conn", fake_get_conn)
    monkeypatch.setattr("etl.us_bulk_run.time.sleep", lambda *_: None)  # 测试不真睡

    log = logging.getLogger("test")
    conn = _open_conn_with_retry(log, max_attempts=4, base_delay_s=0.01)
    assert isinstance(conn, _FakeConn)
    assert attempts["n"] == 3


def test_open_conn_with_retry_recognizes_chinese_lock_message(monkeypatch):
    """Windows 中文系统的锁错误消息也要被识别为可重试，而不是直接抛。"""
    attempts = {"n": 0}
    def fake_get_conn():
        attempts["n"] += 1
        if attempts["n"] < 2:
            raise IOError("另一个程序正在使用此文件，进程无法访问。")
        return _FakeConn()
    monkeypatch.setattr("etl.us_bulk_run.get_conn", fake_get_conn)
    monkeypatch.setattr("etl.us_bulk_run.time.sleep", lambda *_: None)

    log = logging.getLogger("test")
    conn = _open_conn_with_retry(log, max_attempts=3, base_delay_s=0.01)
    assert isinstance(conn, _FakeConn)
    assert attempts["n"] == 2


def test_open_conn_with_retry_does_not_retry_on_non_lock_error(monkeypatch):
    """非锁错误（如配置错、磁盘错）不应被锁路径吃掉；要立刻抛出，便于排查。"""
    def fake_get_conn():
        raise RuntimeError("FMP_API_KEY missing")  # 不像锁错
    monkeypatch.setattr("etl.us_bulk_run.get_conn", fake_get_conn)

    log = logging.getLogger("test")
    with pytest.raises(RuntimeError, match="FMP_API_KEY"):
        _open_conn_with_retry(log, max_attempts=3, base_delay_s=0.01)


def test_open_conn_with_retry_gives_up_after_max_attempts(monkeypatch):
    """如果文件一直被锁，重试用尽必须抛出，不能死循环。"""
    def fake_get_conn():
        raise IOError("Cannot open file")
    monkeypatch.setattr("etl.us_bulk_run.get_conn", fake_get_conn)
    monkeypatch.setattr("etl.us_bulk_run.time.sleep", lambda *_: None)

    log = logging.getLogger("test")
    with pytest.raises(IOError, match="Cannot open file"):
        _open_conn_with_retry(log, max_attempts=3, base_delay_s=0.01)
