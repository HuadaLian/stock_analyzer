"""
测试目标：us_bulk_run._cleanup_stale_running 在启动时把卡住的 'running' 行
迁到 'interrupted'，避免上一次崩溃留下的脏状态被误读为活动进程。

关键不变量：
- 'running' 行被改为 'interrupted'，并保留可读的 last_error 痕迹。
- 已经处于 done / failed / skipped / interrupted 的行不被触碰。
- 函数返回受影响的行数；空表/无脏行时返回 0 且不报错。
- updated_at 被刷新（证明 UPDATE 真的执行了）。
"""

from __future__ import annotations

from etl.us_bulk_run import _cleanup_stale_running


def _seed(conn, rows: list[tuple[str, str]]) -> None:
    """rows = [(ticker, status), ...]; updated_at 设为远古时间方便对比刷新。"""
    for ticker, status in rows:
        conn.execute(
            """
            INSERT INTO etl_us_bulk_state (ticker, status, step, last_error, updated_at)
            VALUES (?, ?, 'seed', NULL, TIMESTAMP '2000-01-01 00:00:00')
            """,
            [ticker, status],
        )


def _status_map(conn) -> dict[str, str]:
    rows = conn.execute("SELECT ticker, status FROM etl_us_bulk_state").fetchall()
    return {t: s for t, s in rows}


def test_cleanup_returns_zero_on_empty_table(in_memory_db):
    assert _cleanup_stale_running(in_memory_db) == 0


def test_cleanup_returns_zero_when_no_running_rows(in_memory_db):
    _seed(in_memory_db, [("AAA", "done"), ("BBB", "failed"), ("CCC", "skipped")])
    assert _cleanup_stale_running(in_memory_db) == 0
    # 状态完全没动
    assert _status_map(in_memory_db) == {"AAA": "done", "BBB": "failed", "CCC": "skipped"}


def test_cleanup_marks_running_rows_as_interrupted(in_memory_db):
    _seed(in_memory_db, [
        ("AAA", "running"),
        ("BBB", "running"),
        ("CCC", "done"),
        ("DDD", "failed"),
    ])
    n = _cleanup_stale_running(in_memory_db)
    assert n == 2
    statuses = _status_map(in_memory_db)
    assert statuses["AAA"] == "interrupted"
    assert statuses["BBB"] == "interrupted"
    # 非 running 不动
    assert statuses["CCC"] == "done"
    assert statuses["DDD"] == "failed"


def test_cleanup_appends_marker_to_last_error(in_memory_db):
    _seed(in_memory_db, [("AAA", "running")])
    _cleanup_stale_running(in_memory_db)
    last_err = in_memory_db.execute(
        "SELECT last_error FROM etl_us_bulk_state WHERE ticker = 'AAA'"
    ).fetchone()[0]
    assert "stale running" in (last_err or "").lower()


def test_cleanup_refreshes_updated_at(in_memory_db):
    _seed(in_memory_db, [("AAA", "running")])
    _cleanup_stale_running(in_memory_db)
    updated = in_memory_db.execute(
        "SELECT updated_at FROM etl_us_bulk_state WHERE ticker = 'AAA'"
    ).fetchone()[0]
    # 种子是 2000-01-01；UPDATE 必须把它推到现在（>= 2024 是绝对安全的下界）
    assert str(updated) >= "2024-01-01"
