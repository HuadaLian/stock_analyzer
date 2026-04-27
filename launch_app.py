"""
与 app.py 同级：先后台启动数据库质量审查，再以前台方式启动 Streamlit。

用法（已 conda activate stock_analyzer，且在项目根目录）：

    python launch_app.py

审查子进程的标准错误会追加到 ``reports/db_quality_cache/audit_subprocess.log``。
关闭 Streamlit 后，后台审查进程会继续运行，直至自行结束；需要时可到任务管理器结束对应 python 进程。
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def main() -> None:
    log_dir = ROOT / "reports" / "db_quality_cache"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "audit_subprocess.log"

    with open(log_path, "ab", buffering=0) as logf:
        proc = subprocess.Popen(
            [sys.executable, "-m", "reports.run_db_quality_audit"],
            cwd=str(ROOT),
            stdout=logf,
            stderr=subprocess.STDOUT,
        )
    print(f"[launch_app] 已后台启动数据审查 PID={proc.pid}")
    print(f"[launch_app] 子进程日志: {log_path}")
    print("[launch_app] 正在启动 Streamlit（前台）…\n")

    sys.path.insert(0, str(ROOT))
    os.chdir(ROOT)

    # Set STOCK_ANALYZER_READ_DB in this process so the Streamlit child inherits it when
    # stock_read.db exists (same rule as app.py bootstrap): UI reads replica, bulk writes stock.db.
    try:
        from dashboards.db_status import bootstrap_read_replica

        chosen = bootstrap_read_replica()
        if chosen:
            print(f"[launch_app] 只读库: STOCK_ANALYZER_READ_DB={chosen}")
    except Exception as e:
        print(f"[launch_app] bootstrap_read_replica 跳过: {e}")

    ret = subprocess.run(
        [sys.executable, "-m", "streamlit", "run", "app.py"],
        cwd=str(ROOT),
        env=os.environ.copy(),
    )
    print(f"\n[launch_app] Streamlit 已退出，退出码={ret.returncode}。")
    print(f"[launch_app] 若审查仍在跑，PID={proc.pid} 可能仍在占用只读库；需要时可手动结束该进程。")
    sys.exit(ret.returncode or 0)


if __name__ == "__main__":
    main()
