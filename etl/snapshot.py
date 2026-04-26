"""Atomic snapshot of the DuckDB main file (and WAL) for read replicas.

Used by `etl.us_bulk_run` during the reconnect window: while the writer's
connection is closed (between batches), copy `stock.db` → `stock_read.db` so
Streamlit can read a recent quiescent view without contending for the write
lock that DuckDB enforces on Windows.

Atomicity: copy to `<dst>.tmp` first, then `os.replace` to the final path.
`os.replace` is atomic on Windows when source and destination live on the
same volume — readers either see the previous snapshot or the new one, never
a half-written file.

The caller is responsible for ensuring `src` is quiescent (i.e. the writer
connection has been closed). This module makes no attempt to coordinate with
DuckDB internals; it is a plain file copy with atomic publish.
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

_log = logging.getLogger(__name__)


def snapshot_db(src: Path | str, dst: Path | str) -> tuple[bool, int, str]:
    """Atomic copy of ``src`` (+ ``src.wal`` when present) to ``dst`` / ``dst.wal``.

    Returns ``(success, bytes_copied, message)``. On failure the destination
    is left untouched (any previously published snapshot is still readable).
    """
    src = Path(src)
    dst = Path(dst)
    if not src.is_file():
        return False, 0, f"snapshot src missing: {src}"

    dst.parent.mkdir(parents=True, exist_ok=True)

    tmp_main = dst.with_name(dst.name + ".tmp")
    src_wal = src.with_name(src.name + ".wal")
    dst_wal = dst.with_name(dst.name + ".wal")
    tmp_wal = dst_wal.with_name(dst_wal.name + ".tmp")

    bytes_total = 0
    try:
        shutil.copy2(src, tmp_main)
        bytes_total += tmp_main.stat().st_size

        wal_present = src_wal.is_file()
        if wal_present:
            shutil.copy2(src_wal, tmp_wal)
            bytes_total += tmp_wal.stat().st_size

        # Atomic publish (main first, then WAL — readers will retry if mismatch).
        os.replace(tmp_main, dst)
        if wal_present:
            os.replace(tmp_wal, dst_wal)
        elif dst_wal.is_file():
            # Source has no WAL but destination has a stale one from a prior
            # snapshot — must remove or replicas will see corrupted state.
            try:
                dst_wal.unlink()
            except OSError as e:
                _log.warning("could not remove stale WAL %s: %s", dst_wal, e)

        return True, bytes_total, f"copied {bytes_total / 1_048_576:.1f} MiB → {dst.name}"
    except OSError as e:
        # Clean up partial temp files; leave previously published snapshot intact.
        for p in (tmp_main, tmp_wal):
            if p.is_file():
                try:
                    p.unlink()
                except OSError:
                    pass
        return False, 0, f"snapshot failed: {e}"
