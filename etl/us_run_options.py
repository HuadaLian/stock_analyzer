"""Shared options / logging for US ticker ETL (pipeline + bundle)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class USRunOptions:
    """Options for ``run_us_ticker`` / fetch+apply bundle."""

    skip_optional: bool = False
    """If True, skip management, segment/geo revenue, interest expense (steps 9–12)."""
    verbose: bool = True
    """Print progress lines to stdout."""
    refresh_mode: str = "full"
    """``full`` | ``ohlcv`` | ``fundamentals`` | ``fmp_dcf`` — FMP fetch + apply subset (see ``us_bulk_run --help``)."""


def _log(msg: str, *, verbose: bool) -> None:
    if verbose:
        print(msg)
