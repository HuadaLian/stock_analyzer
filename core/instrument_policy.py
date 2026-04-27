"""Unified instrument filtering policy across markets."""

from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class InstrumentPolicy:
    name: str
    include_adr_otc: bool
    exclude_funds_etfs: bool
    exclude_preferred_bond_convertible: bool
    cn_exclude_b_share: bool


def get_policy(mode: str | None = None) -> InstrumentPolicy:
    m = (mode or os.environ.get("STOCK_ANALYZER_FILTER_MODE") or "ordinary_common_stock").strip().lower()
    if m == "operating_company":
        return InstrumentPolicy(
            name="operating_company",
            include_adr_otc=True,
            exclude_funds_etfs=True,
            exclude_preferred_bond_convertible=False,
            cn_exclude_b_share=False,
        )
    if m == "broad_equity":
        return InstrumentPolicy(
            name="broad_equity",
            include_adr_otc=True,
            exclude_funds_etfs=False,
            exclude_preferred_bond_convertible=False,
            cn_exclude_b_share=False,
        )
    return InstrumentPolicy(
        name="ordinary_common_stock",
        include_adr_otc=True,
        exclude_funds_etfs=True,
        exclude_preferred_bond_convertible=True,
        cn_exclude_b_share=True,
    )
