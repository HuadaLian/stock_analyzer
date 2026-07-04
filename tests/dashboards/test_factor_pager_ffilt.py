"""因子榜分页 ``ffilt`` 编解码（整页刷新后恢复筛选）。"""

from __future__ import annotations

import pytest

from dashboards.factor_lab import (
    _factor_lab_filter_to_current_options,
    pack_factor_pager_ffilt,
    parse_factor_pager_ffilt,
)


def test_pack_parse_roundtrip_multiselects_and_mcap():
    mk, ct, se, ind = ["US", "HK"], ["美国", "CN"], ["Tech"], []
    mm = 123.45
    enc = pack_factor_pager_ffilt(
        markets=mk, countries=ct, sectors=se, industries=ind, min_mcap_usd_m=mm
    )
    out = parse_factor_pager_ffilt(enc)
    assert out is not None
    assert out["markets"] == mk
    assert out["countries"] == ct
    assert out["sectors"] == se
    assert out["industries"] == ind
    assert out["min_mcap_usd_m"] == pytest.approx(mm)


def test_parse_rejects_empty_markets():
    enc = pack_factor_pager_ffilt(
        markets=[], countries=[], sectors=[], industries=[], min_mcap_usd_m=0.0
    )
    # mk empty -> pack still produces JSON; parser rejects
    assert parse_factor_pager_ffilt(enc) is None


def test_parse_rejects_garbage():
    assert parse_factor_pager_ffilt("%%%") is None
    assert parse_factor_pager_ffilt(None) is None


def test_filter_to_current_options_drops_stale_entries():
    assert _factor_lab_filter_to_current_options(["US", "XX"], ["US", "CN"]) == ["US"]
    assert _factor_lab_filter_to_current_options(None, ["HK"]) == []
