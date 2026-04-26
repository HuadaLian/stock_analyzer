"""
测试目标：_as_profile_dict 与 fetch_profile 对 FMP /profile 异常响应形状的健壮性。

背景：bulk 跑全宇宙时偶发 BJGPY 类崩溃 —— FMP 返回 list/None/空列表，原 fetch_profile
直接 data[0] 后 .get 触发裸 AttributeError，被 bulk 记成"未知错误"难以排查。
新增 _as_profile_dict 把所有形状收口到 dict 或 None；非 dict 时 fetch_profile 必须
抛出可读 ValueError，由 us_bulk_run 写到 etl_us_bulk_state.last_error。

关键不变量：
- dict / [dict] / [dict, dict] / 多 entry 中 symbol 命中 / 空 list / None / 标量
  这几种形状下行为符合预期。
- fetch_profile 在 normalize 失败时抛 ValueError（不是 AttributeError）。
- 错误消息含 ticker 与 shape，便于日志排查。
"""

import pytest

from etl.sources.fmp import _as_profile_dict, fetch_profile


def test_as_profile_dict_passes_dict_through():
    raw = {"companyName": "Foo Corp", "symbol": "FOO"}
    assert _as_profile_dict(raw, "FOO") is raw


def test_as_profile_dict_takes_first_when_list_of_one():
    raw = [{"companyName": "Foo Corp", "symbol": "FOO"}]
    assert _as_profile_dict(raw, "FOO") is raw[0]


def test_as_profile_dict_prefers_symbol_match_in_multi_entry_list():
    """FMP 偶发返回多家公司；优先按 ticker 匹配，避免拿错主体。"""
    raw = [
        {"companyName": "Other Co", "symbol": "OTH"},
        {"companyName": "Bar Corp", "symbol": "BAR"},
    ]
    assert _as_profile_dict(raw, "BAR") is raw[1]
    # ticker 不区分大小写匹配
    assert _as_profile_dict(raw, "bar") is raw[1]


def test_as_profile_dict_falls_back_to_first_when_no_symbol_match():
    raw = [
        {"companyName": "Other Co", "symbol": "OTH"},
        {"companyName": "Else Co", "symbol": "ELS"},
    ]
    # 无 symbol 命中 → 取首条（带 warning，单测不强校验日志）
    assert _as_profile_dict(raw, "ZZZ") is raw[0]


def test_as_profile_dict_returns_none_for_empty_list():
    assert _as_profile_dict([], "ANY") is None


def test_as_profile_dict_returns_none_for_none():
    assert _as_profile_dict(None, "ANY") is None


def test_as_profile_dict_returns_none_for_list_of_non_dict():
    """FMP 偶发返回 [None] 或 [str]，不应误以为是 profile。"""
    assert _as_profile_dict([None], "ANY") is None
    assert _as_profile_dict(["unexpected"], "ANY") is None


def test_as_profile_dict_returns_none_for_scalar():
    assert _as_profile_dict("error string", "ANY") is None
    assert _as_profile_dict(42, "ANY") is None


def test_fetch_profile_raises_value_error_on_empty_list(mock_fmp):
    """以前会在 p.get(...) 触发 AttributeError；现在必须是带 ticker 的 ValueError。"""
    mock_fmp.set("profile", [])
    with pytest.raises(ValueError, match="BJGPY"):
        fetch_profile("BJGPY")


def test_fetch_profile_raises_value_error_on_none(mock_fmp):
    mock_fmp.set("profile", None)
    with pytest.raises(ValueError, match="BJGPY"):
        fetch_profile("BJGPY")


def test_fetch_profile_raises_value_error_on_list_of_non_dict(mock_fmp):
    mock_fmp.set("profile", [None])
    with pytest.raises(ValueError, match="shape="):
        fetch_profile("WEIRD")


def test_fetch_profile_handles_multi_entry_list_with_symbol_match(mock_fmp):
    """多 entry 时按 symbol 命中，确保不拿到错误主体。"""
    mock_fmp.set("profile", [
        {"companyName": "Wrong Co", "symbol": "WRG", "currency": "USD"},
        {"companyName": "Right Co", "symbol": "RGT", "currency": "USD",
         "sharesOutstanding": 1_000_000_000, "mktCap": 50_000_000_000},
    ])
    profile = fetch_profile("RGT")
    assert profile["name"] == "Right Co"
    assert profile["shares_out"] == 1_000.0
