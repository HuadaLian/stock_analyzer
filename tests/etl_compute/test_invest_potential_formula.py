"""
测试目标：compute_invest_potential 严格按公式

    invest_potential = (dcf_14x - latest_price) / dcf_14x

业务含义：当前价格相对保守估值 (14x) 的折价幅度，用 14x 本身归一。
- 价格 < 14x → 正值 (折价，存在投资空间)
- 价格 = 14x → 0
- 价格 > 14x → 负值 (无折价；越大越贵)
- inputs 缺失或 dcf_14x ≤ 0 时返回 None
"""

import pytest
from etl.compute import compute_invest_potential


def test_invest_potential_is_positive_when_price_below_14x():
    # price=10, 14x=50 → (50-10)/50 = 0.8
    assert compute_invest_potential(10.0, 50.0) == pytest.approx(0.8)


def test_invest_potential_is_zero_when_price_equals_14x():
    assert compute_invest_potential(50.0, 50.0) == pytest.approx(0.0)


def test_invest_potential_is_negative_when_price_above_14x():
    """价格 > 14x 时折价为负 — 用于把已被高估的股票排到列表末尾。"""
    # price=70, 14x=50 → (50-70)/50 = -0.4
    assert compute_invest_potential(70.0, 50.0) == pytest.approx(-0.4)


def test_invest_potential_normalizes_by_14x():
    """分母改为 14x 本身：同分子时，14x 越大，潜力百分比越小。"""
    a = compute_invest_potential(10.0, 50.0)   # 40 / 50 = 0.8
    b = compute_invest_potential(10.0, 100.0)  # 90 / 100 = 0.9
    assert a == pytest.approx(0.8)
    assert b == pytest.approx(0.9)


def test_invest_potential_returns_none_when_inputs_missing():
    assert compute_invest_potential(None, 50.0) is None
    assert compute_invest_potential(10.0, None) is None


def test_invest_potential_returns_none_when_14x_nonpositive():
    """亏损股 dcf_14x ≤ 0 时不可作分母。"""
    assert compute_invest_potential(10.0, 0.0)  is None
    assert compute_invest_potential(10.0, -1.0) is None
