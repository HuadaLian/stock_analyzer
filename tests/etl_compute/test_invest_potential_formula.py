"""
测试目标：compute_invest_potential 严格按公式

    invest_potential = (dcf_14x - latest_price) / dcf_24x

业务含义：当前价格相对保守估值 (14x) 的折价幅度，用 24x 做归一便于跨股票比较。
- 价格 < 14x → 正值 (折价，存在投资空间)
- 价格 = 14x → 0
- 价格 > 14x → 负值 (无折价；越大越贵)
- inputs 缺失或 dcf_24x ≤ 0 时返回 None
"""

import pytest
from etl.compute import compute_invest_potential


def test_invest_potential_is_positive_when_price_below_14x():
    # price=10, 14x=50, 24x=80 → (50-10)/80 = 0.5
    assert compute_invest_potential(10.0, 50.0, 80.0) == pytest.approx(0.5)


def test_invest_potential_is_zero_when_price_equals_14x():
    assert compute_invest_potential(50.0, 50.0, 80.0) == pytest.approx(0.0)


def test_invest_potential_is_negative_when_price_above_14x():
    """价格 > 14x 时折价为负 — 用于把已被高估的股票排到列表末尾。"""
    # price=70, 14x=50, 24x=80 → (50-70)/80 = -0.25
    assert compute_invest_potential(70.0, 50.0, 80.0) == pytest.approx(-0.25)


def test_invest_potential_normalizes_by_24x_not_14x():
    """归一分母刻意使用 24x：让两支股票的 invest_potential 处在更接近的量级，
    便于跨股票排序。这里用相同分子、不同 24x 验证缩放效果。"""
    a = compute_invest_potential(10.0, 50.0, 80.0)   # 40 / 80 = 0.5
    b = compute_invest_potential(10.0, 50.0, 100.0)  # 40 / 100 = 0.4
    assert a == pytest.approx(0.5)
    assert b == pytest.approx(0.4)


def test_invest_potential_returns_none_when_inputs_missing():
    assert compute_invest_potential(None, 50.0, 80.0) is None
    assert compute_invest_potential(10.0, None, 80.0) is None
    assert compute_invest_potential(10.0, 50.0, None) is None


def test_invest_potential_returns_none_when_24x_nonpositive():
    """亏损股 dcf_24x ≤ 0 时不可作分母。"""
    assert compute_invest_potential(10.0, 50.0, 0.0)  is None
    assert compute_invest_potential(10.0, 50.0, -1.0) is None
