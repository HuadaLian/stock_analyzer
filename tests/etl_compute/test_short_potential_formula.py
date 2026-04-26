"""
测试目标：compute_short_potential 严格按公式

    short_potential = max(0, (latest_price - dcf_34x) / dcf_34x)

业务含义：当前价格相对 34x DCF (乐观估值上限) 的超额溢价。
- 价格 ≤ 34x 时夹到 0 (没有做空空间)
- 价格 ≫ 34x 时给出相对偏离度，便于跨股票排序
- inputs 缺失或 dcf_34x ≤ 0 时返回 None (估值不可比，避免误判)
"""

import pytest
from etl.compute import compute_short_potential


def test_short_potential_returns_relative_premium_when_price_above_34x():
    # price=120, 34x=80 → (120-80)/80 = 0.5
    assert compute_short_potential(120.0, 80.0) == pytest.approx(0.5)


def test_short_potential_is_zero_when_price_equals_34x():
    assert compute_short_potential(80.0, 80.0) == pytest.approx(0.0)


def test_short_potential_clamps_to_zero_when_price_below_34x():
    """价格远低于 34x 时不该返回负值 — 业务上没有“负的做空潜力”。"""
    assert compute_short_potential(40.0, 80.0) == 0.0
    assert compute_short_potential(0.01, 80.0) == 0.0


def test_short_potential_returns_none_when_price_missing():
    assert compute_short_potential(None, 80.0) is None


def test_short_potential_returns_none_when_dcf_missing_or_nonpositive():
    """dcf_34x ≤ 0 (亏损股，5yr 兜底仍负) 时不可作为分母，返回 None。"""
    assert compute_short_potential(100.0, None) is None
    assert compute_short_potential(100.0, 0.0)  is None
    assert compute_short_potential(100.0, -5.0) is None
