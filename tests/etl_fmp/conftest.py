"""
共用 fixture：把 etl.sources.fmp._get 替换成查表式的 stub，
这样所有 fetch_* 单元测试都能在不发起真实 HTTP 请求的前提下运行。

使用方式：
    def test_xxx(mock_fmp):
        mock_fmp.set("profile", [{...}])
        mock_fmp.set("cash-flow-statement", [{...}])
        result = fetch_profile("NVDA")
"""

import pytest
from etl.sources import fmp as fmp_module


class FMPStub:
    """按 (endpoint, symbol) 返回预设响应，并记录调用参数以便断言。

    同一个 endpoint 可以接多个 symbol（例如 historical-price-eod/full 既要
    返回股票 K 线，也要返回 CNYUSD 等汇率序列），所以 key 必须把 symbol 一起带上。
    set() 不传 symbol 时落到默认槽，匹配任何 symbol。
    """

    _DEFAULT = "__default__"

    def __init__(self):
        self._responses: dict[tuple[str, str], list | dict] = {}
        self.calls: list[tuple[str, dict]] = []

    def set(self, endpoint: str, payload: list | dict, *, symbol: str | None = None) -> None:
        self._responses[(endpoint, symbol or self._DEFAULT)] = payload

    def __call__(self, endpoint: str, **params):
        self.calls.append((endpoint, params))
        sym = params.get("symbol")
        # exact (endpoint, symbol) match wins; falls back to (endpoint, default)
        if sym and (endpoint, sym) in self._responses:
            return self._responses[(endpoint, sym)]
        if (endpoint, self._DEFAULT) in self._responses:
            return self._responses[(endpoint, self._DEFAULT)]
        raise AssertionError(
            f"FMPStub: unconfigured endpoint {endpoint!r} symbol={sym!r}"
        )


@pytest.fixture
def mock_fmp(monkeypatch):
    """替换 _get 与 load_api_key，避免任何真实网络/.env 读取。"""
    stub = FMPStub()
    monkeypatch.setattr(fmp_module, "_get", stub)
    monkeypatch.setattr(fmp_module, "load_api_key", lambda: "TEST_KEY")
    return stub
