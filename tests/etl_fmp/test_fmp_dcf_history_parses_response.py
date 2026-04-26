"""Unit tests for parsing FMP DCF history responses."""

from etl.sources.fmp_dcf import fetch_fmp_dcf_history


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def test_fetch_fmp_dcf_history_parses_and_sorts_rows(monkeypatch):
    payload = [
        {"date": "2025-01-03", "dcf": 15.5, "Stock Price": 150.0},
        {"date": "2025-01-02", "dcf": 14.5, "stockPrice": 148.0},
        {"date": "2025-01-01", "DCF": 14.0},
        {"date": "2025-01-04"},  # missing dcf -> dropped
    ]

    monkeypatch.setattr(
        "etl.sources.fmp_dcf.requests.get",
        lambda *args, **kwargs: _Resp(payload),
    )

    rows = fetch_fmp_dcf_history("nvda", api_key="TEST")

    assert [r["date"] for r in rows] == ["2025-01-01", "2025-01-02", "2025-01-03"]
    assert [r["dcf_value"] for r in rows] == [14.0, 14.5, 15.5]
    assert rows[0]["stock_price"] is None
    assert rows[1]["stock_price"] == 148.0
    assert rows[2]["ticker"] == "NVDA"
