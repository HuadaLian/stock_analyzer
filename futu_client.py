# futu_client.py
import pandas as pd
from datetime import datetime, timedelta
from futu import (
    OpenQuoteContext, RET_OK, KLType, AuType,
    SetPriceReminderOp, PriceReminderType, PriceReminderFreq,
)


class FutuClient:
    """Thin wrapper around Futu OpenD quote context."""

    def __init__(self, host="127.0.0.1", port=11111):
        self.host = host
        self.port = port
        self._ctx = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------
    def __enter__(self):
        self._ctx = OpenQuoteContext(host=self.host, port=self.port)
        return self

    def __exit__(self, *exc):
        if self._ctx:
            self._ctx.close()
            self._ctx = None

    @property
    def ctx(self):
        if self._ctx is None:
            raise RuntimeError("FutuClient must be used inside a `with` block")
        return self._ctx

    # ------------------------------------------------------------------
    # Market-code helpers
    # ------------------------------------------------------------------
    @staticmethod
    def build_code(ticker: str, market: str) -> str:
        """Build Futu market code from user input.

        market: 'US' | 'CN' | 'HK'
        For CN, auto-detect SH/SZ from first digit of 6-digit code.
        """
        ticker = ticker.strip().upper()
        if market == "US":
            return f"US.{ticker}"
        if market == "HK":
            code = ticker.zfill(5)
            return f"HK.{code}"
        # CN: decide SH vs SZ
        if ticker.startswith(("6", "9")):
            return f"SH.{ticker}"
        return f"SZ.{ticker}"

    @staticmethod
    def currency_for_market(market: str) -> str:
        return {"US": "USD", "CN": "CNY", "HK": "HKD"}.get(market, "USD")

    # ------------------------------------------------------------------
    # FX conversion
    # ------------------------------------------------------------------
    def get_fx_rate(self, from_ccy: str) -> float:
        """Return multiplier to convert *from_ccy* → USD."""
        if from_ccy == "USD":
            return 1.0
        # Use Futu snapshot for major FX pairs
        if from_ccy == "HKD":
            ret, data = self.ctx.get_market_snapshot(["US.USDHKD"])
            if ret == RET_OK and not data.empty:
                rate = data["last_price"].iloc[0]
                if rate and rate > 0:
                    return 1.0 / rate
        if from_ccy == "CNY":
            ret, data = self.ctx.get_market_snapshot(["US.USDCNH"])
            if ret == RET_OK and not data.empty:
                rate = data["last_price"].iloc[0]
                if rate and rate > 0:
                    return 1.0 / rate
        # Fallback hard-coded approximations
        return {"HKD": 0.128, "CNY": 0.137}.get(from_ccy, 1.0)

    # ------------------------------------------------------------------
    # Historical K-line
    # ------------------------------------------------------------------
    def get_history_kline(self, code: str, years: int = 15) -> pd.DataFrame:
        """Return daily kline DataFrame with columns: time_key, close."""
        end = datetime.now().strftime("%Y-%m-%d")
        start = (datetime.now() - timedelta(days=365 * years)).strftime("%Y-%m-%d")
        ret, data, page_req_key = self.ctx.request_history_kline(
            code,
            start=start,
            end=end,
            ktype=KLType.K_DAY,
            autype=AuType.QFQ,
            max_count=5000,
        )
        if ret != RET_OK:
            raise RuntimeError(f"request_history_kline failed: {data}")
        # Fetch remaining pages if any
        all_data = [data]
        while page_req_key is not None:
            ret, data, page_req_key = self.ctx.request_history_kline(
                code,
                start=start,
                end=end,
                ktype=KLType.K_DAY,
                autype=AuType.QFQ,
                max_count=5000,
                page_req_key=page_req_key,
            )
            if ret == RET_OK:
                all_data.append(data)
            else:
                break
        result = pd.concat(all_data, ignore_index=True)
        result["time_key"] = pd.to_datetime(result["time_key"])
        return result

    # ------------------------------------------------------------------
    # Market snapshot
    # ------------------------------------------------------------------
    def get_snapshot(self, code: str) -> dict:
        """Return dict with last_price, total_market_val, pe_ratio, etc."""
        ret, data = self.ctx.get_market_snapshot([code])
        if ret != RET_OK or data.empty:
            raise RuntimeError(f"get_market_snapshot failed: {data}")
        row = data.iloc[0]
        return {
            "last_price": row.get("last_price"),
            "total_market_val": row.get("total_market_val"),
            "pe_ratio": row.get("pe_ratio"),
            "pe_ttm_ratio": row.get("pe_ttm_ratio"),
            "issued_shares": row.get("issued_shares"),
            "name": row.get("name"),
        }

    # ------------------------------------------------------------------
    # Free Cash Flow (FCF) — via yfinance fallback
    # ------------------------------------------------------------------
    @staticmethod
    def _yf_ticker(code: str) -> str:
        """Convert Futu market code to yfinance ticker."""
        # US.AAPL -> AAPL, HK.00700 -> 0700.HK, SH.600519 -> 600519.SS, SZ.000001 -> 000001.SZ
        prefix, sym = code.split(".", 1)
        if prefix == "US":
            return sym
        if prefix == "HK":
            return f"{sym.lstrip('0') or '0'}.HK"
        if prefix == "SH":
            return f"{sym}.SS"
        if prefix == "SZ":
            return f"{sym}.SZ"
        return sym

    def get_fcf(self, code: str, years: int = 5) -> dict:
        """Return latest annual FCF and N-year average in *local currency*.

        Uses yfinance cashflow statement as the Futu SDK does not expose
        historical financial report data.
        """
        latest_fcf = None
        avg_fcf = None
        try:
            import yfinance as yf
            yf_sym = self._yf_ticker(code)
            tk = yf.Ticker(yf_sym)
            cf = tk.cashflow  # rows = line items, cols = fiscal years
            if cf is not None and not cf.empty:
                # Look for Operating Cash Flow and Capital Expenditure rows
                ocf_row = None
                capex_row = None
                fcf_row = None
                for idx in cf.index:
                    il = idx.lower() if isinstance(idx, str) else str(idx).lower()
                    if "free cash flow" in il:
                        fcf_row = idx
                    if "operating" in il and "cash" in il:
                        ocf_row = idx
                    if "capital expend" in il:
                        capex_row = idx

                if fcf_row is not None:
                    vals = cf.loc[fcf_row].dropna().values
                elif ocf_row is not None and capex_row is not None:
                    ocf = cf.loc[ocf_row].dropna()
                    capex = cf.loc[capex_row].dropna()
                    common = ocf.index.intersection(capex.index)
                    vals = (ocf[common] + capex[common]).values  # capex is negative
                else:
                    vals = []

                if len(vals) > 0:
                    latest_fcf = float(vals[0])
                    n = min(years, len(vals))
                    avg_fcf = float(sum(vals[:n]) / n)
        except Exception:
            pass
        return {"latest_fcf": latest_fcf, "avg_fcf_5y": avg_fcf}

    # ------------------------------------------------------------------
    # Convenience: get all chart + metric data in one call (USD)
    # ------------------------------------------------------------------
    def get_chart_data(self, ticker: str, market: str, years: int = 15):
        """Return (kline_df, metrics_dict) with everything converted to USD."""
        code = self.build_code(ticker, market)
        ccy = self.currency_for_market(market)
        fx = self.get_fx_rate(ccy)

        # Kline
        kline = self.get_history_kline(code, years)
        kline["close_usd"] = kline["close"] * fx

        # EMA on USD prices
        kline["ema10"] = kline["close_usd"].ewm(span=10, adjust=False).mean()
        kline["ema250"] = kline["close_usd"].ewm(span=250, adjust=False).mean()

        # Snapshot
        snap = self.get_snapshot(code)
        market_cap_usd = (snap["total_market_val"] or 0) * fx

        # FCF
        fcf = self.get_fcf(code, years=5)
        latest_fcf_usd = (fcf["latest_fcf"] or 0) * fx
        avg_fcf_usd = (fcf["avg_fcf_5y"] or 0) * fx

        metrics = {
            "market_cap_usd": market_cap_usd,
            "latest_fcf_usd": latest_fcf_usd,
            "avg_fcf_5y_usd": avg_fcf_usd,
            "last_price_usd": (snap["last_price"] or 0) * fx,
            "name": snap.get("name", ""),
        }
        return kline, metrics

    # ------------------------------------------------------------------
    # Price alert via OpenD
    # ------------------------------------------------------------------
    def set_price_alert(self, code, target_price, note="",
                        reminder_type="PRICE_DOWN"):
        """Add a price reminder via Futu OpenD.

        reminder_type: 'PRICE_DOWN' or 'PRICE_UP'
        """
        rt = (PriceReminderType.PRICE_UP
              if reminder_type == "PRICE_UP"
              else PriceReminderType.PRICE_DOWN)
        ret, data = self.ctx.set_price_reminder(
            code=code,
            op=SetPriceReminderOp.ADD,
            reminder_type=rt,
            value=target_price,
            note=note,
            reminder_freq=PriceReminderFreq.ONCE_A_DAY,
        )
        if ret == RET_OK:
            return True, f"提醒设置成功！ID: {data}"
        return False, f"设置失败: {data}"
