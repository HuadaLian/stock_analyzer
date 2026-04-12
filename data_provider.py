# data_provider.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# ═══════════════════════════════════════════════════════════════════════
#  FCF helpers – per-market
# ═══════════════════════════════════════════════════════════════════════

def _yf_fcf_and_shares(yf_ticker_str):
    """Get annual FCF history + shares from yfinance (US & HK)."""
    import yfinance as yf

    tk = yf.Ticker(yf_ticker_str)
    result = {
        "market_cap": None,
        "currency": "USD",
        "shares_outstanding": None,
        "fcf_table": pd.DataFrame(),          # year, OCF, CapEx, FCF, FCF/share
        "fcf_per_share_by_year": {},
        "latest_fcf": None,
        "avg_fcf_5y": None,
    }

    # --- info ----------------------------------------------------------
    try:
        info = tk.info
        result["market_cap"] = info.get("marketCap")
        result["currency"] = info.get("currency", "USD")
        result["shares_outstanding"] = info.get("sharesOutstanding")
    except Exception:
        pass

    # --- cashflow ------------------------------------------------------
    try:
        cf = tk.cashflow
        if cf is None or cf.empty:
            return result

        ocf_row = capex_row = fcf_row = None
        for idx in cf.index:
            il = str(idx).lower()
            if "free cash flow" in il:
                fcf_row = idx
            if "operating" in il and "cash" in il:
                ocf_row = idx
            if "capital expend" in il:
                capex_row = idx

        rows = []
        total_fcf = {}
        for col in cf.columns:
            ts = pd.Timestamp(col)
            ocf_val = float(cf.loc[ocf_row, col]) if ocf_row and pd.notna(cf.loc[ocf_row, col]) else None
            capex_val = float(cf.loc[capex_row, col]) if capex_row and pd.notna(cf.loc[capex_row, col]) else None

            if fcf_row is not None and pd.notna(cf.loc[fcf_row, col]):
                fcf_val = float(cf.loc[fcf_row, col])
            elif ocf_val is not None and capex_val is not None:
                fcf_val = ocf_val + capex_val   # capex is negative
            else:
                fcf_val = None

            if fcf_val is not None:
                total_fcf[ts] = fcf_val
            rows.append({"年份": ts, "OCF": ocf_val, "CapEx": capex_val, "FCF": fcf_val})

        if not total_fcf:
            return result

        # --- shares by year from balance sheet -------------------------
        shares_by_year = {}
        try:
            bs = tk.balance_sheet
            if bs is not None and not bs.empty:
                for idx in bs.index:
                    il = str(idx).lower()
                    if "ordinary shares" in il or "share issued" in il:
                        for col in bs.columns:
                            val = bs.loc[idx, col]
                            if pd.notna(val) and val > 0:
                                shares_by_year[pd.Timestamp(col)] = float(val)
                        if shares_by_year:
                            break
        except Exception:
            pass

        current_shares = result["shares_outstanding"]

        # --- build table & per-share dict ------------------------------
        for row in rows:
            ts = row["年份"]
            shares = shares_by_year.get(ts) or current_shares
            if shares and shares > 0 and row["FCF"] is not None:
                row["每股FCF"] = row["FCF"] / shares
                result["fcf_per_share_by_year"][ts] = row["每股FCF"]
            else:
                row["每股FCF"] = None

        # Sort descending and compute rolling averages
        sorted_rows = sorted(rows, key=lambda x: x["年份"], reverse=True)
        for i, row in enumerate(sorted_rows):
            w3 = sorted_rows[i: i + 3]
            w5 = sorted_rows[i: i + 5]
            ps3 = [r["每股FCF"] for r in w3 if r["每股FCF"] is not None]
            ps5 = [r["每股FCF"] for r in w5 if r["每股FCF"] is not None]
            row["3年均每股FCF"] = np.mean(ps3) if ps3 else None
            row["5年均每股FCF"] = np.mean(ps5) if ps5 else None

        tbl = pd.DataFrame(sorted_rows).head(10)
        tbl["年份"] = tbl["年份"].dt.strftime("%Y-%m-%d")
        result["fcf_table"] = tbl

        sorted_dates = sorted(total_fcf.keys(), reverse=True)
        result["latest_fcf"] = total_fcf[sorted_dates[0]]
        n = min(5, len(sorted_dates))
        result["avg_fcf_5y"] = sum(total_fcf[d] for d in sorted_dates[:n]) / n

    except Exception:
        pass

    return result


def _ak_fcf_data(symbol):
    """Get annual FCF from akshare (A-share stocks) via 新浪现金流量表."""
    import akshare as ak

    full_symbol = f"sh{symbol}" if symbol.startswith(("6", "9")) else f"sz{symbol}"
    result = {
        "fcf_table": pd.DataFrame(),
        "fcf_per_share_by_year": {},
        "latest_fcf": None,
        "avg_fcf_5y": None,
        "shares_outstanding": None,
    }

    try:
        df = ak.stock_financial_report_sina(stock=full_symbol, symbol="现金流量表")
    except Exception:
        return result

    columns = df.columns.tolist()
    date_col = next((c for c in columns if "期" in c or "日" in c), None)
    ocf_col = next((c for c in columns if "经营活动" in c and "净额" in c), None)
    capex_col = next((c for c in columns if "固定资产" in c and "支付" in c), None)

    if not all([date_col, ocf_col, capex_col]):
        return result

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df_annual = df[df[date_col].dt.month == 12].copy()
    df_annual[ocf_col] = pd.to_numeric(df_annual[ocf_col], errors="coerce")
    df_annual[capex_col] = pd.to_numeric(df_annual[capex_col], errors="coerce")
    df_annual = df_annual.dropna(subset=[ocf_col, capex_col])

    df_annual["FCF"] = df_annual[ocf_col] - df_annual[capex_col]

    # --- 获取总股本 (用 akshare) ----------------------------------------
    shares_by_year = {}
    try:
        bs = ak.stock_financial_report_sina(stock=full_symbol, symbol="资产负债表")
        bs_cols = bs.columns.tolist()
        bs_date_col = next((c for c in bs_cols if "期" in c or "日" in c), None)
        shares_col = next((c for c in bs_cols if "股本" in c and "实收" not in c), None)
        if not shares_col:
            shares_col = next((c for c in bs_cols if "实收资本" in c or "股本" in c), None)
        if bs_date_col and shares_col:
            bs[bs_date_col] = pd.to_datetime(bs[bs_date_col], errors="coerce")
            bs_annual = bs[bs[bs_date_col].dt.month == 12].copy()
            bs_annual[shares_col] = pd.to_numeric(bs_annual[shares_col], errors="coerce")
            for _, row in bs_annual.iterrows():
                if pd.notna(row[bs_date_col]) and pd.notna(row[shares_col]) and row[shares_col] > 0:
                    shares_by_year[pd.Timestamp(row[bs_date_col])] = float(row[shares_col])
    except Exception:
        pass

    # Always try to get latest shares from individual info
    current_shares = None
    try:
        info_df = ak.stock_individual_info_em(symbol=symbol)
        for _, row in info_df.iterrows():
            if "总股本" in str(row.iloc[0]):
                current_shares = float(row.iloc[1])
                break
    except Exception:
        pass

    # Fallback: most recent from balance sheet
    if not current_shares and shares_by_year:
        latest_date = max(shares_by_year.keys())
        current_shares = shares_by_year[latest_date]

    result["shares_outstanding"] = current_shares

    # --- build output --------------------------------------------------
    rows = []
    for _, r in df_annual.iterrows():
        ts = pd.Timestamp(r[date_col])
        shares = shares_by_year.get(ts) or current_shares
        fcf_ps = (r["FCF"] / shares) if (shares and shares > 0) else None
        rows.append({
            "年份": ts,
            "OCF": float(r[ocf_col]),
            "CapEx": float(r[capex_col]),
            "FCF": float(r["FCF"]),
            "每股FCF": fcf_ps,
        })
        if fcf_ps is not None:
            result["fcf_per_share_by_year"][ts] = fcf_ps

    # Sort descending and compute rolling averages
    sorted_rows = sorted(rows, key=lambda x: x["年份"], reverse=True)
    for i, row in enumerate(sorted_rows):
        w3 = sorted_rows[i: i + 3]
        w5 = sorted_rows[i: i + 5]
        ps3 = [r["每股FCF"] for r in w3 if r["每股FCF"] is not None]
        ps5 = [r["每股FCF"] for r in w5 if r["每股FCF"] is not None]
        row["3年均每股FCF"] = np.mean(ps3) if ps3 else None
        row["5年均每股FCF"] = np.mean(ps5) if ps5 else None

    tbl = pd.DataFrame(sorted_rows).head(10)
    tbl["年份"] = tbl["年份"].dt.strftime("%Y-%m-%d")
    result["fcf_table"] = tbl

    if sorted_rows:
        result["latest_fcf"] = sorted_rows[0]["FCF"]
        n = min(5, len(sorted_rows))
        result["avg_fcf_5y"] = sum(r["FCF"] for r in sorted_rows[:n]) / n

    return result


# ═══════════════════════════════════════════════════════════════════════
#  DCF Valuation Lines  (uses 3-year rolling avg per-share FCF)
# ═══════════════════════════════════════════════════════════════════════
def _add_yf_cross_check(primary_fin, yf_fin):
    """Add yfinance cross-check columns to the primary (XBRL) FCF table."""
    tbl = primary_fin.get("fcf_table")
    if tbl is None or tbl.empty:
        return
    yf_ps_by_year = {}
    for ts, val in yf_fin.get("fcf_per_share_by_year", {}).items():
        yf_ps_by_year[pd.Timestamp(ts).year] = val
    if not yf_ps_by_year:
        return  # No yfinance data to cross-check

    yf_vals = []
    for _, row in tbl.iterrows():
        yr = int(str(row["年份"])[:4])
        yf_val = yf_ps_by_year.get(yr)
        yf_vals.append(yf_val)
    tbl["yf每股FCF"] = yf_vals


def compute_dcf_lines(fcf_per_share_by_year):
    """Compute DCF valuation lines at 14x, 24x, 34x multiples.

    For each annual report year, FCF_0 = mean of up to 5 years' per-share FCF
    ending at that year.  Returns a DataFrame for marker+dashed line plotting.
    """
    if not fcf_per_share_by_year:
        return pd.DataFrame()

    sorted_dates = sorted(fcf_per_share_by_year.keys())
    dates, v14, v24, v34 = [], [], [], []

    for i, date in enumerate(sorted_dates):
        start_idx = max(0, i - 4)  # up to 5 years (current + 4 prior)
        window = sorted_dates[start_idx : i + 1]
        avg_fcf = np.mean([fcf_per_share_by_year[d] for d in window])

        dates.append(pd.Timestamp(date))
        v14.append(14 * avg_fcf)
        v24.append(24 * avg_fcf)
        v34.append(34 * avg_fcf)

    # Extend last value to today so the step-line reaches the right edge
    dates.append(pd.Timestamp(datetime.now().date()))
    v14.append(v14[-1])
    v24.append(v24[-1])
    v34.append(v34[-1])

    return pd.DataFrame(
        {"date": dates, "dcf_14x": v14, "dcf_24x": v24, "dcf_34x": v34}
    )


# ═══════════════════════════════════════════════════════════════════════
#  US stocks – XBRL-first FCF, yfinance for OHLCV
# ═══════════════════════════════════════════════════════════════════════
def get_us_data(ticker, years=15):
    """Fetch US OHLCV via yfinance, FCF via SEC XBRL with yfinance cross-check."""
    import yfinance as yf
    from xbrl_parser import get_fcf_from_xbrl
    from downloader import SmartSECDownloader

    ticker = ticker.upper()

    # --- OHLCV from yfinance -------------------------------------------
    tk = yf.Ticker(ticker)
    hist = tk.history(
        period="max",
        auto_adjust=True,  # 使用复权后价格
    )
    if hist.empty:
        raise ValueError(f"未找到 {ticker} 的历史数据")
    hist = hist.reset_index()
    hist["Date"] = pd.to_datetime(hist["Date"]).dt.tz_localize(None)

    # Latest price
    last_price = float(hist["Close"].iloc[-1]) if not hist.empty else None

    # Market cap + currency + latest shares from yfinance
    market_cap = None
    currency = "USD"
    shares_outstanding = None
    try:
        info = tk.info
        market_cap = info.get("marketCap")
        currency = info.get("currency", "USD")
        shares_outstanding = info.get("sharesOutstanding")
    except Exception:
        pass

    # --- Get CIK -------------------------------------------------------
    dl = SmartSECDownloader(email="lianhdff@gmail.com")
    cik = None
    try:
        cik = dl.get_cik(ticker)
    except Exception:
        pass

    # --- FCF: XBRL primary, yfinance cross-check ----------------------
    xbrl_fin = None
    if cik:
        try:
            xbrl_fin = get_fcf_from_xbrl(cik)
            if not xbrl_fin.get("fcf_per_share_by_year"):
                xbrl_fin = None
        except Exception:
            xbrl_fin = None

    yf_fin = _yf_fcf_and_shares(ticker)

    if xbrl_fin:
        fin = xbrl_fin
        # Cross-check XBRL vs yfinance (skip if yfinance has no data)
        _add_yf_cross_check(fin, yf_fin)
    else:
        fin = yf_fin

    # Ensure shares_outstanding is always present (prefer yfinance info)
    fin_shares = fin.get("shares_outstanding")
    best_shares = shares_outstanding or fin_shares

    return {
        "ohlcv": hist,
        "last_price": last_price,
        "market_cap": market_cap,
        "currency": currency,
        "cik": cik,
        "shares_outstanding": best_shares,
        **{k: v for k, v in fin.items() if k not in ("market_cap", "currency", "shares_outstanding")},
    }


# ═══════════════════════════════════════════════════════════════════════
#  A-shares (akshare OHLCV + akshare FCF)
# ═══════════════════════════════════════════════════════════════════════
def get_cn_data(code, years=15):
    """Fetch A-share OHLCV via akshare; FCF via akshare 新浪报表."""
    import akshare as ak

    end = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=365 * years)).strftime("%Y%m%d")

    df = ak.stock_zh_a_hist(
        symbol=code, period="daily",
        start_date=start, end_date=end, adjust="qfq",
    )
    if df.empty:
        raise ValueError(f"未找到 {code} 的历史数据")

    df = df.rename(columns={
        "日期": "Date", "开盘": "Open", "最高": "High",
        "最低": "Low", "收盘": "Close", "成交量": "Volume",
    })
    df["Date"] = pd.to_datetime(df["Date"])

    # Financial data via akshare 新浪现金流量表
    fin = _ak_fcf_data(code)
    fin["currency"] = "CNY"

    # Market cap from akshare
    try:
        info_df = ak.stock_individual_info_em(symbol=code)
        for _, row in info_df.iterrows():
            if "总市值" in str(row.iloc[0]):
                fin["market_cap"] = float(row.iloc[1])
                break
    except Exception:
        pass

    return {"ohlcv": df, **fin}


# ═══════════════════════════════════════════════════════════════════════
#  HK stocks (Futu OpenD kline + yfinance financials)
# ═══════════════════════════════════════════════════════════════════════
def get_hk_data(code, years=15):
    """Fetch HK kline via Futu OpenD; financials via yfinance."""
    from futu_client import FutuClient

    futu_code = FutuClient.build_code(code, "HK")

    with FutuClient() as fc:
        kline = fc.get_history_kline(futu_code, years)
        snap = fc.get_snapshot(futu_code)

    ohlcv = kline.rename(columns={
        "time_key": "Date", "open": "Open", "high": "High",
        "low": "Low", "close": "Close", "volume": "Volume",
    })

    yf_code = f"{code.lstrip('0') or '0'}.HK"
    fin = _yf_fcf_and_shares(yf_code)
    fin["currency"] = "HKD"
    fin["market_cap"] = snap.get("total_market_val")

    return {"ohlcv": ohlcv, **fin}
