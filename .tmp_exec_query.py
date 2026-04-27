import json
import duckdb
from us_universe import fetch_us_universe

# 1) FMP universe
universe = fetch_us_universe(force_refresh=True)
fmp_total = len(universe)
universe_set = set(universe.keys())

# 2) Open db readonly and compute intersections
con = duckdb.connect('stock.db', read_only=True)

def table_tickers(table_name):
    rows = con.execute(f"SELECT DISTINCT ticker FROM {table_name} WHERE ticker IS NOT NULL").fetchall()
    return {r[0] for r in rows if r[0] in universe_set}

companies_set = table_tickers('companies')
ohlcv_set = table_tickers('ohlcv_daily')
fundamentals_set = table_tickers('fundamentals_annual')
dcf_set = table_tickers('dcf_history')
fmp_dcf_set = table_tickers('fmp_dcf_history')

fully_ready = companies_set & ohlcv_set & fundamentals_set & dcf_set & fmp_dcf_set

# 3) Compact JSON counts + percentages

def metric(count):
    pct = round((count / fmp_total * 100.0), 2) if fmp_total else 0.0
    return {"count": count, "pct": pct}

result = {
    "fmp_total": fmp_total,
    "companies": metric(len(companies_set)),
    "ohlcv_daily": metric(len(ohlcv_set)),
    "fundamentals_annual": metric(len(fundamentals_set)),
    "dcf_history": metric(len(dcf_set)),
    "fmp_dcf_history": metric(len(fmp_dcf_set)),
    "fully_ready_count": metric(len(fully_ready)),
}

# 4) sample missing in companies (alphabetical)
missing_companies = sorted(universe_set - companies_set)[:10]

print(json.dumps(result, separators=(',', ':')))
print(json.dumps({"missing_companies_sample": missing_companies}, separators=(',', ':')))
