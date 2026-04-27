"""Single source of truth: what “database quality” means in this repo.

Layers
------
* **Per-ticker (D1 readiness)** — ``db.checks.check_ticker`` + aggregates in
  ``reports.run_db_quality_audit`` (``report.json``).
* **US market / ETL semantics** — ``db.us_data_audit.run_audit`` (Markdown,
  embedded in ``report.json`` excerpt + ``reports/db_quality_cache/us_audit.md``).

Changing detection rules: update the implementing module, then align the
matching row in ``QUALITY_DIMENSIONS`` so UI/JSON stay accurate.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class QualityDimension:
    """One auditable quality facet."""

    code: str
    title_zh: str
    layer_zh: str
    detection_zh: str
    implements: str


# Order: universe → referential → coverage → derived → pipeline
QUALITY_DIMENSIONS: tuple[QualityDimension, ...] = (
    QualityDimension(
        "universe",
        "审查宇宙",
        "全库",
        "固定以 FMP 活跃公司宇宙（``us_universe.fetch_us_universe``）为唯一分母与扫描列表；"
        "不再使用数据库内各业务表的 distinct ticker 并集。",
        "us_universe.fetch_us_universe",
    ),
    QualityDimension(
        "company_row",
        "主体元数据",
        "逐 ticker",
        "``companies`` 是否存在该 ticker 行；缺失会计入 company_missing。",
        "db.checks.check_ticker → company_exists",
    ),
    QualityDimension(
        "ohlcv_coverage",
        "日线覆盖",
        "逐 ticker",
        "``ohlcv_daily`` 行数及日期范围；样本表里暴露 ohlcv_rows。",
        "db.checks.check_ticker → ohlcv_*",
    ),
    QualityDimension(
        "ema_derived",
        "EMA 派生就绪",
        "逐 ticker",
        "在已有日线行上，ema10 与 ema250 各自至少有一条非空（d1_ema_ready）。",
        "db.checks.TickerCompleteness.d1_ema_ready",
    ),
    QualityDimension(
        "fundamentals_coverage",
        "年报行存在",
        "逐 ticker",
        "``fundamentals_annual`` 行数、财年范围、fcf_per_share / filing_date 非空占比（详单见 "
        "``python -m db.checks --ticker X``）。",
        "db.checks.check_ticker",
    ),
    QualityDimension(
        "dcf_history",
        "内部 DCF 历史",
        "逐 ticker",
        "``dcf_history`` 行数 > 0 视为 DCF 历史就绪（d1_dcf_ready）。",
        "db.checks.TickerCompleteness.d1_dcf_ready",
    ),
    QualityDimension(
        "fmp_dcf_history",
        "FMP DCF 历史",
        "逐 ticker",
        "``fmp_dcf_history`` 行数 > 0 且 dcf_value 有非空覆盖（d1_fmp_dcf_ready）。",
        "db.checks.TickerCompleteness.d1_fmp_dcf_ready",
    ),
    QualityDimension(
        "d1_all_ready",
        "D1 全链路就绪",
        "逐 ticker",
        "同时满足 d1_ema_ready、d1_dcf_ready、d1_fmp_dcf_ready；汇总字段 d1_all_ready。",
        "reports.run_db_quality_audit._merge_ticker_into_agg",
    ),
    QualityDimension(
        "us_company_profile",
        "US 公司主数据",
        "US 切片",
        "market='US' 的 companies：currency/country/exchange 等空值计数与 currency 分布。",
        "db.us_data_audit.run_audit §1",
    ),
    QualityDimension(
        "us_fundamentals_currency",
        "US 年报货币一致",
        "US 切片",
        "fundamentals_annual 联 companies：f.currency 应为 USD；reporting_currency、fx_to_usd 非空比例。",
        "db.us_data_audit.run_audit §2",
    ),
    QualityDimension(
        "us_ohlcv_join",
        "US 日线与主体",
        "US 切片",
        "US 公司 ohlcv 行数、market_cap/EMA 空值统计；孤儿 ticker（ohlcv 有而 companies 无）。",
        "db.us_data_audit.run_audit §3",
    ),
    QualityDimension(
        "etl_bulk_state",
        "美股批量 ETL 状态",
        "US 管道",
        "``etl_us_bulk_state`` 按 status 聚合（done/failed/running 等）。",
        "db.us_data_audit.run_audit §4",
    ),
)


def dimensions_payload() -> dict:
    """Serializable block for ``report.json`` (``quality_dimensions``)."""
    return {
        "version": 1,
        "dimensions": [
            {
                "code": d.code,
                "title_zh": d.title_zh,
                "layer_zh": d.layer_zh,
                "detection_zh": d.detection_zh,
                "implements": d.implements,
            }
            for d in QUALITY_DIMENSIONS
        ],
    }
