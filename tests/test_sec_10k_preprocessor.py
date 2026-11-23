from datetime import date

import pytest

from agent_scripts import sec_10k_preprocessor


def test_preprocess_10q_keeps_only_the_unregistered_sales_item():
    text = """\
TABLE OF CONTENTS
Item 2. Unregistered Sales of Equity Securities and Use of Proceeds
Old table of contents text
Item 3. Legal Proceedings
PART II
**Item 2. Unregistered Sales of Equity Securities and Use of Proceeds.**
Issuer Purchases of Equity Securities
| Total Number of Shares Purchased | 123,456 |
Item 3. Legal Proceedings
This section must not be included.
"""

    reduced = sec_10k_preprocessor.preprocess_10q_text(text)

    assert "123,456" in reduced
    assert "Old table of contents text" not in reduced
    assert "This section must not be included" not in reduced
    assert "ITEM 2 - UNREGISTERED SALES" in reduced


def test_10q_without_requested_item_has_no_fallback_sections():
    text = "Item 2. Management's Discussion and Analysis\nQuarterly business discussion."

    assert sec_10k_preprocessor.extract_10q_slices(text) == []
    assert sec_10k_preprocessor.preprocess_10q_text(text) == ""


def test_preprocessing_failure_identifies_source_file(tmp_path):
    source = tmp_path / "BROKEN_2025-12-31_10-K.md"
    source.write_text("No extractable 10-K content.", encoding="utf-8")
    destination = tmp_path / "preprocessed" / source.name

    with pytest.raises(RuntimeError) as exc_info:
        sec_10k_preprocessor.preprocess_10k_file(source, destination)

    assert source.name in str(exc_info.value)
    assert "未能完整识别" in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, ValueError)


def test_10k_keeps_complete_item5_but_not_item6():
    text = """\
ITEM 5. MARKET FOR REGISTRANT'S COMMON EQUITY
Ordinary business text that is unrelated to repurchases.

During fiscal 2025, we repurchased 2.9 million shares.

This table presents share repurchases.
| Shares repurchased | 2.9 million |

ITEM 6. RESERVED
This section must not be included.

ITEM 7. MANAGEMENT'S DISCUSSION AND ANALYSIS
This separate Item 7 discussion must not be included.

ITEM 8. FINANCIAL STATEMENTS AND SUPPLEMENTARY DATA
CONSOLIDATED BALANCE SHEETS
Financial statement values.
"""

    sections = sec_10k_preprocessor.extract_10k_slices(text)
    item5 = next(content for title, content in sections if title.startswith("ITEM 5"))

    assert "Ordinary business text" in item5
    assert "2.9 million shares" in item5
    assert "Shares repurchased" in item5
    assert "This section must not be included." not in item5


def test_10k_keeps_complete_item8_and_skips_table_of_contents():
    text = """\
| Consolidated Balance Sheets | 52 |
| Consolidated Statements of Operations | 53 |
| Consolidated Statements of Stockholders' Equity | 54 |
| Consolidated Statements of Cash Flows | 55 |
| Notes to Consolidated Financial Statements | 56 |
ITEM 5. MARKET FOR REGISTRANT'S COMMON EQUITY
Share repurchase discussion.
ITEM 6. RESERVED
ITEM 7. MANAGEMENT'S DISCUSSION AND ANALYSIS
Unrelated discussion.
ITEM 8. FINANCIAL STATEMENTS AND SUPPLEMENTARY DATA
**CONSOLIDATED BALANCE SHEETS**
Balance sheet table.
**CONSOLIDATED STATEMENTS OF OPERATIONS**
Income statement table.
**CONSOLIDATED STATEMENTS OF STOCKHOLDERS' EQUITY**
Equity statement table.
**CONSOLIDATED STATEMENTS OF CASH FLOWS**
Cash flow statement table.
See Note 1 in the preceding discussion.
**NOTES TO CONSOLIDATED FINANCIAL
STATEMENTS**
Note content must be retained.
ITEM 9. CHANGES IN AND DISAGREEMENTS WITH ACCOUNTANTS
Item 9 content must not be included.
"""

    sections = sec_10k_preprocessor.extract_10k_slices(text)
    financials = next(content for title, content in sections if title.startswith("ITEM 8"))

    assert "Balance sheet table." in financials
    assert "Income statement table." in financials
    assert "Equity statement table." in financials
    assert "Cash flow statement table." in financials
    assert "See Note 1 in the preceding discussion." in financials
    assert "Note content must be retained." in financials
    assert "Item 9 content must not be included." not in financials
    assert "| Consolidated Balance Sheets | 52 |" not in financials


def test_10k_appends_audited_statements_when_item8_only_points_to_item15():
    text = """\
ITEM 5. MARKET FOR REGISTRANT'S COMMON EQUITY
Share repurchase disclosure.
ITEM 6. RESERVED
ITEM 8. FINANCIAL STATEMENTS AND SUPPLEMENTARY DATA
See Index to Consolidated Financial Statements at Item 15.
ITEM 9. CHANGES IN AND DISAGREEMENTS WITH ACCOUNTANTS
Nothing further here.
REPORT OF INDEPENDENT REGISTERED PUBLIC ACCOUNTING FIRM
Auditor's report.
CONSOLIDATED BALANCE SHEETS
Balance sheet values.
"""

    sections = sec_10k_preprocessor.extract_10k_slices(text)

    assert any(title.startswith("ITEM 8") for title, _ in sections)
    appendix = next(
        content for title, content in sections if title.startswith("AUDITED")
    )
    assert "Auditor's report." in appendix
    assert "Balance sheet values." in appendix
    assert "Nothing further here." not in appendix


def test_10k_finds_appendix_when_item8_mentions_but_does_not_include_auditor_report():
    text = """\
ITEM 5. MARKET FOR REGISTRANT'S COMMON EQUITY
Share repurchase disclosure.
ITEM 8. FINANCIAL STATEMENTS AND SUPPLEMENTARY DATA
See the independent auditor's report in Item 15.
ITEM 9. CHANGES IN AND DISAGREEMENTS WITH ACCOUNTANTS
This section must not be included in the appendix.
REPORT OF INDEPENDENT REGISTERED PUBLIC ACCOUNTING FIRM
Auditor's report.
CONSOLIDATED BALANCE SHEETS
Balance sheet values.
"""

    sections = sec_10k_preprocessor.extract_10k_slices(text)
    appendix = next(
        content for title, content in sections if title.startswith("AUDITED")
    )

    assert "Auditor's report." in appendix
    assert "Balance sheet values." in appendix
    assert "This section must not be included in the appendix." not in appendix


def test_10q_handles_split_item_heading_and_keeps_only_unregistered_sales_item():
    text = """\
ITEM 2. MANAGEMENT'S DISCUSSION AND ANALYSIS
MD&A must not be included.
PART II - OTHER INFORMATION
ITEM
2. UNREGISTERED SALES OF EQUITY SECURITIES AND USE OF PROCEEDS
Quarterly issuer repurchase table.
| Shares purchased | 123,456 |
ITEM 3. LEGAL PROCEEDINGS
This section must not be included.
"""

    sections = sec_10k_preprocessor.extract_10q_slices(text)
    assert len(sections) == 1
    quarterly_item = sections[0][1]

    assert "Quarterly issuer repurchase table." in quarterly_item
    assert "123,456" in quarterly_item
    assert "MD&A must not be included." not in quarterly_item
    assert "This section must not be included." not in quarterly_item


def test_prompt_uses_quarterly_sum_and_note14_only_as_crosscheck():
    assert "四份 10-Q Item 2" in sec_10k_preprocessor.TASK_PROMPT
    assert "Note 14 等约数只作 cross-check" in sec_10k_preprocessor.TASK_PROMPT
    assert "Item 7" not in sec_10k_preprocessor.TASK_PROMPT


def test_prompt_distinguishes_debt_linked_share_buybacks_from_bond_repurchase():
    prompt = sec_10k_preprocessor.TASK_PROMPT

    assert "必须执行定向回购专项搜索" in prompt
    assert "W_debt_k_shares=2400" in prompt
    assert "债务工具回购" in prompt
    assert "T_debt 填 null" in prompt
    assert "只有完成上述搜索并确认不存在" in prompt


def test_report_period_uses_and_checks_filename_and_cover_period():
    filename = "ACME_CORP_2025-09-30_2025-11-01_10-Q.md"
    matching_text = "For the quarterly period ended September 30, 2025"
    conflicting_text = "For the quarterly period ended October 1, 2025"

    assert sec_10k_preprocessor.infer_report_period_end(
        filename, matching_text
    ) == date(2025, 9, 30)
    assert sec_10k_preprocessor.infer_report_period_end(
        filename, conflicting_text
    ) == date(2025, 10, 1)
    with pytest.raises(ValueError, match="无法从报告正文确认报告期"):
        sec_10k_preprocessor.infer_report_period_end(filename, "Quarterly filing without a period date")


def test_fiscal_year_uses_current_statement_table_year_not_period_end_calendar_year():
    filename = "CAKE_2024-01-02_2024-02-26_10-K.md"
    text = """\
The fiscal year ended January 2, 2024.
| **Fiscal Year** | | | |
| --- | --- | --- |
| **2023** | **2022** | **2021** |
"""

    assert sec_10k_preprocessor.infer_fiscal_year(filename, text) == "2023"


def test_fiscal_year_uses_current_table_year_over_conflicting_prose_mentions():
    filename = "CAKE_2004-12-28_2005-04-04_10-K.md"
    text = """\
| **Fiscal Year** | | | |
| --- | --- | --- |
| **2003** | **2002** | **2001** |
During fiscal 2004, share repurchases increased.
Fiscal year 2005 will consist of 53 weeks.
"""

    assert sec_10k_preprocessor.infer_fiscal_year(filename, text) == "2003"


def test_fiscal_year_ignores_future_labels_and_selects_latest_current_year():
    filename = "CAKE_2024-01-02_2024-02-26_10-K.md"
    text = "During fiscal 2021 and 2022. The report covers fiscal 2023. Fiscal 2025 is projected."

    assert sec_10k_preprocessor.infer_fiscal_year(filename, text) == "2023"


def test_fiscal_year_ignores_future_year_when_no_statement_table_is_available():
    filename = "CAKE_2024-01-02_2024-02-26_10-K.md"
    text = "Fiscal year 2024 will consist of 52 weeks. The report covers fiscal 2023."

    assert sec_10k_preprocessor.infer_fiscal_year(filename, text) == "2023"


def test_quarterly_filings_are_associated_by_ticker_and_period():
    reports = [
        {
            "report_id": "q1",
            "ticker": "CAKE",
            "form_type": "10-Q",
            "report_period_end": date(2025, 9, 30),
            "source_filename": "CAKE_2025-09-30_10-Q.md",
        },
        {
            "report_id": "k1",
            "ticker": "CAKE",
            "form_type": "10-K",
            "report_period_end": date(2025, 12, 30),
            "source_filename": "CAKE_2025-12-30_10-K.md",
        },
        {
            "report_id": "q2",
            "ticker": "CAKE",
            "form_type": "10-Q",
            "report_period_end": date(2026, 3, 31),
            "source_filename": "CAKE_2026-03-31_10-Q.md",
        },
    ]

    associated, unassigned = sec_10k_preprocessor.associate_quarterly_filings(
        reports
    )

    assert [report["report_id"] for report in associated["k1"]] == ["q1"]
    assert [report["report_id"] for report in unassigned] == ["q2"]


def test_duplicate_form_and_period_is_rejected():
    duplicate_annuals = [
        {
            "report_id": report_id,
            "ticker": "CAKE",
            "form_type": "10-K",
            "report_period_end": date(2025, 12, 30),
            "source_filename": filename,
        }
        for report_id, filename in (
            ("k1", "CAKE_2025-12-30_10-K.md"),
            ("k2", "CAKE_2025-12-30_10-K.htm"),
        )
    ]

    with pytest.raises(ValueError, match="重复文件"):
        sec_10k_preprocessor.associate_quarterly_filings(duplicate_annuals)
