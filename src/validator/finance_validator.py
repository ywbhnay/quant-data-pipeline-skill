"""财务数据域数据校验"""

from __future__ import annotations

from sqlalchemy.orm import Session

from .base import BaseValidator, ValidationReport, ValidationResult


class FinanceValidator(BaseValidator):
    """财务三表勾稽关系校验

    校验项:
    1. 三表 + 财务指标均非空
    2. balancesheet/income 记录数比例 (应在 0.5-1.5x)
    3. cashflow/income 记录数比例 (应在 0.5-1.5x)
    4. fina_indicator 覆盖度 (应覆盖大部分 income 股票)
    """

    name = "finance"

    def validate(self, session: Session) -> ValidationReport:
        report = ValidationReport()

        # 1. 四表均非空
        for table_name in ["income", "balancesheet", "cashflow", "fina_indicator"]:
            report.results.append(
                self._assert_not_empty(session, table_name)
            )

        # 2. 三表记录数比例校验
        income_count = self._count_table(session, "income")
        bs_count = self._count_table(session, "balancesheet")
        cf_count = self._count_table(session, "cashflow")

        if income_count > 0:
            bs_ratio = bs_count / income_count
            cf_ratio = cf_count / income_count

            report.results.append(
                ValidationResult(
                    check_name="finance.balance_sheet_ratio",
                    passed=0.5 <= bs_ratio <= 1.5,
                    detail=f"balancesheet/income = {bs_ratio:.2%}",
                    expected=1.0,
                    actual=bs_ratio,
                )
            )
            report.results.append(
                ValidationResult(
                    check_name="finance.cashflow_ratio",
                    passed=0.5 <= cf_ratio <= 1.5,
                    detail=f"cashflow/income = {cf_ratio:.2%}",
                    expected=1.0,
                    actual=cf_ratio,
                )
            )

        # 3. fina_indicator 覆盖度
        fi_count = self._count_table(session, "fina_indicator")
        if income_count > 0:
            fi_ratio = fi_count / income_count
            report.results.append(
                ValidationResult(
                    check_name="finance.fina_indicator_coverage",
                    passed=fi_ratio >= 0.8,
                    detail=f"fina_indicator/income = {fi_ratio:.2%}",
                    expected=0.8,
                    actual=fi_ratio,
                )
            )

        return report
