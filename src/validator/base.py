"""校验器基类"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy import func, text
from sqlalchemy.orm import Session

import logging

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """单次校验结果"""

    check_name: str
    passed: bool
    detail: str = ""
    expected: float | None = None
    actual: float | None = None


@dataclass
class ValidationReport:
    """完整校验报告"""

    results: list[ValidationResult] = field(default_factory=list)

    @property
    def all_passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def summary(self) -> str:
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        return f"{passed}/{total} checks passed"

    def format_detail(self) -> str:
        lines = [self.summary]
        for r in self.results:
            icon = "OK" if r.passed else "FAIL"
            lines.append(f"  [{icon}] {r.check_name}: {r.detail}")
        return "\n".join(lines)


class BaseValidator(ABC):
    """校验器基类"""

    name: str = "base"

    @abstractmethod
    def validate(self, session: Session) -> ValidationReport:
        """执行校验，返回报告"""
        pass

    @staticmethod
    def _count_table(session: Session, table_name: str) -> int:
        """统计表记录数"""
        result = session.execute(
            text(f"SELECT COUNT(*) FROM {table_name}")
        )
        return result.scalar()

    def _assert_not_empty(
        self, session: Session, table_name: str, min_rows: int = 1
    ) -> ValidationResult:
        """校验表记录数不低于阈值"""
        count = self._count_table(session, table_name)
        passed = count >= min_rows
        return ValidationResult(
            check_name=f"{table_name}.not_empty",
            passed=passed,
            detail=f"记录数 {count} {'≥' if passed else '<'} {min_rows}",
            expected=min_rows,
            actual=count,
        )

    def _assert_date_freshness(
        self,
        session: Session,
        table_name: str,
        date_column: str = "trade_date",
        max_gap_days: int = 3,
    ) -> ValidationResult:
        """校验数据新鲜度：最新日期距今不超过 N 天"""
        result = session.execute(
            text(f"SELECT MAX({date_column}) FROM {table_name}")
        )
        row = result.fetchone()
        if not row or not row[0]:
            return ValidationResult(
                check_name=f"{table_name}.freshness",
                passed=False,
                detail="表中无数据",
            )
        latest_str = str(row[0])
        # 处理 YYYYMMDD 格式
        try:
            latest = datetime.strptime(latest_str, "%Y%m%d")
        except ValueError:
            return ValidationResult(
                check_name=f"{table_name}.freshness",
                passed=False,
                detail=f"日期格式异常: {latest_str}",
            )
        gap = (datetime.now() - latest).days
        passed = gap <= max_gap_days
        return ValidationResult(
            check_name=f"{table_name}.freshness",
            passed=passed,
            detail=f"最新日期 {latest_str}, 距今 {gap} 天 {'≤' if passed else '>'} {max_gap_days}",
            expected=max_gap_days,
            actual=gap,
        )

    def _assert_coverage_ratio(
        self,
        session: Session,
        numerator_table: str,
        denominator_table: str,
        min_ratio: float = 0.95,
        check_name: str | None = None,
    ) -> ValidationResult:
        """校验两表记录数比例"""
        num = self._count_table(session, numerator_table)
        den = self._count_table(session, denominator_table)
        ratio = num / den if den else 0
        passed = ratio >= min_ratio
        name = check_name or f"{numerator_table}.coverage_of_{denominator_table}"
        return ValidationResult(
            check_name=name,
            passed=passed,
            detail=f"{numerator_table}/{denominator_table} = {ratio:.2%}",
            expected=min_ratio,
            actual=ratio,
        )
