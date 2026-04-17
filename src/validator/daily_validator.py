"""日线行情域数据校验"""

from __future__ import annotations

from sqlalchemy.orm import Session

from .base import BaseValidator, ValidationReport


class DailyValidator(BaseValidator):
    """日线行情数据校验

    校验项:
    1. daily 表非空 (A股 ~5800 只，每日应有 ~5000+ 记录)
    2. daily 数据新鲜度 (交易日后应有数据)
    3. daily_basic 覆盖度 (记录数应 ≈ daily 记录数)
    4. adj_factor 覆盖度 (应 ≈ daily 记录数)
    5. stk_limit 覆盖度 (应 ≈ daily 记录数)
    """

    name = "daily"

    def validate(self, session: Session) -> ValidationReport:
        report = ValidationReport()

        # 1. daily 表非空
        report.results.append(
            self._assert_not_empty(session, "daily", min_rows=5000)
        )

        # 2. daily 数据新鲜度
        report.results.append(
            self._assert_date_freshness(session, "daily", "trade_date", max_gap_days=3)
        )

        # 3. daily_basic 覆盖度 (应接近 daily 记录数)
        report.results.append(
            self._assert_coverage_ratio(
                session, "daily_basic", "daily", min_ratio=0.95
            )
        )

        # 4. adj_factor 覆盖度
        report.results.append(
            self._assert_coverage_ratio(
                session, "adj_factor", "daily", min_ratio=0.95
            )
        )

        # 5. stk_limit 覆盖度
        report.results.append(
            self._assert_coverage_ratio(
                session, "stk_limit", "daily", min_ratio=0.95
            )
        )

        return report
