"""财务数据域同步器：fina_indicator, income, balancesheet, cashflow"""

from __future__ import annotations

import pandas as pd
import tushare as ts
from sqlalchemy.orm import Session

from ..rate_limiter import RateLimiter
from ..retry import tushare_retry
from .base import BaseSyncer


class FinanceSyncer(BaseSyncer):
    """财务数据域同步

    覆盖表:
    - fina_indicator: 逐股票拉取财务指标，ON CONFLICT DO NOTHING (ts_code, end_date)
    - income: 逐股票拉取利润表，ON CONFLICT DO NOTHING (ts_code, end_date, report_type)
    - balancesheet: 逐股票拉取资产负债表，ON CONFLICT DO NOTHING (ts_code, end_date, report_type)
    - cashflow: 逐股票拉取现金流量表，ON CONFLICT DO NOTHING (ts_code, end_date, report_type)
    """

    name = "finance"

    # ------------------------------------------------------------------ #
    #  Full backfill
    # ------------------------------------------------------------------ #

    def run_full(self, session: Session) -> None:
        self._sync_fina_indicator(session)
        self._sync_income(session)
        self._sync_balancesheet(session)
        self._sync_cashflow(session)

    # ------------------------------------------------------------------ #
    #  Incremental (财务数据按报告期发布，需全量重扫以捕获新财报和修订)
    # ------------------------------------------------------------------ #

    def run_incremental(self, session: Session) -> None:
        # 财务表按股票拉取，增量 = 重扫全部 L 状态股票以捕获新报表
        self._sync_fina_indicator(session)
        self._sync_income(session)
        self._sync_balancesheet(session)
        self._sync_cashflow(session)

    # ------------------------------------------------------------------ #
    #  fina_indicator — 逐股票拉取财务指标
    # ------------------------------------------------------------------ #

    def _sync_fina_indicator(self, session: Session) -> None:
        self.logger.info("开始同步 fina_indicator")
        pro = ts.pro_api(self._token)

        codes = self.get_all_stock_codes(session, list_status="L")
        self.logger.info("共 %d 只股票需拉取财务指标", len(codes))

        total = 0
        for i, code in enumerate(codes):
            df = self._safe_call(pro, "fina_indicator", ts_code=code)
            if df is not None and not df.empty:
                model = self._model("FinaIndicator")
                count = self.upsert_dataframe(
                    session, model, df, conflict_columns=["ts_code", "end_date"]
                )
                total += count
            self.rate_limiter.acquire()

            if (i + 1) % 500 == 0:
                self.logger.info(
                    "fina_indicator 进度 %d/%d", i + 1, len(codes)
                )

        self.logger.info("fina_indicator 同步完成，共写入 %d 条", total)

    # ------------------------------------------------------------------ #
    #  income — 逐股票拉取利润表
    # ------------------------------------------------------------------ #

    def _sync_income(self, session: Session) -> None:
        self.logger.info("开始同步 income")
        pro = ts.pro_api(self._token)

        codes = self.get_all_stock_codes(session, list_status="L")
        self.logger.info("共 %d 只股票需拉取利润表", len(codes))

        total = 0
        for i, code in enumerate(codes):
            df = self._safe_call(pro, "income", ts_code=code)
            if df is not None and not df.empty:
                model = self._model("Income")
                count = self.upsert_dataframe(
                    session,
                    model,
                    df,
                    conflict_columns=["ts_code", "end_date", "report_type"],
                )
                total += count
            self.rate_limiter.acquire()

            if (i + 1) % 500 == 0:
                self.logger.info("income 进度 %d/%d", i + 1, len(codes))

        self.logger.info("income 同步完成，共写入 %d 条", total)

    # ------------------------------------------------------------------ #
    #  balancesheet — 逐股票拉取资产负债表
    # ------------------------------------------------------------------ #

    def _sync_balancesheet(self, session: Session) -> None:
        self.logger.info("开始同步 balancesheet")
        pro = ts.pro_api(self._token)

        codes = self.get_all_stock_codes(session, list_status="L")
        self.logger.info("共 %d 只股票需拉取资产负债表", len(codes))

        total = 0
        for i, code in enumerate(codes):
            df = self._safe_call(pro, "balancesheet", ts_code=code)
            if df is not None and not df.empty:
                model = self._model("BalanceSheet")
                count = self.upsert_dataframe(
                    session,
                    model,
                    df,
                    conflict_columns=["ts_code", "end_date", "report_type"],
                )
                total += count
            self.rate_limiter.acquire()

            if (i + 1) % 500 == 0:
                self.logger.info("balancesheet 进度 %d/%d", i + 1, len(codes))

        self.logger.info("balancesheet 同步完成，共写入 %d 条", total)

    # ------------------------------------------------------------------ #
    #  cashflow — 逐股票拉取现金流量表
    # ------------------------------------------------------------------ #

    def _sync_cashflow(self, session: Session) -> None:
        self.logger.info("开始同步 cashflow")
        pro = ts.pro_api(self._token)

        codes = self.get_all_stock_codes(session, list_status="L")
        self.logger.info("共 %d 只股票需拉取现金流量表", len(codes))

        total = 0
        for i, code in enumerate(codes):
            df = self._safe_call(pro, "cashflow", ts_code=code)
            if df is not None and not df.empty:
                model = self._model("CashFlow")
                count = self.upsert_dataframe(
                    session,
                    model,
                    df,
                    conflict_columns=["ts_code", "end_date", "report_type"],
                )
                total += count
            self.rate_limiter.acquire()

            if (i + 1) % 500 == 0:
                self.logger.info("cashflow 进度 %d/%d", i + 1, len(codes))

        self.logger.info("cashflow 同步完成，共写入 %d 条", total)

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    @tushare_retry
    def _safe_call(self, pro, method_name: str, **kwargs) -> pd.DataFrame | None:
        """带重试和限流的 API 调用"""
        try:
            df = getattr(pro, method_name)(**kwargs)
            if isinstance(df, pd.DataFrame) and df.empty:
                return None
            return df
        except Exception as e:
            error_msg = str(e).lower()
            if any(k in error_msg for k in ("积分", "流量", "limit", "访问")):
                from ..exceptions import TushareRateLimitError

                raise TushareRateLimitError(e)
            raise

    def _model(self, name: str):
        """懒加载 ORM 模型引用，避免循环导入"""
        from ..models.finance import (
            BalanceSheet,
            CashFlow,
            FinaIndicator,
            Income,
        )

        mapping = {
            "FinaIndicator": FinaIndicator,
            "Income": Income,
            "BalanceSheet": BalanceSheet,
            "CashFlow": CashFlow,
        }
        return mapping[name]
