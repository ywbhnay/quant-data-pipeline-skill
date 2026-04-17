"""财报辅助域同步器：disclosure_date, dividend, fina_mainbz, express"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import tushare as ts
from sqlalchemy.orm import Session

from ..rate_limiter import RateLimiter
from ..retry import tushare_retry
from .base import DEFAULT_START_DATE, BaseSyncer


class FinanceAuxSyncer(BaseSyncer):
    """财报辅助域同步

    覆盖表:
    - disclosure_date: 按报告期拉取全市场财报披露计划，ON CONFLICT DO NOTHING (ts_code, end_date)
    - dividend: 按年度拉取全市场分红送股，ON CONFLICT DO NOTHING (ts_code, end_date)
    - fina_mainbz: 逐股票拉取主营业务构成，ON CONFLICT DO NOTHING (ts_code, end_date, bz_item)
    - express: 逐股票拉取财务快报，ON CONFLICT DO NOTHING (ts_code, end_date)
    """

    name = "finance-aux"

    # ------------------------------------------------------------------ #
    #  Full backfill
    # ------------------------------------------------------------------ #

    def run_full(self, session: Session) -> None:
        self._sync_disclosure_date(session)
        self._sync_dividend(session)
        self._sync_fina_mainbz(session)
        self._sync_express(session)

    # ------------------------------------------------------------------ #
    #  Incremental (辅助表基本一次性数据，增量仅补漏)
    # ------------------------------------------------------------------ #

    def run_incremental(self, session: Session) -> None:
        # disclosure_date / dividend 全量重跑 (数据量小，幂等)
        self._sync_disclosure_date(session)
        self._sync_dividend(session)
        # fina_mainbz / express 重扫全部 L 状态股票
        self._sync_fina_mainbz(session)
        self._sync_express(session)

    # ------------------------------------------------------------------ #
    #  disclosure_date — 按报告期拉取全市场财报披露计划
    # ------------------------------------------------------------------ #

    def _sync_disclosure_date(self, session: Session) -> None:
        self.logger.info("开始同步 disclosure_date")
        pro = ts.pro_api(self._token)

        # 生成报告期列表（2016 年至今，每季度）
        periods = self._generate_periods()
        self.logger.info("共 %d 个报告期需拉取", len(periods))

        total = 0
        for i, period in enumerate(periods):
            df = self._safe_call(pro, "disclosure_date", period=period)
            if df is not None and not df.empty:
                model = self._model("DisclosureDate")
                count = self.upsert_dataframe(
                    session, model, df, conflict_columns=["ts_code", "end_date"]
                )
                total += count
                self.logger.info(
                    "disclosure_date [%s] 写入 %d 条", period, count
                )
            self.rate_limiter.acquire()

            if (i + 1) % 20 == 0:
                self.logger.info(
                    "disclosure_date 进度 %d/%d", i + 1, len(periods)
                )

        self.logger.info("disclosure_date 同步完成，共写入 %d 条", total)

    # ------------------------------------------------------------------ #
    #  dividend — 按年度拉取全市场分红送股
    # ------------------------------------------------------------------ #

    def _sync_dividend(self, session: Session) -> None:
        self.logger.info("开始同步 dividend")
        pro = ts.pro_api(self._token)

        years = list(range(2016, datetime.now().year + 1))
        self.logger.info("共 %d 个年度需拉取", len(years))

        total = 0
        for i, year in enumerate(years):
            df = self._safe_call(pro, "dividend", end_date=f"{year}1231")
            if df is not None and not df.empty:
                model = self._model("Dividend")
                count = self.upsert_dataframe(
                    session, model, df, conflict_columns=["ts_code", "end_date"]
                )
                total += count
                self.logger.info(
                    "dividend [%d年] 写入 %d 条", year, count
                )
            self.rate_limiter.acquire()

            if (i + 1) % 5 == 0:
                self.logger.info(
                    "dividend 进度 %d/%d", i + 1, len(years)
                )

        self.logger.info("dividend 同步完成，共写入 %d 条", total)

    # ------------------------------------------------------------------ #
    #  fina_mainbz — 逐股票拉取主营业务构成
    # ------------------------------------------------------------------ #

    def _sync_fina_mainbz(self, session: Session) -> None:
        self.logger.info("开始同步 fina_mainbz")
        pro = ts.pro_api(self._token)

        codes = self.get_all_stock_codes(session, list_status="L")
        self.logger.info("共 %d 只股票需拉取主营构成", len(codes))

        total = 0
        for i, code in enumerate(codes):
            df = self._safe_call(pro, "fina_mainbz", ts_code=code)
            if df is not None and not df.empty:
                model = self._model("FinaMainbz")
                count = self.upsert_dataframe(
                    session,
                    model,
                    df,
                    conflict_columns=["ts_code", "end_date", "bz_item"],
                )
                total += count
            self.rate_limiter.acquire()

            if (i + 1) % 500 == 0:
                self.logger.info(
                    "fina_mainbz 进度 %d/%d", i + 1, len(codes)
                )

        self.logger.info("fina_mainbz 同步完成，共写入 %d 条", total)

    # ------------------------------------------------------------------ #
    #  express — 逐股票拉取财务快报
    # ------------------------------------------------------------------ #

    def _sync_express(self, session: Session) -> None:
        self.logger.info("开始同步 express")
        pro = ts.pro_api(self._token)

        codes = self.get_all_stock_codes(session, list_status="L")
        self.logger.info("共 %d 只股票需拉取业绩快报", len(codes))

        total = 0
        for i, code in enumerate(codes):
            df = self._safe_call(pro, "express", ts_code=code)
            if df is not None and not df.empty:
                model = self._model("Express")
                count = self.upsert_dataframe(
                    session, model, df, conflict_columns=["ts_code", "end_date"]
                )
                total += count
            self.rate_limiter.acquire()

            if (i + 1) % 500 == 0:
                self.logger.info(
                    "express 进度 %d/%d", i + 1, len(codes)
                )

        self.logger.info("express 同步完成，共写入 %d 条", total)

    # ------------------------------------------------------------------ #
    #  Helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _generate_periods() -> list[str]:
        """生成报告期列表，从 2016 年至今，每季度一个"""
        periods = []
        current_year = datetime.now().year
        for year in range(2016, current_year + 1):
            for q in ["0331", "0630", "0930", "1231"]:
                period = f"{year}{q}"
                if period <= datetime.now().strftime("%Y%m%d"):
                    periods.append(period)
        return periods

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
        from ..models.finance_aux import (
            DisclosureDate,
            Dividend,
            Express,
            FinaMainbz,
        )

        mapping = {
            "DisclosureDate": DisclosureDate,
            "Dividend": Dividend,
            "FinaMainbz": FinaMainbz,
            "Express": Express,
        }
        return mapping[name]
