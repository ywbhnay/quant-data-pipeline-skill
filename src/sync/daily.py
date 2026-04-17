"""日线行情域同步器：daily, daily_basic, adj_factor, stk_limit, suspend_d"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import tushare as ts
from sqlalchemy.orm import Session

from ..rate_limiter import RateLimiter
from ..retry import tushare_retry
from .base import DEFAULT_START_DATE, BaseSyncer


class DailySyncer(BaseSyncer):
    """日线行情域同步

    覆盖表:
    - daily: 按交易日拉取全市场日线，ON CONFLICT DO NOTHING (ts_code, trade_date)
    - daily_basic: 按交易日拉取全市场每日指标，ON CONFLICT DO NOTHING (ts_code, trade_date)
    - adj_factor: 逐股票拉取全量复权因子，ON CONFLICT DO NOTHING (ts_code, trade_date)
    - stk_limit: 按交易日拉取全市场涨跌停价格，ON CONFLICT DO NOTHING (ts_code, trade_date)
    - suspend_d: 按交易日拉取全市场停复牌信息，ON CONFLICT DO NOTHING (ts_code, trade_date)
    """

    name = "daily"

    # ------------------------------------------------------------------ #
    #  Full backfill
    # ------------------------------------------------------------------ #

    def run_full(self, session: Session) -> None:
        self._sync_daily(session, DEFAULT_START_DATE)
        self._sync_daily_basic(session, DEFAULT_START_DATE)
        self._sync_stk_limit(session, DEFAULT_START_DATE)
        self._sync_suspend_d(session, DEFAULT_START_DATE)
        self._sync_adj_factor(session)

    # ------------------------------------------------------------------ #
    #  Incremental
    # ------------------------------------------------------------------ #

    def run_incremental(self, session: Session) -> None:
        today = datetime.now().strftime("%Y%m%d")

        # 按日期的表从 MAX(trade_date) 之后拉
        for sync_func, model_name in [
            (self._sync_daily, "Daily"),
            (self._sync_daily_basic, "DailyBasic"),
            (self._sync_stk_limit, "StkLimit"),
            (self._sync_suspend_d, "SuspendD"),
        ]:
            latest = self.get_latest_date(
                session, self._model(model_name), "trade_date"
            )
            start = latest and f"{int(latest) + 1:08d}" or DEFAULT_START_DATE
            if start <= today:
                sync_func(session, start)

        # adj_factor 按股票拉，检查是否有新股需补
        self._sync_adj_factor(session)

    # ------------------------------------------------------------------ #
    #  daily — 按交易日拉取全市场日线
    # ------------------------------------------------------------------ #

    def _sync_daily(self, session: Session, start_date: str) -> None:
        self.logger.info("开始同步 daily [%s 至今]", start_date)
        pro = ts.pro_api(self._token)

        today = datetime.now().strftime("%Y%m%d")
        trade_dates = self.get_trade_dates(session, start_date, today)
        self.logger.info("共 %d 个交易日需拉取", len(trade_dates))

        total = 0
        for i, trade_date in enumerate(trade_dates):
            df = self._safe_call(pro, "daily", trade_date=trade_date)
            if df is not None and not df.empty:
                model = self._model("Daily")
                count = self.upsert_dataframe(
                    session, model, df, conflict_columns=["ts_code", "trade_date"]
                )
                total += count
                self.logger.info(
                    "daily [%s] 写入 %d 条", trade_date, count
                )
            self.rate_limiter.acquire()

            if (i + 1) % 20 == 0:
                self.logger.info("daily 进度 %d/%d", i + 1, len(trade_dates))

        self.logger.info("daily 同步完成，共写入 %d 条", total)

    # ------------------------------------------------------------------ #
    #  daily_basic — 按交易日拉取全市场每日指标
    # ------------------------------------------------------------------ #

    def _sync_daily_basic(self, session: Session, start_date: str) -> None:
        self.logger.info("开始同步 daily_basic [%s 至今]", start_date)
        pro = ts.pro_api(self._token)

        today = datetime.now().strftime("%Y%m%d")
        trade_dates = self.get_trade_dates(session, start_date, today)
        self.logger.info("共 %d 个交易日需拉取", len(trade_dates))

        total = 0
        for i, trade_date in enumerate(trade_dates):
            df = self._safe_call(
                pro,
                "daily_basic",
                trade_date=trade_date,
                fields="ts_code,trade_date,turnover_rate,volume_ratio,pe,pe_ttm,pb,ps,ps_ttm,dv_ratio,dv_ttm,total_mv,circ_mv",
            )
            if df is not None and not df.empty:
                model = self._model("DailyBasic")
                count = self.upsert_dataframe(
                    session, model, df, conflict_columns=["ts_code", "trade_date"]
                )
                total += count
                self.logger.info(
                    "daily_basic [%s] 写入 %d 条", trade_date, count
                )
            self.rate_limiter.acquire()

            if (i + 1) % 20 == 0:
                self.logger.info("daily_basic 进度 %d/%d", i + 1, len(trade_dates))

        self.logger.info("daily_basic 同步完成，共写入 %d 条", total)

    # ------------------------------------------------------------------ #
    #  adj_factor — 逐股票拉取全量复权因子
    # ------------------------------------------------------------------ #

    def _sync_adj_factor(self, session: Session) -> None:
        self.logger.info("开始同步 adj_factor")
        pro = ts.pro_api(self._token)

        codes = self.get_all_stock_codes(session, list_status="L")
        self.logger.info("共 %d 只股票需拉取复权因子", len(codes))

        total = 0
        for i, code in enumerate(codes):
            df = self._safe_call(pro, "adj_factor", ts_code=code)
            if df is not None and not df.empty:
                model = self._model("AdjFactor")
                count = self.upsert_dataframe(
                    session, model, df, conflict_columns=["ts_code", "trade_date"]
                )
                total += count
            self.rate_limiter.acquire()

            if (i + 1) % 500 == 0:
                self.logger.info("adj_factor 进度 %d/%d", i + 1, len(codes))

        self.logger.info("adj_factor 同步完成，共写入 %d 条", total)

    # ------------------------------------------------------------------ #
    #  stk_limit — 按交易日拉取全市场涨跌停价格
    # ------------------------------------------------------------------ #

    def _sync_stk_limit(self, session: Session, start_date: str) -> None:
        self.logger.info("开始同步 stk_limit [%s 至今]", start_date)
        pro = ts.pro_api(self._token)

        today = datetime.now().strftime("%Y%m%d")
        trade_dates = self.get_trade_dates(session, start_date, today)
        self.logger.info("共 %d 个交易日需拉取", len(trade_dates))

        total = 0
        for i, trade_date in enumerate(trade_dates):
            df = self._safe_call(pro, "stk_limit", trade_date=trade_date)
            if df is not None and not df.empty:
                model = self._model("StkLimit")
                count = self.upsert_dataframe(
                    session, model, df, conflict_columns=["ts_code", "trade_date"]
                )
                total += count
                self.logger.info(
                    "stk_limit [%s] 写入 %d 条", trade_date, count
                )
            self.rate_limiter.acquire()

            if (i + 1) % 20 == 0:
                self.logger.info("stk_limit 进度 %d/%d", i + 1, len(trade_dates))

        self.logger.info("stk_limit 同步完成，共写入 %d 条", total)

    # ------------------------------------------------------------------ #
    #  suspend_d — 按交易日拉取全市场停复牌信息
    # ------------------------------------------------------------------ #

    def _sync_suspend_d(self, session: Session, start_date: str) -> None:
        self.logger.info("开始同步 suspend_d [%s 至今]", start_date)
        pro = ts.pro_api(self._token)

        today = datetime.now().strftime("%Y%m%d")
        trade_dates = self.get_trade_dates(session, start_date, today)
        self.logger.info("共 %d 个交易日需拉取", len(trade_dates))

        total = 0
        for i, trade_date in enumerate(trade_dates):
            df = self._safe_call(pro, "suspend_d", trade_date=trade_date)
            if df is not None and not df.empty:
                model = self._model("SuspendD")
                count = self.upsert_dataframe(
                    session, model, df, conflict_columns=["ts_code", "trade_date"]
                )
                total += count
                self.logger.info(
                    "suspend_d [%s] 写入 %d 条", trade_date, count
                )
            self.rate_limiter.acquire()

            if (i + 1) % 20 == 0:
                self.logger.info("suspend_d 进度 %d/%d", i + 1, len(trade_dates))

        self.logger.info("suspend_d 同步完成，共写入 %d 条", total)

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
        """懒加载 ORM 模型引用"""
        from ..models.daily import (
            AdjFactor,
            Daily,
            DailyBasic,
            StkLimit,
            SuspendD,
        )

        mapping = {
            "Daily": Daily,
            "DailyBasic": DailyBasic,
            "AdjFactor": AdjFactor,
            "StkLimit": StkLimit,
            "SuspendD": SuspendD,
        }
        return mapping[name]
