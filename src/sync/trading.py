"""交易相关域同步器：top10_floatholders, margin"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import tushare as ts
from sqlalchemy.orm import Session

from ..rate_limiter import RateLimiter
from ..retry import tushare_retry
from .base import DEFAULT_START_DATE, BaseSyncer


class TradingSyncer(BaseSyncer):
    """交易相关域同步

    覆盖表:
    - top10_floatholders: 逐股票拉取前十大流通股东，ON CONFLICT DO NOTHING (ts_code, end_date, holder_name)
    - margin: 按交易日拉取融资融券交易汇总，ON CONFLICT DO NOTHING (trade_date, exchange_id)
    """

    name = "trading"

    # ------------------------------------------------------------------ #
    #  Full backfill
    # ------------------------------------------------------------------ #

    def run_full(self, session: Session) -> None:
        self._sync_top10_floatholders(session)
        self._sync_margin(session, DEFAULT_START_DATE)

    # ------------------------------------------------------------------ #
    #  Incremental
    # ------------------------------------------------------------------ #

    def run_incremental(self, session: Session) -> None:
        # top10_floatholders 按股票拉，重扫全部 L 状态股票
        self._sync_top10_floatholders(session)

        # margin 按交易日拉，从 MAX(trade_date) 之后
        today = datetime.now().strftime("%Y%m%d")
        latest = self.get_latest_date(session, self._model("Margin"), "trade_date")
        start = latest and f"{int(latest) + 1:08d}" or DEFAULT_START_DATE
        if start <= today:
            self._sync_margin(session, start)

    # ------------------------------------------------------------------ #
    #  top10_floatholders — 逐股票拉取前十大流通股东
    # ------------------------------------------------------------------ #

    def _sync_top10_floatholders(self, session: Session) -> None:
        self.logger.info("开始同步 top10_floatholders")
        pro = ts.pro_api(self._token)

        codes = self.get_all_stock_codes(session, list_status="L")
        self.logger.info("共 %d 只股票需拉取前十大流通股东", len(codes))

        total = 0
        for i, code in enumerate(codes):
            df = self._safe_call(pro, "top10_floatholders", ts_code=code)
            if df is not None and not df.empty:
                model = self._model("Top10FloatHolders")
                count = self.upsert_dataframe(
                    session,
                    model,
                    df,
                    conflict_columns=["ts_code", "end_date", "holder_name"],
                )
                total += count
            self.rate_limiter.acquire()

            if (i + 1) % 500 == 0:
                self.logger.info(
                    "top10_floatholders 进度 %d/%d", i + 1, len(codes)
                )

        self.logger.info("top10_floatholders 同步完成，共写入 %d 条", total)

    # ------------------------------------------------------------------ #
    #  margin — 按交易日拉取融资融券交易汇总
    # ------------------------------------------------------------------ #

    def _sync_margin(self, session: Session, start_date: str) -> None:
        self.logger.info("开始同步 margin [%s 至今]", start_date)
        pro = ts.pro_api(self._token)

        today = datetime.now().strftime("%Y%m%d")
        trade_dates = self.get_trade_dates(session, start_date, today)
        self.logger.info("共 %d 个交易日需拉取", len(trade_dates))

        total = 0
        for i, trade_date in enumerate(trade_dates):
            df = self._safe_call(pro, "margin", trade_date=trade_date)
            if df is not None and not df.empty:
                model = self._model("Margin")
                count = self.upsert_dataframe(
                    session, model, df, conflict_columns=["trade_date", "exchange_id"]
                )
                total += count
                self.logger.info(
                    "margin [%s] 写入 %d 条", trade_date, count
                )
            self.rate_limiter.acquire()

            if (i + 1) % 20 == 0:
                self.logger.info("margin 进度 %d/%d", i + 1, len(trade_dates))

        self.logger.info("margin 同步完成，共写入 %d 条", total)

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
        from ..models.trading import Margin, Top10FloatHolders

        mapping = {
            "Top10FloatHolders": Top10FloatHolders,
            "Margin": Margin,
        }
        return mapping[name]
