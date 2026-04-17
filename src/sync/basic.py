"""基础信息域同步器：stock_basic, stock_company, namechange, trade_cal"""

from __future__ import annotations

import re
from datetime import datetime

import pandas as pd
import tushare as ts
from sqlalchemy.orm import Session

from ..exceptions import TushareRateLimitError
from ..rate_limiter import RateLimiter
from ..retry import tushare_retry
from .base import DEFAULT_START_DATE, BaseSyncer

ST_PATTERN = re.compile(r"\*?ST|ＳＴ", re.IGNORECASE)


class BasicSyncer(BaseSyncer):
    """基础信息域同步

    覆盖表:
    - stock_basic: 全量拉取，ON CONFLICT DO UPDATE (ts_code)
    - stock_company: 逐股票拉取，ON CONFLICT DO NOTHING (ts_code)
    - namechange: 逐股票拉取，自动标记 is_st，ON CONFLICT DO NOTHING (ts_code, start_date)
    - trade_cal: 全量拉取 SSE/SZSE/BSE 交易所
    """

    name = "basic"

    # ------------------------------------------------------------------ #
    #  Full backfill
    # ------------------------------------------------------------------ #

    def run_full(self, session: Session) -> None:
        self._sync_stock_basic(session)
        self._sync_trade_cal(session)
        self._sync_stock_company(session)
        self._sync_namechange(session)

    # ------------------------------------------------------------------ #
    #  Incremental (basic 域基本为一次性数据，增量仅补漏)
    # ------------------------------------------------------------------ #

    def run_incremental(self, session: Session) -> None:
        # stock_basic / trade_cal 全量重跑 (数据量小，幂等)
        self._sync_stock_basic(session)
        self._sync_trade_cal(session)
        # stock_company / namechange 仅补缺失的股票
        self._sync_stock_company(session)
        self._sync_namechange(session)

    # ------------------------------------------------------------------ #
    #  stock_basic — 全量拉取，按上市状态分三次
    # ------------------------------------------------------------------ #

    def _sync_stock_basic(self, session: Session) -> None:
        self.logger.info("开始同步 stock_basic")
        pro = ts.pro_api(self._token)

        total = 0
        for status in ["L", "D", "P"]:
            df = self._safe_call(pro, "stock_basic", exchange="", list_status=status)
            if df is not None and not df.empty:
                # 对齐 ORM 列
                model_cols = [c.name for c in self._model("StockBasic").__table__.columns]
                df = df[[c for c in model_cols if c in df.columns]]
                count = self.upsert_dataframe(
                    session,
                    self._model("StockBasic"),
                    df,
                    conflict_columns=["ts_code"],
                    update_columns=[c for c in model_cols if c != "ts_code"],
                )
                total += count
                self.logger.info("stock_basic [%s] 写入 %d 条", status, count)
            self.rate_limiter.acquire()

        self.logger.info("stock_basic 同步完成，共写入 %d 条", total)

    # ------------------------------------------------------------------ #
    #  trade_cal — 全量拉取 SSE / SZSE / BSE
    # ------------------------------------------------------------------ #

    def _sync_trade_cal(self, session: Session) -> None:
        self.logger.info("开始同步 trade_cal")
        pro = ts.pro_api(self._token)

        total = 0
        for exchange in ["SSE", "SZSE", "BSE"]:
            df = self._safe_call(pro, "trade_cal", exchange=exchange)
            if df is not None and not df.empty:
                model = self._model("TradeCal")
                count = self.upsert_dataframe(
                    session, model, df, conflict_columns=["exchange", "cal_date"]
                )
                total += count
                self.logger.info("trade_cal [%s] 写入 %d 条", exchange, count)
            self.rate_limiter.acquire()

        self.logger.info("trade_cal 同步完成，共写入 %d 条", total)

    # ------------------------------------------------------------------ #
    #  stock_company — 逐股票拉取
    # ------------------------------------------------------------------ #

    def _sync_stock_company(self, session: Session) -> None:
        self.logger.info("开始同步 stock_company")
        pro = ts.pro_api(self._token)

        codes = self.get_all_stock_codes(session, list_status="L")
        self.logger.info("共 %d 只股票需同步公司信息", len(codes))

        total = 0
        for i, code in enumerate(codes):
            df = self._rate_limited_stock_company(pro, code)
            if df is not None and not df.empty:
                # 去重：同一只股票可能有多个交易所记录，取最新
                df = df.drop_duplicates(subset=["ts_code"], keep="last")
                model = self._model("StockCompany")
                count = self.upsert_dataframe(
                    session, model, df, conflict_columns=["ts_code"]
                )
                total += count
            self.rate_limiter.acquire()

            if (i + 1) % 100 == 0:
                self.logger.info("stock_company 进度 %d/%d", i + 1, len(codes))

        self.logger.info("stock_company 同步完成，共写入 %d 条", total)

    def _rate_limited_stock_company(self, pro, code: str) -> pd.DataFrame | None:
        """处理 stock_company 接口的 10 次/分钟限速。"""
        import time
        while True:
            try:
                return self._safe_call(pro, "stock_company", ts_code=code)
            except TushareRateLimitError:
                self.logger.info("stock_company 触发限速，等待 65 秒后重试")
                time.sleep(65)

    # ------------------------------------------------------------------ #
    #  namechange — 逐股票拉取，自动标记 ST
    # ------------------------------------------------------------------ #

    def _sync_namechange(self, session: Session) -> None:
        self.logger.info("开始同步 namechange")
        pro = ts.pro_api(self._token)

        codes = self.get_all_stock_codes(session)
        self.logger.info("共 %d 只股票需同步曾用名", len(codes))

        total = 0
        for i, code in enumerate(codes):
            df = self._safe_call(pro, "namechange", ts_code=code)
            if df is not None and not df.empty:
                # 自动标记 ST 状态
                df["is_st"] = df["name"].apply(
                    lambda x: bool(ST_PATTERN.search(str(x))) if pd.notna(x) else False
                )
                model = self._model("NameChange")
                count = self.upsert_dataframe(
                    session, model, df, conflict_columns=["ts_code", "start_date"]
                )
                total += count
            self.rate_limiter.acquire()

            if (i + 1) % 500 == 0:
                self.logger.info("namechange 进度 %d/%d", i + 1, len(codes))

        self.logger.info("namechange 同步完成，共写入 %d 条", total)

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
        from ..models.basic import (
            NameChange,
            StockBasic,
            StockCompany,
            TradeCal,
        )

        mapping = {
            "StockBasic": StockBasic,
            "StockCompany": StockCompany,
            "NameChange": NameChange,
            "TradeCal": TradeCal,
        }
        return mapping[name]
