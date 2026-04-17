"""自动检测并回补 daily 表中的交易日断层。"""

from __future__ import annotations

import logging
import time

import pandas as pd
import tushare as ts
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine

from ..models.daily import Daily
from ..rate_limiter import RateLimiter
from ..retry import tushare_retry

logger = logging.getLogger(__name__)


class BackfillGapsSyncer:
    """
    动态检测 daily 表中缺失的交易日，逐日回补。

    流程:
    1. 查询 daily 表中所有已存在的 trade_date
    2. 查询 trade_cal 获取所有已开盘交易日 (exchange='SSE', is_open=1)
    3. 计算差集 = 缺失的交易日
    4. 逐日调用 Tushare daily API 回补
    """

    name = "backfill_gaps"

    def __init__(
        self,
        engine: Engine,
        tushare_client,
        rate_limiter: RateLimiter,
        token: str = "",
    ):
        self.engine = engine
        self.tushare = tushare_client
        self.rate_limiter = rate_limiter
        self._token = token
        self.logger = logger

    @tushare_retry
    def _safe_call(self, pro, method_name: str, **kwargs) -> pd.DataFrame | None:
        """带重试的 API 调用"""
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

    def _insert_daily(self, df: pd.DataFrame, session) -> int:
        """将 DataFrame 写入 daily 表，ON CONFLICT DO NOTHING。"""
        if df.empty:
            return 0
        records = df.to_dict("records")
        model_cols = [c.name for c in Daily.__table__.columns]
        records = [{k: v for k, v in rec.items() if k in model_cols} for rec in records]
        stmt = insert(Daily).values(records)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["ts_code", "trade_date"]
        )
        result = session.execute(stmt)
        return result.rowcount or 0

    def backfill_gaps(self, session) -> dict:
        """检测断层并回补。返回统计信息。"""
        self.logger.info("[backfill_gaps] 开始检测交易日断层...")

        # 1. 找出缺失的交易日
        rows = session.execute(
            text(
                "SELECT cal_date FROM trade_cal "
                "WHERE exchange = 'SSE' AND is_open = 1 "
                "  AND cal_date NOT IN (SELECT DISTINCT trade_date FROM daily) "
                "ORDER BY cal_date"
            )
        ).fetchall()
        missing_dates = [r[0] for r in rows]

        if not missing_dates:
            self.logger.info("[backfill_gaps] 未发现缺失交易日，数据完整")
            return {
                "total_missing": 0,
                "success": 0,
                "failed": 0,
                "total_inserted": 0,
                "date_range": None,
            }

        date_range = f"{missing_dates[0]} ~ {missing_dates[-1]}"
        self.logger.info(
            "[backfill_gaps] 发现 %d 个缺失交易日: %s",
            len(missing_dates),
            date_range,
        )

        pro = ts.pro_api(self._token)
        total_inserted = 0
        success_count = 0
        fail_count = 0
        fail_dates: list[str] = []

        # 2. 逐日回补
        for i, trade_date in enumerate(missing_dates):
            try:
                df = self._safe_call(pro, "daily", trade_date=trade_date)

                if df is not None and not df.empty:
                    n = self._insert_daily(df, session)
                    session.commit()
                    total_inserted += n
                    success_count += 1
                    self.logger.info(
                        "[%d/%d] %s — 插入 %d 条记录",
                        i + 1,
                        len(missing_dates),
                        trade_date,
                        n,
                    )
                else:
                    self.logger.info(
                        "[%d/%d] %s — 无数据返回",
                        i + 1,
                        len(missing_dates),
                        trade_date,
                    )

            except Exception as e:
                session.rollback()
                fail_count += 1
                fail_dates.append(trade_date)
                self.logger.warning(
                    "[%d/%d] %s 失败: %s",
                    i + 1,
                    len(missing_dates),
                    trade_date,
                    e,
                )

            # 限流控制
            if i < len(missing_dates) - 1:
                time.sleep(0.4)
                self.rate_limiter.acquire()

        result = {
            "total_missing": len(missing_dates),
            "success": success_count,
            "failed": fail_count,
            "total_inserted": total_inserted,
            "date_range": date_range,
        }

        self.logger.info(
            "[backfill_gaps] 回补完成: 成功 %d/%d, 插入 %d 条记录",
            success_count,
            len(missing_dates),
            total_inserted,
        )
        if fail_dates:
            self.logger.warning("[backfill_gaps] 失败日期: %s", fail_dates)

        return result
