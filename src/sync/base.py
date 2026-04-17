"""同步框架基类：封装 upsert、日志、断点续传、速率控制"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta

import logging
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from ..rate_limiter import RateLimiter


DEFAULT_START_DATE = "20000101"


class BaseSyncer(ABC):
    """同步任务基类

    子类需实现:
    - name: 业务域标识 (如 "basic", "daily")
    - run_full(session): 全量回填逻辑
    - run_incremental(session): 增量更新逻辑
    """

    name: str = "base"

    def __init__(
        self,
        engine: Engine,
        tushare_client: Any,
        rate_limiter: RateLimiter,
        token: str = "",
    ):
        self.engine = engine
        self.tushare = tushare_client
        self.rate_limiter = rate_limiter
        self._token = token
        self.logger = logging.getLogger(f"sync.{self.name}")

    # ------------------------------------------------------------------ #
    #  Public interface
    # ------------------------------------------------------------------ #

    @abstractmethod
    def run_full(self, session: Session) -> None:
        """全量回填模式：从 DEFAULT_START_DATE (或更早) 拉到今天"""
        ...

    @abstractmethod
    def run_incremental(self, session: Session) -> None:
        """每日增量模式：从表中 MAX(date) 之后拉到今天"""
        ...

    # ------------------------------------------------------------------ #
    #  Upsert helpers
    # ------------------------------------------------------------------ #

    UPSERT_BATCH_SIZE = 500  # psycopg2 参数化大批量会挂，拆小批

    def _upsert_batch(
        self,
        session: Session,
        table,
        records: list[dict],
        conflict_columns: list[str],
        update_columns: list[str] | None,
    ) -> int:
        """执行单批 upsert。"""
        if not records:
            return 0

        stmt = insert(table).values(records)

        if update_columns:
            update_dict = {col: getattr(stmt.excluded, col) for col in update_columns}
            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_columns,
                set_=update_dict,
            )
        else:
            stmt = stmt.on_conflict_do_nothing(
                index_elements=conflict_columns,
            )

        session.execute(stmt)
        return len(records)

    def upsert_dataframe(
        self,
        session: Session,
        table,
        df: pd.DataFrame,
        conflict_columns: list[str],
        update_columns: list[str] | None = None,
    ) -> int:
        """通用 upsert: ON CONFLICT DO UPDATE 或 DO NOTHING（分批执行避免大批量挂起）

        自动过滤掉 ORM 模型中不存在的列（如 Tushare 新增字段）。

        Args:
            session: 当前数据库会话
            table: SQLAlchemy ORM 模型类
            df: 待写入的 DataFrame
            conflict_columns: 冲突判定列 (对应唯一索引或主键)
            update_columns: 冲突时更新的列; None → DO NOTHING
        """
        if df.empty:
            return 0

        model_cols = {c.name for c in table.__table__.columns}
        valid_cols = [c for c in df.columns if c in model_cols]
        extra = set(df.columns) - model_cols
        if extra:
            self.logger.debug("过滤掉模型中不存在的列: %s", extra)

        if not valid_cols:
            self.logger.warning("DataFrame 所有列都不在模型中，跳过写入")
            return 0

        records = df[valid_cols].to_dict(orient="records")
        total = 0
        batch_size = self.UPSERT_BATCH_SIZE

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            total += self._upsert_batch(
                session, table, batch, conflict_columns, update_columns,
            )

        return total

    # ------------------------------------------------------------------ #
    #  Date helpers
    # ------------------------------------------------------------------ #

    def get_latest_date(
        self, session: Session, table, date_column: str = "trade_date"
    ) -> str | None:
        """查询表中最新日期，用于增量断点续传"""
        result = session.execute(
            text(f"SELECT MAX({date_column}) FROM {table.__tablename__}")
        )
        row = result.fetchone()
        if row and row[0]:
            val = row[0]
            if hasattr(val, "strftime"):
                return val.strftime("%Y%m%d")
            return str(val).strip()
        return None

    def get_trade_dates(
        self,
        session: Session,
        start_date: str,
        end_date: str,
        exchange: str = "SSE",
    ) -> list[str]:
        """从 trade_cal 表获取指定日期范围内的所有交易日

        Args:
            session: 数据库会话
            start_date: 起始日期 YYYYMMDD (含)
            end_date: 结束日期 YYYYMMDD (含)
            exchange: 交易所代码，默认 SSE
        """
        result = session.execute(
            text(
                "SELECT cal_date FROM trade_cal "
                "WHERE exchange = :exchange "
                "  AND cal_date >= :start AND cal_date <= :end "
                "  AND is_open = 1 "
                "ORDER BY cal_date ASC"
            ),
            {
                "exchange": exchange,
                "start": start_date,
                "end": end_date,
            },
        )
        rows = result.fetchall()
        return [
            row[0].strftime("%Y%m%d") if hasattr(row[0], "strftime") else str(row[0]).strip()
            for row in rows
        ]

    def get_next_trading_day(
        self, session: Session, from_date: str, exchange: str = "SSE"
    ) -> str | None:
        """获取 from_date 之后的下一个交易日"""
        result = session.execute(
            text(
                "SELECT cal_date FROM trade_cal "
                "WHERE exchange = :exchange AND cal_date > :date AND is_open = 1 "
                "ORDER BY cal_date ASC LIMIT 1"
            ),
            {"exchange": exchange, "date": from_date},
        )
        row = result.fetchone()
        if not row or not row[0]:
            return None
        val = row[0]
        return val.strftime("%Y%m%d") if hasattr(val, "strftime") else str(val).strip()

    def is_trading_day(
        self, session: Session, target_date: str | None = None, exchange: str = "SSE"
    ) -> bool:
        """检查指定日期是否为交易日"""
        if target_date is None:
            target_date = datetime.now().strftime("%Y%m%d")
        result = session.execute(
            text(
                "SELECT is_open FROM trade_cal "
                "WHERE exchange = :exchange AND cal_date = :date"
            ),
            {"exchange": exchange, "date": target_date},
        )
        row = result.fetchone()
        return row[0] == 1 if row else False

    # ------------------------------------------------------------------ #
    #  Stock list helpers
    # ------------------------------------------------------------------ #

    def get_all_stock_codes(
        self, session: Session, list_status: str | None = None
    ) -> list[str]:
        """获取所有股票代码

        Args:
            session: 数据库会话
            list_status: 'L'(上市) / 'D'(退市) / 'P'(暂停上市), None=全部
        """
        from sqlalchemy import text

        query = "SELECT ts_code FROM stock_basic"
        params: dict[str, Any] = {}
        if list_status:
            query += " WHERE list_status = :status"
            params["status"] = list_status

        result = session.execute(text(query), params)
        return [row[0] for row in result.fetchall()]

    # ------------------------------------------------------------------ #
    #  Batch processing
    # ------------------------------------------------------------------ #

    def process_in_batches(
        self,
        session: Session,
        items: list,
        batch_size: int = 100,
        process_func=None,
    ) -> int:
        """分批处理列表，每批处理后 commit

        Args:
            session: 数据库会话
            items: 待处理项列表
            batch_size: 每批大小
            process_func: 处理函数, 签名 func(batch_items) -> int(处理数量)
        """
        total = 0
        for i in range(0, len(items), batch_size):
            batch = items[i : i + batch_size]
            if process_func:
                count = process_func(batch)
                total += count
            session.commit()
            self.logger.debug(
                "batch %d/%d done", i + len(batch), len(items)
            )
        return total
