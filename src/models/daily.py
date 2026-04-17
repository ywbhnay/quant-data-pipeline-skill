"""日线行情域 ORM 模型：Daily, DailyBasic, AdjFactor, StkLimit, SuspendD"""

from sqlalchemy import Column, String, Numeric, Index, PrimaryKeyConstraint

from .base import Base


class Daily(Base):
    """日线行情"""
    __tablename__ = "daily"
    __table_args__ = (
        PrimaryKeyConstraint("ts_code", "trade_date"),
    )

    ts_code = Column(String(20), comment="TS股票代码")
    trade_date = Column(String(8), comment="交易日期")
    open = Column(Numeric(12, 4), comment="开盘价")
    high = Column(Numeric(12, 4), comment="最高价")
    low = Column(Numeric(12, 4), comment="最低价")
    close = Column(Numeric(12, 4), comment="收盘价")
    pre_close = Column(Numeric(12, 4), comment="昨收价")
    change = Column(Numeric(12, 4), comment="涨跌额")
    pct_chg = Column(Numeric(12, 4), comment="涨跌幅")
    vol = Column(Numeric(20, 4), comment="成交量(手)")
    amount = Column(Numeric(20, 4), comment="成交额(千元)")


class DailyBasic(Base):
    """每日指标"""
    __tablename__ = "daily_basic"
    __table_args__ = (
        PrimaryKeyConstraint("ts_code", "trade_date"),
        Index("idx_daily_basic_date", "trade_date"),
    )

    ts_code = Column(String(20), comment="TS股票代码")
    trade_date = Column(String(8), comment="交易日期")
    turnover_rate = Column(Numeric(12, 4), comment="换手率")
    volume_ratio = Column(Numeric(12, 4), comment="量比")
    pe = Column(Numeric(12, 4), comment="市盈率")
    pe_ttm = Column(Numeric(12, 4), comment="市盈率(TTM)")
    pb = Column(Numeric(12, 4), comment="市净率")
    ps = Column(Numeric(12, 4), comment="市销率")
    ps_ttm = Column(Numeric(12, 4), comment="市销率(TTM)")
    dv_ratio = Column(Numeric(12, 4), comment="股息率")
    dv_ttm = Column(Numeric(12, 4), comment="股息率(TTM)")
    total_mv = Column(Numeric(20, 4), comment="总市值")
    circ_mv = Column(Numeric(20, 4), comment="流通市值")


class AdjFactor(Base):
    """复权因子"""
    __tablename__ = "adj_factor"
    __table_args__ = (
        PrimaryKeyConstraint("ts_code", "trade_date"),
        Index("idx_adj_date", "trade_date"),
    )

    ts_code = Column(String(20), comment="TS股票代码")
    trade_date = Column(String(8), comment="交易日期")
    adj_factor = Column(Numeric(12, 6), comment="复权因子")
    forward_adj = Column(Numeric(12, 6), comment="前复权因子")
    back_adj = Column(Numeric(12, 6), comment="后复权因子")


class StkLimit(Base):
    """每日涨跌停价格"""
    __tablename__ = "stk_limit"
    __table_args__ = (
        PrimaryKeyConstraint("ts_code", "trade_date"),
        Index("idx_limit_date", "trade_date"),
    )

    ts_code = Column(String(20), comment="TS股票代码")
    trade_date = Column(String(8), comment="交易日期")
    pre_close = Column(Numeric(12, 4), comment="昨收价")
    up_limit = Column(Numeric(12, 4), comment="涨停价")
    down_limit = Column(Numeric(12, 4), comment="跌停价")


class SuspendD(Base):
    """每日停复牌信息"""
    __tablename__ = "suspend_d"
    __table_args__ = (
        PrimaryKeyConstraint("ts_code", "trade_date"),
        Index("idx_suspend_date", "trade_date"),
    )

    ts_code = Column(String(20), comment="TS股票代码")
    trade_date = Column(String(8), comment="交易日期")
    suspend_timing = Column(String(20), comment="停复牌时间")
    suspend_type = Column(String(20), comment="停复牌类型")
    reason = Column(String(200), comment="停复牌原因")
