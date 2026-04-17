"""交易相关域 ORM 模型：Top10FloatHolders, Margin"""

from sqlalchemy import Column, String, Numeric, Index, PrimaryKeyConstraint

from .base import Base


class Top10FloatHolders(Base):
    """前十大流通股东"""
    __tablename__ = "top10_floatholders"
    __table_args__ = (
        PrimaryKeyConstraint("ts_code", "end_date", "holder_name"),
        Index("idx_holder_end_date", "end_date"),
        Index("idx_holder_name", "holder_name"),
    )

    ts_code = Column(String(20), comment="TS股票代码")
    end_date = Column(String(8), comment="报告期")
    holder_name = Column(String(200), comment="股东名称")
    ann_date = Column(String(8), comment="公告日期")
    hold_amount = Column(Numeric(20, 4), comment="持有数量")
    hold_ratio = Column(Numeric(12, 6), comment="占总股本比例")
    hold_float_ratio = Column(Numeric(12, 6), comment="占流通股本比例")
    hold_change = Column(String(20), comment="持股变动")
    holder_type = Column(String(50), comment="股东类型")


class Margin(Base):
    """融资融券交易数据"""
    __tablename__ = "margin"
    __table_args__ = (
        PrimaryKeyConstraint("trade_date", "exchange_id"),
        Index("idx_margin_date", "trade_date"),
    )

    trade_date = Column(String(8), comment="交易日期")
    exchange_id = Column(String(10), comment="交易所代码")
    rzye = Column(Numeric(20, 4), comment="融资余额")
    rzmre = Column(Numeric(20, 4), comment="融资买入额")
    rzche = Column(Numeric(20, 4), comment="融资偿还额")
    rqye = Column(Numeric(20, 4), comment="融券余量")
    rqmcl = Column(Numeric(20, 4), comment="融券卖出量")
    rzrqye = Column(Numeric(20, 4), comment="融资融券余额")
    rzrqyecz = Column(Numeric(20, 4), comment="融资融券余额差值")
    rqyl = Column(Numeric(12, 6), comment="融券收益率")
