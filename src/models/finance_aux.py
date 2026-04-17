"""财报辅助域 ORM 模型：DisclosureDate, Dividend, FinaMainbz, Express"""

from sqlalchemy import Column, String, Numeric, Index, PrimaryKeyConstraint

from .base import Base


class DisclosureDate(Base):
    """财报披露计划日期"""
    __tablename__ = "disclosure_date"
    __table_args__ = (
        PrimaryKeyConstraint("ts_code", "end_date"),
        Index("idx_disc_actual", "actual_date"),
    )

    ts_code = Column(String(20), comment="TS股票代码")
    end_date = Column(String(8), comment="报告期")
    pre_date = Column(String(8), comment="预计披露日期")
    actual_date = Column(String(8), comment="实际披露日期")


class Dividend(Base):
    """分红送股数据"""
    __tablename__ = "dividend"
    __table_args__ = (
        PrimaryKeyConstraint("ts_code", "end_date"),
        Index("idx_div_end_date", "end_date"),
    )

    ts_code = Column(String(20), comment="TS股票代码")
    end_date = Column(String(8), comment="报告期")
    ann_date = Column(String(8), comment="公告日期")
    div_proc = Column(String(20), comment="分红进度")
    stk_div = Column(Numeric(12, 6), comment="每股送股比例")
    stk_bo = Column(Numeric(12, 6), comment="每股转增股比例")
    div_cash = Column(Numeric(12, 6), comment="每股分红(税前)")
    cash_div = Column(Numeric(12, 6), comment="每股分红(税后)")
    div_listdate = Column(String(8), comment="除权除息日")


class FinaMainbz(Base):
    """主营业务构成"""
    __tablename__ = "fina_mainbz"
    __table_args__ = (
        PrimaryKeyConstraint("ts_code", "end_date", "bz_item"),
        Index("idx_mainbz_end_date", "end_date"),
        Index("idx_mainbz_ratio", "bz_ratio"),
    )

    ts_code = Column(String(20), comment="TS股票代码")
    end_date = Column(String(8), comment="报告期")
    bz_item = Column(String(200), comment="主营业务项目")
    bz_code = Column(String(20), comment="项目代码")
    bz_sales = Column(Numeric(20, 4), comment="主营业务收入")
    bz_profit = Column(Numeric(20, 4), comment="主营业务利润")
    bz_cost = Column(Numeric(20, 4), comment="主营业务成本")
    curr_type = Column(String(10), comment="货币代码")
    update_flag = Column(String(4), comment="更新标志")
    bz_ratio = Column(Numeric(12, 6), comment="主营业务收入占比")


class Express(Base):
    """财务快报数据"""
    __tablename__ = "express"
    __table_args__ = (
        PrimaryKeyConstraint("ts_code", "end_date"),
        Index("idx_express_end_date", "end_date"),
    )

    ts_code = Column(String(20), comment="TS股票代码")
    end_date = Column(String(8), comment="报告期")
    ann_date = Column(String(8), comment="公告日期")
    revenue = Column(Numeric(20, 4), comment="营业收入")
    operate_profit = Column(Numeric(20, 4), comment="营业利润")
    total_profit = Column(Numeric(20, 4), comment="利润总额")
    n_income = Column(Numeric(20, 4), comment="净利润")
    dt_n_income = Column(Numeric(20, 4), comment="扣非净利润")
    basic_eps = Column(Numeric(12, 6), comment="基本每股收益")
    dilut_eps = Column(Numeric(12, 6), comment="稀释每股收益")
    total_share = Column(Numeric(20, 4), comment="总股本")
    bp_share = Column(Numeric(12, 6), comment="每股净资产")
