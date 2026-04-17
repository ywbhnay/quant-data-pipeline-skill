"""基础信息域 ORM 模型：StockBasic, StockCompany, NameChange, TradeCal"""

from sqlalchemy import Column, String, Numeric, Boolean, Index, PrimaryKeyConstraint

from .base import Base


class StockBasic(Base):
    """股票基本信息"""
    __tablename__ = "stock_basic"
    __table_args__ = (
        Index("idx_basic_list_status", "list_status"),
        Index("idx_basic_exchange", "exchange"),
    )

    ts_code = Column(String(20), primary_key=True, comment="TS股票代码")
    name = Column(String(50), comment="股票名称")
    industry = Column(String(50), comment="所属行业")
    exchange = Column(String(10), comment="交易所")
    list_status = Column(String(4), comment="上市状态 L上市 D退市 P暂停上市")
    list_date = Column(String(8), comment="上市日期")
    delist_date = Column(String(8), comment="退市日期")
    act_name = Column(String(200), comment="实控人名称")
    act_ent_type = Column(String(50), comment="实控人企业性质")


class StockCompany(Base):
    """上市公司基本信息"""
    __tablename__ = "stock_company"
    __table_args__ = (
        Index("idx_company_province", "province"),
        Index("idx_company_city", "city"),
    )

    ts_code = Column(String(20), primary_key=True, comment="TS股票代码")
    exchange = Column(String(10), comment="交易所")
    chairman = Column(String(200), comment="法人代表")
    manager = Column(String(200), comment="总经理")
    secretary = Column(String(200), comment="董秘")
    reg_capital = Column(Numeric(20, 4), comment="注册资本")
    province = Column(String(20), comment="所在省份")
    city = Column(String(20), comment="所在城市")
    introduction = Column(String(2000), comment="公司介绍")
    website = Column(String(200), comment="公司主页")
    employees = Column(Numeric(10), comment="员工人数")
    main_business = Column(String(500), comment="主要业务")


class NameChange(Base):
    """历史名称变更"""
    __tablename__ = "namechange"
    __table_args__ = (
        PrimaryKeyConstraint("ts_code", "start_date"),
        Index("idx_nc_ts_code", "ts_code"),
        Index("idx_nc_is_st", "is_st"),
    )

    ts_code = Column(String(20), comment="TS股票代码")
    name = Column(String(50), comment="公告当日名称")
    start_date = Column(String(8), comment="开始日期")
    end_date = Column(String(8), comment="结束日期")
    ann_date = Column(String(8), comment="公告日期")
    change_reason = Column(String(200), comment="变更原因")
    is_st = Column(Boolean, comment="是否ST股")


class TradeCal(Base):
    """交易日历"""
    __tablename__ = "trade_cal"
    __table_args__ = (
        PrimaryKeyConstraint("exchange", "cal_date"),
    )

    exchange = Column(String(10), comment="交易所 SSE上交所 SZSE深交所")
    cal_date = Column(String(8), comment="日历日期")
    is_open = Column(Numeric(1), comment="是否交易 0休市 1交易")
    pretrade_date = Column(String(8), comment="上一个交易日")
