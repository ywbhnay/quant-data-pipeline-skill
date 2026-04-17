"""宏观/指数域 ORM 模型：MacroIndicators, IndexDailyBasic, SwIndustry"""

from sqlalchemy import Column, String, Numeric, Index, PrimaryKeyConstraint

from .base import Base


class MacroIndicators(Base):
    """宏观经济指标"""
    __tablename__ = "macro_indicators"
    __table_args__ = (
        Index("idx_macro_pmi", "pmi"),
        Index("idx_macro_cpi", "cpi_yoy"),
    )

    month = Column(String(8), primary_key=True, comment="月份")
    cpi_yoy = Column(Numeric(12, 6), comment="CPI同比增长")
    cpi_mom = Column(Numeric(12, 6), comment="CPI环比增长")
    ppi_yoy = Column(Numeric(12, 6), comment="PPI同比增长")
    ppi_mom = Column(Numeric(12, 6), comment="PPI环比增长")
    pmi = Column(Numeric(12, 6), comment="采购经理指数")
    non_pmi = Column(Numeric(12, 6), comment="非制造业PMI")
    shibor_on = Column(Numeric(12, 6), comment="Shibor隔夜")
    shibor_1w = Column(Numeric(12, 6), comment="Shibor 1周")
    shibor_1m = Column(Numeric(12, 6), comment="Shibor 1月")
    shibor_3m = Column(Numeric(12, 6), comment="Shibor 3月")
    lpr_1y = Column(Numeric(12, 6), comment="LPR 1年期")
    lpr_5y = Column(Numeric(12, 6), comment="LPR 5年期")
    m2_yoy = Column(Numeric(12, 6), comment="M2同比增长")
    total_social_financing = Column(Numeric(20, 4), comment="社会融资规模")


class IndexDailyBasic(Base):
    """指数每日指标"""
    __tablename__ = "index_dailybasic"
    __table_args__ = (
        PrimaryKeyConstraint("ts_code", "trade_date"),
        Index("idx_idx_date", "trade_date"),
        Index("idx_idx_pe_ttm", "pe_ttm"),
    )

    ts_code = Column(String(20), comment="指数代码")
    trade_date = Column(String(8), comment="交易日期")
    total_mv = Column(Numeric(20, 4), comment="总市值")
    float_mv = Column(Numeric(20, 4), comment="流通市值")
    total_share = Column(Numeric(20, 4), comment="总股本")
    float_share = Column(Numeric(20, 4), comment="流通股本")
    free_share = Column(Numeric(20, 4), comment="自由流通股本")
    turnover_rate = Column(Numeric(12, 4), comment="换手率")
    pe = Column(Numeric(12, 4), comment="市盈率")
    pe_ttm = Column(Numeric(12, 4), comment="市盈率(TTM)")
    pb = Column(Numeric(12, 4), comment="市净率")
    dv_ratio = Column(Numeric(12, 4), comment="股息率")
    dv_ttm = Column(Numeric(12, 4), comment="股息率(TTM)")


class SwIndustry(Base):
    """申万行业分类"""
    __tablename__ = "sw_industry"
    __table_args__ = (
        PrimaryKeyConstraint("ts_code", "trade_date"),
        Index("idx_sw_l1", "l1_name"),
        Index("idx_sw_l2", "l2_name"),
    )

    ts_code = Column(String(20), comment="TS股票代码")
    trade_date = Column(String(8), comment="交易日期")
    l1_code = Column(String(20), comment="申万一级行业代码")
    l1_name = Column(String(50), comment="申万一级行业名称")
    l2_code = Column(String(20), comment="申万二级行业代码")
    l2_name = Column(String(50), comment="申万二级行业名称")
    l3_code = Column(String(20), comment="申万三级行业代码")
    l3_name = Column(String(50), comment="申万三级行业名称")
