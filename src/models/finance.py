"""财务数据域 ORM 模型：FinaIndicator, Income, BalanceSheet, CashFlow"""

from sqlalchemy import Column, String, Numeric, Index, PrimaryKeyConstraint

from .base import Base


class FinaIndicator(Base):
    """财务指标数据"""
    __tablename__ = "fina_indicator"
    __table_args__ = (
        PrimaryKeyConstraint("ts_code", "end_date"),
        Index("idx_fina_end_date", "end_date"),
        Index("idx_fina_roe", "roe"),
    )

    ts_code = Column(String(20), comment="TS股票代码")
    ann_date = Column(String(8), comment="公告日期")
    end_date = Column(String(8), comment="报告期")
    eps = Column(Numeric(12, 6), comment="基本每股收益")
    dt_eps = Column(Numeric(12, 6), comment="稀释每股收益")
    roe = Column(Numeric(12, 6), comment="净资产收益率")
    q_roe = Column(Numeric(12, 6), comment="单季度净资产收益率")
    q_dt_roe = Column(Numeric(12, 6), comment="单季度扣非净资产收益率")
    dt_roe = Column(Numeric(12, 6), comment="扣非每股收益")
    roa = Column(Numeric(12, 6), comment="总资产报酬率")
    nprofit_margin = Column(Numeric(12, 6), comment="销售净利率")
    grossprofit_margin = Column(Numeric(12, 6), comment="销售毛利率")
    current_ratio = Column(Numeric(12, 6), comment="流动比率")
    quick_ratio = Column(Numeric(12, 6), comment="速动比率")
    cash_ratio = Column(Numeric(12, 6), comment="保守速动比率")
    ar_turn = Column(Numeric(12, 6), comment="应收账款周转率")
    ca_turn = Column(Numeric(12, 6), comment="流动资产周转率")
    fa_turn = Column(Numeric(12, 6), comment="固定资产周转率")
    assets_turn = Column(Numeric(12, 6), comment="总资产周转率")
    liab_to_asset = Column(Numeric(12, 6), comment="资产负债率")
    ebitda = Column(Numeric(20, 4), comment="息税折旧摊销前利润")


class Income(Base):
    """利润表"""
    __tablename__ = "income"
    __table_args__ = (
        PrimaryKeyConstraint("ts_code", "end_date", "report_type"),
        Index("idx_income_end_date", "end_date"),
    )

    ts_code = Column(String(20), comment="TS股票代码")
    ann_date = Column(String(8), comment="公告日期")
    f_ann_date = Column(String(8), comment="实际公告日期")
    end_date = Column(String(8), comment="报告期")
    report_type = Column(String(4), comment="报表类型")
    comp_type = Column(String(4), comment="公司类型")
    basic_eps = Column(Numeric(12, 6), comment="基本每股收益")
    diluted_eps = Column(Numeric(12, 6), comment="稀释每股收益")
    total_revenue = Column(Numeric(20, 4), comment="营业总收入")
    revenue = Column(Numeric(20, 4), comment="营业收入")
    total_exp = Column(Numeric(20, 4), comment="营业总成本")
    operate_exp = Column(Numeric(20, 4), comment="营业支出")
    sell_exp = Column(Numeric(20, 4), comment="销售费用")
    admin_exp = Column(Numeric(20, 4), comment="管理费用")
    fin_exp = Column(Numeric(20, 4), comment="财务费用")
    r_and_d_exp = Column(Numeric(20, 4), comment="研发费用")
    operate_profit = Column(Numeric(20, 4), comment="营业利润")
    total_profit = Column(Numeric(20, 4), comment="利润总额")
    income_tax = Column(Numeric(20, 4), comment="所得税")
    n_income = Column(Numeric(20, 4), comment="净利润")
    n_income_attr_p = Column(Numeric(20, 4), comment="归属于母公司股东的净利润")
    dt_profit = Column(Numeric(20, 4), comment="扣非净利润")
    dt_n_income_attr_p = Column(Numeric(20, 4), comment="归属于母公司股东的扣非净利润")


class BalanceSheet(Base):
    """资产负债表"""
    __tablename__ = "balancesheet"
    __table_args__ = (
        PrimaryKeyConstraint("ts_code", "end_date", "report_type"),
        Index("idx_bs_end_date", "end_date"),
    )

    ts_code = Column(String(20), comment="TS股票代码")
    ann_date = Column(String(8), comment="公告日期")
    f_ann_date = Column(String(8), comment="实际公告日期")
    end_date = Column(String(8), comment="报告期")
    report_type = Column(String(4), comment="报表类型")
    total_share = Column(Numeric(20, 4), comment="总股本")
    cap_rese = Column(Numeric(20, 4), comment="资本公积金")
    undistr_porfit = Column(Numeric(20, 4), comment="未分配利润")
    money_cap = Column(Numeric(20, 4), comment="货币资金")
    account_receiv = Column(Numeric(20, 4), comment="应收账款")
    inventories = Column(Numeric(20, 4), comment="存货")
    total_cur_asset = Column(Numeric(20, 4), comment="流动资产合计")
    fa = Column(Numeric(20, 4), comment="固定资产")
    goodwill = Column(Numeric(20, 4), comment="商誉")
    total_assets = Column(Numeric(20, 4), comment="资产总计")
    st_borrow = Column(Numeric(20, 4), comment="短期借款")
    lt_borrow = Column(Numeric(20, 4), comment="长期借款")
    total_cur_liab = Column(Numeric(20, 4), comment="流动负债合计")
    total_liab = Column(Numeric(20, 4), comment="负债合计")
    total_hldr_eqy = Column(Numeric(20, 4), comment="股东权益合计")


class CashFlow(Base):
    """现金流量表"""
    __tablename__ = "cashflow"
    __table_args__ = (
        PrimaryKeyConstraint("ts_code", "end_date", "report_type"),
        Index("idx_cf_end_date", "end_date"),
    )

    ts_code = Column(String(20), comment="TS股票代码")
    ann_date = Column(String(8), comment="公告日期")
    f_ann_date = Column(String(8), comment="实际公告日期")
    end_date = Column(String(8), comment="报告期")
    report_type = Column(String(4), comment="报表类型")
    n_income = Column(Numeric(20, 4), comment="净利润")
    oper_cf = Column(Numeric(20, 4), comment="经营活动产生的现金流量净额")
    cp_paid = Column(Numeric(20, 4), comment="购买商品、接受劳务支付的现金")
    taxes_surcharges_paid = Column(Numeric(20, 4), comment="支付的各项税费")
    n_cashflow_act = Column(Numeric(20, 4), comment="经营活动产生的现金流量净额")
    n_cashflow_inv_act = Column(Numeric(20, 4), comment="投资活动产生的现金流量净额")
    n_cashflow_fnc_act = Column(Numeric(20, 4), comment="筹资活动产生的现金流量净额")
    net_cashflow = Column(Numeric(20, 4), comment="现金及现金等价物净增加额")
    fcf = Column(Numeric(20, 4), comment="自由现金流量")
