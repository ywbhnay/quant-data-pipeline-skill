"""ORM 模型包 — 统一导出所有表定义"""

from .base import Base

# 基础信息域
from .basic import StockBasic, StockCompany, NameChange, TradeCal

# 日线行情域
from .daily import Daily, DailyBasic, AdjFactor, StkLimit, SuspendD

# 财务数据域
from .finance import FinaIndicator, Income, BalanceSheet, CashFlow

# 财报辅助域
from .finance_aux import DisclosureDate, Dividend, FinaMainbz, Express

# 交易相关域
from .trading import Top10FloatHolders, Margin

# 宏观/指数域
from .macro import MacroIndicators, IndexDailyBasic, SwIndustry

__all__ = [
    "Base",
    # basic
    "StockBasic",
    "StockCompany",
    "NameChange",
    "TradeCal",
    # daily
    "Daily",
    "DailyBasic",
    "AdjFactor",
    "StkLimit",
    "SuspendD",
    # finance
    "FinaIndicator",
    "Income",
    "BalanceSheet",
    "CashFlow",
    # finance_aux
    "DisclosureDate",
    "Dividend",
    "FinaMainbz",
    "Express",
    # trading
    "Top10FloatHolders",
    "Margin",
    # macro
    "MacroIndicators",
    "IndexDailyBasic",
    "SwIndustry",
]
