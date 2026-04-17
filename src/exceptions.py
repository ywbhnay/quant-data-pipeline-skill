"""Custom exceptions for Tushare API interactions."""


class TushareRateLimitError(Exception):
    """Tushare API 频次限制触发"""

    pass


class TushareAPIError(Exception):
    """Tushare API 调用失败"""

    pass
