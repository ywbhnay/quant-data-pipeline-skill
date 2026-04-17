"""Retry decorators and safe API call wrappers using tenacity."""

from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .exceptions import TushareRateLimitError


def safe_api_call(func, *args, **kwargs):
    """带重试的 API 调用包装器"""
    return func(*args, **kwargs)


# Decorator for Tushare API methods
tushare_retry = retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=5, max=120),
    retry=retry_if_exception_type((TushareRateLimitError, ConnectionError)),
    reraise=True,
)
