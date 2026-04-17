"""Sliding window rate limiter for Tushare API calls."""

import threading
import time
from collections import deque


class RateLimiter:
    """滑动窗口限速器：限制每分钟 API 调用次数"""

    def __init__(self, rate_limit_per_min: int = 200):
        self.rate_limit_per_min = rate_limit_per_min
        self.window_seconds = 60.0
        self._calls: deque[float] = deque()
        self._lock = threading.Lock()

    def acquire(self):
        """阻塞直到可以发起下一次 API 调用"""
        while True:
            with self._lock:
                now = time.time()
                # 清理窗口外的调用记录
                cutoff = now - self.window_seconds
                while self._calls and self._calls[0] < cutoff:
                    self._calls.popleft()

                if len(self._calls) < self.rate_limit_per_min:
                    self._calls.append(now)
                    return

                # 计算需要等待的时间
                wait_time = self._calls[0] + self.window_seconds - now

            if wait_time > 0:
                time.sleep(wait_time + 0.1)  # 额外缓冲
