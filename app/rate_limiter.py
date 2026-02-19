"""Token bucket rate limiter for API requests."""
import asyncio
import time
from typing import Optional


class TokenBucketRateLimiter:
    """
    Thread-safe async token bucket rate limiter.
    Tokens refill at `rate` per second, max `capacity` tokens.
    All concurrent callers share the same bucket.
    """

    def __init__(self, rate: float, capacity: Optional[int] = None):
        """
        Args:
            rate: Tokens per second (requests per second).
            capacity: Max tokens (default: ceil(rate) or 1).
        """
        self.rate = max(0.01, rate)
        self.capacity = capacity if capacity is not None else max(1, int(rate) + 1)
        self._tokens = float(self.capacity)
        self._last_update = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_update
        self._tokens = min(
            self.capacity,
            self._tokens + elapsed * self.rate
        )
        self._last_update = now

    async def acquire(self) -> None:
        """Block until a token is available."""
        async with self._lock:
            self._refill()
            while self._tokens < 1:
                wait_time = (1 - self._tokens) / self.rate
                self._tokens = 1
                self._last_update = time.monotonic()
                await asyncio.sleep(wait_time)
                self._refill()
            self._tokens -= 1

    def set_rate(self, rate: float) -> None:
        """Update rate (takes effect on next acquire)."""
        self.rate = max(0.01, rate)
        if self.capacity < self.rate:
            self.capacity = max(1, int(self.rate) + 1)
