import random
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional


@dataclass
class RateLimiter:
    """
    Simple request rate limiter.

    - Enforces a minimum interval between calls.
    - Optionally enforces an RPM cap using a sliding window.
    - Adds random jitter to make traffic less "machine-like".
    """

    min_interval_s: float = 0.8
    requests_per_minute: Optional[int] = 50
    jitter_s: float = 0.2

    _last_ts: float = 0.0
    _window: Deque[float] = deque()

    def wait(self) -> None:
        now = time.monotonic()

        # Enforce RPM cap (sliding window)
        if self.requests_per_minute:
            window_s = 60.0
            cutoff = now - window_s
            while self._window and self._window[0] < cutoff:
                self._window.popleft()

            if len(self._window) >= self.requests_per_minute:
                oldest = self._window[0]
                sleep_s = max(0.0, (oldest + window_s) - now)
                self._sleep(sleep_s)
                now = time.monotonic()

        # Enforce minimum interval between requests
        if self._last_ts:
            gap = now - self._last_ts
            sleep_s = max(0.0, self.min_interval_s - gap)
            self._sleep(sleep_s)

        # Add jitter
        if self.jitter_s and self.jitter_s > 0:
            self._sleep(random.uniform(0.0, self.jitter_s))

        self._last_ts = time.monotonic()
        if self.requests_per_minute:
            self._window.append(self._last_ts)

    @staticmethod
    def _sleep(seconds: float) -> None:
        if seconds and seconds > 0:
            time.sleep(seconds)

