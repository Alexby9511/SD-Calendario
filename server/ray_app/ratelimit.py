"""Small in-process rate limiter for the unauthenticated auth endpoints.

Each API process keeps its own counters: this is a cheap defence against a
single client burning CPU on PBKDF2 or flooding the Raft log with account
registrations, not a cluster-wide quota.
"""

import os
import threading
import time
from typing import Dict, Optional, Tuple

DEFAULT_LIMIT = 30
DEFAULT_WINDOW_SECONDS = 60.0
MAX_TRACKED_KEYS = 10_000


class SlidingWindowLimiter:
    """Fixed-window counter per key with a bounded amount of memory."""

    def __init__(
        self, limit: int = DEFAULT_LIMIT, window_seconds: float = DEFAULT_WINDOW_SECONDS
    ) -> None:
        if limit < 1:
            raise ValueError("rate limit must be at least 1")
        if window_seconds <= 0:
            raise ValueError("rate window must be positive")
        self.limit = limit
        self.window_seconds = window_seconds
        self._hits: Dict[str, Tuple[float, int]] = {}
        self._lock = threading.Lock()

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        with self._lock:
            started, count = self._hits.get(key, (now, 0))
            if now - started >= self.window_seconds:
                started, count = now, 0
            count += 1
            self._hits[key] = (started, count)
            if len(self._hits) > MAX_TRACKED_KEYS:
                self._prune(now)
            return count <= self.limit

    def _prune(self, now: float) -> None:
        expired = [
            key
            for key, (started, _count) in self._hits.items()
            if now - started >= self.window_seconds
        ]
        for key in expired:
            self._hits.pop(key, None)
        if len(self._hits) > MAX_TRACKED_KEYS:
            # Fail open rather than growing without bound.
            self._hits.clear()


def _positive_int(value: Optional[str], default: int) -> int:
    try:
        parsed = int(value) if value is not None else default
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _positive_float(value: Optional[str], default: float) -> float:
    try:
        parsed = float(value) if value is not None else default
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def limiter_from_environment() -> SlidingWindowLimiter:
    return SlidingWindowLimiter(
        limit=_positive_int(os.environ.get("SDCAL_AUTH_RATE_LIMIT"), DEFAULT_LIMIT),
        window_seconds=_positive_float(
            os.environ.get("SDCAL_AUTH_RATE_WINDOW_S"), DEFAULT_WINDOW_SECONDS
        ),
    )
