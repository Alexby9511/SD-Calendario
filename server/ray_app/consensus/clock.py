"""Clock abstraction so Raft timers can be tested deterministically."""

import time
from typing import Protocol


class Clock(Protocol):
    """Minimal monotonic clock used by election and heartbeat timers."""

    def now(self) -> float:
        """Return the current time in seconds."""


class RealClock:
    """Monotonic clock for production use."""

    def now(self) -> float:
        return time.monotonic()


class FakeClock:
    """Controllable clock for tests; time only moves when ``advance`` is called."""

    def __init__(self, start: float = 0.0) -> None:
        if start < 0:
            raise ValueError("clock: start must be non-negative")
        self._now = start

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("clock: cannot move backwards")
        self._now += seconds
