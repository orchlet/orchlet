from __future__ import annotations

import asyncio
import heapq
import itertools
import math
import time
from collections.abc import Callable
from dataclasses import dataclass

from .contracts import Clock


class MonotonicClock(Clock):
    def now(self) -> float:
        return time.monotonic()

    def schedule_at(self, when: float, callback: Callable[[], None]) -> asyncio.TimerHandle:
        return asyncio.get_running_loop().call_later(max(0, when - self.now()), callback)


@dataclass
class _VirtualTimer:
    callback: Callable[[], None]
    cancelled: bool = False

    def cancel(self) -> None:
        self.cancelled = True


class VirtualClock(Clock):
    """Manually advanced clock. Pair with simulated execution, not real subprocess waits."""

    def __init__(self, start: float = 0.0) -> None:
        if not math.isfinite(start):
            raise ValueError("Clock start must be finite")
        self._now = float(start)
        self._timers: list[tuple[float, int, _VirtualTimer]] = []
        self._sequence = itertools.count()

    def now(self) -> float:
        return self._now

    def schedule_at(self, when: float, callback: Callable[[], None]) -> _VirtualTimer:
        if not math.isfinite(when):
            raise ValueError("Timer deadline must be finite")
        timer = _VirtualTimer(callback)
        heapq.heappush(self._timers, (max(when, self._now), next(self._sequence), timer))
        return timer

    def advance(self, seconds: float) -> None:
        if not math.isfinite(seconds) or seconds < 0:
            raise ValueError("Cannot move virtual time backwards or to infinity")
        target = self._now + seconds
        while self._timers and self._timers[0][0] <= target:
            when, _, timer = heapq.heappop(self._timers)
            self._now = when
            if not timer.cancelled:
                timer.callback()
        self._now = target

    def advance_to_next(self) -> bool:
        deadline = self.next_deadline
        if deadline is None:
            return False
        self.advance(deadline - self._now)
        return True

    @property
    def next_deadline(self) -> float | None:
        while self._timers and self._timers[0][2].cancelled:
            heapq.heappop(self._timers)
        if not self._timers:
            return None
        return self._timers[0][0]
