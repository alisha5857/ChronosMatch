"""Throughput measurement utilities for ChronosMatch."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ThroughputStats:
    """Summary of a throughput sample."""

    observed: int
    elapsed_seconds: float

    @property
    def orders_per_second(self) -> float:
        if self.elapsed_seconds <= 0:
            return 0.0
        return self.observed / self.elapsed_seconds


class ThroughputMeter:
    """Track processed order counts over a time interval."""

    def __init__(self) -> None:
        self._count = 0
        self._start = None

    def start(self) -> None:
        import time

        self._count = 0
        self._start = time.perf_counter()

    def mark(self, count: int = 1) -> None:
        if count < 0:
            raise ValueError("count must be non-negative")
        self._count += count

    def snapshot(self) -> ThroughputStats:
        import time

        elapsed = 0.0
        if self._start is not None:
            elapsed = max(0.0, time.perf_counter() - self._start)
        return ThroughputStats(observed=self._count, elapsed_seconds=elapsed)
