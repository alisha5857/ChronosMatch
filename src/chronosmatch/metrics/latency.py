"""Latency measurement utilities for the matching engine."""

from __future__ import annotations

import statistics
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LatencyStats:
    """Summary statistics for a set of latency measurements."""

    samples: list[int]

    @property
    def average_ns(self) -> float:
        return statistics.mean(self.samples)

    @property
    def median_ns(self) -> float:
        return statistics.median(self.samples)

    @property
    def p95_ns(self) -> float:
        return percentile(self.samples, 95)

    @property
    def p99_ns(self) -> float:
        return percentile(self.samples, 99)

    @property
    def max_ns(self) -> int:
        return max(self.samples) if self.samples else 0


class LatencyRecorder:
    """Record nanosecond durations and summarize them."""

    def __init__(self) -> None:
        self._samples: list[int] = []

    def record(self, duration_ns: int) -> None:
        if duration_ns < 0:
            raise ValueError("Duration must be non-negative")
        self._samples.append(int(duration_ns))

    def snapshot(self) -> LatencyStats:
        return LatencyStats(list(self._samples))

    def clear(self) -> None:
        self._samples.clear()


def percentile(values: list[int], percentage: float) -> float:
    """Return the nearest-rank percentile in nanoseconds."""
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, int((percentage / 100) * len(ordered) + 0.999999) - 1)
    return float(ordered[index])
