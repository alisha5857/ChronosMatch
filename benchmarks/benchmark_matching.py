
"""Benchmark individual order-matching latency."""

import argparse
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from chronosmatch.engine.matching_engine import MatchingEngine
from chronosmatch.ipc.order_protocol import Order, OrderSide


def percentile(values: list[int], percentage: float) -> float:
    """Return a nearest-rank percentile in nanoseconds."""
    ordered = sorted(values)
    index = max(0, int((percentage / 100) * len(ordered) + 0.999999) - 1)
    return float(ordered[index])


def create_order(order_id: int) -> Order:
    """Create alternating buy and sell orders around a common price."""
    side = OrderSide.BUY if order_id % 2 == 0 else OrderSide.SELL
    price = 100_000 + (order_id % 20)

    return Order(
        order_id=order_id,
        price=price,
        quantity=1 + (order_id % 10),
        side=side,
    )


def benchmark(iterations: int) -> None:
    engine = MatchingEngine()

    # Warm-up orders are separate from the reported measurements.
    for order_id in range(1, min(iterations, 1_000) + 1):
        engine.process_order(create_order(order_id))

    latencies_ns = []

    for order_id in range(1_000_001, 1_000_001 + iterations):
        order = create_order(order_id)

        start = time.perf_counter_ns()
        engine.process_order(order)
        elapsed = time.perf_counter_ns() - start

        latencies_ns.append(elapsed)

    print("\nChronosMatch — Matching Latency Benchmark")
    print("=" * 48)
    print(f"Measured orders:        {iterations:,}")
    print(f"Average latency:        {statistics.mean(latencies_ns):,.1f} ns")
    print(f"Median latency:         {statistics.median(latencies_ns):,.1f} ns")
    print(f"P95 latency:            {percentile(latencies_ns, 95):,.1f} ns")
    print(f"P99 latency:            {percentile(latencies_ns, 99):,.1f} ns")
    print(f"Maximum latency:        {max(latencies_ns):,} ns")
    print("\nNote: Results include Python call and timing overhead.")
    print("Run multiple times for more reliable comparisons.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--iterations", type=int, default=100_000)
    args = parser.parse_args()

    if args.iterations < 1:
        parser.error("--iterations must be at least 1")

    benchmark(args.iterations)
