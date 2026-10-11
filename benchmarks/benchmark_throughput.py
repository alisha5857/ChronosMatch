
"""Benchmark matching-engine order throughput."""

import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from chronosmatch.engine.matching_engine import MatchingEngine
from chronosmatch.ipc.order_protocol import Order, OrderSide


def generate_order(order_id: int) -> Order:
    """Generate reproducible mock market orders."""
    side = OrderSide.BUY if order_id % 2 == 0 else OrderSide.SELL

    return Order(
        order_id=order_id,
        price=100_000 + (order_id % 50),
        quantity=1 + (order_id % 20),
        side=side,
    )


def benchmark(order_count: int) -> None:
    engine = MatchingEngine()

    # Prepare orders outside the timed section so generation time
    # does not inflate the engine's processing time.
    orders = [
        generate_order(order_id)
        for order_id in range(1, order_count + 1)
    ]

    # Warm up the engine with a separate instance.
    warmup_engine = MatchingEngine()
    for order_id in range(1, min(order_count, 1_000) + 1):
        warmup_engine.process_order(generate_order(order_id + order_count))

    start = time.perf_counter_ns()

    for order in orders:
        engine.process_order(order)

    elapsed_ns = time.perf_counter_ns() - start
    elapsed_seconds = elapsed_ns / 1_000_000_000

    throughput = order_count / elapsed_seconds
    average_ns = elapsed_ns / order_count

    print("\nChronosMatch — Throughput Benchmark")
    print("=" * 48)
    print(f"Orders processed:       {order_count:,}")
    print(f"Elapsed time:           {elapsed_seconds:.6f} seconds")
    print(f"Average time/order:     {average_ns:,.1f} ns")
    print(f"Throughput:             {throughput:,.0f} orders/sec")
    print("\nNote: This measures in-process engine throughput.")
    print("It does not measure end-to-end shared-memory IPC.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--orders", type=int, default=100_000)
    args = parser.parse_args()

    if args.orders < 1:
        parser.error("--orders must be at least 1")

    benchmark(args.orders)
