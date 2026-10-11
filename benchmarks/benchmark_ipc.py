
"""Benchmark binary order encoding and decoding."""

import argparse
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from chronosmatch.ipc.order_protocol import (
    ORDER_SIZE,
    Order,
    OrderSide,
    decode_order,
    encode_order,
)


def benchmark(iterations: int) -> None:
    order = Order(
        order_id=1,
        price=185250,
        quantity=10,
        side=OrderSide.BUY,
    )

    # Warm up Python and the functions before measuring.
    encoded = encode_order(order)
    for _ in range(min(iterations, 10_000)):
        decode_order(encoded)

    encode_times = []
    decode_times = []

    for _ in range(iterations):
        start = time.perf_counter_ns()
        encoded = encode_order(order)
        encode_times.append(time.perf_counter_ns() - start)

        start = time.perf_counter_ns()
        decoded = decode_order(encoded)
        decode_times.append(time.perf_counter_ns() - start)

        if decoded != order:
            raise RuntimeError("Decoded order does not match original order")

    total_encode_ns = sum(encode_times)
    total_decode_ns = sum(decode_times)

    print("\nChronosMatch — Binary Protocol Benchmark")
    print("=" * 48)
    print(f"Iterations:             {iterations:,}")
    print(f"Record size:            {ORDER_SIZE} bytes")
    print(f"Encode average:         {statistics.mean(encode_times):,.1f} ns")
    print(f"Decode average:         {statistics.mean(decode_times):,.1f} ns")
    print(f"Encode median:          {statistics.median(encode_times):,.1f} ns")
    print(f"Decode median:          {statistics.median(decode_times):,.1f} ns")
    print(f"Encode throughput:      {iterations / (total_encode_ns / 1e9):,.0f} ops/sec")
    print(f"Decode throughput:      {iterations / (total_decode_ns / 1e9):,.0f} ops/sec")
    print("\nNote: These are protocol microbenchmarks, not IPC latency.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--iterations",
        type=int,
        default=100_000,
        help="Number of encode/decode iterations",
    )
    args = parser.parse_args()

    if args.iterations < 1:
        parser.error("--iterations must be at least 1")

    benchmark(args.iterations)
