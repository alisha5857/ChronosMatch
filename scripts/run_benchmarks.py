 
"""Run all ChronosMatch benchmark scripts."""

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

BENCHMARKS = (
    "benchmark_ipc.py",
    "benchmark_matching.py",
    "benchmark_throughput.py",
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the ChronosMatch benchmark suite."
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=100_000,
        help="Iterations for IPC and matching benchmarks.",
    )
    parser.add_argument(
        "--orders",
        type=int,
        default=100_000,
        help="Number of orders for the throughput benchmark.",
    )
    args = parser.parse_args()

    if args.iterations < 1 or args.orders < 1:
        parser.error("--iterations and --orders must be positive")

    benchmark_dir = ROOT / "benchmarks"
    commands = [
        (
            "IPC benchmark",
            [
                sys.executable,
                str(benchmark_dir / "benchmark_ipc.py"),
                "--iterations",
                str(args.iterations),
            ],
        ),
        (
            "Matching benchmark",
            [
                sys.executable,
                str(benchmark_dir / "benchmark_matching.py"),
                "--iterations",
                str(args.iterations),
            ],
        ),
        (
            "Throughput benchmark",
            [
                sys.executable,
                str(benchmark_dir / "benchmark_throughput.py"),
                "--orders",
                str(args.orders),
            ],
        ),
    ]

    for name, command in commands:
        print(f"\n{'=' * 60}\n{name}\n{'=' * 60}", flush=True)

        result = subprocess.run(command, cwd=ROOT, check=False)

        if result.returncode != 0:
            print(
                f"\n{name} failed with exit code {result.returncode}.",
                file=sys.stderr,
            )
            return result.returncode

    print("\nAll benchmarks completed successfully.")
    print("Review the output above for measured results.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
