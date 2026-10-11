
"""Launch the ChronosMatch market simulation."""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from chronosmatch.simulator.market_simulator import MarketSimulator


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run a ChronosMatch mock market simulation."
    )
    parser.add_argument(
        "--orders",
        type=int,
        default=100_000,
        help="Number of mock orders to generate.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible input generation.",
    )
    parser.add_argument(
        "--order-rate",
        type=float,
        help="Optional maximum generation rate in orders per second.",
    )
    parser.add_argument(
        "--duration",
        type=float,
        help="Optional run duration in seconds; requires --order-rate.",
    )
    parser.add_argument(
        "--buy-ratio",
        type=float,
        default=0.5,
        help="Fraction of generated orders that are buys (0.0 to 1.0).",
    )
    parser.add_argument(
        "--events-file",
        type=Path,
        help="Destination for the live worker event stream.",
    )
    parser.add_argument(
        "--trades-db",
        type=Path,
        help="Optional SQLite database for executed trades.",
    )
    parser.add_argument(
        "--journal",
        type=Path,
        help=(
            "Durable append-only order journal. Defaults to a sidecar journal "
            "when --trades-db is supplied."
        ),
    )
    args = parser.parse_args()

    if args.orders < 1:
        parser.error("--orders must be at least 1")
    if args.duration is not None and args.order_rate is None:
        parser.error("--duration requires --order-rate")
    if args.order_rate is not None and args.order_rate <= 0:
        parser.error("--order-rate must be positive")
    if args.duration is not None and args.duration <= 0:
        parser.error("--duration must be positive")
    if not 0.0 <= args.buy_ratio <= 1.0:
        parser.error("--buy-ratio must be between 0 and 1")

    simulator = MarketSimulator(
        order_count=args.orders,
        seed=args.seed,
        order_rate=args.order_rate,
        duration_seconds=args.duration,
        buy_ratio=args.buy_ratio,
        events_file=args.events_file,
        trades_db=args.trades_db,
        journal_path=args.journal,
    )

    print("ChronosMatch market simulation")
    print(f"Order limit:      {args.orders:,}")
    print(f"Random seed:      {args.seed}")
    print(f"Worker events:    {simulator.events_file}")
    if simulator.trades_db is not None:
        print(f"Trade database:   {simulator.trades_db}")
    if simulator.journal_path is not None:
        print(f"Order journal:    {simulator.journal_path}")
    print("-" * 40)

    result = simulator.run()

    if result is not None:
        print(f"Simulation result: {result}")

    print("Simulation completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
