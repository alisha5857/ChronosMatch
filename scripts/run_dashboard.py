
"""Launch the ChronosMatch terminal dashboard."""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from chronosmatch.dashboard.terminal_dashboard import TerminalDashboard


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Launch the ChronosMatch terminal dashboard."
    )
    parser.add_argument(
        "--refresh",
        type=float,
        default=1.0,
        help="Dashboard refresh interval in seconds.",
    )
    parser.add_argument(
        "--events-file",
        required=True,
        help="JSONL event stream produced by run_simulation.py.",
    )
    args = parser.parse_args()

    if args.refresh <= 0:
        parser.error("--refresh must be greater than zero")

    dashboard = TerminalDashboard(refresh_interval=args.refresh)

    try:
        dashboard.run(args.events_file)
    except KeyboardInterrupt:
        print("\nDashboard stopped.")
    finally:
        close = getattr(dashboard, "close", None)
        if callable(close):
            close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
