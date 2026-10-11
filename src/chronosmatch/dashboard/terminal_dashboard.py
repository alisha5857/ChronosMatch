"""Text dashboard for live engine-worker events."""

from __future__ import annotations

import shutil
import sys
import time
from typing import Any

from chronosmatch.metrics.event_stream import EventFileReader


class DashboardState:
    """Latest metrics and trades received from a worker event stream."""

    def __init__(self) -> None:
        self.status = "waiting"
        self.best_bid: int | None = None
        self.best_ask: int | None = None
        self.spread: int | None = None
        self.orders_processed = 0
        self.orders_accepted = 0
        self.orders_rejected = 0
        self.trades = 0
        self.throughput = 0.0
        self.latest_latency_ns: int | None = None
        self.latest_timestamp_ns: int | None = None
        self.latest_trades: list[dict[str, Any]] = []
        self._latest_trade_id_by_run: dict[str, int] = {}

    def apply(self, event: dict[str, Any]) -> None:
        event_type = event.get("event_type")
        self.latest_timestamp_ns = event.get("timestamp_ns")

        if event_type == "worker_started":
            self.status = "running"
        elif event_type == "worker_shutdown":
            self.status = str(event.get("status", "stopped"))

        self.best_bid = event.get("best_bid", self.best_bid)
        self.best_ask = event.get("best_ask", self.best_ask)
        self.spread = event.get("spread", self.spread)
        self.orders_processed = event.get(
            "total_orders_processed", self.orders_processed
        )
        self.orders_accepted = event.get(
            "total_orders_accepted", self.orders_accepted
        )
        self.orders_rejected = event.get(
            "total_orders_rejected", self.orders_rejected
        )
        self.trades = event.get("total_trades", self.trades)
        self.throughput = event.get(
            "throughput_orders_per_second", self.throughput
        )
        if "latency_ns" in event:
            self.latest_latency_ns = event["latency_ns"]
        if event_type == "trade_executed":
            trade = event["trade"]
            run_id = str(event.get("run_id", ""))
            trade_id = int(trade["trade_id"])
            latest_trade_id = self._latest_trade_id_by_run.get(run_id, 0)
            if trade_id > latest_trade_id:
                self._latest_trade_id_by_run[run_id] = trade_id
                self.latest_trades.append(trade)
                self.latest_trades = self.latest_trades[-5:]


class TerminalDashboard:
    """Render worker metrics outside the matching engine process."""

    def __init__(self, refresh_interval: float = 1.0) -> None:
        if refresh_interval <= 0:
            raise ValueError("refresh_interval must be positive")
        self.refresh_interval = refresh_interval
        self.state = DashboardState()

    def render(
        self,
        *,
        order_book: dict[str, list[tuple[int, int]]] | None = None,
        trades: int | None = None,
        state: DashboardState | None = None,
    ) -> str:
        if state is None:
            state = self.state
        if order_book is None:
            order_book = {"bids": [], "asks": []}

        bid = (
            state.best_bid
            if state.best_bid is not None
            else (order_book.get("bids") or [(None, 0)])[0][0]
        )
        ask = (
            state.best_ask
            if state.best_ask is not None
            else (order_book.get("asks") or [(None, 0)])[0][0]
        )
        spread = (
            state.spread
            if bid is not None and ask is not None
            else None
        )
        trade_count = state.trades if trades is None else trades
        latest = state.latest_trades[-1] if state.latest_trades else None
        trade_line = (
            "Latest trade: N/A"
            if latest is None
            else (
                "Latest trade: "
                f"id={latest['trade_id']} price={latest['price']} "
                f"quantity={latest['quantity']} "
                f"buy={latest['buy_order_id']} "
                f"sell={latest['sell_order_id']}"
            )
        )
        latency = (
            "N/A"
            if state.latest_latency_ns is None
            else f"{state.latest_latency_ns:,} ns"
        )

        lines = [
            "ChronosMatch Dashboard",
            f"Worker: {state.status} | Refresh: {self.refresh_interval:.2f}s",
            f"Best bid: {bid if bid is not None else 'N/A'}",
            f"Best ask: {ask if ask is not None else 'N/A'}",
            f"Spread: {spread if spread is not None else 'N/A'}",
            f"Orders processed: {state.orders_processed}",
            f"Orders accepted: {state.orders_accepted}",
            f"Orders rejected: {state.orders_rejected}",
            f"Trades: {trade_count}",
            f"Throughput: {state.throughput:.2f} orders/s",
            f"Latest matching latency: {latency}",
            trade_line,
        ]
        width = max(20, shutil.get_terminal_size(fallback=(80, 24)).columns)
        return "\n".join(line[:width] for line in lines)

    def run(self, events_file: str | None = None) -> None:
        """Poll and render a worker's JSONL event stream until it shuts down."""
        if events_file is None:
            raise ValueError("An engine-worker events_file is required")
        reader = EventFileReader(events_file)
        is_tty = sys.stdout.isatty()
        while self.state.status not in {"stopped", "interrupted", "error"}:
            events = reader.poll()
            for event in events:
                self.state.apply(event)

            if is_tty:
                print("\033[2J\033[H", end="")
            print(self.render(), flush=True)
            if self.state.status not in {"stopped", "interrupted", "error"}:
                time.sleep(self.refresh_interval)

    def close(self) -> None:
        """Included for launcher compatibility; no persistent resources exist."""
        return None
