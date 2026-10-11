import json
from multiprocessing import shared_memory
from pathlib import Path
import subprocess
import sys
import time
import uuid

from chronosmatch.dashboard.terminal_dashboard import (
    DashboardState,
    TerminalDashboard,
)
from chronosmatch.ipc.order_protocol import Order, OrderSide, encode_order
from chronosmatch.ipc.ring_buffer import SharedMemoryRingBuffer
from chronosmatch.metrics.event_stream import EventFileReader


def test_dashboard_deduplicates_replayed_trade_notifications_by_run() -> None:
    state = DashboardState()
    event = {
        "event_type": "trade_executed",
        "run_id": "run-1",
        "total_trades": 1,
        "trade": {
            "trade_id": 1,
            "buy_order_id": 2,
            "sell_order_id": 1,
            "price": 101,
            "quantity": 2,
        },
    }
    state.apply(event)
    state.apply(event)
    state.apply({**event, "run_id": "run-2"})

    assert len(state.latest_trades) == 2
    assert [trade["trade_id"] for trade in state.latest_trades] == [1, 1]


def test_live_dashboard_receives_worker_book_and_trade_events(
    tmp_path: Path,
) -> None:
    buffer_name = f"chronos_dashboard_{uuid.uuid4().hex}"
    event_path = tmp_path / "worker.events.jsonl"
    buffer = SharedMemoryRingBuffer(buffer_name, capacity=2, create=True)
    lock_path = buffer.lock_path
    worker = None
    reader = EventFileReader(event_path)
    state = DashboardState()

    worker_script = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "run_engine_worker.py"
    )

    def receive_until(predicate, timeout: float = 10.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for event in reader.poll():
                state.apply(event)
            if predicate():
                return
            if worker is not None and worker.poll() is not None:
                stdout, stderr = worker.communicate()
                raise AssertionError(
                    "Worker exited before expected dashboard event: "
                    f"{stderr.strip() or stdout.strip()}"
                )
            time.sleep(0.005)
        raise AssertionError("Timed out waiting for worker event")

    def publish_order(order: Order) -> None:
        record = encode_order(order)
        deadline = time.monotonic() + 10.0
        while not buffer.write(record):
            if worker is not None and worker.poll() is not None:
                raise AssertionError("Worker stopped while producer was writing")
            if time.monotonic() >= deadline:
                raise AssertionError("Timed out waiting for ring-buffer space")
            time.sleep(0.001)

    try:
        worker = subprocess.Popen(
            [
                sys.executable,
                str(worker_script),
                "--buffer-name",
                buffer_name,
                "--max-orders",
                "3",
                "--poll-interval",
                "0.001",
                "--json-result",
                "--events-file",
                str(event_path),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        receive_until(lambda: state.status == "running")
        empty_book = TerminalDashboard().render(state=state)
        assert "Best bid: N/A" in empty_book
        assert "Best ask: N/A" in empty_book
        assert "Spread: N/A" in empty_book

        publish_order(Order(1, 101, 5, OrderSide.SELL))
        receive_until(lambda: state.orders_processed == 1)
        assert worker.poll() is None
        assert state.best_bid is None
        assert state.best_ask == 101
        assert "Best bid: N/A" in TerminalDashboard().render(state=state)

        publish_order(Order(2, 99, 2, OrderSide.BUY))
        receive_until(lambda: state.orders_processed == 2)
        assert worker.poll() is None
        dashboard = TerminalDashboard()
        rendered = dashboard.render(state=state)
        assert state.best_bid == 99
        assert state.best_ask == 101
        assert state.spread == 2
        assert "Spread: 2" in rendered

        publish_order(Order(3, 101, 2, OrderSide.BUY))
        receive_until(
            lambda: state.orders_processed == 3 and state.trades == 1
        )
        rendered = dashboard.render(state=state)
        assert state.best_bid == 99
        assert state.best_ask == 101
        assert state.latest_trades == [
            {
                "trade_id": 1,
                "buy_order_id": 3,
                "sell_order_id": 1,
                "price": 101,
                "quantity": 2,
            }
        ]
        assert "Trades: 1" in rendered
        assert "price=101 quantity=2" in rendered
        assert "Throughput:" in rendered
        assert "Latest matching latency:" in rendered

        stdout, stderr = worker.communicate(timeout=10)
        assert worker.returncode == 0, stderr
        assert json.loads(stdout) == {
            "processed": 3,
            "accepted": 3,
            "rejected": 0,
            "trades": 1,
            "recovered_orders": 0,
            "final_bid": 99,
            "final_ask": 101,
        }
        receive_until(lambda: state.status == "stopped")
        events = reader.poll()
        for event in events:
            state.apply(event)
        assert state.status == "stopped"
    finally:
        if worker is not None and worker.poll() is None:
            worker.terminate()
            worker.communicate(timeout=10)
        buffer.close()
        buffer.unlink()
        lock_path.unlink(missing_ok=True)

    try:
        leftover = shared_memory.SharedMemory(name=buffer_name, create=False)
    except FileNotFoundError:
        pass
    else:
        leftover.close()
        raise AssertionError("Worker integration leaked shared memory")
    assert not lock_path.exists()
