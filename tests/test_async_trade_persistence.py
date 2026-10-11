import json
from multiprocessing import shared_memory
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
import time
import uuid

import pytest

from chronosmatch.engine.matching_engine import MatchingEngine
from chronosmatch.ipc.order_protocol import Order, OrderSide, encode_order
from chronosmatch.ipc.ring_buffer import SharedMemoryRingBuffer
from chronosmatch.storage.async_trade_persister import AsyncTradePersister
from chronosmatch.storage.trade_ledger import TradeLedger, TradeRecord


def trade_event(
    trade_id: int,
    *,
    run_id: str = "test-run",
    timestamp_ns: int | None = None,
) -> dict:
    return {
        "event_type": "trade_executed",
        "run_id": run_id,
        "timestamp_ns": timestamp_ns or 1_000 + trade_id,
        "trade": {
            "trade_id": trade_id,
            "buy_order_id": trade_id + 10,
            "sell_order_id": trade_id + 20,
            "price": 100 + trade_id,
            "quantity": trade_id + 1,
        },
    }


def test_empty_queue_batches_duplicates_and_shutdown(tmp_path: Path) -> None:
    database = tmp_path / "trades.sqlite"
    persister = AsyncTradePersister(
        database,
        capacity=4,
        batch_size=2,
    )
    persister.close()
    with TradeLedger(database) as ledger:
        assert ledger.fetch_all() == []

    first = trade_event(1)
    second = trade_event(2)
    with AsyncTradePersister(
        database,
        capacity=4,
        batch_size=2,
    ) as persister:
        persister.submit_event(first)
        persister.submit_event(second)
        persister.submit_event(first)

    with TradeLedger(database) as ledger:
        assert ledger.fetch_all() == [
            TradeRecord(1, 11, 21, 101, 2, 1001, "test-run"),
            TradeRecord(2, 12, 22, 102, 3, 1002, "test-run"),
        ]
        assert ledger._connection.execute(
            "PRAGMA synchronous"
        ).fetchone()[0] == 2


def test_batch_size_and_final_shutdown_batch_are_flushed(
    tmp_path: Path,
) -> None:
    batches: list[list[TradeRecord]] = []

    class RecordingLedger:
        def __init__(self, _path) -> None:
            pass

        def append_many(self, trades: list[TradeRecord]) -> None:
            batches.append(list(trades))

        def close(self) -> None:
            pass

    with AsyncTradePersister(
        tmp_path / "unused.sqlite",
        capacity=4,
        batch_size=2,
        ledger_factory=RecordingLedger,
    ) as persister:
        for trade_id in range(1, 4):
            persister.submit_event(trade_event(trade_id))

    assert [len(batch) for batch in batches] == [2, 1]


def test_worker_persists_actual_matching_trades_from_real_ipc(
    tmp_path: Path,
) -> None:
    buffer_name = f"chronos_trade_test_{uuid.uuid4().hex}"
    database = tmp_path / "worker-trades.sqlite"
    run_id = "ipc-trade-test"
    orders = [
        Order(1, 101, 5, OrderSide.SELL),
        Order(2, 99, 2, OrderSide.BUY),
        Order(3, 101, 2, OrderSide.BUY),
        Order(4, 99, 1, OrderSide.SELL),
    ]
    expected_engine = MatchingEngine()
    expected_trades = [
        trade
        for order in orders
        for trade in expected_engine.process_order(order)
    ]
    buffer = SharedMemoryRingBuffer(buffer_name, capacity=2, create=True)
    lock_path = buffer.lock_path
    worker = None

    try:
        worker_script = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "run_engine_worker.py"
        )
        worker = subprocess.Popen(
            [
                sys.executable,
                str(worker_script),
                "--buffer-name",
                buffer_name,
                "--max-orders",
                str(len(orders)),
                "--json-result",
                "--trades-db",
                str(database),
                "--run-id",
                run_id,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        deadline = time.monotonic() + 10
        for order in orders:
            record = encode_order(order)
            while not buffer.write(record):
                if worker.poll() is not None:
                    stdout, stderr = worker.communicate()
                    raise AssertionError(
                        f"Worker exited before queueing orders: "
                        f"{stderr.strip() or stdout.strip()}"
                    )
                if time.monotonic() >= deadline:
                    raise AssertionError("Timed out waiting for IPC capacity")
                time.sleep(0.001)

        stdout, stderr = worker.communicate(timeout=10)
        assert worker.returncode == 0, stderr
        assert json.loads(stdout) == {
            "processed": len(orders),
            "accepted": len(orders),
            "rejected": 0,
            "trades": len(expected_trades),
            "recovered_orders": 0,
            "final_bid": 99,
            "final_ask": 101,
        }

        with TradeLedger(database) as ledger:
            stored = ledger.fetch_all()
        assert len(stored) == len(expected_trades) == 2
        assert [
            (
                row.trade_id,
                row.buy_order_id,
                row.sell_order_id,
                row.price,
                row.quantity,
                row.run_id,
            )
            for row in stored
        ] == [
            (
                trade.trade_id,
                trade.buy_order_id,
                trade.sell_order_id,
                trade.price,
                trade.quantity,
                run_id,
            )
            for trade in expected_trades
        ]
        assert all(row.timestamp_ns > 0 for row in stored)
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
        raise AssertionError("Trade persistence test leaked shared memory")
    assert not lock_path.exists()


def test_bounded_queue_applies_backpressure_without_dropping(
    tmp_path: Path,
) -> None:
    append_started = threading.Event()
    release_append = threading.Event()
    stored: list[TradeRecord] = []

    class GatedLedger:
        def __init__(self, _path) -> None:
            pass

        def append_many(self, trades: list[TradeRecord]) -> None:
            append_started.set()
            if not release_append.wait(timeout=5):
                raise TimeoutError("Test did not release blocked batch")
            stored.extend(trades)

        def close(self) -> None:
            pass

    persister = AsyncTradePersister(
        tmp_path / "unused.sqlite",
        capacity=1,
        batch_size=1,
        ledger_factory=GatedLedger,
    )
    persister.submit_event(trade_event(1))
    assert append_started.wait(timeout=5)

    persister.submit_event(trade_event(2))
    assert persister._queue.full()
    submit_started = threading.Event()
    submit_finished = threading.Event()
    submit_error: list[Exception] = []

    def submit_last() -> None:
        submit_started.set()
        try:
            persister.submit_event(trade_event(3))
        except Exception as exc:
            submit_error.append(exc)
        finally:
            submit_finished.set()

    submitter = threading.Thread(target=submit_last)
    submitter.start()
    assert submit_started.wait(timeout=5)
    assert not submit_finished.wait(timeout=0.05)
    release_append.set()
    assert submit_finished.wait(timeout=5)
    submitter.join(timeout=5)
    assert not submit_error
    persister.close()
    assert [record.trade_id for record in stored] == [1, 2, 3]


def test_database_errors_are_reported_and_worker_is_closed(
    tmp_path: Path,
) -> None:
    ledger_closed = threading.Event()

    class FailingLedger:
        def __init__(self, _path) -> None:
            pass

        def append_many(self, _trades: list[TradeRecord]) -> None:
            raise sqlite3.OperationalError("database unavailable")

        def close(self) -> None:
            ledger_closed.set()

    persister = AsyncTradePersister(
        tmp_path / "unused.sqlite",
        ledger_factory=FailingLedger,
    )
    persister.submit_event(trade_event(1))
    with pytest.raises(RuntimeError, match="persistence failed"):
        persister.close()
    assert ledger_closed.is_set()


def test_conflicting_duplicate_trade_id_rolls_back_batch(
    tmp_path: Path,
) -> None:
    database = tmp_path / "conflicts.sqlite"
    with TradeLedger(database) as ledger:
        ledger.append_many([TradeRecord(1, 11, 21, 101, 2, 1001, "run")])
        with pytest.raises(ValueError, match="conflicts"):
            ledger.append_many(
                [
                    TradeRecord(2, 12, 22, 102, 3, 1002, "run"),
                    TradeRecord(1, 11, 21, 999, 2, 1001, "run"),
                ]
            )
        assert ledger.fetch_all() == [
            TradeRecord(1, 11, 21, 101, 2, 1001, "run")
        ]


def test_migrates_legacy_trade_table_without_losing_existing_rows(
    tmp_path: Path,
) -> None:
    database = tmp_path / "legacy.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            CREATE TABLE trades (
                trade_id INTEGER PRIMARY KEY,
                buy_order_id INTEGER NOT NULL,
                sell_order_id INTEGER NOT NULL,
                price INTEGER NOT NULL,
                quantity INTEGER NOT NULL
            )
            """
        )
        connection.execute(
            "INSERT INTO trades VALUES (?, ?, ?, ?, ?)",
            (1, 11, 21, 101, 2),
        )

    with TradeLedger(database) as ledger:
        ledger.append(TradeRecord(1, 12, 22, 102, 3, 2000, "new-run"))
        rows = ledger.fetch_all()
    assert rows == [
        TradeRecord(1, 11, 21, 101, 2, 0, ""),
        TradeRecord(1, 12, 22, 102, 3, 2000, "new-run"),
    ]
