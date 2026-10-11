from __future__ import annotations

import json
from multiprocessing import shared_memory
from pathlib import Path
import subprocess
import sys
import time
import uuid

import pytest

from chronosmatch.engine.matching_engine import MatchingEngine
from chronosmatch.ipc.order_protocol import Order, OrderSide, encode_order
from chronosmatch.ipc.ring_buffer import SharedMemoryRingBuffer
from chronosmatch.storage.order_journal import (
    JournalCorruptionError,
    OrderJournal,
)
from chronosmatch.storage.trade_ledger import TradeLedger, TradeRecord


ORDERS = [
    Order(1, 101, 5, OrderSide.SELL),
    Order(2, 99, 2, OrderSide.BUY),
    Order(3, 101, 2, OrderSide.BUY),
    Order(4, 99, 1, OrderSide.SELL),
]


def _worker_command(
    buffer_name: str,
    journal: Path,
    database: Path,
    run_id: str,
    *,
    max_orders: int,
    ready: Path | None = None,
    crash_point: str | None = None,
) -> list[str]:
    worker = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "run_engine_worker.py"
    )
    command = [
        sys.executable,
        str(worker),
        "--buffer-name",
        buffer_name,
        "--max-orders",
        str(max_orders),
        "--json-result",
        "--trades-db",
        str(database),
        "--journal",
        str(journal),
        "--run-id",
        run_id,
    ]
    if ready is not None:
        command.extend(["--ready-file", str(ready)])
    if crash_point is not None:
        command.extend(["--test-crash-point", crash_point])
    return command


def _wait_for_ready(process: subprocess.Popen, ready_file: Path) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if ready_file.exists():
            return
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise AssertionError(
                "Worker failed before readiness: "
                f"{stderr.strip() or stdout.strip()}"
            )
        time.sleep(0.005)
    raise AssertionError("Worker readiness timed out")


def _publish(buffer: SharedMemoryRingBuffer, orders: list[Order]) -> None:
    for order in orders:
        deadline = time.monotonic() + 10
        while not buffer.write(encode_order(order)):
            if time.monotonic() >= deadline:
                raise AssertionError("Timed out waiting for ring-buffer space")
            time.sleep(0.001)


def _reference(orders: list[Order]):
    engine = MatchingEngine()
    trades = [
        trade
        for order in orders
        for trade in engine.process_order(order)
    ]
    return engine, trades


def _trade_tuples(trades):
    return sorted(
        (
            trade.trade_id,
            trade.buy_order_id,
            trade.sell_order_id,
            trade.price,
            trade.quantity,
        )
        for trade in trades
    )


def test_journal_fsync_records_and_recovers_incomplete_trailing_record(
    tmp_path: Path,
) -> None:
    path = tmp_path / "orders.journal"
    journal = OrderJournal(path)
    first = journal.ensure_order(ORDERS[0])
    second = journal.ensure_order(ORDERS[1])
    assert (first.sequence, second.sequence) == (1, 2)
    run_id = journal.run_id
    journal.close()

    with path.open("ab") as stream:
        stream.write(b"ORDR0001\x03")
        stream.flush()

    recovered = OrderJournal(path)
    assert recovered.run_id == run_id
    assert [record.order for record in recovered.records] == ORDERS[:2]
    assert path.stat().st_size == 24 + 2 * 60


def test_journal_rejects_complete_corrupt_record(tmp_path: Path) -> None:
    path = tmp_path / "orders.journal"
    journal = OrderJournal(path)
    journal.ensure_order(ORDERS[0])
    journal.close()
    with path.open("r+b") as stream:
        stream.seek(-1, 2)
        last = stream.read(1)
        stream.seek(-1, 2)
        stream.write(bytes([last[0] ^ 0xFF]))

    with pytest.raises(JournalCorruptionError, match="Corrupt complete"):
        OrderJournal(path)


@pytest.mark.parametrize(
    "crash_point",
    [
        "after_journal_before_match",
        "after_match_before_persist",
        "after_queue_submit",
        "during_shutdown_with_queued_work",
    ],
)
def test_real_worker_crash_replays_journaled_order_once(
    tmp_path: Path,
    crash_point: str,
) -> None:
    journal_path = tmp_path / "recovery.journal"
    database = tmp_path / "recovery.sqlite"
    journal = OrderJournal(journal_path)
    run_id = journal.run_id
    _buffer_name = f"chronos_recovery_{uuid.uuid4().hex}"
    buffer = SharedMemoryRingBuffer(_buffer_name, capacity=4, create=True)
    lock_path = buffer.lock_path
    worker = None
    try:
        ready = tmp_path / "first.ready"
        worker = subprocess.Popen(
            _worker_command(
                _buffer_name,
                journal_path,
                database,
                run_id,
                max_orders=1,
                ready=ready,
                crash_point=crash_point,
            ),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        _wait_for_ready(worker, ready)
        journal.ensure_order(ORDERS[0])
        _publish(buffer, ORDERS[:1])
        stdout, stderr = worker.communicate(timeout=10)
        assert worker.returncode == 70, stderr or stdout

        # The restart rebuilds the book, replays the uncheckpointed result,
        # and then consumes the producer's idempotent retry through IPC.
        buffer_name = f"chronos_restart_{uuid.uuid4().hex}"
        restart_buffer = SharedMemoryRingBuffer(
            buffer_name,
            capacity=4,
            create=True,
        )
        restart_lock = restart_buffer.lock_path
        try:
            restarted = subprocess.Popen(
                _worker_command(
                    buffer_name,
                    journal_path,
                    database,
                    run_id,
                    max_orders=1,
                ),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            _publish(restart_buffer, ORDERS[:1])
            stdout, stderr = restarted.communicate(timeout=10)
            assert restarted.returncode == 0, stderr
            summary = json.loads(stdout)
        finally:
            restart_buffer.close()
            restart_buffer.unlink()
            restart_lock.unlink(missing_ok=True)

        reference_engine, reference_trades = _reference(ORDERS[:1])
        assert summary["processed"] == 1
        assert summary["recovered_orders"] == 1
        assert summary["accepted"] == 1
        assert summary["final_bid"] == reference_engine.order_book.best_bid()
        assert summary["final_ask"] == reference_engine.order_book.best_ask()
        with TradeLedger(database) as ledger:
            records = [
                row for row in ledger.fetch_all() if row.run_id == run_id
            ]
            checkpoint = ledger.processed_sequence(run_id)
        assert checkpoint == 1
        assert _trade_tuples(records) == _trade_tuples(reference_trades)
        assert len({record.trade_id for record in records}) == len(records)
    finally:
        journal.close()
        buffer.close()
        buffer.unlink()
        lock_path.unlink(missing_ok=True)


@pytest.mark.parametrize(
    "crash_point, expected_checkpoint",
    [
        ("after_trade_rows_before_checkpoint", 0),
        ("after_transaction_commit", 4),
    ],
)
def test_sqlite_commit_and_checkpoint_recover_atomically(
    tmp_path: Path,
    crash_point: str,
    expected_checkpoint: int,
) -> None:
    journal_path = tmp_path / f"{crash_point}.journal"
    database = tmp_path / f"{crash_point}.sqlite"
    journal = OrderJournal(journal_path)
    run_id = journal.run_id
    for order in ORDERS:
        journal.ensure_order(order)
    journal.close()
    buffer_name = f"chronos_tx_crash_{uuid.uuid4().hex}"
    buffer = SharedMemoryRingBuffer(buffer_name, capacity=4, create=True)
    lock_path = buffer.lock_path
    crashing = None
    try:
        crashing = subprocess.Popen(
            _worker_command(
                buffer_name,
                journal_path,
                database,
                run_id,
                max_orders=1,
                crash_point=crash_point,
            ),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stdout, stderr = crashing.communicate(timeout=30)
        assert crashing.returncode == 70, stderr or stdout
        with TradeLedger(database) as ledger:
            checkpoint = ledger.processed_sequence(run_id)
            committed = [
                row for row in ledger.fetch_all() if row.run_id == run_id
            ]
        assert checkpoint == expected_checkpoint
        if expected_checkpoint == 0:
            assert committed == []

        restart_name = f"chronos_tx_restart_{uuid.uuid4().hex}"
        restart_buffer = SharedMemoryRingBuffer(
            restart_name,
            capacity=4,
            create=True,
        )
        restart_lock = restart_buffer.lock_path
        try:
            restarted = subprocess.Popen(
                _worker_command(
                    restart_name,
                    journal_path,
                    database,
                    run_id,
                    max_orders=len(ORDERS),
                ),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            _publish(restart_buffer, ORDERS)
            stdout, stderr = restarted.communicate(timeout=10)
            assert restarted.returncode == 0, stderr
            summary = json.loads(stdout)
        finally:
            restart_buffer.close()
            restart_buffer.unlink()
            restart_lock.unlink(missing_ok=True)

        reference_engine, reference_trades = _reference(ORDERS)
        assert summary["recovered_orders"] == len(ORDERS)
        assert summary["final_bid"] == reference_engine.order_book.best_bid()
        assert summary["final_ask"] == reference_engine.order_book.best_ask()
        with TradeLedger(database) as ledger:
            committed = [
                row for row in ledger.fetch_all() if row.run_id == run_id
            ]
            checkpoint = ledger.processed_sequence(run_id)
        assert checkpoint == len(ORDERS)
        assert _trade_tuples(committed) == _trade_tuples(reference_trades)
        assert len(committed) == len(reference_trades)
    finally:
        if crashing is not None and crashing.poll() is None:
            crashing.terminate()
            crashing.communicate(timeout=10)
        buffer.close()
        buffer.unlink()
        lock_path.unlink(missing_ok=True)


def test_worker_fails_closed_on_committed_trade_journal_mismatch(
    tmp_path: Path,
) -> None:
    journal_path = tmp_path / "mismatch.journal"
    database = tmp_path / "mismatch.sqlite"
    journal = OrderJournal(journal_path)
    run_id = journal.run_id
    journal_records = [journal.ensure_order(order) for order in ORDERS]
    journal.close()

    reference = MatchingEngine()
    with TradeLedger(database) as ledger:
        for record in journal_records:
            generated = reference.process_order(record.order)
            rows = [
                TradeRecord(
                    trade.trade_id,
                    trade.buy_order_id,
                    trade.sell_order_id,
                    trade.price,
                    trade.quantity,
                    record.timestamp_ns,
                    run_id,
                )
                for trade in generated
            ]
            ledger.append_order_results(run_id, [(record.sequence, rows)])
        ledger._connection.execute(
            "UPDATE trades SET price = price + 1 WHERE run_id = ?",
            (run_id,),
        )
        ledger._connection.commit()

    buffer_name = f"chronos_mismatch_{uuid.uuid4().hex}"
    buffer = SharedMemoryRingBuffer(buffer_name, capacity=1, create=True)
    lock_path = buffer.lock_path
    try:
        worker = subprocess.Popen(
            _worker_command(
                buffer_name,
                journal_path,
                database,
                run_id,
                max_orders=0,
            ),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stdout, stderr = worker.communicate(timeout=10)
        assert worker.returncode != 0
        assert "do not reconcile" in stderr
    finally:
        buffer.close()
        buffer.unlink()
        lock_path.unlink(missing_ok=True)
