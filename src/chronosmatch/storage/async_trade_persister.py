"""Bounded asynchronous batching of authoritative worker trade events."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
import queue
import threading
from typing import Any

from chronosmatch.storage.trade_ledger import TradeLedger, TradeRecord


@dataclass(frozen=True, slots=True)
class _PersistenceWork:
    run_id: str
    trades: list[TradeRecord]
    journal_sequence: int | None = None


@dataclass(slots=True)
class _FlushBarrier:
    completed: threading.Event = field(default_factory=threading.Event)


class AsyncTradePersister:
    """Persist trade events on a dedicated thread with bounded backpressure."""

    def __init__(
        self,
        path: str | Path,
        *,
        capacity: int = 1024,
        batch_size: int = 64,
        flush_interval: float = 0.05,
        ledger_factory: Callable[[str | Path], TradeLedger] = TradeLedger,
        crash_hook: Callable[[str], None] | None = None,
    ) -> None:
        if capacity < 1:
            raise ValueError("capacity must be at least 1")
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        if flush_interval <= 0:
            raise ValueError("flush_interval must be positive")

        self.path = Path(path)
        self.capacity = capacity
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._ledger_factory = ledger_factory
        self._crash_hook = crash_hook
        self._queue: queue.Queue[
            _PersistenceWork | _FlushBarrier | None
        ] = queue.Queue(
            maxsize=capacity
        )
        self._state_lock = threading.Lock()
        self._error: Exception | None = None
        self._closing = False
        self._closed = False
        self._sentinel_enqueued = False
        self._thread = threading.Thread(
            target=self._write_batches,
            name="chronosmatch-trade-persister",
            daemon=True,
        )
        self._thread.start()

    def submit_event(self, event: dict[str, Any]) -> None:
        """Queue a worker's actual ``trade_executed`` event.

        When the finite queue is full, the caller waits for capacity. No trade
        is dropped; database work remains on the persistence thread.
        """
        if event.get("event_type") != "trade_executed":
            raise ValueError("Only trade_executed events may be persisted")

        trade = event.get("trade")
        if not isinstance(trade, dict):
            raise ValueError("Trade event has no trade payload")
        try:
            record = TradeRecord(
                trade_id=int(trade["trade_id"]),
                buy_order_id=int(trade["buy_order_id"]),
                sell_order_id=int(trade["sell_order_id"]),
                price=int(trade["price"]),
                quantity=int(trade["quantity"]),
                timestamp_ns=int(event["timestamp_ns"]),
                run_id=str(event.get("run_id", "")),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("Invalid trade execution event") from exc

        with self._state_lock:
            if self._closing or self._closed:
                raise RuntimeError("Trade persister is closing or closed")
            self._raise_if_failed()
            self._put_with_backpressure(
                _PersistenceWork(record.run_id, [record])
            )

    def submit_order_result(
        self,
        journal_sequence: int,
        run_id: str,
        trades: list[TradeRecord],
    ) -> None:
        """Queue one journal order and its deterministic trade results."""
        if journal_sequence < 1:
            raise ValueError("journal_sequence must be positive")
        if any(trade.run_id != run_id for trade in trades):
            raise ValueError("Trade run_id does not match order result")
        with self._state_lock:
            if self._closing or self._closed:
                raise RuntimeError("Trade persister is closing or closed")
            self._raise_if_failed()
            self._put_with_backpressure(
                _PersistenceWork(run_id, list(trades), journal_sequence)
            )

    def _put_with_backpressure(self, item) -> None:
        while True:
            try:
                self._queue.put(item, timeout=0.05)
                return
            except queue.Full:
                self._raise_if_failed()

    def flush(self) -> None:
        """Wait until all queued work is committed by the writer thread."""
        barrier = _FlushBarrier()
        with self._state_lock:
            if self._closing or self._closed:
                raise RuntimeError("Trade persister is closing or closed")
            self._raise_if_failed()
            self._put_with_backpressure(barrier)
        barrier.completed.wait()
        self._raise_if_failed()

    def close(self) -> None:
        """Flush queued trades, commit the final batch, and join the writer."""
        with self._state_lock:
            if self._closed:
                self._raise_if_failed()
                return
            self._closing = True
            while self._thread.is_alive() and not self._sentinel_enqueued:
                if self._error is not None:
                    break
                try:
                    self._queue.put(None, timeout=0.05)
                    self._sentinel_enqueued = True
                    break
                except queue.Full:
                    continue

        self._thread.join()
        self._closed = True
        self._raise_if_failed()

    def _raise_if_failed(self) -> None:
        if self._error is not None:
            raise RuntimeError("Asynchronous trade persistence failed") from (
                self._error
            )

    def _write_batches(self) -> None:
        ledger = None
        pending: list[_PersistenceWork] = []

        def flush_pending() -> None:
            if not pending:
                return
            checkpointed = [
                work for work in pending
                if work.journal_sequence is not None
            ]
            plain = [work for work in pending if work.journal_sequence is None]
            if checkpointed and plain:
                raise RuntimeError(
                    "Cannot mix checkpointed and ordinary trade batches"
                )
            if checkpointed:
                run_ids = {work.run_id for work in checkpointed}
                if len(run_ids) != 1:
                    raise RuntimeError("A persistence batch crossed runs")
                ledger.append_order_results(
                    checkpointed[0].run_id,
                    [
                        (int(work.journal_sequence), work.trades)
                        for work in checkpointed
                    ],
                    crash_hook=self._crash_hook,
                )
            else:
                ledger.append_many(
                    [trade for work in plain for trade in work.trades]
                )
            pending.clear()

        try:
            ledger = self._ledger_factory(self.path)
            while True:
                try:
                    item = self._queue.get(timeout=self.flush_interval)
                except queue.Empty:
                    flush_pending()
                    continue

                if item is None:
                    flush_pending()
                    self._queue.task_done()
                    break

                if isinstance(item, _FlushBarrier):
                    try:
                        flush_pending()
                    finally:
                        item.completed.set()
                        self._queue.task_done()
                    continue

                if pending and (
                    pending[0].journal_sequence is None
                ) != (item.journal_sequence is None):
                    flush_pending()
                pending.append(item)
                self._queue.task_done()
                if len(pending) >= self.batch_size:
                    flush_pending()
        except Exception as exc:
            self._error = exc
            while True:
                try:
                    self._queue.get_nowait()
                except queue.Empty:
                    break
                else:
                    if isinstance(item, _FlushBarrier):
                        item.completed.set()
                    self._queue.task_done()
        finally:
            if ledger is not None:
                try:
                    ledger.close()
                except Exception as exc:
                    if self._error is None:
                        self._error = exc

    def __enter__(self) -> "AsyncTradePersister":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
