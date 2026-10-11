"""Run a ChronosMatch matching-engine worker."""

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from chronosmatch.engine.matching_engine import MatchingEngine
from chronosmatch.ipc.order_protocol import Order, decode_order
from chronosmatch.ipc.ring_buffer import SharedMemoryRingBuffer
from chronosmatch.metrics.event_stream import EventStreamWriter
from chronosmatch.storage.async_trade_persister import AsyncTradePersister
from chronosmatch.storage.order_journal import JournalRecord, OrderJournal
from chronosmatch.storage.trade_ledger import TradeLedger, TradeRecord


def _crash_at(requested: str | None, point: str) -> None:
    if requested == point:
        os._exit(70)


def _trade_records(
    generated,
    *,
    timestamp_ns: int,
    run_id: str,
) -> list[TradeRecord]:
    return [
        TradeRecord(
            trade_id=trade.trade_id,
            buy_order_id=trade.buy_order_id,
            sell_order_id=trade.sell_order_id,
            price=trade.price,
            quantity=trade.quantity,
            timestamp_ns=timestamp_ns,
            run_id=run_id,
        )
        for trade in generated
    ]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Consume orders from a shared-memory ring buffer."
    )
    parser.add_argument("--buffer-name", required=True)
    parser.add_argument(
        "--max-orders",
        type=int,
        default=0,
        help="Maximum buffer records to consume; 0 means run until interrupted.",
    )
    parser.add_argument("--poll-interval", type=float, default=0.0001)
    parser.add_argument("--json-result", action="store_true")
    parser.add_argument("--events-file", type=Path)
    parser.add_argument("--event-capacity", type=int, default=4096)
    parser.add_argument("--ready-file", type=Path)
    parser.add_argument("--trades-db", type=Path)
    parser.add_argument("--journal", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument(
        "--test-crash-point",
        choices=(
            "after_journal_before_match",
            "after_match_before_persist",
            "after_queue_submit",
            "during_shutdown_with_queued_work",
            "after_trade_rows_before_checkpoint",
            "after_transaction_commit",
        ),
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args()

    if args.max_orders < 0:
        parser.error("--max-orders cannot be negative")
    if args.poll_interval < 0:
        parser.error("--poll-interval cannot be negative")
    if args.event_capacity < 1:
        parser.error("--event-capacity must be positive")

    buffer = SharedMemoryRingBuffer(args.buffer_name, create=False)
    event_writer = None
    trade_persister = None
    journal = None
    worker_status = "stopped"
    processed = accepted = rejected = trades = recovered_orders = 0
    started_ns = time.perf_counter_ns()
    event_sequence = 0
    publish = None
    engine = None

    try:
        if args.events_file is not None:
            event_writer = EventStreamWriter(
                args.events_file,
                capacity=args.event_capacity,
            )
        if args.journal is not None:
            journal = OrderJournal(args.journal, args.run_id)
            run_id = journal.run_id
        else:
            run_id = args.run_id or uuid.uuid4().hex

        def persistence_crash_hook(point: str) -> None:
            _crash_at(args.test_crash_point, point)

        if args.trades_db is not None:
            trade_persister = AsyncTradePersister(
                args.trades_db,
                crash_hook=persistence_crash_hook,
            )

        engine = MatchingEngine()
        engine_sequence = 0
        initial_journal_sequence = 0
        recovered_trade_events: list[tuple[JournalRecord, list]] = []
        if journal is not None:
            records = journal.records
            initial_journal_sequence = len(records)
            recovered_orders = len(records)
            replay_results: list[tuple[JournalRecord, list]] = []
            for journal_record in records:
                generated = engine.process_order(journal_record.order)
                replay_results.append((journal_record, generated))
            engine_sequence = len(records)
            trades = len(engine.trades)
            accepted = recovered_orders

            checkpoint = 0
            if args.trades_db is not None:
                with TradeLedger(args.trades_db) as ledger:
                    checkpoint = ledger.processed_sequence(run_id)
                    committed = ledger.fetch_run(run_id)
                if checkpoint > initial_journal_sequence:
                    raise RuntimeError(
                        "SQLite recovery checkpoint is ahead of the journal"
                    )
                expected_committed = [
                    trade_row
                    for journal_record, generated in replay_results
                    if journal_record.sequence <= checkpoint
                    for trade_row in _trade_records(
                        generated,
                        timestamp_ns=journal_record.timestamp_ns,
                        run_id=run_id,
                    )
                ]
                committed_key = lambda trade: (
                    trade.trade_id,
                    trade.buy_order_id,
                    trade.sell_order_id,
                    trade.price,
                    trade.quantity,
                    trade.timestamp_ns,
                )
                if sorted(map(committed_key, committed)) != sorted(
                    map(committed_key, expected_committed)
                ):
                    raise RuntimeError(
                        "Committed SQLite trades do not reconcile with "
                        "the durable order journal"
                    )
                for journal_record, generated in replay_results:
                    if journal_record.sequence <= checkpoint:
                        continue
                    recovered_trade_events.append(
                        (journal_record, generated)
                    )
                    trade_rows = _trade_records(
                        generated,
                        timestamp_ns=journal_record.timestamp_ns,
                        run_id=run_id,
                    )
                    trade_persister.submit_order_result(
                        journal_record.sequence,
                        run_id,
                        trade_rows,
                    )
                trade_persister.flush()

        def publish(event_type: str, **fields) -> dict:
            nonlocal event_sequence
            event_sequence += 1
            event = {
                "sequence": event_sequence,
                "run_id": run_id,
                "event_type": event_type,
                "timestamp_ns": time.time_ns(),
                "monotonic_ns": time.perf_counter_ns(),
                **fields,
            }
            if event_writer is not None:
                event_writer.emit(event)
            return event

        def book_metrics() -> dict:
            bid = engine.order_book.best_bid()
            ask = engine.order_book.best_ask()
            return {
                "best_bid": bid,
                "best_ask": ask,
                "spread": (
                    ask - bid if bid is not None and ask is not None else None
                ),
                "order_book": engine.get_order_book(),
            }

        elapsed_ns = max(1, time.perf_counter_ns() - started_ns)
        publish(
            "worker_started",
            total_orders_processed=recovered_orders,
            total_orders_accepted=accepted,
            total_orders_rejected=rejected,
            total_trades=trades,
            throughput_orders_per_second=0.0,
            **book_metrics(),
        )
        if recovered_orders:
            publish(
                "worker_recovered",
                recovered_orders=recovered_orders,
                recovered_trades=trades,
                **book_metrics(),
            )
        for _journal_record, recovered_trades in recovered_trade_events:
            for trade in recovered_trades:
                publish(
                    "trade_executed",
                    recovered=True,
                    trade={
                        "trade_id": trade.trade_id,
                        "buy_order_id": trade.buy_order_id,
                        "sell_order_id": trade.sell_order_id,
                        "price": trade.price,
                        "quantity": trade.quantity,
                    },
                    total_orders_processed=recovered_orders,
                    total_trades=trades,
                    **book_metrics(),
                )
        if args.ready_file is not None:
            args.ready_file.parent.mkdir(parents=True, exist_ok=True)
            with args.ready_file.open("x", encoding="utf-8"):
                pass
        if not args.json_result:
            print(f"ChronosMatch worker started; buffer={args.buffer_name}")
            print("Press Ctrl+C to stop.")

        while args.max_orders == 0 or processed < args.max_orders:
            encoded = buffer.read()
            if encoded is None:
                if args.poll_interval:
                    time.sleep(args.poll_interval)
                continue

            order = decode_order(encoded)
            processed += 1
            order_started_ns = time.perf_counter_ns()
            journal_record = None
            if journal is not None:
                records = journal.refresh()
                journal_record = journal.by_order_id.get(order.order_id)
                if journal_record is None or journal_record.order != order:
                    raise RuntimeError(
                        "IPC order does not match a durable journal record"
                    )
                if journal_record.sequence <= engine_sequence:
                    # Producer retries after restart are acknowledged from replay.
                    continue

                for missing in records[engine_sequence:journal_record.sequence]:
                    if missing.sequence != engine_sequence + 1:
                        raise RuntimeError("Journal sequence gap during recovery")
                    _crash_at(
                        args.test_crash_point,
                        "after_journal_before_match",
                    )
                    generated = engine.process_order(missing.order)
                    _crash_at(
                        args.test_crash_point,
                        "after_match_before_persist",
                    )
                    trade_rows = _trade_records(
                        generated,
                        timestamp_ns=missing.timestamp_ns,
                        run_id=run_id,
                    )
                    if trade_persister is not None:
                        trade_persister.submit_order_result(
                            missing.sequence,
                            run_id,
                            trade_rows,
                        )
                        _crash_at(args.test_crash_point, "after_queue_submit")
                    engine_sequence = missing.sequence
                    accepted += 1
                    trades += len(generated)
                    if missing.sequence != journal_record.sequence:
                        continue
            else:
                _crash_at(
                    args.test_crash_point,
                    "after_journal_before_match",
                )
                try:
                    generated = engine.process_order(order)
                except ValueError:
                    rejected += 1
                    elapsed_ns = max(1, time.perf_counter_ns() - started_ns)
                    publish(
                        "order_rejected",
                        order_id=order.order_id,
                        side=order.side.name,
                        price=order.price,
                        quantity=order.quantity,
                        reason="Matching engine rejected the order",
                        latency_ns=time.perf_counter_ns() - order_started_ns,
                        total_orders_processed=accepted + rejected,
                        total_orders_accepted=accepted,
                        total_orders_rejected=rejected,
                        total_trades=trades,
                        throughput_orders_per_second=(
                            (accepted + rejected)
                            * 1_000_000_000
                            / elapsed_ns
                        ),
                        **book_metrics(),
                    )
                    continue
                _crash_at(args.test_crash_point, "after_match_before_persist")
                accepted += 1
                trades += len(generated)

            # In journal mode `generated` is the trade list for this newly
            # accepted order; historical replay notifications are suppressed.
            latency_ns = time.perf_counter_ns() - order_started_ns
            elapsed_ns = max(1, time.perf_counter_ns() - started_ns)
            total_processed = accepted + rejected
            metrics = {
                "order_id": order.order_id,
                "side": order.side.name,
                "price": order.price,
                "quantity": order.quantity,
                "latency_ns": latency_ns,
                "total_orders_processed": total_processed,
                "total_orders_accepted": accepted,
                "total_orders_rejected": rejected,
                "total_trades": trades,
                "throughput_orders_per_second": (
                    total_processed * 1_000_000_000 / elapsed_ns
                ),
                **book_metrics(),
            }
            publish("order_accepted", **metrics)
            publish("book_changed", **metrics)
            for trade in generated:
                trade_event = publish(
                    "trade_executed",
                    trade={
                        "trade_id": trade.trade_id,
                        "buy_order_id": trade.buy_order_id,
                        "sell_order_id": trade.sell_order_id,
                        "price": trade.price,
                        "quantity": trade.quantity,
                    },
                    total_orders_processed=total_processed,
                    total_trades=trades,
                    **book_metrics(),
                )
                if trade_persister is not None and journal is None:
                    trade_persister.submit_event(trade_event)

            if not args.json_result and processed % 10_000 == 0:
                print(f"Processed {processed:,} orders", flush=True)

    except KeyboardInterrupt:
        worker_status = "interrupted"
        if not args.json_result:
            print("\nShutdown requested.")
    except BaseException:
        worker_status = "error"
        raise
    finally:
        persistence_flush_ns = 0
        persistence_errors = 0
        persistence_error = None
        if trade_persister is not None:
            flush_started_ns = time.perf_counter_ns()
            try:
                _crash_at(
                    args.test_crash_point,
                    "during_shutdown_with_queued_work",
                )
                trade_persister.close()
            except Exception as exc:
                worker_status = "error"
                persistence_errors += 1
                persistence_error = str(exc)
            persistence_flush_ns = time.perf_counter_ns() - flush_started_ns

        try:
            if publish is not None and engine is not None:
                elapsed_ns = max(1, time.perf_counter_ns() - started_ns)
                publish(
                    "worker_shutdown",
                    status=worker_status,
                    total_orders_processed=accepted + rejected,
                    total_orders_accepted=accepted,
                    total_orders_rejected=rejected,
                    total_trades=trades,
                    persistence_flush_ns=persistence_flush_ns,
                    persistence_errors=persistence_errors,
                    persistence_error=persistence_error,
                    throughput_orders_per_second=(
                        (accepted + rejected)
                        * 1_000_000_000
                        / elapsed_ns
                    ),
                    **book_metrics(),
                )
            # Emit shutdown only after recovery commits and normal queue flush.
            if event_writer is not None:
                event_writer.close()
        finally:
            if journal is not None:
                journal.close()
            buffer.close()

        if persistence_error is not None:
            raise RuntimeError(
                f"Trade persistence failed during shutdown: "
                f"{persistence_error}"
            )

    if args.json_result:
        print(
            json.dumps(
                {
                    "processed": processed,
                    "accepted": accepted,
                    "rejected": rejected,
                    "trades": trades,
                    "recovered_orders": recovered_orders,
                    "final_bid": engine.order_book.best_bid(),
                    "final_ask": engine.order_book.best_ask(),
                }
            ),
            flush=True,
        )
    else:
        print(f"Worker stopped. Total orders processed: {processed:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
