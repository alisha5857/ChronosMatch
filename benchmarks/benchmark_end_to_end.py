"""Reproducible end-to-end IPC/engine benchmark with optional SQLite writes."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import sys
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from chronosmatch.engine.matching_engine import MatchingEngine
from chronosmatch.ipc.order_protocol import (
    Order,
    OrderSide,
    encode_order,
)
from chronosmatch.ipc.ring_buffer import SharedMemoryRingBuffer
from chronosmatch.metrics.event_stream import EventFileReader
from chronosmatch.metrics.latency import percentile
from chronosmatch.storage.trade_ledger import TradeLedger


def build_workload(order_count: int, seed: int) -> list[Order]:
    """Create a reproducible workload with known resting and crossing orders."""
    if order_count < 1:
        raise ValueError("order_count must be positive")
    if seed < 0:
        raise ValueError("seed must be non-negative")

    scenario = [
        Order(1, 100, 10, OrderSide.SELL),
        Order(2, 90, 8, OrderSide.BUY),
        Order(3, 101, 4, OrderSide.BUY),
        Order(4, 89, 3, OrderSide.SELL),
        Order(5, 89, 6, OrderSide.SELL),
        Order(6, 100, 2, OrderSide.BUY),
    ][:order_count]
    rng = random.Random(seed)
    orders = list(scenario)

    while len(orders) < order_count:
        order_id = len(orders) + 1
        side = OrderSide.BUY if rng.random() < 0.5 else OrderSide.SELL
        orders.append(
            Order(
                order_id=order_id,
                price=rng.randint(85, 115),
                quantity=rng.randint(1, 10),
                side=side,
            )
        )
    return orders


def workload_digest(orders: list[Order]) -> str:
    digest = hashlib.sha256()
    for order in orders:
        digest.update(encode_order(order))
    return digest.hexdigest()


def reference_trades(orders: list[Order]) -> list[dict[str, int]]:
    engine = MatchingEngine()
    trades = [
        trade
        for order in orders
        for trade in engine.process_order(order)
    ]
    return [
        {
            "trade_id": trade.trade_id,
            "buy_order_id": trade.buy_order_id,
            "sell_order_id": trade.sell_order_id,
            "price": trade.price,
            "quantity": trade.quantity,
        }
        for trade in trades
    ]


def latency_summary(samples: list[int]) -> dict[str, int | float | None]:
    if not samples:
        return {
            "count": 0,
            "p50_ns": None,
            "p95_ns": None,
            "p99_ns": None,
            "max_ns": None,
        }
    return {
        "count": len(samples),
        "p50_ns": percentile(samples, 50),
        "p95_ns": percentile(samples, 95),
        "p99_ns": percentile(samples, 99),
        "max_ns": max(samples),
    }


def _run_worker(
    orders: list[Order],
    *,
    mode: str,
    database: Path,
    work_dir: Path,
    timeout: float,
) -> dict:
    run_id = uuid.uuid4().hex
    buffer_name = f"chronos_bench_{uuid.uuid4().hex}"
    events_path = work_dir / f"{run_id}.events.jsonl"
    ready_path = work_dir / f"{run_id}.ready"
    database.parent.mkdir(parents=True, exist_ok=True)
    if database.exists():
        raise FileExistsError(f"Benchmark database already exists: {database}")

    # Identical schema setup is outside the timed interval for both modes.
    with TradeLedger(database):
        pass

    buffer = SharedMemoryRingBuffer(
        buffer_name,
        capacity=min(4096, max(1, len(orders))),
        create=True,
    )
    lock_path = buffer.lock_path
    process = None
    try:
        worker_script = ROOT / "scripts" / "run_engine_worker.py"
        command = [
            sys.executable,
            str(worker_script),
            "--buffer-name",
            buffer_name,
            "--max-orders",
            str(len(orders)),
            "--json-result",
            "--events-file",
            str(events_path),
            "--event-capacity",
            str(max(4096, len(orders) * 4 + 32)),
            "--ready-file",
            str(ready_path),
            "--run-id",
            run_id,
        ]
        if mode == "enabled":
            command.extend(["--trades-db", str(database)])

        process = subprocess.Popen(
            command,
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        ready_deadline = time.monotonic() + timeout
        while not ready_path.exists():
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                raise RuntimeError(
                    f"Worker startup failed: {stderr.strip() or stdout.strip()}"
                )
            if time.monotonic() >= ready_deadline:
                raise TimeoutError("Worker readiness timed out")
            time.sleep(0.001)

        published_ns: dict[int, int] = {}
        started_ns = time.perf_counter_ns()
        for order in orders:
            encoded = encode_order(order)
            publish_timestamp = time.perf_counter_ns()
            while not buffer.write(encoded):
                if process.poll() is not None:
                    stdout, stderr = process.communicate()
                    raise RuntimeError(
                        "Worker exited while producer was backpressured: "
                        f"{stderr.strip() or stdout.strip()}"
                    )
                if time.perf_counter_ns() - started_ns > timeout * 1e9:
                    raise TimeoutError("Order publication timed out")
                time.sleep(0.0001)
            published_ns[order.order_id] = publish_timestamp

        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            process.terminate()
            process.communicate(timeout=10)
            raise TimeoutError("Worker did not complete before timeout") from exc
        completed_ns = time.perf_counter_ns()
        if process.returncode != 0:
            raise RuntimeError(
                f"Worker failed with exit code {process.returncode}: "
                f"{stderr.strip() or stdout.strip()}"
            )

        summary = json.loads(stdout)
        events = EventFileReader(events_path).poll()
        statuses = {
            event["order_id"]: event
            for event in events
            if event["event_type"] in {"order_accepted", "order_rejected"}
        }
        if len(statuses) != len(orders):
            raise RuntimeError(
                "Worker event stream did not report every submitted order"
            )
        if any(order_id not in statuses for order_id in published_ns):
            raise RuntimeError("A submitted order has no reported outcome")

        end_to_end_samples = [
            statuses[order.order_id]["monotonic_ns"]
            - published_ns[order.order_id]
            for order in orders
        ]
        if any(sample < 0 for sample in end_to_end_samples):
            raise RuntimeError(
                "Worker and producer monotonic clocks were not comparable"
            )
        engine_samples = [
            event["latency_ns"] for event in statuses.values()
        ]
        trades = [
            event["trade"]
            for event in events
            if event["event_type"] == "trade_executed"
        ]
        shutdown = next(
            (
                event
                for event in reversed(events)
                if event["event_type"] == "worker_shutdown"
            ),
            None,
        )
        if shutdown is None:
            raise RuntimeError("Worker did not publish shutdown metrics")
        if shutdown["status"] != "stopped":
            raise RuntimeError(
                f"Worker reported unsuccessful shutdown: {shutdown['status']}"
            )

        with TradeLedger(database) as ledger:
            stored = [
                row
                for row in ledger.fetch_all()
                if row.run_id == run_id
            ]
        expected_trade_rows = sorted(
            (
                trade["trade_id"],
                trade["buy_order_id"],
                trade["sell_order_id"],
                trade["price"],
                trade["quantity"],
            )
            for trade in trades
        )
        stored_trade_rows = sorted(
            (
                row.trade_id,
                row.buy_order_id,
                row.sell_order_id,
                row.price,
                row.quantity,
            )
            for row in stored
        )
        expected_reference = sorted(
            (
                trade["trade_id"],
                trade["buy_order_id"],
                trade["sell_order_id"],
                trade["price"],
                trade["quantity"],
            )
            for trade in reference_trades(orders)
        )
        if expected_trade_rows != expected_reference:
            raise RuntimeError(
                "Worker trade events disagree with reference engine output"
            )
        if mode == "enabled" and stored_trade_rows != expected_trade_rows:
            raise RuntimeError(
                "Persisted trades disagree with authoritative worker trades"
            )
        if mode == "disabled" and stored:
            raise RuntimeError(
                "Persistence-disabled run unexpectedly wrote trade rows"
            )

        processed = int(summary["processed"])
        accepted = int(summary["accepted"])
        rejected = int(summary["rejected"])
        failed = len(orders) - accepted - rejected
        if processed != len(orders) or failed != 0:
            raise RuntimeError(
                "Order outcome reconciliation failed: "
                f"submitted={len(orders)}, processed={processed}, "
                f"accepted={accepted}, rejected={rejected}, failed={failed}"
            )
        if len(trades) != int(summary["trades"]):
            raise RuntimeError("Trade-event count disagrees with worker summary")

        elapsed_ns = completed_ns - started_ns
        return {
            "status": "passed",
            "mode": mode,
            "run_id": run_id,
            "database_path": str(database),
            "submitted": len(orders),
            "processed": processed,
            "accepted": accepted,
            "rejected": rejected,
            "failed": failed,
            "trades": len(trades),
            "total_executed_quantity": sum(
                trade["quantity"] for trade in trades
            ),
            "elapsed_ns": elapsed_ns,
            "end_to_end_orders_per_second": (
                len(orders) * 1_000_000_000 / elapsed_ns
            ),
            "end_to_end_latency": latency_summary(end_to_end_samples),
            "engine_processing_latency": latency_summary(engine_samples),
            "persistence": {
                "enabled": mode == "enabled",
                "trades_persisted": len(stored),
                "flush_ns": shutdown["persistence_flush_ns"],
                "errors": shutdown["persistence_errors"],
                "shutdown_flush_included_in_elapsed": True,
            },
            "trade_reconciliation": {
                "authoritative_event_trades": len(expected_trade_rows),
                "persisted_trades": len(stored),
                "matched": (
                    mode == "disabled"
                    or stored_trade_rows == expected_trade_rows
                ),
            },
            "raw_end_to_end_latency_ns": end_to_end_samples,
            "raw_engine_processing_latency_ns": engine_samples,
        }
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            process.communicate(timeout=10)
        buffer.close()
        buffer.unlink()
        lock_path.unlink(missing_ok=True)
        events_path.unlink(missing_ok=True)
        ready_path.unlink(missing_ok=True)


def _distribution(values: list[float | int]) -> dict[str, float]:
    return {
        "median": float(statistics.median(values)),
        "minimum": float(min(values)),
        "maximum": float(max(values)),
    }


def _failed_result(mode: str, exc: Exception, repetition: int | None = None) -> dict:
    result = {
        "status": "failed",
        "mode": mode,
        "error": f"{type(exc).__name__}: {exc}",
    }
    if repetition is not None:
        result["repetition"] = repetition
    return result


def run_benchmark(
    *,
    order_count: int,
    seed: int,
    warmup_count: int,
    repetitions: int,
    timeout: float = 120.0,
    database_path: Path | None = None,
) -> dict:
    if order_count < 1:
        raise ValueError("order_count must be positive")
    if warmup_count < 0:
        raise ValueError("warmup_count cannot be negative")
    if repetitions < 1:
        raise ValueError("repetitions must be positive")
    if seed < 0:
        raise ValueError("seed must be non-negative")
    if timeout <= 0:
        raise ValueError("timeout must be positive")

    temp_context = (
        tempfile.TemporaryDirectory(prefix="chronosmatch_benchmark_")
        if database_path is None
        else None
    )
    root = (
        Path(temp_context.name)
        if temp_context is not None
        else database_path.parent
    )
    base_path = (
        Path(database_path)
        if database_path is not None
        else root / "benchmark.sqlite"
    )
    base_path.parent.mkdir(parents=True, exist_ok=True)
    work_dir = root / "worker_files"
    work_dir.mkdir(parents=True, exist_ok=True)

    try:
        expected_orders = build_workload(order_count, seed)
        expected_digest = workload_digest(expected_orders)
        expected_trades = reference_trades(expected_orders)
        if not expected_trades:
            raise RuntimeError("Benchmark workload must execute trades")

        warmup_results = {}
        failures = []
        for mode in ("disabled", "enabled"):
            if warmup_count:
                warmup_orders = build_workload(warmup_count, seed + 1)
                warmup_path = base_path.with_name(
                    f"{base_path.stem}-{mode}-warmup.sqlite"
                )
                try:
                    warmup_results[mode] = {
                        "status": "passed",
                        "submitted": _run_worker(
                            warmup_orders,
                            mode=mode,
                            database=warmup_path,
                            work_dir=work_dir,
                            timeout=timeout,
                        )["submitted"],
                    }
                except Exception as exc:
                    failed = _failed_result(mode, exc)
                    warmup_results[mode] = failed
                    failures.append(failed)
            else:
                warmup_results[mode] = {
                    "status": "skipped",
                    "submitted": 0,
                }

        repetitions_by_mode: dict[str, list[dict]] = {
            "disabled": [],
            "enabled": [],
        }
        for repetition in range(repetitions):
            modes = (
                ("disabled", "enabled")
                if repetition % 2 == 0
                else ("enabled", "disabled")
            )
            for mode in modes:
                repetition_path = base_path.with_name(
                    f"{base_path.stem}-{mode}-rep{repetition + 1}.sqlite"
                )
                try:
                    result = _run_worker(
                        expected_orders,
                        mode=mode,
                        database=repetition_path,
                        work_dir=work_dir,
                        timeout=timeout,
                    )
                    result["repetition"] = repetition + 1
                    result["workload_sha256"] = expected_digest
                    result["expected_trade_count"] = len(expected_trades)
                except Exception as exc:
                    result = _failed_result(mode, exc, repetition + 1)
                    failures.append(result)
                repetitions_by_mode[mode].append(result)

        summaries = {}
        for mode, results in repetitions_by_mode.items():
            successful = [
                result for result in results if result["status"] == "passed"
            ]
            metrics = {
                "throughput_orders_per_second": [
                    result["end_to_end_orders_per_second"]
                    for result in successful
                ],
                "elapsed_ns": [
                    result["elapsed_ns"] for result in successful
                ],
            }
            for metric_name, result_key in (
                ("end_to_end_latency", "end_to_end_latency"),
                ("engine_processing_latency", "engine_processing_latency"),
            ):
                for percentile_name in ("p50_ns", "p95_ns", "p99_ns", "max_ns"):
                    metrics[f"{metric_name}_{percentile_name}"] = [
                        result[result_key][percentile_name]
                        for result in successful
                    ]
            summaries[mode] = {
                "status": (
                    "passed"
                    if len(successful) == len(results)
                    else "failed"
                ),
                "successful_repetitions": len(successful),
                "failed_repetitions": len(results) - len(successful),
                **{
                    metric_name: (
                        _distribution(values) if values else None
                    )
                    for metric_name, values in metrics.items()
                },
            }
        output = {
            "status": "failed" if failures else "passed",
            "failures": failures,
            "configuration": {
                "order_count": order_count,
                "seed": seed,
                "warmup_count_per_mode": warmup_count,
                "repetitions": repetitions,
                "timeout_seconds": timeout,
                "python_version": sys.version,
                "operating_system": platform.platform(),
                "machine": platform.machine(),
                "processor": platform.processor(),
                "engine_implementation": "active engine selected by matching_core",
                "workload_sha256": expected_digest,
                "same_workload_for_both_modes": True,
                "event_delivery": "same JSONL event stream in both modes",
                "timed_interval": (
                    "first order publication through worker exit; includes "
                    "event delivery and, when enabled, persistence queueing "
                    "and final shutdown flush; excludes database setup, worker "
                    "startup, warm-up, reconciliation, and cleanup"
                ),
                "end_to_end_latency": (
                    "worker perf_counter_ns timestamp after order processing "
                    "minus producer perf_counter_ns timestamp immediately "
                    "before the first ring-buffer write; includes producer "
                    "backpressure and IPC wait"
                ),
                "engine_processing_latency": (
                    "worker perf_counter_ns duration around "
                    "MatchingEngine.process_order"
                ),
                "throughput": (
                    "submitted orders divided by timed end-to-end duration"
                ),
            },
            "warmup_orders_processed": warmup_results,
            "aggregate": summaries,
            "results_by_mode": repetitions_by_mode,
            "limitations": [
                "Results depend on machine load, OS scheduling, filesystem "
                "cache, SQLite configuration, and interpreter overhead.",
                "The active engine may use the Python fallback; no native "
                "Cython compilation was attempted.",
                "Event-stream serialization and file writes are included "
                "equally in both measured modes.",
                "Warm-up and measured runs use fresh worker processes and "
                "fresh per-run databases.",
            ],
        }
        return output
    finally:
        if temp_context is not None:
            temp_context.cleanup()


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Compare end-to-end IPC and matching with SQLite trade "
            "persistence disabled and enabled."
        )
    )
    parser.add_argument("--orders", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--warmup-count", type=int, default=1_000)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument(
        "--database-path",
        type=Path,
        help=(
            "Base path for isolated per-mode/per-repetition SQLite databases. "
            "Files must not already exist."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write machine-readable JSON here instead of stdout.",
    )
    args = parser.parse_args()

    try:
        result = run_benchmark(
            order_count=args.orders,
            seed=args.seed,
            warmup_count=args.warmup_count,
            repetitions=args.repetitions,
            timeout=args.timeout,
            database_path=args.database_path,
        )
    except Exception as exc:
        failure = {
            "status": "failed",
            "error": f"{type(exc).__name__}: {exc}",
            "configuration": vars(args)
            | {
                "database_path": (
                    str(args.database_path)
                    if args.database_path is not None
                    else None
                ),
                "output": str(args.output) if args.output is not None else None,
            },
        }
        rendered = json.dumps(failure, indent=2)
        print(rendered)
        return 1

    for mode, summary in result["aggregate"].items():
        throughput = summary["throughput_orders_per_second"]
        if throughput is not None:
            print(
                f"{mode}: throughput median={throughput['median']:.2f} "
                f"orders/s (min={throughput['minimum']:.2f}, "
                f"max={throughput['maximum']:.2f})",
                file=sys.stderr,
            )
    rendered = json.dumps(result, indent=2)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + os.linesep, encoding="utf-8")
    else:
        print(rendered)
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
