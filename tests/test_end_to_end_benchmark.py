from pathlib import Path

import pytest

import benchmarks.benchmark_end_to_end as benchmark
from benchmarks.benchmark_end_to_end import (
    build_workload,
    latency_summary,
    run_benchmark,
    workload_digest,
)


def test_benchmark_workload_is_deterministic_and_has_partial_trades() -> None:
    first = build_workload(12, 87)
    second = build_workload(12, 87)
    assert first == second
    assert workload_digest(first) == workload_digest(second)

    from chronosmatch.engine.matching_engine import MatchingEngine

    engine = MatchingEngine()
    trades = [
        trade
        for order in first
        for trade in engine.process_order(order)
    ]
    assert trades
    assert any(
        trade.buy_order_id == 3
        and trade.sell_order_id == 1
        and trade.price == 100
        and trade.quantity == 4
        for trade in trades
    )


def test_latency_summary_reports_nearest_rank_percentiles() -> None:
    assert latency_summary([10, 20, 30, 40, 50]) == {
        "count": 5,
        "p50_ns": 30.0,
        "p95_ns": 50.0,
        "p99_ns": 50.0,
        "max_ns": 50,
    }
    assert latency_summary([]) == {
        "count": 0,
        "p50_ns": None,
        "p95_ns": None,
        "p99_ns": None,
        "max_ns": None,
    }


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"order_count": 0, "seed": 1, "warmup_count": 0, "repetitions": 1}, "order_count"),
        ({"order_count": 1, "seed": 1, "warmup_count": -1, "repetitions": 1}, "warmup_count"),
        ({"order_count": 1, "seed": 1, "warmup_count": 0, "repetitions": 0}, "repetitions"),
        ({"order_count": 1, "seed": -1, "warmup_count": 0, "repetitions": 1}, "seed"),
        ({"order_count": 1, "seed": 1, "warmup_count": 0, "repetitions": 1, "timeout": 0}, "timeout"),
    ],
)
def test_benchmark_rejects_invalid_configuration(kwargs, message) -> None:
    with pytest.raises(ValueError, match=message):
        run_benchmark(**kwargs)


def test_benchmark_compares_identical_workloads_and_reconciles_trades(
    tmp_path: Path,
) -> None:
    result = run_benchmark(
        order_count=12,
        seed=41,
        warmup_count=3,
        repetitions=1,
        timeout=20,
        database_path=tmp_path / "comparison.sqlite",
    )

    disabled = result["results_by_mode"]["disabled"][0]
    enabled = result["results_by_mode"]["enabled"][0]
    assert result["configuration"]["same_workload_for_both_modes"] is True
    assert disabled["workload_sha256"] == enabled["workload_sha256"]
    assert disabled["accepted"] == enabled["accepted"] == 12
    assert disabled["rejected"] == enabled["rejected"] == 0
    assert disabled["failed"] == enabled["failed"] == 0
    assert disabled["trades"] == enabled["trades"] > 0
    assert disabled["trade_reconciliation"]["matched"] is True
    assert enabled["trade_reconciliation"] == {
        "authoritative_event_trades": enabled["trades"],
        "persisted_trades": enabled["trades"],
        "matched": True,
    }
    assert disabled["persistence"]["trades_persisted"] == 0
    assert enabled["persistence"]["trades_persisted"] == enabled["trades"]
    assert enabled["persistence"]["shutdown_flush_included_in_elapsed"]
    assert len(disabled["raw_end_to_end_latency_ns"]) == 12
    assert len(enabled["raw_engine_processing_latency_ns"]) == 12
    assert result["aggregate"]["disabled"]["throughput_orders_per_second"][
        "median"
    ] > 0


def test_benchmark_reports_failed_mode_without_success_shaped_metrics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    actual_run_worker = benchmark._run_worker

    def fail_enabled_mode(orders, *, mode, database, work_dir, timeout):
        if mode == "enabled":
            raise OSError("database unavailable")
        return actual_run_worker(
            orders,
            mode=mode,
            database=database,
            work_dir=work_dir,
            timeout=timeout,
        )

    monkeypatch.setattr(benchmark, "_run_worker", fail_enabled_mode)
    result = run_benchmark(
        order_count=12,
        seed=17,
        warmup_count=0,
        repetitions=1,
        timeout=20,
        database_path=tmp_path / "failure.sqlite",
    )

    assert result["status"] == "failed"
    assert result["results_by_mode"]["disabled"][0]["status"] == "passed"
    assert result["results_by_mode"]["enabled"][0] == {
        "status": "failed",
        "mode": "enabled",
        "error": "OSError: database unavailable",
        "repetition": 1,
    }
    assert result["aggregate"]["enabled"]["throughput_orders_per_second"] is None
