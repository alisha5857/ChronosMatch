import asyncio
from multiprocessing import shared_memory
import os
from pathlib import Path

import pytest

from chronosmatch.engine.matching_engine import MatchingEngine
from chronosmatch.dashboard.terminal_dashboard import DashboardState
from chronosmatch.ipc.order_protocol import Order, OrderSide
from chronosmatch.ipc.ring_buffer import SharedMemoryRingBuffer
from chronosmatch.metrics.event_stream import EventFileReader
from chronosmatch.simulator import market_simulator
from chronosmatch.simulator.market_simulator import MarketSimulator, SimulationResult
from chronosmatch.storage.trade_ledger import TradeLedger


def test_simulator_sends_orders_to_worker_and_cleans_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    buffer_names: list[str] = []
    lock_paths: list[Path] = []
    processes: list[asyncio.subprocess.Process] = []
    create_buffer = market_simulator.SharedMemoryRingBuffer
    create_process = market_simulator.asyncio.create_subprocess_exec

    def tracking_buffer(*args, **kwargs):
        buffer = create_buffer(*args, **kwargs)
        buffer_names.append(buffer.name)
        lock_paths.append(buffer.lock_path)
        return buffer

    async def tracking_process(*args, **kwargs):
        process = await create_process(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(
        market_simulator, "SharedMemoryRingBuffer", tracking_buffer
    )
    monkeypatch.setattr(
        market_simulator.asyncio, "create_subprocess_exec", tracking_process
    )

    simulator = MarketSimulator(order_count=40, seed=17)
    orders = simulator.generator.generate_batch()
    expected_engine = MatchingEngine()
    expected_trades = sum(
        len(expected_engine.process_order(order)) for order in orders
    )
    expected = SimulationResult(
        orders_submitted=len(orders),
        orders_accepted=len(orders),
        trades=expected_trades,
        final_bid=expected_engine.order_book.best_bid(),
        final_ask=expected_engine.order_book.best_ask(),
    )

    assert simulator.run() == expected
    assert len(buffer_names) == 1
    assert len(processes) == 1
    assert processes[0].returncode is not None

    with pytest.raises(FileNotFoundError):
        shared_memory.SharedMemory(name=buffer_names[0], create=False)
    assert not lock_paths[0].exists()


def test_simulator_failure_stops_worker_and_releases_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    buffer_names: list[str] = []
    lock_paths: list[Path] = []
    processes: list[asyncio.subprocess.Process] = []
    create_buffer = market_simulator.SharedMemoryRingBuffer
    create_process = market_simulator.asyncio.create_subprocess_exec

    def tracking_buffer(*args, **kwargs):
        buffer = create_buffer(*args, **kwargs)
        buffer_names.append(buffer.name)
        lock_paths.append(buffer.lock_path)
        return buffer

    async def tracking_process(*args, **kwargs):
        process = await create_process(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(
        market_simulator, "SharedMemoryRingBuffer", tracking_buffer
    )
    monkeypatch.setattr(
        market_simulator.asyncio, "create_subprocess_exec", tracking_process
    )

    class FailingGenerator:
        def generate(self, order_id):
            if order_id == 1:
                return Order(1, 100, 1, OrderSide.BUY)
            raise RuntimeError("synthetic producer failure")

    simulator = MarketSimulator(order_count=2)
    simulator.generator = FailingGenerator()

    with pytest.raises(RuntimeError, match="synthetic producer failure"):
        simulator.run()

    assert len(processes) == 1
    assert processes[0].returncode is not None
    with pytest.raises(FileNotFoundError):
        shared_memory.SharedMemory(name=buffer_names[0], create=False)
    assert not lock_paths[0].exists()


def test_async_simulator_is_responsive_and_respects_duration() -> None:
    simulator = MarketSimulator(
        order_count=100,
        seed=31,
        order_rate=1_000,
        duration_seconds=0.05,
        buy_ratio=1.0,
    )
    heartbeat_ticks = [0]

    async def heartbeat() -> None:
        while True:
            heartbeat_ticks[0] += 1
            await asyncio.sleep(0.001)

    async def run_with_heartbeat():
        heartbeat_task = asyncio.create_task(heartbeat())
        try:
            result = await simulator.run_async()
        finally:
            heartbeat_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await heartbeat_task
        return result

    result = asyncio.run(run_with_heartbeat())
    assert result is not None
    assert result.orders_submitted == 50
    assert result.orders_accepted == 50
    assert heartbeat_ticks[0] >= 5
    assert all(
        simulator.generator.generate(order_id).side == OrderSide.BUY
        for order_id in range(1, result.orders_submitted + 1)
    )


def test_simulator_exposes_worker_events_to_dashboard(tmp_path) -> None:
    event_path = tmp_path / "simulation.events.jsonl"
    simulator = MarketSimulator(order_count=12, seed=22, events_file=event_path)

    result = simulator.run()
    reader = EventFileReader(event_path)
    state = DashboardState()
    events = reader.poll()
    for event in events:
        state.apply(event)

    assert result is not None
    assert events[0]["event_type"] == "worker_started"
    assert events[-1]["event_type"] == "worker_shutdown"
    assert state.status == "stopped"
    assert state.orders_processed == result.orders_submitted
    assert state.orders_accepted == result.orders_accepted
    assert state.trades == result.trades
    assert state.best_bid == result.final_bid
    assert state.best_ask == result.final_ask


def test_simulator_delivers_live_events_and_persists_authoritative_trades(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event_path = tmp_path / "integrated.events.jsonl"
    database = tmp_path / "integrated-trades.sqlite"
    simulator = MarketSimulator(
        order_count=4,
        seed=17,
        order_rate=4,
        events_file=event_path,
        trades_db=database,
    )

    class ScriptedGenerator:
        orders = {
            1: Order(1, 101, 2, OrderSide.SELL),
            2: Order(2, 101, 3, OrderSide.SELL),
            3: Order(3, 101, 4, OrderSide.BUY),
            4: Order(4, 99, 1, OrderSide.BUY),
        }

        def generate(self, order_id: int) -> Order:
            return self.orders[order_id]

    simulator.generator = ScriptedGenerator()
    expected_engine = MatchingEngine()
    expected_trades = [
        trade
        for order in ScriptedGenerator.orders.values()
        for trade in expected_engine.process_order(order)
    ]
    original_create_process = market_simulator.asyncio.create_subprocess_exec
    workers: list[asyncio.subprocess.Process] = []

    async def track_worker(*args, **kwargs):
        process = await original_create_process(*args, **kwargs)
        workers.append(process)
        return process

    monkeypatch.setattr(
        market_simulator.asyncio,
        "create_subprocess_exec",
        track_worker,
    )

    reader = EventFileReader(event_path)
    state = DashboardState()
    events: list[dict] = []
    live_book_snapshot: tuple[int | None, int | None] | None = None
    live_trade_received = False

    async def simulate_while_consuming_events():
        nonlocal live_book_snapshot, live_trade_received
        simulation = asyncio.create_task(simulator.run_async())
        deadline = asyncio.get_running_loop().time() + 10
        while (
            not simulation.done()
            and asyncio.get_running_loop().time() < deadline
        ):
            for event in reader.poll():
                events.append(event)
                state.apply(event)
                if (
                    event["event_type"] == "order_accepted"
                    and live_book_snapshot is None
                ):
                    assert workers
                    assert workers[0].pid != os.getpid()
                    assert workers[0].returncode is None
                    live_book_snapshot = (state.best_bid, state.best_ask)
                if event["event_type"] == "trade_executed":
                    assert workers[0].returncode is None
                    live_trade_received = True
            await asyncio.sleep(0.005)

        assert simulation.done(), "Simulation did not finish before timeout"
        result = await simulation
        for event in reader.poll():
            events.append(event)
            state.apply(event)
        return result

    result = asyncio.run(simulate_while_consuming_events())

    assert result is not None
    assert result.orders_submitted == result.orders_accepted == 4
    assert result.trades == len(expected_trades) == 2
    assert live_book_snapshot == (None, 101)
    assert live_trade_received
    assert state.status == "stopped"
    assert state.orders_processed == 4
    assert state.best_bid == result.final_bid == 99
    assert state.best_ask == result.final_ask == 101
    recovered = sum(
        event.get("recovered_orders", 0)
        for event in events
        if event["event_type"] == "worker_recovered"
    )
    book_updates = sum(
        event["event_type"] == "book_changed" for event in events
    )
    assert recovered + book_updates == 4

    event_trades = [
        event["trade"]
        for event in events
        if event["event_type"] == "trade_executed"
    ]
    expected_rows = [
        (
            trade.trade_id,
            trade.buy_order_id,
            trade.sell_order_id,
            trade.price,
            trade.quantity,
        )
        for trade in expected_trades
    ]
    assert [
        (
            trade["trade_id"],
            trade["buy_order_id"],
            trade["sell_order_id"],
            trade["price"],
            trade["quantity"],
        )
        for trade in event_trades
    ] == expected_rows

    with TradeLedger(database) as ledger:
        stored = ledger.fetch_all()
    assert [
        (
            trade.trade_id,
            trade.buy_order_id,
            trade.sell_order_id,
            trade.price,
            trade.quantity,
        )
        for trade in stored
    ] == expected_rows
    assert {trade.run_id for trade in stored} == {simulator.run_id}
    assert all(trade.timestamp_ns > 0 for trade in stored)


def test_simulator_restart_rebuilds_state_and_reuses_committed_trades(
    tmp_path: Path,
) -> None:
    database = tmp_path / "restart.sqlite"
    journal_path = tmp_path / "restart.orders.journal"
    orders = [
        Order(1, 101, 5, OrderSide.SELL),
        Order(2, 99, 2, OrderSide.BUY),
        Order(3, 101, 2, OrderSide.BUY),
        Order(4, 99, 1, OrderSide.SELL),
    ]

    def simulator(event_file: Path) -> MarketSimulator:
        instance = MarketSimulator(
            order_count=len(orders),
            events_file=event_file,
            trades_db=database,
            journal_path=journal_path,
        )

        class ScriptedGenerator:
            def generate(self, order_id: int) -> Order:
                return orders[order_id - 1]

        instance.generator = ScriptedGenerator()
        return instance

    first = simulator(tmp_path / "first.events.jsonl")
    first_result = first.run()
    assert first_result is not None

    second = simulator(tmp_path / "second.events.jsonl")
    second_result = second.run()
    assert second_result == first_result

    recovered_events = EventFileReader(tmp_path / "second.events.jsonl").poll()
    recovered = [
        event["recovered_orders"]
        for event in recovered_events
        if event["event_type"] == "worker_recovered"
    ]
    assert recovered == [len(orders)]

    reference = MatchingEngine()
    expected_trades = [
        trade
        for order in orders
        for trade in reference.process_order(order)
    ]
    with TradeLedger(database) as ledger:
        stored = [
            row for row in ledger.fetch_all() if row.run_id == first.run_id
        ]
        checkpoint = ledger.processed_sequence(first.run_id)
    assert checkpoint == len(orders)
    assert [
        (
            trade.trade_id,
            trade.buy_order_id,
            trade.sell_order_id,
            trade.price,
            trade.quantity,
        )
        for trade in stored
    ] == [
        (
            trade.trade_id,
            trade.buy_order_id,
            trade.sell_order_id,
            trade.price,
            trade.quantity,
        )
        for trade in expected_trades
    ]


def test_simulator_recovers_after_real_worker_process_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "crash-recovery.sqlite"
    journal_path = tmp_path / "crash-recovery.orders.journal"
    orders = [
        Order(1, 101, 5, OrderSide.SELL),
        Order(2, 99, 2, OrderSide.BUY),
        Order(3, 101, 2, OrderSide.BUY),
        Order(4, 99, 1, OrderSide.SELL),
    ]

    class ScriptedGenerator:
        def generate(self, order_id: int) -> Order:
            return orders[order_id - 1]

    simulator = MarketSimulator(
        order_count=len(orders),
        events_file=tmp_path / "crashed.events.jsonl",
        trades_db=database,
        journal_path=journal_path,
    )
    simulator.generator = ScriptedGenerator()
    original_create_process = market_simulator.asyncio.create_subprocess_exec
    workers: list[asyncio.subprocess.Process] = []

    async def crash_first_worker(*args, **kwargs):
        if not workers:
            ready = tmp_path / "crashing-worker.ready"
            args = (
                *args,
                "--test-crash-point",
                "after_match_before_persist",
                "--ready-file",
                str(ready),
            )
        process = await original_create_process(*args, **kwargs)
        workers.append(process)
        if len(workers) == 1:
            deadline = asyncio.get_running_loop().time() + 10
            while not ready.exists():
                if process.returncode is not None:
                    stdout, stderr = await process.communicate()
                    raise AssertionError(
                        f"Crash-test worker failed during startup: "
                        f"{stderr.decode() or stdout.decode()}"
                    )
                if asyncio.get_running_loop().time() >= deadline:
                    raise AssertionError("Crash-test worker readiness timed out")
                await asyncio.sleep(0.005)
        return process

    monkeypatch.setattr(
        market_simulator.asyncio,
        "create_subprocess_exec",
        crash_first_worker,
    )
    with pytest.raises(RuntimeError, match="status 70"):
        simulator.run()
    assert workers[0].returncode == 70

    recovered_simulator = MarketSimulator(
        order_count=len(orders),
        events_file=tmp_path / "recovered.events.jsonl",
        trades_db=database,
        journal_path=journal_path,
    )
    recovered_simulator.generator = ScriptedGenerator()
    result = recovered_simulator.run()
    assert result is not None
    assert result.orders_submitted == result.orders_accepted == len(orders)
    assert result.trades == 2
    assert len(workers) == 2
    assert workers[1].returncode == 0

    recovery_events = EventFileReader(
        tmp_path / "recovered.events.jsonl"
    ).poll()
    assert [
        event["recovered_orders"]
        for event in recovery_events
        if event["event_type"] == "worker_recovered"
    ] == [len(orders)]
    reference = MatchingEngine()
    expected_trades = [
        trade
        for order in orders
        for trade in reference.process_order(order)
    ]
    with TradeLedger(database) as ledger:
        stored = [
            row
            for row in ledger.fetch_all()
            if row.run_id == simulator.run_id
        ]
        assert ledger.processed_sequence(simulator.run_id) == len(orders)
    assert [
        (
            trade.trade_id,
            trade.buy_order_id,
            trade.sell_order_id,
            trade.price,
            trade.quantity,
        )
        for trade in stored
    ] == [
        (
            trade.trade_id,
            trade.buy_order_id,
            trade.sell_order_id,
            trade.price,
            trade.quantity,
        )
        for trade in expected_trades
    ]
