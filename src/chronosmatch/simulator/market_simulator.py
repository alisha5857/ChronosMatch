"""Run a synthetic ChronosMatch market simulation."""

from __future__ import annotations

import asyncio
import json
import math
import sys
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path

from chronosmatch.ipc.order_protocol import encode_order
from chronosmatch.ipc.ring_buffer import SharedMemoryRingBuffer
from chronosmatch.simulator.order_generator import OrderGenerator
from chronosmatch.storage.order_journal import OrderJournal


@dataclass(frozen=True, slots=True)
class SimulationResult:
    """Summary of an executed simulation."""

    orders_submitted: int
    orders_accepted: int
    trades: int
    final_bid: int | None
    final_ask: int | None


class MarketSimulator:
    """Generate synthetic orders and push them through the real engine."""

    def __init__(
        self,
        order_count: int = 100,
        seed: int = 42,
        *,
        min_price: int = 100_000,
        max_price: int = 110_000,
        min_quantity: int = 1,
        max_quantity: int = 20,
        order_rate: float | None = None,
        duration_seconds: float | None = None,
        buy_ratio: float = 0.5,
        events_file: str | Path | None = None,
        trades_db: str | Path | None = None,
        journal_path: str | Path | None = None,
    ) -> None:
        if order_rate is not None and (
            not math.isfinite(order_rate) or order_rate <= 0
        ):
            raise ValueError("order_rate must be a finite positive number")
        if duration_seconds is not None and (
            not math.isfinite(duration_seconds) or duration_seconds <= 0
        ):
            raise ValueError(
                "duration_seconds must be a finite positive number"
            )
        if duration_seconds is not None and order_rate is None:
            raise ValueError(
                "duration_seconds requires an order_rate to determine "
                "the finite order count"
            )

        self.order_count = order_count
        self.order_rate = order_rate
        self.duration_seconds = duration_seconds
        self.events_file = Path(
            events_file
            if events_file is not None
            else Path(tempfile.gettempdir())
            / f"chronosmatch_{uuid.uuid4().hex}.events.jsonl"
        )
        self.trades_db = Path(trades_db) if trades_db is not None else None
        self.journal_path = (
            Path(journal_path)
            if journal_path is not None
            else (
                self.trades_db.with_suffix(
                    self.trades_db.suffix + ".orders.journal"
                )
                if self.trades_db is not None
                else None
            )
        )
        self.run_id = uuid.uuid4().hex
        self.generator = OrderGenerator(
            seed=seed,
            min_price=min_price,
            max_price=max_price,
            min_quantity=min_quantity,
            max_quantity=max_quantity,
            order_count=order_count,
            buy_ratio=buy_ratio,
        )
        self.seed = seed

    def run(self) -> SimulationResult | None:
        """Synchronously run the finite simulation."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.run_async())
        raise RuntimeError(
            "MarketSimulator.run() cannot run inside an event loop; "
            "use 'await run_async()' instead"
        )

    async def run_async(self) -> SimulationResult | None:
        """Stream generated orders through the worker without blocking the loop."""
        if self.order_count < 1:
            return None

        target_count = self.order_count
        if self.duration_seconds is not None:
            target_count = min(
                target_count,
                int(
                    min(
                        self.order_rate * self.duration_seconds,
                        self.order_count,
                    )
                ),
            )
        if target_count < 1:
            return None

        submitted = 0
        journal = (
            OrderJournal(self.journal_path)
            if self.journal_path is not None
            else None
        )
        if journal is not None:
            self.run_id = journal.run_id
            existing_records = journal.refresh()
            if len(existing_records) > target_count:
                raise ValueError(
                    "Existing journal contains more orders than this "
                    "simulation workload"
                )
        buffer_name = f"chronosmatch_{uuid.uuid4().hex}"
        buffer = SharedMemoryRingBuffer(
            name=buffer_name,
            capacity=4096,
            create=True,
        )
        lock_path = buffer.lock_path
        process = None

        try:
            worker_script = (
                Path(__file__).resolve().parents[3]
                / "scripts"
                / "run_engine_worker.py"
            )
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                str(worker_script),
                "--buffer-name",
                buffer_name,
                "--max-orders",
                str(target_count),
                "--json-result",
                "--events-file",
                str(self.events_file),
                "--run-id",
                self.run_id,
                *(
                    ["--journal", str(self.journal_path)]
                    if self.journal_path is not None
                    else []
                ),
                *(
                    ["--trades-db", str(self.trades_db)]
                    if self.trades_db is not None
                    else []
                ),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            loop = asyncio.get_running_loop()
            start_time = loop.time()
            for order_id in range(1, target_count + 1):
                if self.order_rate is not None:
                    due_time = start_time + (order_id - 1) / self.order_rate
                    delay = due_time - loop.time()
                    if delay > 0:
                        await asyncio.sleep(delay)

                order = self.generator.generate(order_id)
                submitted += 1
                record = encode_order(order)
                if journal is not None:
                    journal.ensure_order(order)

                while not await asyncio.to_thread(buffer.write, record):
                    if process.returncode is not None:
                        stdout, stderr = await process.communicate()
                        detail = (
                            stderr.decode().strip()
                            or stdout.decode().strip()
                        )
                        raise RuntimeError(
                            "Engine worker stopped before consuming all "
                            f"orders: {detail}"
                        )
                    await asyncio.sleep(0.0001)

            stdout, stderr = await process.communicate()
            if process.returncode != 0:
                detail = stderr.decode().strip() or stdout.decode().strip()
                raise RuntimeError(
                    f"Engine worker exited with status {process.returncode}: "
                    f"{detail}"
                )

            worker_result = json.loads(stdout.decode())
            if worker_result["processed"] != submitted:
                raise RuntimeError(
                    "Engine worker processed "
                    f"{worker_result['processed']} of {submitted} submitted "
                    "orders"
                )

            return SimulationResult(
                orders_submitted=submitted,
                orders_accepted=worker_result["accepted"],
                trades=worker_result["trades"],
                final_bid=worker_result["final_bid"],
                final_ask=worker_result["final_ask"],
            )
        finally:
            if process is not None and process.returncode is None:
                process.terminate()
                await process.wait()
                await process.communicate()
            try:
                buffer.close()
            finally:
                buffer.unlink()
                lock_path.unlink(missing_ok=True)
                if journal is not None:
                    journal.close()
