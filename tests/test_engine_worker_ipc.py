import json
from multiprocessing import shared_memory
from pathlib import Path
import subprocess
import sys
import time
import uuid

from chronosmatch.ipc.order_protocol import Order, OrderSide, encode_order
from chronosmatch.ipc.ring_buffer import SharedMemoryRingBuffer


def test_worker_consumes_orders_matches_and_handles_full_buffer() -> None:
    name = f"chronos_worker_test_{uuid.uuid4().hex}"
    buffer = SharedMemoryRingBuffer(name, capacity=1, create=True)
    lock_path = buffer.lock_path
    process = None

    sell = encode_order(Order(1, 100, 3, OrderSide.SELL))
    buy = encode_order(Order(2, 105, 2, OrderSide.BUY))

    try:
        assert buffer.write(sell) is True
        assert buffer.is_full is True
        assert buffer.write(buy) is False

        worker_script = (
            Path(__file__).resolve().parents[1]
            / "scripts"
            / "run_engine_worker.py"
        )
        process = subprocess.Popen(
            [
                sys.executable,
                str(worker_script),
                "--buffer-name",
                name,
                "--max-orders",
                "2",
                "--json-result",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        deadline = time.monotonic() + 10
        while not buffer.write(buy):
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                raise AssertionError(
                    "Worker exited before the producer could retry: "
                    f"{stderr.strip() or stdout.strip()}"
                )
            if time.monotonic() >= deadline:
                raise AssertionError(
                    "Worker did not make room in the full buffer"
                )
            time.sleep(0.001)

        stdout, stderr = process.communicate(timeout=10)
        assert process.returncode == 0, stderr
        assert json.loads(stdout) == {
            "processed": 2,
            "accepted": 2,
            "rejected": 0,
            "trades": 1,
            "recovered_orders": 0,
            "final_bid": None,
            "final_ask": 100,
        }

    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            process.communicate(timeout=10)
        buffer.close()
        buffer.unlink()
        lock_path.unlink(missing_ok=True)

    try:
        leftover = shared_memory.SharedMemory(name=name, create=False)
    except FileNotFoundError:
        pass
    else:
        leftover.close()
        raise AssertionError("Shared-memory segment was not unlinked")
    assert not lock_path.exists()
