"""Shared-memory ring buffer for ChronosMatch orders."""

import os
import tempfile
import time
from contextlib import contextmanager
from multiprocessing import shared_memory
from pathlib import Path

from chronosmatch.ipc.order_protocol import ORDER_SIZE


MAGIC = b"CHRONOS1"
HEADER_SIZE = 32


class SharedMemoryRingBuffer:
    """Bounded, cross-process ring buffer for fixed-size records.

    This implementation is intentionally single-producer/single-consumer (SPSC):
    it allows one writer and one reader to coordinate via a shared-memory header and
    file-based lock. It is not a general multi-producer or multi-consumer queue.
    """

    def __init__(
        self,
        name: str,
        capacity: int = 4096,
        create: bool = False,
    ):
        if not name or not name.replace("_", "").isalnum():
            raise ValueError("Invalid buffer name")

        if create and capacity < 1:
            raise ValueError("Capacity must be at least 1")

        self.name = name
        self.capacity = capacity
        self.owner = create
        self.closed = False

        self.lock_path = Path(tempfile.gettempdir()) / (
            f"{name}.chronos.lock"
        )
        self.lock_file = open(self.lock_path, "a+b")

        try:
            if create:
                self.shm = shared_memory.SharedMemory(
                    name=name,
                    create=True,
                    size=HEADER_SIZE + capacity * ORDER_SIZE,
                )

                with self._locked():
                    self.shm.buf[:HEADER_SIZE] = bytes(HEADER_SIZE)
                    self.shm.buf[:8] = MAGIC
                    self._put(8, capacity)
                    self._put(16, 0)
                    self._put(24, 0)
            else:
                self.shm = shared_memory.SharedMemory(
                    name=name,
                    create=False,
                )

            with self._locked():
                if bytes(self.shm.buf[:8]) != MAGIC:
                    raise ValueError("Invalid buffer header")

                self.capacity = self._get(8)

                if self.capacity < 1:
                    raise ValueError("Invalid buffer capacity")

                expected_size = HEADER_SIZE + self.capacity * ORDER_SIZE
                if len(self.shm.buf) < expected_size:
                    raise ValueError("Shared-memory region is too small")

        except Exception:
            self.lock_file.close()
            raise

    def __len__(self) -> int:
        with self._locked():
            read_seq = self._get(16)
            write_seq = self._get(24)
            return max(0, write_seq - read_seq)

    @property
    def is_empty(self) -> bool:
        return len(self) == 0

    @property
    def is_full(self) -> bool:
        return len(self) >= self.capacity

    def _get(self, offset: int) -> int:
        return int.from_bytes(
            self.shm.buf[offset:offset + 8], "little"
        )

    def _put(self, offset: int, value: int) -> None:
        self.shm.buf[offset:offset + 8] = value.to_bytes(
            8, "little"
        )

    @contextmanager
    def _locked(self):
        """Acquire an operating-system lock shared between processes."""
        if os.name == "nt":
            import msvcrt

            self.lock_file.seek(0, os.SEEK_END)
            if self.lock_file.tell() == 0:
                self.lock_file.write(b"\0")
                self.lock_file.flush()

            while True:
                try:
                    self.lock_file.seek(0)
                    msvcrt.locking(
                        self.lock_file.fileno(),
                        msvcrt.LK_LOCK,
                        1,
                    )
                    break
                except OSError:
                    time.sleep(0.001)

            try:
                yield
            finally:
                self.lock_file.seek(0)
                msvcrt.locking(
                    self.lock_file.fileno(),
                    msvcrt.LK_UNLCK,
                    1,
                )
        else:
            import fcntl

            fcntl.flock(self.lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(
                    self.lock_file.fileno(), fcntl.LOCK_UN
                )

    def write(self, record: bytes) -> bool:
        """Write a record; return False if the buffer is full."""
        if self.closed:
            raise RuntimeError("Buffer is closed")

        if len(record) != ORDER_SIZE:
            raise ValueError(f"Record must be {ORDER_SIZE} bytes")

        with self._locked():
            read_seq = self._get(16)
            write_seq = self._get(24)

            if write_seq - read_seq >= self.capacity:
                return False

            slot = write_seq % self.capacity
            offset = HEADER_SIZE + slot * ORDER_SIZE

            self.shm.buf[offset:offset + ORDER_SIZE] = record
            self._put(24, write_seq + 1)

            return True

    def read(self) -> bytes | None:
        """Read a record; return None if the buffer is empty."""
        if self.closed:
            raise RuntimeError("Buffer is closed")

        with self._locked():
            read_seq = self._get(16)
            write_seq = self._get(24)

            if read_seq == write_seq:
                return None

            slot = read_seq % self.capacity
            offset = HEADER_SIZE + slot * ORDER_SIZE

            record = bytes(
                self.shm.buf[offset:offset + ORDER_SIZE]
            )

            self._put(16, read_seq + 1)

            return record

    def close(self) -> None:
        """Detach this process from shared memory."""
        if self.closed:
            return

        self.shm.close()
        self.lock_file.close()
        self.closed = True

    def unlink(self) -> None:
        """Remove the shared-memory region created by this instance."""
        if not self.owner:
            raise PermissionError("Only the creator can unlink the buffer")

        self.shm.unlink()