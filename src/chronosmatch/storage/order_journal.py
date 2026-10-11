"""Append-only durable journal for deterministic engine replay."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import struct
import threading
import time
import uuid
import zlib

from chronosmatch.ipc.order_protocol import (
    ORDER_SIZE,
    Order,
    decode_order,
    encode_order,
)


_FILE_MAGIC = b"CHRJNL01"
_RECORD_MAGIC = b"ORDR0001"
_HEADER_SIZE = 24
_RECORD = struct.Struct(f"<8sQQ{ORDER_SIZE}sI")


class JournalCorruptionError(ValueError):
    """Raised when a complete journal record is corrupt or inconsistent."""


@dataclass(frozen=True, slots=True)
class JournalRecord:
    sequence: int
    timestamp_ns: int
    order: Order


class OrderJournal:
    """Fsync each order record before the producer publishes it to IPC.

    The file header stores a stable run ID. Records are fixed-size and contain
    a sequence, wall-clock creation timestamp, existing binary order record,
    and CRC32. An incomplete final record is truncated during startup recovery;
    a complete but invalid record fails closed.
    """

    def __init__(self, path: str | Path, run_id: str | None = None) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self.records: list[JournalRecord] = []
        self.by_order_id: dict[int, JournalRecord] = {}

        if self.path.exists():
            self.run_id, self.records = self._read(repair_trailing=True)
            if run_id is not None and run_id != self.run_id:
                raise ValueError(
                    "Provided run_id does not match the existing order journal"
                )
        else:
            self.run_id = run_id or uuid.uuid4().hex
            try:
                run_bytes = uuid.UUID(self.run_id).bytes
            except ValueError as exc:
                raise ValueError("run_id must be a UUID hex string") from exc
            with self.path.open("xb") as stream:
                stream.write(_FILE_MAGIC + run_bytes)
                stream.flush()
                os.fsync(stream.fileno())

        self.by_order_id = {
            record.order.order_id: record for record in self.records
        }

    def _read(self, *, repair_trailing: bool) -> tuple[str, list[JournalRecord]]:
        with self.path.open("rb+") as stream:
            header = stream.read(_HEADER_SIZE)
            if len(header) != _HEADER_SIZE or header[:8] != _FILE_MAGIC:
                raise JournalCorruptionError("Invalid order journal header")
            run_id = uuid.UUID(bytes=header[8:24]).hex
            records: list[JournalRecord] = []
            seen_ids: set[int] = set()
            valid_end = _HEADER_SIZE
            expected_sequence = 1

            while True:
                raw = stream.read(_RECORD.size)
                if not raw:
                    break
                if len(raw) != _RECORD.size:
                    if not repair_trailing:
                        break
                    stream.truncate(valid_end)
                    stream.flush()
                    os.fsync(stream.fileno())
                    break

                magic, sequence, timestamp_ns, encoded_order, checksum = (
                    _RECORD.unpack(raw)
                )
                expected_crc = zlib.crc32(raw[:-4]) & 0xFFFFFFFF
                if magic != _RECORD_MAGIC or checksum != expected_crc:
                    raise JournalCorruptionError(
                        f"Corrupt complete journal record at sequence "
                        f"{expected_sequence}"
                    )
                if sequence != expected_sequence:
                    raise JournalCorruptionError(
                        f"Expected journal sequence {expected_sequence}, "
                        f"found {sequence}"
                    )
                order = decode_order(encoded_order)
                if order.order_id in seen_ids:
                    raise JournalCorruptionError(
                        f"Duplicate journal order ID {order.order_id}"
                    )
                records.append(JournalRecord(sequence, timestamp_ns, order))
                seen_ids.add(order.order_id)
                expected_sequence += 1
                valid_end += _RECORD.size

            return run_id, records

    def refresh(self) -> list[JournalRecord]:
        """Read committed records without truncating a concurrent partial tail."""
        _run_id, records = self._read(repair_trailing=False)
        if _run_id != self.run_id:
            raise JournalCorruptionError("Order journal run ID changed")
        self.records = records
        self.by_order_id = {
            record.order.order_id: record for record in records
        }
        return records

    def ensure_order(self, order: Order) -> JournalRecord:
        """Return an existing matching record or durably append a new order."""
        with self._lock:
            current = self.by_order_id.get(order.order_id)
            if current is not None:
                if current.order != order:
                    raise ValueError(
                        f"Order ID {order.order_id} conflicts with journal"
                    )
                return current

            sequence = len(self.records) + 1
            timestamp_ns = time.time_ns()
            prefix = _RECORD.pack(
                _RECORD_MAGIC,
                sequence,
                timestamp_ns,
                encode_order(order),
                0,
            )[:-4]
            raw = prefix + struct.pack("<I", zlib.crc32(prefix) & 0xFFFFFFFF)
            with self.path.open("ab") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())

            record = JournalRecord(sequence, timestamp_ns, order)
            self.records.append(record)
            self.by_order_id[order.order_id] = record
            return record

    def close(self) -> None:
        """Included for symmetry; journal writes do not retain open handles."""
