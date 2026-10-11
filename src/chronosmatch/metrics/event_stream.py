"""File-backed worker events for dashboards and monitoring tools."""

from __future__ import annotations

import json
from pathlib import Path
import queue
import threading
from typing import Any


class EventStreamWriter:
    """Write worker events off-thread through a bounded queue."""

    def __init__(self, path: str | Path, capacity: int = 4096) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("x", encoding="utf-8"):
            pass
        self._queue: queue.Queue[dict[str, Any] | None] = queue.Queue(
            maxsize=capacity
        )
        self._error: Exception | None = None
        self._thread = threading.Thread(
            target=self._write_events,
            name="chronosmatch-event-writer",
            daemon=True,
        )
        self._thread.start()

    def emit(self, event: dict[str, Any]) -> None:
        if self._error is not None:
            raise RuntimeError("Worker event stream failed") from self._error
        try:
            self._queue.put_nowait(event)
        except queue.Full as exc:
            raise RuntimeError(
                "Worker event queue is full; refusing to block matching"
            ) from exc
        if self._error is not None:
            raise RuntimeError("Worker event stream failed") from self._error

    def close(self) -> None:
        self._queue.put(None)
        self._thread.join()
        if self._error is not None:
            raise RuntimeError("Worker event stream failed") from self._error

    def _write_events(self) -> None:
        try:
            with self.path.open("a", encoding="utf-8") as stream:
                while True:
                    event = self._queue.get()
                    try:
                        if event is None:
                            return
                        stream.write(json.dumps(event, separators=(",", ":")))
                        stream.write("\n")
                        stream.flush()
                    finally:
                        self._queue.task_done()
        except Exception as exc:
            self._error = exc
            while True:
                try:
                    self._queue.get_nowait()
                except queue.Empty:
                    return
                else:
                    self._queue.task_done()


class EventFileReader:
    """Incrementally read complete JSONL events from a worker event file."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._offset = 0
        self._pending = ""

    def poll(self) -> list[dict[str, Any]]:
        """Return newly appended events, preserving incomplete trailing lines."""
        try:
            with self.path.open("r", encoding="utf-8") as stream:
                stream.seek(self._offset)
                chunk = stream.read()
                self._offset = stream.tell()
        except FileNotFoundError:
            return []

        lines = (self._pending + chunk).splitlines(keepends=True)
        self._pending = ""
        events = []
        for index, line in enumerate(lines):
            if not line.endswith(("\n", "\r")):
                self._pending = line
                break
            event = json.loads(line)
            if not isinstance(event, dict):
                raise ValueError("Worker event must be a JSON object")
            events.append(event)
        return events
