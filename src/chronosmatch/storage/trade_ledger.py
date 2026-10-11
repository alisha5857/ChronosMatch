"""SQLite-backed persistence for executed trades."""

from __future__ import annotations

from collections.abc import Callable
import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class TradeRecord:
    """A persisted trade record."""

    trade_id: int
    buy_order_id: int
    sell_order_id: int
    price: int
    quantity: int
    timestamp_ns: int = 0
    run_id: str = ""


class TradeLedger:
    """Persist matching engine trades to SQLite."""

    def __init__(self, path: str | Path = "data/trades.db") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.path)
        self._connection.execute("PRAGMA synchronous = FULL")
        with self._connection:
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS trades (
                    trade_id INTEGER PRIMARY KEY,
                    buy_order_id INTEGER NOT NULL,
                    sell_order_id INTEGER NOT NULL,
                    price INTEGER NOT NULL,
                    quantity INTEGER NOT NULL,
                    timestamp_ns INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            columns = {
                row[1]
                for row in self._connection.execute(
                    "PRAGMA table_info(trades)"
                )
            }
            if "timestamp_ns" not in columns:
                self._connection.execute(
                    "ALTER TABLE trades ADD COLUMN timestamp_ns "
                    "INTEGER NOT NULL DEFAULT 0"
                )
            if "run_id" not in columns:
                self._connection.execute(
                    "ALTER TABLE trades ADD COLUMN run_id "
                    "TEXT NOT NULL DEFAULT ''"
                )
            trade_id_is_primary = any(
                row[1] == "trade_id" and row[5] == 1
                for row in self._connection.execute(
                    "PRAGMA table_info(trades)"
                )
            )
            if trade_id_is_primary:
                self._connection.execute(
                    """
                    CREATE TABLE trades_migrated (
                        run_id TEXT NOT NULL,
                        trade_id INTEGER NOT NULL,
                        buy_order_id INTEGER NOT NULL,
                        sell_order_id INTEGER NOT NULL,
                        price INTEGER NOT NULL,
                        quantity INTEGER NOT NULL,
                        timestamp_ns INTEGER NOT NULL DEFAULT 0,
                        PRIMARY KEY (run_id, trade_id)
                    )
                    """
                )
                self._connection.execute(
                    """
                    INSERT INTO trades_migrated (
                        run_id, trade_id, buy_order_id, sell_order_id,
                        price, quantity, timestamp_ns
                    )
                    SELECT run_id, trade_id, buy_order_id, sell_order_id,
                           price, quantity, timestamp_ns
                    FROM trades
                    """
                )
                self._connection.execute("DROP TABLE trades")
                self._connection.execute(
                    "ALTER TABLE trades_migrated RENAME TO trades"
                )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_orders (
                    run_id TEXT PRIMARY KEY,
                    journal_sequence INTEGER NOT NULL
                )
                """
            )

    def append(self, trade: TradeRecord) -> None:
        self.append_many([trade])

    def append_many(self, trades: list[TradeRecord]) -> None:
        """Atomically insert a batch, accepting identical duplicate deliveries."""
        if not trades:
            return

        try:
            with self._connection:
                for trade in trades:
                    self._insert_trade(trade)
        except Exception:
            raise

    def _insert_trade(self, trade: TradeRecord) -> None:
        self._connection.execute(
            """
            INSERT INTO trades (
                run_id, trade_id, buy_order_id, sell_order_id,
                price, quantity, timestamp_ns
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, trade_id) DO NOTHING
            """,
            (
                trade.run_id,
                trade.trade_id,
                trade.buy_order_id,
                trade.sell_order_id,
                trade.price,
                trade.quantity,
                trade.timestamp_ns,
            ),
        )
        stored = self._connection.execute(
            """
            SELECT buy_order_id, sell_order_id, price, quantity, timestamp_ns
            FROM trades WHERE run_id = ? AND trade_id = ?
            """,
            (trade.run_id, trade.trade_id),
        ).fetchone()
        expected = (
            trade.buy_order_id,
            trade.sell_order_id,
            trade.price,
            trade.quantity,
            trade.timestamp_ns,
        )
        if stored != expected:
            raise ValueError(
                "Trade ID conflicts with a different stored trade: "
                f"{trade.trade_id}"
            )

    def processed_sequence(self, run_id: str) -> int:
        row = self._connection.execute(
            "SELECT journal_sequence FROM processed_orders WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        return int(row[0]) if row is not None else 0

    def fetch_run(self, run_id: str) -> list[TradeRecord]:
        """Return one run's persisted trades for recovery reconciliation."""
        rows = self._connection.execute(
            """
            SELECT trade_id, buy_order_id, sell_order_id, price, quantity,
                   timestamp_ns, run_id
            FROM trades WHERE run_id = ? ORDER BY trade_id
            """,
            (run_id,),
        ).fetchall()
        return [
            TradeRecord(
                trade_id=row[0],
                buy_order_id=row[1],
                sell_order_id=row[2],
                price=row[3],
                quantity=row[4],
                timestamp_ns=row[5],
                run_id=row[6],
            )
            for row in rows
        ]

    def append_order_results(
        self,
        run_id: str,
        results: list[tuple[int, list[TradeRecord]]],
        *,
        crash_hook: Callable[[str], None] | None = None,
    ) -> None:
        """Atomically commit contiguous order results and their checkpoint."""
        if not results:
            return
        with self._connection:
            checkpoint = self.processed_sequence(run_id)
            for sequence, trades in results:
                if sequence != checkpoint + 1:
                    raise ValueError(
                        f"Non-contiguous journal checkpoint: expected "
                        f"{checkpoint + 1}, got {sequence}"
                    )
                if any(trade.run_id != run_id for trade in trades):
                    raise ValueError("Trade run_id does not match checkpoint")
                for trade in trades:
                    self._insert_trade(trade)
                if crash_hook is not None:
                    crash_hook("after_trade_rows_before_checkpoint")
                self._connection.execute(
                    """
                    INSERT INTO processed_orders (run_id, journal_sequence)
                    VALUES (?, ?)
                    ON CONFLICT(run_id) DO UPDATE SET
                        journal_sequence = excluded.journal_sequence
                    """,
                    (run_id, sequence),
                )
                checkpoint = sequence
            if crash_hook is not None:
                crash_hook("before_transaction_commit")
        if crash_hook is not None:
            crash_hook("after_transaction_commit")

    def fetch_all(self) -> list[TradeRecord]:
        rows = self._connection.execute(
            """
            SELECT trade_id, buy_order_id, sell_order_id, price, quantity,
                   timestamp_ns, run_id
            FROM trades ORDER BY run_id, trade_id
            """
        ).fetchall()
        return [
            TradeRecord(
                trade_id=row[0],
                buy_order_id=row[1],
                sell_order_id=row[2],
                price=row[3],
                quantity=row[4],
                timestamp_ns=row[5],
                run_id=row[6],
            )
            for row in rows
        ]

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> "TradeLedger":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()
