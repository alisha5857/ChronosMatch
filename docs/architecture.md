
# ChronosMatch — System Architecture

## 1. Overview

ChronosMatch is a low-latency trading-engine prototype designed to explore binary data protocols, shared-memory communication, price-time priority matching, and performance measurement.

The system uses Python for orchestration and simulation, a fixed-size binary protocol for orders, memory-mapped files for inter-process communication, and Cython for performance-critical matching logic.

The target of under 50 microseconds is an engineering goal to investigate, not a guaranteed result.

## 2. High-Level Architecture

```text
             Market Simulator
                    |
                    v
             Order Generator
                    |
                    v
          Binary Order Protocol
                    |
                    v
          Shared-Memory Ring Buffer
                    |
                    v
             Engine Worker
                    |
                    v
             Matching Engine
                    |
             +------+------+
             |             |
             v             v
     In-Memory Trades   Worker Summary
                           |
                           v
                   Simulation Result

       TradeLedger (SQLite) and Metrics/Dashboard
                 are separate components
```

## 3. Main Components

### 3.1 Order Protocol

Location: `src/chronosmatch/ipc/order_protocol.py`

Converts orders into fixed-size binary records using Python's `struct` module.

Responsibilities:
- Encode order fields into bytes.
- Decode binary records.
- Validate order IDs, prices, quantities, and sides.
- Maintain a consistent record format between processes.

### 3.2 Ring Buffer

Location: `src/chronosmatch/ipc/ring_buffer.py`

Provides a bounded shared-memory communication mechanism.

Responsibilities:
- Store fixed-size order records.
- Track read and write positions.
- Handle full and empty buffer conditions.
- Support producer and consumer workflows.

A correct multi-process implementation must also handle synchronization and memory visibility. Shared memory alone does not make a ring buffer lock-free or race-free. The ring buffer remains volatile; durable acceptance and replay are provided by the separate order journal when enabled.

### 3.2.1 Durable Order Journal

Location: `src/chronosmatch/storage/order_journal.py`

With `--trades-db`, the simulator creates a sidecar order journal by default.
`--journal` can select a journal explicitly, including when trade persistence is
disabled. Every order record is appended and `fsync`ed before its unchanged
32-byte binary record is published to the shared-memory ring. Reusing the same
journal resumes the stable run ID stored in its header; regenerated orders must
match the existing order-ID prefix.

The file begins with a 24-byte header: eight-byte `CHRJNL01` magic followed by
the run ID as 16 UUID bytes. Each subsequent fixed 60-byte record uses the
little-endian layout `<8sQQ32sI`: `ORDR0001` magic, 64-bit one-based journal
sequence, 64-bit Unix-nanosecond timestamp, the existing 32-byte encoded order,
and CRC32 over the preceding 56 bytes. A short final record is truncated back
to the previous complete boundary at startup; an invalid complete record,
checksum, sequence, or duplicate order ID stops recovery rather than accepting
corrupt data.

The journal is an append-only replay source, not a snapshot. Recovery rebuilds
the order book by replaying journal orders in sequence and regenerates stable
trade IDs from that deterministic replay. Orders with no resting quantity are
still journaled and replayed, so trade sequence and duplicate-order state are
reconstructed as well.

### 3.3 Matching Engine

Location: `src/chronosmatch/engine/`

Processes buy and sell orders according to price-time priority.

Responsibilities:
- Maintain bid and ask order books.
- Match compatible orders.
- Support partial fills and remaining quantities.
- Record completed trades.
- Preserve deterministic order-processing behavior.

### 3.4 Cython Core

Location: `src/chronosmatch/engine/matching_core.pyx`

Contains performance-critical matching operations implemented using Cython.

Static C types and suitable data structures may reduce Python overhead. Performance improvements must be measured against the Python implementation.

### 3.5 Market Simulator

Location: `src/chronosmatch/simulator/`

Generates reproducible mock market activity and feeds orders to the engine
through the shared-memory ring buffer. `MarketSimulator.run_async()` streams
orders to the worker without blocking the event loop; `run()` is a synchronous
wrapper for command-line callers. The simulator creates the buffer, starts
`scripts/run_engine_worker.py`, and writes encoded order records; the worker
decodes each record and passes the resulting order to `MatchingEngine`. The
producer waits asynchronously when the bounded buffer is full. An optional
order rate paces generation, and an optional duration (which requires a rate)
limits the finite order count to the lesser of the order cap and the floored
rate-duration product. For a finite simulation, the worker returns
order/trade counts and final best prices as a JSON summary, then the producer
reaps the worker and releases the shared memory and per-run lock file.

When `--events-file` is supplied, the worker writes timestamped JSONL events
(`worker_started`, accepted/rejected orders, book updates, executions, and
`worker_shutdown`) through a bounded background writer queue. Matching and
dashboard rendering remain in separate threads/processes. To watch a simulation,
start it with:

```powershell
.\.venv\Scripts\python.exe scripts\run_simulation.py --orders 100000 --events-file data\run.events.jsonl
```

Then launch the dashboard in another terminal:

```powershell
.\.venv\Scripts\python.exe scripts\run_dashboard.py --events-file data\run.events.jsonl
```

Dashboard metrics include the current top of book,
spread, processed/accepted counts, worker throughput, latest measured
order-processing duration, and recent actual trades. `timestamp_ns` is a wall
clock timestamp; `latency_ns` is measured within the worker using its monotonic
clock and is a duration, not a cross-process clock value.

The matching engine retains its trade history in memory. With
`--trades-db data\trades.sqlite`, the worker forwards each actual
`trade_executed` event to a bounded asynchronous persister. Its queue holds at
most 1,024 waiting events; when full, submission applies backpressure until a
slot is available rather than dropping a trade. A dedicated thread batches up
to 64 rows in one SQLite transaction and flushes partial batches every 50 ms
and during shutdown. Persistence errors fail the worker and are reported to the
producer. Trades are keyed by `(run_id, trade_id)`, making identical redelivery
idempotent while allowing engine trade IDs to restart in later runs. Event
timestamps are stored as Unix nanoseconds. Existing ledger databases are
migrated while preserving existing rows.

The persister queue is bounded and in memory. In journal mode, one SQLite
transaction writes a contiguous batch of trade rows and advances the run's
processed journal sequence. SQLite uses `synchronous=FULL`; an order is never
checkpointed before its trades are in the same committed transaction. Worker
startup rebuilds engine state from the full journal, checks the SQLite
checkpoint, and asynchronously persists every uncheckpointed order result
before it accepts new work. A crash before commit therefore causes deterministic
replay; a crash after commit sees both trades and checkpoint. Replayed trade
events for uncommitted work may be delivered again; consumers should deduplicate
by `(run_id, trade_id)`. Historical trades already committed are not republished
on restart.

An orderly worker shutdown drains and commits queued work. A worker-process
crash leaves the journal available for restart and SQLite rolls back an
interrupted transaction; restart recovery is verified by process-level crash
injection tests. A machine/power-loss guarantee is not claimed: the journal
calls `fsync` and SQLite requests `synchronous=FULL`, but storage-device,
filesystem, and OS durability under real power loss have not been tested.
The journal and database must both remain available and paired with the same
run ID. Deleting either, changing the journal contents, or restoring them from
different points can prevent correct recovery. Event JSONL output is not a
durable outbox and is not transactionally coupled to SQLite.

To resume a deterministic simulation, rerun it with the same journal, trade
database, order count, seed, and generator settings, while choosing a new
events-file path:

```powershell
.\.venv-1\Scripts\python.exe scripts\run_simulation.py --orders 100 --seed 42 --journal data\run.orders.journal --trades-db data\trades.sqlite --events-file data\run-restart.events.jsonl
```

The producer verifies and reuses matching journaled order IDs, then republishes
the generated stream. The restarted worker reconstructs state and skips IPC
records already represented by the journal. Live consumers deduplicate trade
notifications by `(run_id, trade_id)`; event JSONL files themselves are created
exclusively and should be unique per worker attempt.

Example simulation with both live events and persisted trades:

```powershell
.\.venv\Scripts\python.exe scripts\run_simulation.py --orders 100 --events-file data\run.events.jsonl --trades-db data\trades.sqlite
```

### 3.6 Metrics

Location: `src/chronosmatch/metrics/`

Collects:
- Order-processing latency.
- Throughput in orders per second.
- Latency percentiles.
- Benchmark timing data.

### 3.7 Trade Storage

Location: `src/chronosmatch/storage/trade_ledger.py`

Provides SQLite persistence for completed trades and subsequent analysis.
`AsyncTradePersister` consumes actual worker execution events and uses
`TradeLedger.append_many()` to commit bounded batches outside the matching
engine's database I/O path.

### 3.8 Terminal Dashboard

Location: `src/chronosmatch/dashboard/terminal_dashboard.py`

Consumes the worker's JSONL event stream and displays live engine activity,
throughput, matching duration, best bid/ask, spread, and actual trade events.
It renders a text fallback that does not require `curses`.

### 3.9 End-to-End Benchmark

Run the persistence comparison with the repository's existing Windows
interpreter:

```powershell
.\.venv-1\Scripts\python.exe benchmarks\benchmark_end_to_end.py --orders 10000 --seed 42 --warmup-count 1000 --repetitions 3 --database-path benchmark-results\comparison.sqlite --output benchmark-results\comparison.json
```

The benchmark sends the same deterministic binary order workload to a separate
worker process in each mode. It measures persistence disabled and enabled with
the same JSONL event instrumentation, alternating mode order across
repetitions. It writes fresh isolated SQLite files named from the configured
database-path stem; those generated files must not already exist. Database
schema setup, worker startup, warm-up, reconciliation, and cleanup are outside
the measured duration. The measured duration starts immediately before the
first publication and ends after worker exit, so publication backpressure,
event delivery, and (when enabled) the final persistence flush are included.

The report includes per-repetition raw latency samples, nearest-rank p50/p95/p99
and maximum order latencies, engine processing durations, outcome counts,
executed quantity, throughput, and persistence flush/reconciliation data.
Aggregate values are median/minimum/maximum across successful independent
repetitions; a failed mode or repetition is explicitly marked failed and is
not represented as a zero-valued measurement. Cross-process order latency uses
`perf_counter_ns()` in both processes and is only reported if the observed
timestamps are comparable. Results are machine- and workload-dependent and do
not establish a throughput or matching-latency target.

## 4. Technology Stack

| Technology | Purpose |
|---|---|
| Python | Simulation, orchestration, testing |
| struct | Binary order encoding |
| mmap | Memory-mapped communication |
| Cython | Compiled performance-critical logic |
| asyncio | Asynchronous simulation |
| SQLite | Trade persistence |
| pytest | Automated tests |
| perf_counter_ns | High-resolution elapsed-time measurement |

## 5. Performance Strategy

The project investigates:
1. Fixed-size binary records instead of JSON or pickle for order transport.
2. Shared-memory communication instead of repeatedly serializing objects between processes.
3. Efficient order-book data structures.
4. Cython for selected hot paths.
5. Batching and reduced unnecessary allocations.
6. Latency and throughput measurement under repeatable workloads.

Python's garbage collector is only one possible source of latency variation. Scheduling, operating-system activity, allocation patterns, synchronization, and CPU behavior can also affect results.

## 6. Reliability and Limitations

- Orders must be validated before processing.
- Ring-buffer overflow must be handled explicitly.
- Producer and consumer synchronization must be correct.
- Trade records must be persisted consistently.
- Performance measurements must distinguish warm-up from measured runs.
- Results depend on the hardware, operating system, workload, and implementation.

This is an experimental educational engine, not a production exchange or a certified high-frequency trading system.

## 7. Repository Structure

```text
benchmarks/             Performance benchmark scripts
docs/                   Architecture and project documentation
scripts/                Application entry points
src/chronosmatch/
    dashboard/          Terminal monitoring
    engine/              Matching engine and Cython core
    ipc/                 Binary protocol and ring buffer
    metrics/             Latency and throughput
    simulator/           Mock market activity
    storage/             Trade persistence
tests/                  Automated tests
```
