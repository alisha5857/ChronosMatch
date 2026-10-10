# ChronosMatch — Zero-Copy High-Frequency Trading Engine

**Domain:** FinTech & Low-Latency Systems  
**Project Type:** Python Development | Systems Programming | Performance Engineering  
**Status:** In Development

## Overview

ChronosMatch is a low-latency trading engine designed to simulate high-frequency order processing using Python, Cython, memory-mapped files, and asynchronous programming.

The project explores how shared-memory communication, statically typed Cython code, and efficient data structures can reduce serialization overhead and improve order-processing performance.

It simulates a stream of buy and sell orders, processes them through a price-time-priority matching engine, and monitors execution latency through a real-time terminal dashboard.

**Note:** ChronosMatch is an educational trading simulator, not a production-grade financial trading system. Performance targets are subject to hardware, operating-system, and implementation constraints.

## Objectives

- Implement a memory-mapped ring buffer for inter-process communication (IPC).
- Minimize unnecessary data copying and avoid JSON/Pickle serialization in the IPC data path.
- Develop a Cython-based Limit Order Book (LOB) matching engine.
- Implement price-time-priority order matching and partial order execution.
- Simulate high-throughput market order streams using `asyncio`.
- Measure order-processing latency using `time.perf_counter_ns()`.
- Display order-book activity and latency metrics in a terminal dashboard.
- Persist executed trades to SQLite for auditing and analysis.

## Key Features

### 1. Zero-Copy IPC Bus

Uses Python's `mmap` and `struct` modules to create a memory-mapped ring buffer for transferring fixed-format order records between processes.

Features include:
- Fixed-size binary order records.
- Ring buffer read/write operations.
- Reduced serialization and allocation overhead.
- Buffer-capacity and overflow handling.
- Producer-consumer coordination.

**Implementation note:** Memory mapping enables shared access to mapped data, but true zero-copy behavior depends on the complete data path. Synchronization, memory ordering, and concurrent access must be handled explicitly.

### 2. Cython Matching Engine

Implements a Limit Order Book using Cython and C-compatible data types to reduce Python object overhead in performance-critical sections.

Features include:
- Buy and sell order management.
- Price-time-priority matching.
- Partial and complete order fills.
- Order cancellation and validation.
- Trade execution records.

### 3. Asynchronous Market Simulator

Uses Python's `asyncio` to generate synthetic market activity and feed orders into the matching engine.

Features include:
- Configurable order volume and arrival rates.
- Synthetic buy and sell orders.
- Variable prices and quantities.
- Burst-load simulation.
- Producer-consumer throughput measurements.

The simulator uses synthetic data by default and does not connect to a live exchange.

### 4. Real-Time Latency Dashboard

Uses Python's `curses` module to display trading activity in a terminal interface.

Dashboard metrics include:
- Best bid and best ask.
- Bid-ask spread.
- Orders processed.
- Trades executed.
- Average and percentile latency.
- Throughput in orders per second.
- Ring buffer utilization.

### 5. Trade Persistence and Auditing

Uses SQLite to persist executed trades and support basic audit queries.

Features include:
- Trade IDs and order references.
- Execution prices and quantities.
- Execution timestamps.
- Historical trade retrieval.
- Background persistence and error handling.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Application logic and market simulation |
| Cython | Compiled matching engine |
| `mmap` | Memory-mapped inter-process communication |
| `struct` | Binary order serialization |
| `asyncio` | Asynchronous order generation |
| `curses` | Terminal-based monitoring |
| SQLite | Trade persistence and auditing |
| `pytest` | Automated testing |
| `time.perf_counter_ns()` | High-resolution duration measurement |
| Git and GitHub | Version control and collaboration |

## Architecture

```text
Synthetic Market Simulator
        |
        v
 Async Order Producer
        |
        v
Memory-Mapped Ring Buffer
        |
        v
Cython Matching Engine
        |
        +------> Limit Order Book
        |              |
        |              v
        |        Matched Trades
        |              |
        v              v
Latency Metrics     SQLite Ledger
        |
        v
Terminal Dashboard
```

## Order Matching Strategy

ChronosMatch follows **price-time priority**:

1. Better-priced orders receive priority.
2. Orders at the same price are processed in arrival order.
3. Compatible buy and sell orders execute when their prices cross.
4. Partially filled orders retain their remaining quantities.
5. Completed or cancelled orders are removed from the active book.

For example, a buy order for 100 shares at ₹150 can match a sell order for 60 shares at ₹148. The engine executes 60 shares at the price defined by its execution policy, leaving 40 shares in the buy order if no other compatible sell orders are available.

## Performance Evaluation

ChronosMatch evaluates performance using repeatable benchmarks rather than assumed speedup figures.

| Metric | Measurement |
|---|---|
| Order throughput | Orders processed per second |
| Processing latency | Time spent processing an order |
| End-to-end latency | Order publication to execution or processing completion |
| P50 latency | Median observed latency |
| P95 latency | Latency below which 95% of observations fall |
| P99 latency | Latency below which 99% of observations fall |
| IPC throughput | Orders transferred per second |
| Memory utilization | Ring buffer and process memory usage |
| Matching correctness | Expected versus actual executions |

Benchmark reports should record the operating system, CPU, Python version, compiler settings, workload, warm-up procedure, and number of repetitions.

A target of under 50 microseconds per match and a throughput target of 100,000 orders per second are experimental goals, not guaranteed performance results.

## Project Roadmap

### Week 1 — Memory Mapping and Market Simulation
- Implement the binary ring buffer.
- Define the order record format.
- Develop the synthetic asynchronous producer.
- Validate record integrity and buffer boundaries.
- Establish an initial Python benchmark.

### Week 2 — Matching Engine and Dashboard
- Implement the Limit Order Book.
- Add the Cython matching implementation.
- Test price-time priority and partial fills.
- Create the terminal dashboard.
- Compare baseline and compiled implementations.

### Week 3 — Optimization and Metrics
- Reduce Python object interactions in the critical matching loop.
- Add high-resolution timing instrumentation.
- Benchmark IPC and matching throughput.
- Test burst loads and concurrent producer-consumer behavior.
- Report latency percentiles and correctness results.

### Week 4 — Persistence and Finalization
- Integrate SQLite trade persistence.
- Add graceful shutdown and error recovery.
- Test buffer overflow, cancellation, and edge cases.
- Finalize documentation and performance reports.
- Prepare the project demonstration and presentation.

## Installation

**Prerequisites**
- Python 3.11 or another supported version.
- A C compiler compatible with the selected Python and Cython versions.
- Cython and the project's Python dependencies.

Create and activate a virtual environment:

```bash
python -m venv .venv
```

Windows:

```bat
.venv\Scripts\activate
```

Linux/macOS:

```bash
source .venv/bin/activate
```

Install dependencies:

```bash
python -m pip install --upgrade pip
pip install cython pytest
```

Install the remaining dependencies after adding them to `requirements.txt`.

**Platform note:** The `curses` interface requires additional setup on some Windows environments. A compatible terminal or a Windows-supported curses implementation may be needed.

## Testing

Run the automated test suite:

```bash
pytest -v
```

Tests should cover:
- Ring buffer wraparound and overflow.
- Binary record encoding and decoding.
- Buy/sell matching.
- Price-time priority.
- Partial and complete fills.
- Invalid orders and cancellation.
- Concurrent producer-consumer behavior.
- Trade persistence and recovery.

## Project Structure

```text
ChronosMatch/
├── src/
│   ├── ipc/
│   │   └── ring_buffer.py
│   ├── engine/
│   │   ├── order_book.py
│   │   └── matching_engine.pyx
│   ├── simulator/
│   │   └── market_simulator.py
│   ├── dashboard/
│   │   └── terminal_ui.py
│   ├── storage/
│   │   └── trade_ledger.py
│   └── metrics/
│       └── latency.py
├── tests/
├── benchmarks/
├── docs/
├── requirements.txt
├── README.md
└── setup.py
```

This is the planned structure; modules can be added as their implementations become available.

## Deliverables

- Python source code and Cython extension source.
- Memory-mapped ring buffer implementation.
- Order-matching engine and automated tests.
- Synthetic market simulator.
- Terminal monitoring dashboard.
- Benchmark results and performance analysis.
- Project report and architecture documentation.
- Final presentation and working demonstration.

## Limitations and Future Enhancements

Potential future improvements include:
- Lock-free ring buffer coordination with appropriate memory-ordering guarantees.
- Multi-producer and multi-consumer support.
- More advanced market-order and limit-order handling.
- Improved memory allocation strategies.
- Alternative compiled implementations using C or C++.
- Exchange protocol integration in a separate, controlled environment.
- More comprehensive crash recovery and trade reconciliation.

## Disclaimer

ChronosMatch is an educational software engineering and performance-testing project. It does not execute real financial trades, guarantee trading profitability, or provide financial advice. All market data is synthetic unless explicitly configured otherwise.

## Author

**Kanika Sharma**  
Python Development | Systems Programming | Performance Engineering

**Project:** ChronosMatch — Zero-Copy High-Frequency Trading Engine
