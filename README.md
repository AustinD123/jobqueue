# jobqueue

A durable, at-least-once job queue engine written in C++20, backed directly by
SQLite (WAL mode) instead of an external broker like Redis or RabbitMQ.

Lease-based delivery — the same core mechanism as SQS visibility timeouts and
Faktory's broker model — implemented over SQLite's write-ahead log for
single-writer durability instead of a distributed store.

## State machine

```
        enqueue
           |
           v
        [ready] <------------------.
           |                       |
     claim | (lease starts)        | lease expires (reaper) OR nack
           v                       |     [attempts < max]
        [leased] --------->--------'
           |
     ack   |                nack / lease expires [attempts >= max]
           v                       |
        [done]                  [dead]  (DLQ)
```

## Why SQLite for a job queue

- **`journal_mode=WAL`** — writers append to a separate write-ahead log
  instead of the main DB file, so readers and a single writer don't block
  each other on file locks.
- **`synchronous=FULL`** — every commit's `fsync` happens before the call
  returns, not just at checkpoints. "`enqueue()` returned" and "the job is
  on durable storage" are the same statement.
- **`claim()` is a single `UPDATE ... RETURNING`** — SQLite serializes
  writers at the connection/file level, so selecting a ready row and
  marking it leased happen inside one indivisible statement. There is no
  window where two callers can both observe the same row as claimable,
  which is what makes concurrent `claim()` calls safe without any
  application-level locking.

## How this compares to real systems

| System | Storage | Claim mechanism | Notes |
|---|---|---|---|
| **Sidekiq** | Redis | `BRPOPLPUSH` (atomic move between lists) | No visibility timeout by default in OSS — a crashed worker's job is just gone unless you use Sidekiq Pro. |
| **Faktory** | RocksDB (custom broker, Go) | Broker-mediated lease, same idea as this project | Closest architectural analog — single broker process owns storage, workers talk over TCP. |
| **BullMQ** | Redis | Lua scripts for atomic claim | Atomicity via scripting instead of a single SQL statement — same underlying principle. |
| **AWS SQS** | Proprietary distributed store | Visibility timeout (literally this pattern) | At-least-once, no ordering guarantee on a standard queue. |
| **RabbitMQ** | Erlang/OTP, disk or memory | `ack`/`nack` per consumer, no built-in backoff | This project gets retry+backoff+DLQ out of the box; vanilla RabbitMQ requires dead-letter exchanges + TTLs. |

## Project layout

```
CMakeLists.txt
include/jobqueue/engine.hpp   # public API: Job, JobStatus, RetryPolicy, Engine
src/engine.cpp                # engine implementation over the SQLite C API
tests/engine_tests.cpp        # unit tests
cli/                          # (planned) command-line worker/producer
bench/                        # (planned) throughput/latency benchmarks
```

## Building

Requires a system install of `libsqlite3` (>= 3.35, for `RETURNING`) and a
C++20 compiler.

```sh
cmake -S . -B build
cmake --build build
ctest --test-dir build
```

`Engine` is linked against SQLite via `pkg-config` — no amalgamation is
vendored.

## Status

- [x] **Day 1** — core engine: `enqueue`, `claim`, `ack`, `nack`,
      `reap_expired_leases`, `stats`, all implemented over the SQLite C API
      and manually verified against real (non-mocked) SQLite databases.
- [ ] **Day 2** — concurrency test proving `claim()` is race-free under
      real threads, plus a background-timer lease reaper.
- [ ] Networking, CLI, and benchmarks (`cli/`, `bench/`).
