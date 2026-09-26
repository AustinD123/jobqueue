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

The broker (`broker` target, `tools/`) is POSIX-socket code with no
Windows path by design, and is gated behind `if(UNIX)` in
`CMakeLists.txt`. Build and run it on Linux/WSL:

```sh
./build/broker <port> [db_path]
python3 tools/send.py '{"cmd":"enqueue","queue":"default","payload":"hi","priority":0}' <port>
python3 tools/integration_check.py --port <port>          # full lifecycle check
python3 tools/integration_check.py --port <port> --max-attempts 5   # DLQ path
```

## Status

- [x] **Day 1** — core engine: `enqueue`, `claim`, `ack`, `nack`,
      `reap_expired_leases`, `stats`, all implemented over the SQLite C API
      and manually verified against real (non-mocked) SQLite databases.
- [x] **Day 2** — `claim_race_test` proves `claim()` is race-free under
      real threads (verified clean at 2/8/32 threads); `LeaseReaper` runs
      `reap_expired_leases()` on a background, interruptibly-sleeping
      thread. Also found and fixed two real bugs this surfaced: a pragma
      ordering issue causing intermittent "database is locked" under
      concurrent connection setup, and duplicated Dead-vs-Ready+backoff
      logic between `nack()` and `reap_expired_leases()` (extracted into
      `transition_after_failure()`).
- [x] **Day 3** — `Broker`: newline-delimited JSON over TCP, one
      connection-per-thread `Engine`, `dispatch()` routing
      enqueue/claim/ack/nack/stats. `tools/send.py` for manual protocol
      testing, `tools/integration_check.py` for an automated
      enqueue→claim→ack/nack→stats lifecycle check (including the DLQ
      path) against a running broker — verified 20/20 passing over real
      TCP connections.
- [ ] CLI worker/producer client, benchmarks (`cli/`, `bench/`).
