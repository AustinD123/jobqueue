#!/usr/bin/env python3
"""Benchmark claim() throughput at different queue depths, before/after
replacing idx_claim with two partial indexes.

For each depth in {1000, 10000, 100000}:
  1. Seed `depth` ready jobs directly via sqlite3 (one transaction,
     executemany) into a fresh db under ~/bench (not /mnt/c -- see
     crash_test.py's docstring for why).
  2. Start the broker against that db (current schema: idx_claim only),
     time 1000 claim() calls -> "before".
  3. Stop the broker, drop idx_claim, create idx_ready/idx_leased, run
     EXPLAIN QUERY PLAN on claim()'s inner SELECT and the reaper's
     expired-lease SELECT, and reset the 1000 jobs claimed by "before"
     back to Ready (so "after" starts from the same depth, not depth-1000
     -- otherwise the comparison would be confounded by working-set size,
     not just the index change).
  4. Start the broker again against the SAME (now re-indexed) db, time
     1000 more claim() calls -> "after".
Prints a before/after table at the end.

Usage:
    python3 tools/depth_bench.py
"""
import json
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

from broker_client import PersistentConnection

REPO_ROOT = Path(__file__).resolve().parent.parent
BROKER_BIN = REPO_ROOT / "build_linux" / "broker"
BENCH_DIR = Path.home() / "bench"

PORT = 9600
QUEUE = "default"
DEPTHS = [1_000, 10_000, 100_000]
CLAIMS_PER_DEPTH = 1000

# Matches Engine's constructor schema exactly (src/engine.cpp).
SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY, queue TEXT, payload TEXT, priority INTEGER,
    status INTEGER, attempts INTEGER, lease_expires_at INTEGER,
    run_after INTEGER, created_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_claim ON jobs(queue, status, run_after, priority);
"""

PARTIAL_INDEXES_SQL = """
DROP INDEX IF EXISTS idx_claim;
CREATE INDEX IF NOT EXISTS idx_ready
  ON jobs(queue, priority DESC, id, run_after) WHERE status = 0;
CREATE INDEX IF NOT EXISTS idx_leased
  ON jobs(lease_expires_at) WHERE status = 1;
"""

CLAIM_QUERY = (
    "SELECT id FROM jobs WHERE queue = 'default' AND status = 0 "
    "AND run_after <= 9999999999999 ORDER BY priority DESC, id ASC LIMIT 1"
)
REAP_QUERY = "SELECT id, attempts FROM jobs WHERE status = 1 AND lease_expires_at < 9999999999999"


def filesystem_type(path: Path) -> str:
    try:
        result = subprocess.run(["findmnt", "-no", "FSTYPE", "--target", str(path)],
                                 capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except Exception as ex:
        return f"unknown ({ex})"


def seed_direct(db_path: Path, n: int) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(SCHEMA_SQL)
        now_ms = int(time.time() * 1000)
        rows = [(QUEUE, f"bench-{i}", i % 5, 0, 0, 0, now_ms, now_ms) for i in range(n)]
        conn.execute("BEGIN")
        conn.executemany(
            "INSERT INTO jobs (queue, payload, priority, status, attempts, "
            "lease_expires_at, run_after, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
    finally:
        conn.close()


def apply_partial_indexes(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executescript(PARTIAL_INDEXES_SQL)
    finally:
        conn.close()


def explain_query_plan(db_path: Path) -> str:
    conn = sqlite3.connect(str(db_path))
    try:
        lines = []
        for label, query in (("claim() inner SELECT", CLAIM_QUERY),
                              ("reaper expired-lease SELECT", REAP_QUERY)):
            lines.append(f"  -- {label} --")
            for row in conn.execute("EXPLAIN QUERY PLAN " + query):
                lines.append(f"  {row}")
        return "\n".join(lines)
    finally:
        conn.close()


def reset_leased_to_ready(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("UPDATE jobs SET status = 0 WHERE status = 1")
        conn.commit()
    finally:
        conn.close()


def start_broker(db_path: Path) -> subprocess.Popen:
    proc = subprocess.Popen([str(BROKER_BIN), str(PORT), str(db_path)],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(0.5)  # give it a moment to bind before anyone connects
    return proc


def stop_broker(proc: subprocess.Popen) -> None:
    proc.kill()
    proc.wait()


def time_claims(conn: PersistentConnection, n: int) -> tuple[float, int]:
    """Returns (claims/sec, jobs actually claimed) over n claim() calls."""
    start = time.monotonic()
    claimed = 0
    for _ in range(n):
        req = json.dumps({"cmd": "claim", "queue": QUEUE, "lease_ms": 30000})
        resp = json.loads(conn.send_request(req))
        if not resp.get("ok"):
            raise RuntimeError(f"claim failed: {resp}")
        if resp.get("job") is not None:
            claimed += 1
    elapsed = time.monotonic() - start
    rate = n / elapsed if elapsed > 0 else float("inf")
    return rate, claimed


def main() -> int:
    BENCH_DIR.mkdir(parents=True, exist_ok=True)
    print(f"~/bench filesystem: {filesystem_type(BENCH_DIR)}\n")

    results: dict[int, tuple[float, float]] = {}

    for depth in DEPTHS:
        db_path = BENCH_DIR / f"depth_bench_{depth}.db"
        for p in (db_path, Path(str(db_path) + "-wal"), Path(str(db_path) + "-shm")):
            if p.exists():
                p.unlink()

        print(f"=== depth={depth} ===")
        t0 = time.monotonic()
        seed_direct(db_path, depth)
        print(f"seeded {depth} jobs directly via sqlite3 in {time.monotonic() - t0:.2f}s")

        broker_proc = start_broker(db_path)
        with PersistentConnection(PORT) as conn:
            before_rate, before_claimed = time_claims(conn, CLAIMS_PER_DEPTH)
        stop_broker(broker_proc)
        print(f"before (idx_claim):      {before_rate:8.1f} claims/sec "
              f"({before_claimed}/{CLAIMS_PER_DEPTH} claimed)")

        apply_partial_indexes(db_path)
        print(explain_query_plan(db_path))
        reset_leased_to_ready(db_path)

        broker_proc = start_broker(db_path)
        with PersistentConnection(PORT) as conn:
            after_rate, after_claimed = time_claims(conn, CLAIMS_PER_DEPTH)
        stop_broker(broker_proc)
        print(f"after (partial indexes): {after_rate:8.1f} claims/sec "
              f"({after_claimed}/{CLAIMS_PER_DEPTH} claimed)")

        results[depth] = (before_rate, after_rate)
        print()

    print("=== before/after summary ===")
    print(f"{'depth':>8}  {'before':>14}  {'after':>14}  {'speedup':>8}")
    for depth in DEPTHS:
        before_rate, after_rate = results[depth]
        speedup = after_rate / before_rate if before_rate > 0 else float("inf")
        print(f"{depth:>8}  {before_rate:>10.1f}/s  {after_rate:>10.1f}/s  {speedup:>7.2f}x")

    return 0


if __name__ == "__main__":
    sys.exit(main())
