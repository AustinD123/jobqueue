#!/usr/bin/env python3
"""Crash-resilience harness for jobqueue.

Starts a broker on a database under ~/bench -- deliberately NOT under
/mnt/c. WSL's /mnt/c is backed by a 9P mount to the Windows filesystem,
which does not give SQLite the same POSIX file-locking/fsync guarantees
as a real Linux filesystem; running the crash test against a /mnt/c db
would be testing WSL's DrvFs layer, not WAL + synchronous=FULL. ~/bench
is on the WSL VM's own native ext4 disk.

Starts 4 workers and a producer that enqueues at a fixed rate, logging
every acknowledged (`"ok":true`) enqueue's job id to a ledger file. Every
2-8 seconds (random), the broker is killed with SIGKILL -- not a clean
shutdown -- and restarted against the SAME db file, to actually exercise
crash recovery rather than graceful-shutdown durability. Runs for 60s,
then stops the producer, gives the still-running workers a chance to
drain the queue, and stops everything.

verify() is intentionally left as a stub -- the actual ledger-vs-database
consistency check (what counts as a violation) is a separate exercise.

Usage:
    python3 tools/crash_test.py

Requires `broker` and `worker` already built (cmake --build build_linux).
"""
import json
import random
import subprocess
import sys
import threading
import time
from pathlib import Path

from broker_client import send_line

REPO_ROOT = Path(__file__).resolve().parent.parent
BROKER_BIN = REPO_ROOT / "build_linux" / "broker"
WORKER_BIN = REPO_ROOT / "build_linux" / "worker"

BENCH_DIR = Path.home() / "bench"  # WSL-native filesystem, NOT /mnt/c
DB_PATH = BENCH_DIR / "crash_test.db"
LEDGER_PATH = BENCH_DIR / "ledger.txt"

PORT = 9500
QUEUE = "default"
NUM_WORKERS = 4
RUN_SECONDS = 60
KILL_INTERVAL_MIN_S = 2.0
KILL_INTERVAL_MAX_S = 8.0
PRODUCER_RATE_PER_S = 20
DRAIN_TIMEOUT_S = 30
DRAIN_POLL_INTERVAL_S = 0.5


def start_broker() -> subprocess.Popen:
    proc = subprocess.Popen(
        [str(BROKER_BIN), str(PORT), str(DB_PATH)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    time.sleep(0.5)  # give it a moment to bind before anyone connects
    return proc


def start_workers(n: int) -> list[subprocess.Popen]:
    return [
        subprocess.Popen(
            [str(WORKER_BIN), "--host", "127.0.0.1", "--port", str(PORT),
             "--queue", QUEUE, "--job-ms", "20", "--lease-ms", "10000"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        for _ in range(n)
    ]


def producer_loop(stop_event: threading.Event, ledger_file) -> None:
    i = 0
    interval = 1.0 / PRODUCER_RATE_PER_S
    while not stop_event.is_set():
        i += 1
        req = json.dumps({"cmd": "enqueue", "queue": QUEUE,
                           "payload": f"crash-job-{i}", "priority": 0})
        try:
            resp = json.loads(send_line(PORT, req, timeout=2.0))
            if resp.get("ok"):
                ledger_file.write(f"{resp['id']}\n")
                ledger_file.flush()
        except Exception:
            pass  # broker likely mid-restart -- this enqueue just didn't happen, skip it
        time.sleep(interval)


def wait_for_drain(timeout_s: float = DRAIN_TIMEOUT_S) -> bool:
    """Poll stats until ready==0 and leased==0, or timeout. Returns
    whether it actually drained (vs. timed out still having work left).
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            stats = json.loads(send_line(PORT, json.dumps({"cmd": "stats", "queue": QUEUE})))
            if stats.get("ok") and stats.get("ready") == 0 and stats.get("leased") == 0:
                return True
        except Exception:
            pass  # broker mid-restart -- just keep polling
        time.sleep(DRAIN_POLL_INTERVAL_S)
    return False


def verify(ledger_ids: list[int], db_path: str) -> list[str]:
    """TODO: compare the ledger (job ids the producer was actually told
    "ok":true for) against the database's final state and return a list
    of human-readable violation descriptions. Empty list = no violations
    found. E.g.: a ledger id with no matching row in `jobs` at all would
    mean an acknowledged enqueue was lost across a crash -- that's the
    core property this harness exists to catch. Decide what else counts.
    """
    return []


def main() -> int:
    BENCH_DIR.mkdir(parents=True, exist_ok=True)
    for p in (DB_PATH, Path(str(DB_PATH) + "-wal"), Path(str(DB_PATH) + "-shm"), LEDGER_PATH):
        if p.exists():
            p.unlink()

    print(f"db: {DB_PATH}")
    broker_proc = start_broker()
    worker_procs = start_workers(NUM_WORKERS)
    print(f"broker pid {broker_proc.pid}, {NUM_WORKERS} workers started")

    stop_event = threading.Event()
    ledger_file = open(LEDGER_PATH, "a")
    producer_thread = threading.Thread(target=producer_loop, args=(stop_event, ledger_file),
                                        daemon=True)
    producer_thread.start()

    start = time.monotonic()
    next_kill = start + random.uniform(KILL_INTERVAL_MIN_S, KILL_INTERVAL_MAX_S)
    kill_count = 0

    while time.monotonic() - start < RUN_SECONDS:
        if time.monotonic() >= next_kill:
            elapsed = time.monotonic() - start
            print(f"[{elapsed:5.1f}s] SIGKILL broker (pid {broker_proc.pid})")
            broker_proc.kill()
            broker_proc.wait()
            broker_proc = start_broker()
            kill_count += 1
            print(f"[{elapsed:5.1f}s] broker restarted, pid {broker_proc.pid}")
            next_kill = time.monotonic() + random.uniform(KILL_INTERVAL_MIN_S, KILL_INTERVAL_MAX_S)
        time.sleep(0.1)

    print(f"\n{RUN_SECONDS}s elapsed, {kill_count} broker restart(s). Stopping producer...")
    stop_event.set()
    producer_thread.join(timeout=5)
    ledger_file.close()

    print("waiting for workers to drain the queue...")
    drained = wait_for_drain()
    print("drained cleanly" if drained else f"did not drain within {DRAIN_TIMEOUT_S}s")

    for p in worker_procs:
        p.kill()
    broker_proc.kill()
    for p in worker_procs:
        p.wait()
    broker_proc.wait()

    ledger_ids = [int(line) for line in LEDGER_PATH.read_text().splitlines() if line.strip()]
    print(f"\nledger: {len(ledger_ids)} acknowledged enqueues")

    violations = verify(ledger_ids, str(DB_PATH))
    if violations:
        print(f"{len(violations)} VIOLATION(S):")
        for v in violations:
            print(f"  - {v}")
        return 1

    print("verify() reported no violations (note: verify() is still a stub -- fill it in)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
