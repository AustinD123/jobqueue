#!/usr/bin/env python3
"""Benchmark suite for the dockerized broker. Runs on the host, drives
containers via the docker CLI.

Every run gets a fresh named volume and a fresh broker container, so no run
inherits another's WAL, page cache warmth, or leftover rows.

Measurement code (stats sampling, config B's clients) runs INSIDE the
docker network in a python:3.12-slim container, not on the host -- so the
numbers don't include Docker Desktop's host->VM port forwarding. This same
file is that container's entry point (the `_seed` / `_measure`
subcommands); the repo is bind-mounted read-only at /repo.

Configs:
  A  throughput vs worker --concurrency {1,2,4,8,16,32} x sync {FULL,NORMAL}.
     A worker container drains a pre-seeded backlog (--job-ms 0, so this is
     queue overhead, not simulated work). jobs/sec = delta(done) over the
     measurement window. p50/p99 are blank: stats gives counts, not
     per-job latency.
  B  enqueue+claim round-trip latency, 8 closed-loop clients, both sync
     modes, no workers. Each client uses its own queue so the claim always
     returns the job it just enqueued; the ack after each round trip is
     outside the timed section and only keeps the table clean.
  C  worker-kill reclaim time: worker 1 (--job-ms 5000) claims the only
     job, gets `docker kill -s KILL` 1-4s into it, worker 2 starts, and we
     time kill -> done. Results go to reclaim.csv (different shape from
     results.csv) plus a summary compared against lease_ms + reap_ms.

Each A/B run: 5s warmup (discarded), 30s measurement, 3 repeats. Repeats
are the OUTER loop, so drift over the ~40 min suite (thermal, background
load) spreads across configs instead of landing on one.

Usage:
    python bench/run_all.py                    # everything, ~40 min
    python bench/run_all.py --only C
    python bench/run_all.py --quick            # smoke test, ~3 min
"""
import argparse
import csv
import json
import multiprocessing as mp
import random
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "tools"))
from broker_client import PersistentConnection, send_line  # noqa: E402

IMAGE = "jobqueue:latest"
PY_IMAGE = "python:3.12-slim"
NET = "jqbench"
BROKER = "jqb-broker"
VOLUME = "jqb-data"
HOST_PORT = 9100          # published broker port for config C's host-side polling
QUEUE = "default"

CONCURRENCIES = [1, 2, 4, 8, 16, 32]
SYNC_MODES = ["FULL", "NORMAL"]
B_CLIENTS = 8
SEED_JOBS = 400_000       # A's backlog; a run that drains it shows depth 0 in its samples
DEFAULT_LEASE_MS = 30_000  # worker default, matches include/jobqueue/config.hpp
DEFAULT_REAP_MS = 5_000
FIRST_RETRY_BACKOFF_MS = 1_000  # RetryPolicy::base_backoff_ms, applied by the reaper on requeue
WORKER_IDLE_POLL_MS = 100       # worker.cpp sleep on an empty claim

RESULTS_FIELDS = ["config", "run", "jobs_per_sec", "p50_ms", "p99_ms", "queue_depth_samples"]
RECLAIM_FIELDS = ["trial", "kill_after_claim_ms", "kill_to_done_ms", "claim_to_done_ms",
                  "lease_ms", "reap_ms"]


def percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return float("nan")
    i = min(len(sorted_vals) - 1, int(round(p / 100 * (len(sorted_vals) - 1))))
    return sorted_vals[i]


# ---------------------------------------------------------------------------
# In-container side: _seed and _measure
# ---------------------------------------------------------------------------

def cmd_seed(a: argparse.Namespace) -> int:
    import sqlite3
    # The broker is already running and created the schema; WAL lets this
    # writer coexist with it (the broker is idle -- no worker yet).
    conn = sqlite3.connect(a.db, timeout=30)
    now_ms = int(time.time() * 1000)
    conn.execute("BEGIN")
    conn.executemany(
        "INSERT INTO jobs (queue, payload, priority, status, attempts, "
        "lease_expires_at, run_after, created_at) VALUES (?, ?, 0, 0, 0, 0, 0, ?)",
        ((a.queue, f"seed-{i}", now_ms) for i in range(a.n)),
    )
    conn.commit()
    conn.close()
    print(json.dumps({"seeded": a.n}))
    return 0


def stats(conn: PersistentConnection, queue: str) -> dict:
    return json.loads(conn.send_request(json.dumps({"cmd": "stats", "queue": queue})))


def b_client(idx: int, host: str, port: int, measure_start: float, end: float,
             out: mp.Queue) -> None:
    queue = f"b{idx}"
    enqueue = json.dumps({"cmd": "enqueue", "queue": queue, "payload": "x", "priority": 0})
    claim = json.dumps({"cmd": "claim", "queue": queue, "lease_ms": DEFAULT_LEASE_MS})
    latencies, errors, messages = [], 0, set()
    with PersistentConnection(port, host=host, timeout=30) as conn:
        while (wall := time.time()) < end:
            t0 = time.perf_counter()
            enq = json.loads(conn.send_request(enqueue))
            clm = json.loads(conn.send_request(claim))
            t1 = time.perf_counter()
            job = clm.get("job")
            if not enq.get("ok") or not clm.get("ok") or job is None:
                # The broker answers any engine exception with
                # {"ok":false,"error":...}; keep the text so it's visible.
                errors += 1
                for r, what in ((enq, "enqueue"), (clm, "claim")):
                    if not r.get("ok"):
                        messages.add(f"{what}: {r.get('error')}")
                continue
            if wall >= measure_start:
                latencies.append(t1 - t0)
            conn.send_request(json.dumps({"cmd": "ack", "queue": queue, "job_id": job["id"],
                                          "attempt": job["attempts"]}))
    out.put({"latencies": latencies, "errors": errors, "messages": sorted(messages)})


def cmd_measure(a: argparse.Namespace) -> int:
    start = time.time()
    measure_start = start + a.warmup
    end = measure_start + a.measure

    procs, out = [], mp.Queue()
    if a.mode == "b":
        procs = [mp.Process(target=b_client, args=(i, a.host, a.port, measure_start, end, out))
                 for i in range(a.clients)]
        for p in procs:
            p.start()
        queues = [f"b{i}" for i in range(a.clients)]
    else:
        queues = [a.queue]

    # 1 Hz on an absolute schedule; sample k lands at start + k seconds.
    samples = []  # (t, ready, done)
    with PersistentConnection(a.port, host=a.host, timeout=30) as conn:
        k = 0
        while True:
            due = start + k
            if due > end + 1e-9:
                break
            time.sleep(max(0.0, due - time.time()))
            ready = done = 0
            for q in queues:
                s = stats(conn, q)
                ready += s["ready"]
                done += s["done"]
            samples.append((time.time(), ready, done))
            k += 1

    window = [s for s in samples if s[0] >= measure_start - 0.5]
    depth = [s[1] for s in window]
    result = {"depth_samples": depth, "p50_ms": None, "p99_ms": None, "errors": 0}

    if a.mode == "a":
        (t0, _, d0), (t1, _, d1) = window[0], window[-1]
        result["jobs_per_sec"] = (d1 - d0) / (t1 - t0)
    else:
        parts = [out.get() for _ in procs]
        for p in procs:
            p.join()
        lat = sorted(x for r in parts for x in r["latencies"])
        result["jobs_per_sec"] = len(lat) / a.measure
        result["p50_ms"] = percentile(lat, 50) * 1e3
        result["p99_ms"] = percentile(lat, 99) * 1e3
        result["errors"] = sum(r["errors"] for r in parts)
        result["error_messages"] = sorted({m for r in parts for m in r["messages"]})

    print(json.dumps(result))
    return 0


# ---------------------------------------------------------------------------
# Host side: docker orchestration
# ---------------------------------------------------------------------------

def docker(*args: str, check: bool = True) -> str:
    r = subprocess.run(["docker", *args], capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)} failed:\n{r.stderr.strip()}")
    return r.stdout.strip()


def cleanup() -> None:
    ids = docker("ps", "-aq", "--filter", "name=^jqb-")
    if ids:
        docker("rm", "-f", *ids.split(), check=False)
    docker("volume", "rm", "-f", VOLUME, check=False)


def fresh_env() -> None:
    cleanup()
    docker("volume", "create", VOLUME)


def start_broker(sync_mode: str, reap_ms: int | None = None) -> None:
    env = ["-e", "JQ_DB_PATH=/data/jobqueue.db", "-e", f"JQ_SYNC_MODE={sync_mode}"]
    if reap_ms is not None:
        env += ["-e", f"JQ_REAP_MS={reap_ms}"]
    docker("run", "-d", "--name", BROKER, "--network", NET, "-p", f"{HOST_PORT}:9000",
           *env, "-v", f"{VOLUME}:/data", IMAGE, "9000")
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            if json.loads(send_line(HOST_PORT, '{"cmd":"ping"}', host="127.0.0.1"))["ok"]:
                return
        except (OSError, ValueError):
            pass
        time.sleep(0.2)
    raise RuntimeError("broker did not become ready within 30s")


def start_worker(name: str, concurrency: int, job_ms: int, lease_ms: int) -> None:
    docker("run", "-d", "--name", name, "--network", NET, "--entrypoint", "worker", IMAGE,
           "--host", BROKER, "--port", "9000", "--concurrency", str(concurrency),
           "--job-ms", str(job_ms), "--lease-ms", str(lease_ms))


def in_network(*args: str) -> dict:
    """Run this script inside the docker network; return its last stdout line as JSON."""
    out = docker("run", "--rm", "--network", NET, "-e", "PYTHONDONTWRITEBYTECODE=1",
                 "--mount", f"type=bind,src={REPO},dst=/repo,readonly",
                 "-v", f"{VOLUME}:/data", PY_IMAGE,
                 "python", "/repo/bench/run_all.py", *args)
    return json.loads(out.splitlines()[-1])


def append_csv(path: Path, fields: list[str], row: dict) -> None:
    new = not path.exists()
    with path.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if new:
            w.writeheader()
        w.writerow(row)


def fmt(v) -> str:
    return "" if v is None else f"{v:.3f}"


def write_env(out_dir: Path) -> None:
    fresh_env()
    # Probed from the runtime image with the bench volume mounted: that's
    # the kernel/CPU/sqlite/filesystem the broker actually runs on (Docker
    # Desktop's VM), not the Windows host.
    probe = docker("run", "--rm", "-v", f"{VOLUME}:/data", "--entrypoint", "bash", IMAGE, "-c",
                   "echo nproc: $(nproc); echo kernel: $(uname -srm); "
                   "echo sqlite_version: $(dpkg-query -W -f='${Version}' libsqlite3-0); "
                   "echo fs_type: $(awk '$2==\"/data\"{print $3}' /proc/mounts) "
                   "'(named volume at /data)'; "
                   "echo mem_total: $(awk '/MemTotal/{print $2, $3}' /proc/meminfo)")
    server = docker("version", "--format", "{{.Server.Version}} ({{.Server.Os}}/{{.Server.Arch}})")
    (out_dir / "env.txt").write_text(
        f"{probe}\n"
        f"docker_server: {server}\n"
        f"recorded_at: {datetime.now(timezone.utc).isoformat(timespec='seconds')}\n")
    print((out_dir / "env.txt").read_text())


def run_config_a(a, results: Path) -> None:
    for run in range(1, a.repeats + 1):
        for sync in SYNC_MODES:
            for conc in a.concurrencies:
                name = f"A_conc{conc}_{sync}"
                fresh_env()
                start_broker(sync)
                in_network("_seed", "--db", "/data/jobqueue.db", "--n", str(a.seed_jobs),
                           "--queue", QUEUE)
                start_worker("jqb-worker", conc, job_ms=0, lease_ms=DEFAULT_LEASE_MS)
                r = in_network("_measure", "--mode", "a", "--host", BROKER, "--port", "9000",
                               "--queue", QUEUE, "--warmup", str(a.warmup),
                               "--measure", str(a.measure))
                starved = " (BACKLOG DRAINED -- raise --seed-jobs)" if 0 in r["depth_samples"] else ""
                print(f"{name} run {run}: {r['jobs_per_sec']:.0f} jobs/s{starved}", flush=True)
                append_csv(results, RESULTS_FIELDS, {
                    "config": name, "run": run, "jobs_per_sec": fmt(r["jobs_per_sec"]),
                    "p50_ms": "", "p99_ms": "",
                    "queue_depth_samples": ";".join(map(str, r["depth_samples"]))})


def run_config_b(a, results: Path) -> None:
    for run in range(1, a.repeats + 1):
        for sync in SYNC_MODES:
            name = f"B_conc{B_CLIENTS}_{sync}"
            fresh_env()
            start_broker(sync)
            r = in_network("_measure", "--mode", "b", "--host", BROKER, "--port", "9000",
                           "--clients", str(B_CLIENTS), "--warmup", str(a.warmup),
                           "--measure", str(a.measure))
            # An empty claim with no error text means the job was committed
            # but not yet claimable: run_after uses the wall clock, so a
            # backward clock step (seen on Docker Desktop's VM) hides it
            # for a few ms. Reported, not retried -- it's real behavior.
            err = (f", {r['errors']} failed round-trips "
                   f"{r['error_messages'] or '(claim returned no job)'}" if r["errors"] else "")
            print(f"{name} run {run}: {r['jobs_per_sec']:.0f} round-trips/s  "
                  f"p50 {r['p50_ms']:.2f} ms  p99 {r['p99_ms']:.2f} ms{err}", flush=True)
            append_csv(results, RESULTS_FIELDS, {
                "config": name, "run": run, "jobs_per_sec": fmt(r["jobs_per_sec"]),
                "p50_ms": fmt(r["p50_ms"]), "p99_ms": fmt(r["p99_ms"]),
                "queue_depth_samples": ";".join(map(str, r["depth_samples"]))})


def poll_stats(until, timeout_s: float, interval_s: float) -> tuple[float, dict]:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        s = json.loads(send_line(HOST_PORT, json.dumps({"cmd": "stats", "queue": QUEUE}),
                                 host="127.0.0.1"))
        if until(s):
            return time.time(), s
        time.sleep(interval_s)
    raise RuntimeError(f"condition not reached within {timeout_s}s")


def run_config_c(a, out_dir: Path) -> None:
    lease_ms, reap_ms = a.lease_ms, a.reap_ms
    job_ms = 5000
    reclaim_csv = out_dir / "reclaim.csv"
    kill_to_done, claim_to_done = [], []

    for trial in range(1, a.trials + 1):
        fresh_env()
        start_broker("FULL", reap_ms=reap_ms)
        start_worker("jqb-w1", 1, job_ms=job_ms, lease_ms=lease_ms)
        # The reaper's timer starts with the broker, and setup takes about
        # the same time every trial -- without this, every claim lands at
        # the same phase of the reap cycle and all trials sample one point
        # of the [0, reap_ms) wait instead of its distribution.
        time.sleep(random.uniform(0, reap_ms / 1000))
        send_line(HOST_PORT, json.dumps({"cmd": "enqueue", "queue": QUEUE, "payload": "c",
                                         "priority": 0}), host="127.0.0.1")
        t_claim, _ = poll_stats(lambda s: s["leased"] == 1, 10, 0.02)

        # Kill somewhere in the middle of the 5s job, never near its end.
        time.sleep(max(0.0, random.uniform(1.0, 4.0) - (time.time() - t_claim)))
        t_kill = time.time()
        docker("kill", "-s", "KILL", "jqb-w1")
        s = json.loads(send_line(HOST_PORT, json.dumps({"cmd": "stats", "queue": QUEUE}),
                                 host="127.0.0.1"))
        if s["done"] != 0:
            raise RuntimeError("job finished before the kill landed -- kill window missed")

        start_worker("jqb-w2", 1, job_ms=0, lease_ms=lease_ms)
        t_done, _ = poll_stats(lambda s: s["done"] == 1,
                               (lease_ms + reap_ms + FIRST_RETRY_BACKOFF_MS) / 1000 + 15, 0.05)

        row = {"trial": trial,
               "kill_after_claim_ms": round((t_kill - t_claim) * 1000),
               "kill_to_done_ms": round((t_done - t_kill) * 1000),
               "claim_to_done_ms": round((t_done - t_claim) * 1000),
               "lease_ms": lease_ms, "reap_ms": reap_ms}
        kill_to_done.append(row["kill_to_done_ms"])
        claim_to_done.append(row["claim_to_done_ms"])
        append_csv(reclaim_csv, RECLAIM_FIELDS, row)
        print(f"C trial {trial}: killed {row['kill_after_claim_ms']} ms after claim, "
              f"kill->done {row['kill_to_done_ms']} ms, claim->done {row['claim_to_done_ms']} ms",
              flush=True)

    documented = lease_ms + reap_ms
    actual = documented + FIRST_RETRY_BACKOFF_MS + WORKER_IDLE_POLL_MS
    print("=" * 72)
    print(f"C reclaim over {a.trials} trials (lease {lease_ms} ms, reap {reap_ms} ms):")
    for label, xs in (("kill  -> done", kill_to_done), ("claim -> done", claim_to_done)):
        print(f"  {label}: median {statistics.median(xs):.0f}  min {min(xs)}  max {max(xs)} ms")
    print(f"  documented bound (lease + reap):                     {documented} ms")
    print(f"  bound incl. first-retry backoff + worker idle poll:  {actual} ms")
    over = [x for x in claim_to_done if x > documented]
    print(f"  claim->done over documented bound: {len(over)}/{len(claim_to_done)} trials"
          + (f" (worst +{max(over) - documented} ms)" if over else ""))
    print("  (The lease clock starts at claim, not kill, so claim->done is what the bound")
    print("   governs; kill->done is shorter by however long the job ran before dying.)")
    print("=" * 72)


def cmd_bench(a: argparse.Namespace) -> int:
    if a.quick:
        a.warmup, a.measure, a.repeats = 1, 5, 1
        a.concurrencies, a.trials = [1, 8], 2
        a.lease_ms, a.reap_ms = 3000, 1000
        a.seed_jobs = 50_000
    out_dir = Path(a.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    results = out_dir / "results.csv"

    if docker("network", "ls", "-q", "--filter", f"name=^{NET}$") == "":
        docker("network", "create", NET)
    try:
        write_env(out_dir)
        if "A" in a.only:
            run_config_a(a, results)
        if "B" in a.only:
            run_config_b(a, results)
        if "C" in a.only:
            run_config_c(a, out_dir)
    finally:
        cleanup()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd")

    seed = sub.add_parser("_seed")
    seed.add_argument("--db", required=True)
    seed.add_argument("--n", type=int, required=True)
    seed.add_argument("--queue", default=QUEUE)
    seed.set_defaults(func=cmd_seed)

    m = sub.add_parser("_measure")
    m.add_argument("--mode", choices=["a", "b"], required=True)
    m.add_argument("--host", required=True)
    m.add_argument("--port", type=int, required=True)
    m.add_argument("--queue", default=QUEUE)
    m.add_argument("--clients", type=int, default=B_CLIENTS)
    m.add_argument("--warmup", type=float, required=True)
    m.add_argument("--measure", type=float, required=True)
    m.set_defaults(func=cmd_measure)

    ap.add_argument("--only", default="ABC", help="subset of configs, e.g. AB or C")
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--warmup", type=float, default=5.0)
    ap.add_argument("--measure", type=float, default=30.0)
    ap.add_argument("--concurrencies", type=lambda s: [int(x) for x in s.split(",")],
                    default=CONCURRENCIES)
    ap.add_argument("--seed-jobs", type=int, default=SEED_JOBS)
    ap.add_argument("--trials", type=int, default=10)
    ap.add_argument("--lease-ms", type=int, default=DEFAULT_LEASE_MS)
    ap.add_argument("--reap-ms", type=int, default=DEFAULT_REAP_MS)
    ap.add_argument("--quick", action="store_true",
                    help="smoke test: short windows, 1 repeat, conc 1/8, 3s lease")
    ap.add_argument("--out-dir", default=str(REPO / "bench" / "results" / "run_all"))
    ap.set_defaults(func=cmd_bench)

    a = ap.parse_args()
    a.only = a.only.upper() if hasattr(a, "only") else ""
    return a.func(a)


if __name__ == "__main__":
    sys.exit(main())
