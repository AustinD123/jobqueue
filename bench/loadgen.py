#!/usr/bin/env python3
"""Open-loop load generator for the jobqueue broker.

N producer processes, each holding one persistent socket, send requests on
an ABSOLUTE schedule: the k-th request of a producer is due at
start + offset + k * interval, regardless of how long earlier requests
took. Latency is measured from that intended send time, not from when the
request actually went out:

    latency = response_time - intended_send_time

Why: a closed loop (`send; wait; sleep(interval)`) quietly slows down when
the server slows down, so the requests that *would* have been sent during a
stall are never sent and never measured -- "coordinated omission". With an
absolute schedule, a stall leaves the producer behind schedule; it then
sends back-to-back to catch up, and every one of those late requests is
charged the time it spent waiting to be sent. That's the latency a real
client arriving at that moment would have seen.

Each producer has one request in flight at a time (send, read reply), so
one producer can never exceed 1 / round-trip-time. If the offered rate is
above what the producers + broker can do, the run takes longer than
--duration, and "achieved" (completed / wall time) falls below "offered".

Subcommands:
    run        one fixed-rate run against --target ping|enqueue
    calibrate  ramp the offered rate against `ping` (touches no database)
               and report the highest rate where achieved stays within 5%
               of offered AND p99 <= --max-p99-ms (default 20). That is
               the ceiling of this generator (+ the
               broker's protocol loop) -- any enqueue result near or above
               it is measuring the load generator, not SQLite.

Usage:
    python bench/loadgen.py --target ping --rate 5000 --producers 4
    python bench/loadgen.py --target enqueue --rate 2000 --producers 8 --duration 20
    python bench/loadgen.py --port 9001 --target ping --rate 1000   # `run` is implied
    python bench/loadgen.py calibrate --producers 4

Note: `--target enqueue` inserts real jobs. With workers running they get
drained; without, they accumulate in the queue.
"""
import argparse
import json
import multiprocessing as mp
from queue import Empty
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
from broker_client import PersistentConnection  # noqa: E402

# A behind-schedule producer stops sending once it is this far past the
# planned end of the run, so an overloaded run can't drag on indefinitely.
# Requests it never got to count as `unsent` (and push achieved down).
OVERRUN_GRACE_S = 10.0


def build_request(target: str, queue: str, payload: str) -> str:
    if target == "ping":
        return json.dumps({"cmd": "ping"})
    return json.dumps({"cmd": "enqueue", "queue": queue, "payload": payload, "priority": 0})


def producer(idx: int, args: dict, start_wall: float, out: mp.Queue) -> None:
    # time.time() is shared across processes but coarse/adjustable;
    # perf_counter() is precise but per-process. Map the shared wall-clock
    # start onto this process's perf_counter once, then schedule on that.
    start = time.perf_counter() + (start_wall - time.time())

    rate_per_producer = args["rate"] / args["producers"]
    interval = 1.0 / rate_per_producer
    # Stagger producers across one interval so the aggregate stream is
    # evenly spaced instead of N requests arriving in lockstep.
    offset = interval * idx / args["producers"]
    total = int(args["duration"] * rate_per_producer)
    hard_stop = start + args["duration"] + OVERRUN_GRACE_S

    request = build_request(args["target"], args["queue"], "x" * args["payload_bytes"])
    latencies: list[float] = []
    errors = 0
    last_response = start
    sent = 0

    conn = PersistentConnection(args["port"], host=args["host"])
    try:
        for k in range(total):
            intended = start + offset + k * interval
            now = time.perf_counter()
            if now >= hard_stop:
                break
            if now < intended:
                time.sleep(intended - now)
            # else: behind schedule -- send immediately, no sleep. The lag
            # shows up in this request's latency, which is the point.
            sent += 1
            try:
                resp = conn.send_request(request)
                done = time.perf_counter()
                if json.loads(resp).get("ok") is True:
                    latencies.append(done - intended)
                else:
                    errors += 1
                last_response = done
            except (OSError, ConnectionError, ValueError):
                errors += 1
                conn.close()
                conn = PersistentConnection(args["port"], host=args["host"])
    finally:
        conn.close()

    out.put({
        "latencies": latencies,
        "errors": errors,
        "unsent": total - sent,
        "planned": total,
        "elapsed": last_response - start,
    })


def percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return float("nan")
    i = min(len(sorted_vals) - 1, int(round(p / 100 * (len(sorted_vals) - 1))))
    return sorted_vals[i]


def run_once(target: str, rate: float, producers: int, duration: float,
             host: str, port: int, queue: str, payload_bytes: int) -> dict:
    args = dict(target=target, rate=rate, producers=producers, duration=duration,
                host=host, port=port, queue=queue, payload_bytes=payload_bytes)
    out: mp.Queue = mp.Queue()
    # Start far enough ahead that every process (spawned, on Windows) is
    # up and connected before the first scheduled send.
    start_wall = time.time() + 1.0 + 0.1 * producers
    procs = [mp.Process(target=producer, args=(i, args, start_wall, out))
             for i in range(producers)]
    for p in procs:
        p.start()
    # Drain the queue before join(): a child blocks on exit until its
    # queued (large) result has been read. A producer that crashed (e.g.
    # couldn't connect) never puts a result, so a plain blocking get()
    # would wait forever -- poll, and bail out if one has died.
    results = []
    while len(results) < len(procs):
        try:
            results.append(out.get(timeout=1.0))
        except Empty:
            crashed = [p for p in procs if p.exitcode not in (None, 0)]
            if crashed:
                for p in procs:
                    p.terminate()
                raise SystemExit(f"error: {len(crashed)} producer(s) crashed "
                                 f"(traceback above) -- is the broker up on "
                                 f"{host}:{port}?")
    for p in procs:
        p.join()

    lat = sorted(x for r in results for x in r["latencies"])
    elapsed = max(r["elapsed"] for r in results)
    completed = len(lat)
    return {
        "target": target,
        "producers": producers,
        "offered": rate,
        "achieved": completed / elapsed if elapsed > 0 else 0.0,
        "planned": sum(r["planned"] for r in results),
        "completed": completed,
        "errors": sum(r["errors"] for r in results),
        "unsent": sum(r["unsent"] for r in results),
        "p50_ms": percentile(lat, 50) * 1e3,
        "p90_ms": percentile(lat, 90) * 1e3,
        "p99_ms": percentile(lat, 99) * 1e3,
        "p999_ms": percentile(lat, 99.9) * 1e3,
        "max_ms": (lat[-1] if lat else float("nan")) * 1e3,
    }


def print_header(verdict: bool = False) -> None:
    print(f"{'offered/s':>10} {'achieved/s':>11} {'ratio':>6} {'p50ms':>8} {'p90ms':>8} "
          f"{'p99ms':>8} {'p99.9ms':>8} {'maxms':>9} {'err':>5} {'unsent':>7}"
          + ("  verdict" if verdict else ""))


def print_row(r: dict, verdict: str = "") -> None:
    ratio = r["achieved"] / r["offered"]
    print(f"{r['offered']:>10.0f} {r['achieved']:>11.0f} {ratio:>6.3f} {r['p50_ms']:>8.2f} "
          f"{r['p90_ms']:>8.2f} {r['p99_ms']:>8.2f} {r['p999_ms']:>8.2f} {r['max_ms']:>9.2f} "
          f"{r['errors']:>5} {r['unsent']:>7}" + (f"  {verdict}" if verdict else ""), flush=True)


def failures(r: dict, tolerance: float, max_p99_ms: float) -> list[str]:
    """Why a calibration step failed; empty list = pass. Throughput alone
    isn't enough: a generator can keep achieved ~= offered while requests
    queue up behind each other (p50 in the 100ms range at 98% achieved in
    early runs), so p99 has to stay under a ceiling too."""
    why = []
    if r["achieved"] < (1 - tolerance) * r["offered"]:
        why.append(f"achieved<{1 - tolerance:.0%}")
    if not r["p99_ms"] <= max_p99_ms:  # `not <=` so a NaN p99 fails too
        why.append(f"p99>{max_p99_ms:g}ms")
    if r["errors"]:
        why.append("errors")
    if r["unsent"]:
        why.append("unsent")
    return why


def calibrate_step(rate: float, a: argparse.Namespace, common: tuple) -> tuple[dict, bool]:
    r = run_once("ping", rate, *common)
    why = failures(r, a.tolerance, a.max_p99_ms)
    print_row(r, "pass" if not why else "FAIL " + ",".join(why))
    return r, not why


def cmd_run(a: argparse.Namespace) -> int:
    r = run_once(a.target, a.rate, a.producers, a.duration, a.host, a.port, a.queue,
                 a.payload_bytes)
    print(f"target={a.target} producers={a.producers} duration={a.duration}s")
    print_header()
    print_row(r)
    if a.json:
        print(json.dumps(r))
    return 0


def cmd_calibrate(a: argparse.Namespace) -> int:
    common = (a.producers, a.duration, a.host, a.port, a.queue, 0)
    print(f"calibrating against ping: producers={a.producers} duration={a.duration}s/step "
          f"tolerance={a.tolerance:.0%} max_p99={a.max_p99_ms:g}ms")
    print_header(verdict=True)

    # Phase 1: geometric ramp until the first failing rate.
    best, fail = None, None
    rate = a.start
    while rate <= a.max_rate:
        r, ok = calibrate_step(rate, a, common)
        if ok:
            best = r
            rate *= a.factor
        else:
            fail = rate
            break

    # Phase 2: bisect between the last pass and the first fail.
    if best is not None and fail is not None:
        lo, hi = best["offered"], fail
        for _ in range(a.refine):
            mid = (lo + hi) / 2
            r, ok = calibrate_step(mid, a, common)
            if ok:
                best, lo = r, mid
            else:
                hi = mid

    bar = "=" * 64
    print(bar)
    if best is None:
        print(f"  GENERATOR CEILING: below {a.start:.0f} req/s -- even the starting rate failed.")
        print("  Lower --start, or check the broker is reachable.")
    elif fail is None:
        print(f"  GENERATOR CEILING: >= {best['offered']:.0f} req/s "
              f"(never failed up to --max-rate {a.max_rate:.0f})")
    else:
        print(f"  GENERATOR CEILING: {best['offered']:.0f} req/s  "
              f"({a.producers} producers, ping, achieved within {a.tolerance:.0%}, "
              f"p99 <= {a.max_p99_ms:g} ms)")
        print(f"  p50 {best['p50_ms']:.2f} ms   p99 {best['p99_ms']:.2f} ms at that rate")
    print("  Enqueue results at or near this rate measure the generator, not SQLite.")
    print(bar)
    return 0


def main() -> int:
    # Connection options live on a parent parser shared by both
    # subcommands, so they're accepted after the subcommand name too --
    # which is what lets `run` be implied below.
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--host", default="localhost")
    common.add_argument("--port", type=int, default=9000)
    common.add_argument("--queue", default="default")

    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", parents=[common], help="one fixed-rate run (the default)")
    run.add_argument("--target", choices=["ping", "enqueue"], required=True)
    run.add_argument("--rate", type=float, required=True, help="total offered req/s")
    run.add_argument("--producers", type=int, default=4)
    run.add_argument("--duration", type=float, default=10.0, help="seconds")
    run.add_argument("--payload-bytes", type=int, default=64)
    run.add_argument("--json", action="store_true", help="also print the result as JSON")
    run.set_defaults(func=cmd_run)

    cal = sub.add_parser("calibrate", parents=[common], help="find the generator's own ceiling against ping")
    cal.add_argument("--producers", type=int, default=4)
    cal.add_argument("--duration", type=float, default=5.0, help="seconds per step")
    cal.add_argument("--start", type=float, default=500.0)
    cal.add_argument("--factor", type=float, default=1.5, help="ramp multiplier per step")
    cal.add_argument("--max-rate", type=float, default=200_000.0)
    cal.add_argument("--refine", type=int, default=4, help="bisection steps after first failure")
    cal.add_argument("--tolerance", type=float, default=0.05)
    cal.add_argument("--max-p99-ms", type=float, default=20.0,
                     help="a step also fails if p99 latency exceeds this")
    cal.set_defaults(func=cmd_calibrate)

    argv = sys.argv[1:]
    # `loadgen.py --target enqueue ...` means `loadgen.py run --target enqueue ...`
    if argv and argv[0] not in ("run", "calibrate", "-h", "--help"):
        argv = ["run", *argv]
    a = ap.parse_args(argv)
    return a.func(a)


if __name__ == "__main__":
    sys.exit(main())
