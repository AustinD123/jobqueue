#!/usr/bin/env python3
"""Automated six-step enqueue/claim/ack/nack/stats lifecycle check against
a RUNNING broker (this script does not start one -- launch it yourself
first, e.g. `./build_linux/broker 9000 somefile.db`).

    python3 tools/integration_check.py [--port 9000] [--max-attempts N]

--max-attempts N: instead of a single nack (expect the job to land back
in Ready), nack the job N times and expect it to end up Dead. Set this to
match whatever RetryPolicy.max_attempts the broker was actually started
with, or the Dead assertion will fail.

PROTOCOL ASSUMPTIONS -- dispatch() isn't implemented yet as of this
script, so these are this script's best guess at the wire shape, not a
confirmed spec. If dispatch() ends up shaped differently, this script
needs updating to match (or tell me and I'll update it):

  {"cmd":"enqueue","queue":<str>,"payload":<str>,"priority":<int>}
    -> {"ok":true,"id":<int>}

  {"cmd":"claim","queue":<str>,"lease_ms":<int>}
    -> {"ok":true,"job":{"id":..,"payload":..,"queue":..,"priority":..,
                          "status":..,"attempts":..,...}}   (job present)
    -> {"ok":true,"job":null}                                (queue empty)

  {"cmd":"ack","job_id":<int>}  -> {"ok":true} on success,
                                    {"ok":false,"error":".."} if the job
                                    wasn't leased / already resolved
  {"cmd":"nack","job_id":<int>} -> same shape as ack

  {"cmd":"stats","queue":<str>}
    -> {"ok":true,"ready":..,"leased":..,"done":..,"dead":..}  (flat,
       not nested under a "stats" key)

  Any error (bad request, exception in dispatch, ack/nack no-op, etc.):
    -> {"ok":false,"error":<str>}   (confirmed -- matches both
                                      broker.cpp's catch block and
                                      dispatch()'s own ack_nack_response)

Note: this test enqueues into queue "default" and checks exact stats
counts. It assumes "default" is empty when the run starts -- pre-existing
jobs from a prior run (or a shared broker) will make the exact-count
assertions fail even if dispatch() is correct. Point it at a fresh db for
a clean run.
"""
import argparse
import json
import sys
import time
import uuid

from broker_client import send_line

DEFAULT_PORT = 9000
DEFAULT_LEASE_MS = 30000
CLAIM_RETRY_TIMEOUT_S = 60.0   # generous: must ride out nack's exponential backoff
CLAIM_RETRY_INTERVAL_S = 0.2

_failures = 0


def check(step: str, ok: bool, actual=None, expected=None) -> bool:
    global _failures
    if ok:
        print(f"PASS: {step}")
    else:
        _failures += 1
        print(f"FAIL: {step} -- expected {expected!r}, got {actual!r}")
    return ok


def rpc(port: int, request: dict) -> dict:
    """Send one request, parse the response as JSON. Raises on transport
    failure or invalid JSON -- those are infrastructure problems, not
    lifecycle-check failures, so they're allowed to crash the script
    loudly rather than being reported as a PASS/FAIL step.
    """
    raw = send_line(port, json.dumps(request))
    return json.loads(raw)


def claim_with_retry(port: int, queue: str, lease_ms: int = DEFAULT_LEASE_MS,
                      timeout_s: float = CLAIM_RETRY_TIMEOUT_S) -> dict:
    """Poll claim() until it returns a job or timeout_s elapses. Needed
    because a job that was just nack()'d isn't immediately claimable
    again -- it's back in Ready, but with run_after pushed into the
    future by backoff.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        response = rpc(port, {"cmd": "claim", "queue": queue, "lease_ms": lease_ms})
        if not response.get("ok"):
            # A definite error (bad request, exception in dispatch) -- not
            # the same thing as "no job ready yet". Fail fast instead of
            # retrying an error for up to timeout_s.
            raise RuntimeError(f"claim() returned an error: {response.get('error')}")
        if response.get("job") is not None:
            return response
        time.sleep(CLAIM_RETRY_INTERVAL_S)
    raise TimeoutError(f"claim() on queue {queue!r} returned no job within {timeout_s}s")


def run_enqueue_claim_resolve(port: int, queue: str, resolve_cmd: str, label: str) -> None:
    """Runs steps 1-3 (or 1-3 repeated for the nack path): enqueue,
    claim-and-check-payload, then resolve with either 'ack' or 'nack'.
    """
    payload = f"integration-{label}-{uuid.uuid4().hex[:8]}"

    enqueue_resp = rpc(port, {"cmd": "enqueue", "queue": queue, "payload": payload, "priority": 0})
    check(f"[{label}] enqueue succeeded", enqueue_resp.get("ok") is True,
          enqueue_resp, {"ok": True})
    job_id = enqueue_resp.get("id")
    check(f"[{label}] enqueue returned an id", job_id is not None, enqueue_resp, "id present")

    claim_resp = claim_with_retry(port, queue)
    job = claim_resp.get("job") or {}
    check(f"[{label}] claimed job payload matches enqueued payload",
          job.get("payload") == payload, job.get("payload"), payload)

    resolve_resp = rpc(port, {"cmd": resolve_cmd, "job_id": job_id})
    check(f"[{label}] {resolve_cmd} succeeded", resolve_resp.get("ok") is True,
          resolve_resp, {"ok": True})

    return job_id


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                      formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--queue", default="default")
    parser.add_argument("--max-attempts", type=int, default=None,
                         help="if set, nack the job this many times and expect Dead "
                              "instead of a single nack expecting Ready")
    args = parser.parse_args()

    # Steps 1-4: enqueue, claim (check payload), ack, then stats.
    run_enqueue_claim_resolve(args.port, args.queue, "ack", "ack-path")

    stats = rpc(args.port, {"cmd": "stats", "queue": args.queue})
    check("[ack-path] stats.done == 1", stats.get("done") == 1, stats.get("done"), 1)
    check("[ack-path] stats.ready == 0", stats.get("ready") == 0, stats.get("ready"), 0)
    check("[ack-path] stats.leased == 0", stats.get("leased") == 0, stats.get("leased"), 0)

    # Steps 5-6: repeat 1-3 with nack instead of ack, then stats again.
    if args.max_attempts is None:
        job_id = run_enqueue_claim_resolve(args.port, args.queue, "nack", "nack-once")

        stats = rpc(args.port, {"cmd": "stats", "queue": args.queue})
        check("[nack-once] stats.ready == 1 (job requeued)", stats.get("ready") == 1,
              stats.get("ready"), 1)
        check("[nack-once] stats.leased == 0", stats.get("leased") == 0, stats.get("leased"), 0)
        check("[nack-once] stats.done == 1 (unchanged from ack-path)", stats.get("done") == 1,
              stats.get("done"), 1)
    else:
        payload = f"integration-dead-path-{uuid.uuid4().hex[:8]}"
        enqueue_resp = rpc(args.port, {"cmd": "enqueue", "queue": args.queue,
                                        "payload": payload, "priority": 0})
        check("[dead-path] enqueue succeeded", enqueue_resp.get("ok") is True,
              enqueue_resp, {"ok": True})
        job_id = enqueue_resp.get("id")

        for attempt in range(1, args.max_attempts + 1):
            print(f"[dead-path] attempt {attempt}/{args.max_attempts}: "
                  f"claiming (may wait out backoff)...")
            claim_resp = claim_with_retry(args.port, args.queue)
            job = claim_resp.get("job") or {}
            check(f"[dead-path] attempt {attempt}: claimed the same job",
                  job.get("id") == job_id, job.get("id"), job_id)
            nack_resp = rpc(args.port, {"cmd": "nack", "job_id": job_id})
            check(f"[dead-path] attempt {attempt}: nack succeeded",
                  nack_resp.get("ok") is True, nack_resp, {"ok": True})

        stats = rpc(args.port, {"cmd": "stats", "queue": args.queue})
        check("[dead-path] stats.dead == 1 (job exhausted retries)", stats.get("dead") == 1,
              stats.get("dead"), 1)
        check("[dead-path] stats.ready == 0", stats.get("ready") == 0, stats.get("ready"), 0)
        check("[dead-path] stats.done == 1 (unchanged from ack-path)", stats.get("done") == 1,
              stats.get("done"), 1)

    print()
    if _failures == 0:
        print("ALL PASSED")
        return 0
    else:
        print(f"{_failures} CHECK(S) FAILED")
        return 1


if __name__ == "__main__":
    sys.exit(main())
