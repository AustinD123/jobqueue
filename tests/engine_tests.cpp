#include "jobqueue/engine.hpp"

#include <cassert>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>

namespace {

// RAII helper: gives each test a fresh, unique SQLite file and deletes it
// (plus any WAL/SHM sidecars) when the test finishes.
class TempDbFile {
public:
    TempDbFile()
        : path_((std::filesystem::temp_directory_path() /
                 ("jobqueue_test_" + std::to_string(counter_++) + ".db"))
                    .string()) {
        std::filesystem::remove(path_);
    }
    ~TempDbFile() {
        std::filesystem::remove(path_);
        std::filesystem::remove(path_ + "-wal");
        std::filesystem::remove(path_ + "-shm");
    }
    const std::string& path() const { return path_; }

private:
    static inline int counter_ = 0;
    std::string path_;
};

void test_enqueue_then_claim_returns_job() {
    TempDbFile db;
    jobqueue::Engine engine(db.path());
    // TODO: enqueue a job, claim it, assert the returned Job has the
    // expected queue/payload/priority and status == Leased.
}

void test_claim_on_empty_queue_returns_nullopt() {
    TempDbFile db;
    jobqueue::Engine engine(db.path());
    // TODO: claim() on a queue with no ready jobs should return
    // std::nullopt. Also verify claiming twice in a row (drain then
    // claim again) returns nullopt the second time.
}

void test_two_instances_share_persisted_state() {
    TempDbFile db;
    // TODO: open a first Engine against db.path(), enqueue + claim a job.
    // Destroy it, open a second Engine against the same file, and assert
    // the second instance's claim() does NOT see the already-claimed job
    // (proves persistence + the WHERE status='ready' filter works).
}

void test_ack_on_already_acked_job_returns_false() {
    TempDbFile db;
    jobqueue::Engine engine(db.path());
    // TODO: enqueue, claim (note the returned Job's `attempts` -- ack()
    // now takes (job_id, attempt), the fencing token), ack with that
    // attempt (expect true), ack again with the SAME attempt (expect
    // false -- already Done, not a fencing mismatch this time).
}

void test_nack_past_max_attempts_results_in_dead() {
    TempDbFile db;
    jobqueue::Engine engine(db.path());
    // TODO: enqueue, then repeatedly claim+nack until attempts >=
    // max_attempts; assert the job's status is JobStatus::Dead. nack()
    // takes (job_id, attempt) now -- use the attempts value from each
    // claim() call, not a stale one.
}

void test_reap_expired_leases_requeues_job() {
    TempDbFile db;
    jobqueue::Engine engine(db.path());
    // TODO: enqueue + claim a job, force its lease_expires_at into the
    // past (raw SQL helper or setter, your call), call
    // reap_expired_leases(), then assert it's Ready again with run_after
    // pushed into the future.
}

void test_stale_ack_rejected_by_fencing_token() {
    TempDbFile db;
    jobqueue::RetryPolicy policy;
    policy.base_backoff_ms = 1;  // negligible, so re-claiming after reap doesn't need a long wait
    jobqueue::Engine engine(db.path(), policy);

    const int64_t id = engine.enqueue("q", "payload", 0);

    // Claim with a very short lease so it expires almost immediately --
    // avoids needing a raw-SQL helper to force lease_expires_at into the
    // past.
    auto first = engine.claim("q", /*lease_ms=*/5);
    assert(first.has_value());
    const int old_attempt = first->attempts;

    std::this_thread::sleep_for(std::chrono::milliseconds(20));  // let the lease expire
    const int reaped = engine.reap_expired_leases();
    assert(reaped == 1);

    std::this_thread::sleep_for(std::chrono::milliseconds(5));  // ride out reap's backoff
    auto second = engine.claim("q", 30000);
    assert(second.has_value());
    assert(second->id == id);
    const int new_attempt = second->attempts;
    assert(new_attempt != old_attempt);

    // The stale ack carries the OLD fencing token -- the job is now
    // leased under a different one (the reap+reclaim cycle bumped
    // attempts), so this must be rejected.
    const bool stale_ack_result = engine.ack(id, old_attempt);
    assert(stale_ack_result == false);

    // Confirm the job genuinely stays leased, not corrupted by the stale
    // ack -- and that the CORRECT current attempt still works.
    const auto stats = engine.stats("q");
    assert(stats.leased == 1);
    assert(stats.done == 0);

    const bool correct_ack_result = engine.ack(id, new_attempt);
    assert(correct_ack_result == true);
}

}  // namespace

int main() {
    test_enqueue_then_claim_returns_job();
    test_claim_on_empty_queue_returns_nullopt();
    test_two_instances_share_persisted_state();
    test_ack_on_already_acked_job_returns_false();
    test_nack_past_max_attempts_results_in_dead();
    test_reap_expired_leases_requeues_job();
    test_stale_ack_rejected_by_fencing_token();

    std::cout << "engine_tests: all stubs ran (fill in TODOs)\n";
    return 0;
}
