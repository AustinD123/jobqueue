#include "jobqueue/engine.hpp"

#include <cassert>
#include <cstdio>
#include <filesystem>
#include <iostream>
#include <string>

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
    // TODO: enqueue, claim, ack (expect true), ack again (expect false).
}

void test_nack_past_max_attempts_results_in_dead() {
    TempDbFile db;
    jobqueue::Engine engine(db.path());
    // TODO: enqueue, then repeatedly claim+nack until attempts >=
    // max_attempts; assert the job's status is JobStatus::Dead.
}

void test_reap_expired_leases_requeues_job() {
    TempDbFile db;
    jobqueue::Engine engine(db.path());
    // TODO: enqueue + claim a job, force its lease_expires_at into the
    // past (raw SQL helper or setter, your call), call
    // reap_expired_leases(), then assert it's Ready again with run_after
    // pushed into the future.
}

}  // namespace

int main() {
    test_enqueue_then_claim_returns_job();
    test_claim_on_empty_queue_returns_nullopt();
    test_two_instances_share_persisted_state();
    test_ack_on_already_acked_job_returns_false();
    test_nack_past_max_attempts_results_in_dead();
    test_reap_expired_leases_requeues_job();

    std::cout << "engine_tests: all stubs ran (fill in TODOs)\n";
    return 0;
}
