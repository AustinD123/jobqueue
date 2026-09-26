#include "jobqueue/reaper.hpp"

namespace jobqueue {

LeaseReaper::LeaseReaper(std::string db_path, std::chrono::milliseconds interval,
                          RetryPolicy policy)
    : db_path_(std::move(db_path)), interval_(interval), policy_(policy) {}

LeaseReaper::~LeaseReaper() {
    stop();
}

void LeaseReaper::start() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (running_) {
        return;
    }
    stop_requested_ = false;
    running_ = true;
    thread_ = std::thread(&LeaseReaper::run, this);
}

void LeaseReaper::stop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_) {
            return;
        }
        stop_requested_ = true;
    }
    cv_.notify_all();
    if (thread_.joinable()) {
        thread_.join();
    }
    std::lock_guard<std::mutex> lock(mutex_);
    running_ = false;
}

void LeaseReaper::run() {
    // Own connection, created on this thread -- same connection-per-thread
    // discipline as claim_race_test's workers, not shared with the caller.
    Engine engine(db_path_, policy_);

    std::unique_lock<std::mutex> lock(mutex_);
    while (!cv_.wait_for(lock, interval_, [this] { return stop_requested_; })) {
        // wait_for returned false: the interval elapsed with no stop
        // request. Unlock for the actual DB work so stop() is never
        // blocked waiting on a reap pass -- it only needs the mutex
        // briefly to set the flag and can join as soon as this loop
        // notices it on the next wait_for check.
        lock.unlock();
        engine.reap_expired_leases();
        lock.lock();
    }
}

}  // namespace jobqueue
