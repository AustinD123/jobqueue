#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <sqlite3.h>

namespace jobqueue {

enum class JobStatus { Ready, Leased, Done, Dead };

const char* to_string(JobStatus status);

struct Job {
    int64_t id;
    std::string queue;
    std::string payload;
    int priority;
    JobStatus status;
    int attempts;
    int64_t lease_expires_at;
    int64_t run_after;
    int64_t created_at;
};

struct RetryPolicy {
    int max_attempts = 5;
    int64_t base_backoff_ms = 1000;
    int64_t max_backoff_ms = 60'000;
};

class Engine {
public:
    explicit Engine(const std::string& db_path, RetryPolicy policy = {});
    ~Engine();
    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;

    int64_t enqueue(const std::string& queue, const std::string& payload,
                     int priority = 0);
    std::optional<Job> claim(const std::string& queue, int64_t lease_ms);
    bool ack(int64_t job_id);
    bool nack(int64_t job_id);
    int reap_expired_leases();

    struct Stats { int64_t ready, leased, dead, done; };
    Stats stats(const std::string& queue);

private:
    sqlite3* db_;
    RetryPolicy policy_;
    int64_t now_ms() const;
    int64_t backoff_for_attempt(int attempt) const;
    bool transition_after_failure(int64_t job_id, int current_attempts);
};

}  // namespace jobqueue
