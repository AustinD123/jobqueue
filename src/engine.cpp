#include "jobqueue/engine.hpp"

#include <algorithm>
#include <chrono>
#include <stdexcept>
#include <utility>
#include <vector>

namespace jobqueue {

Engine::Engine(const std::string& db_path, RetryPolicy policy)
    : db_(nullptr), policy_(policy) {
    if (sqlite3_open(db_path.c_str(), &db_) != SQLITE_OK) {
        std::string err = sqlite3_errmsg(db_);
        sqlite3_close(db_);
        throw std::runtime_error("Failed to open database: " + err);
    }
    // busy_timeout must be set FIRST: it controls what happens when this
    // connection can't get the write lock. Set it before journal_mode/
    // synchronous, or those two pragmas have no retry/wait behavior yet
    // and can throw immediately under concurrent connection setup.
    if (sqlite3_exec(db_, "PRAGMA busy_timeout=5000;", nullptr, nullptr, nullptr) != SQLITE_OK) {
        std::string err = sqlite3_errmsg(db_);
        sqlite3_close(db_);
        throw std::runtime_error("Failed to set busy_timeout: " + err);
    }
    if (sqlite3_exec(db_, "PRAGMA journal_mode=WAL;", nullptr, nullptr, nullptr) != SQLITE_OK) {
        std::string err = sqlite3_errmsg(db_);
        sqlite3_close(db_);
        throw std::runtime_error("Failed to set journal_mode: " + err);
    }
    if (sqlite3_exec(db_, "PRAGMA synchronous=FULL;", nullptr, nullptr, nullptr) != SQLITE_OK) {
        std::string err = sqlite3_errmsg(db_);
        sqlite3_close(db_);
        throw std::runtime_error("Failed to set synchronous: " + err);
    }
    const char* schema_sql =
        "CREATE TABLE IF NOT EXISTS jobs ("
        "id INTEGER PRIMARY KEY, queue TEXT, payload TEXT, priority INTEGER, "
        "status INTEGER, attempts INTEGER, lease_expires_at INTEGER, "
        "run_after INTEGER, created_at INTEGER);"
        "CREATE INDEX IF NOT EXISTS idx_claim ON jobs(queue, status, run_after, priority);";
    if (sqlite3_exec(db_, schema_sql, nullptr, nullptr, nullptr) != SQLITE_OK) {
        std::string err = sqlite3_errmsg(db_);
        sqlite3_close(db_);
        throw std::runtime_error("Failed to create schema: " + err);
    }
}

int64_t Engine::now_ms() const {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

int64_t Engine::enqueue(const std::string& queue, const std::string& payload,
                         int priority) {
    const char* sql =
        "INSERT INTO jobs (queue, payload, priority, status, attempts, "
        "lease_expires_at, run_after, created_at) "
        "VALUES (?, ?, ?, ?, 0, 0, ?, ?);";
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare enqueue statement: " +
                                  std::string(sqlite3_errmsg(db_)));
    }

    const int64_t now = now_ms();
    sqlite3_bind_text(stmt, 1, queue.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, payload.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 3, priority);
    sqlite3_bind_int(stmt, 4, static_cast<int>(JobStatus::Ready));
    sqlite3_bind_int64(stmt, 5, now);
    sqlite3_bind_int64(stmt, 6, now);

    if (sqlite3_step(stmt) != SQLITE_DONE) {
        std::string err = sqlite3_errmsg(db_);
        sqlite3_finalize(stmt);
        throw std::runtime_error("Failed to enqueue job: " + err);
    }

    const int64_t id = sqlite3_last_insert_rowid(db_);
    sqlite3_finalize(stmt);
    return id;
}

Engine::~Engine() {
    if (db_) {
        sqlite3_close(db_);
    }
}

std::optional<Job> Engine::claim(const std::string& queue, int64_t lease_ms) {
    // TODO: single UPDATE ... RETURNING statement.
    // Must, in one statement: pick one row for this `queue` where
    // status = Ready and run_after <= now (ORDER BY priority DESC, id ASC
    // LIMIT 1 to pick deterministically), set status = Leased,
    // attempts = attempts + 1, lease_expires_at = now + lease_ms, and
    // RETURNING every column you need to build a Job.
    const char* sql = "UPDATE jobs SET status = ?, attempts = attempts + 1, lease_expires_at = ? "
                      "WHERE id = (SELECT id FROM jobs WHERE queue = ? AND status = ? AND run_after <= ? "
                      "ORDER BY priority DESC, id ASC LIMIT 1) "
                      "RETURNING id, queue, payload, priority, status, attempts, lease_expires_at, run_after, created_at;";
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare claim statement: " +
                                  std::string(sqlite3_errmsg(db_)));
    }

    const int64_t now = now_ms();
    sqlite3_bind_int(stmt, 1, static_cast<int>(JobStatus::Leased));
    sqlite3_bind_int64(stmt, 2, now + lease_ms);
    sqlite3_bind_text(stmt, 3, queue.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 4, static_cast<int>(JobStatus::Ready));
    sqlite3_bind_int64(stmt, 5, now);

    (void)now;
    (void)queue;
    (void)lease_ms;
    // TODO: bind params here — order depends on where your `?` placeholders
    // land in the SQL above (queue, now, lease_expires_at, etc).

    std::optional<Job> result;
    const int rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        // TODO: build a Job from sqlite3_column_* calls, column indices
        // matching the order of your RETURNING clause (0-indexed).
        result = Job{
            .id = sqlite3_column_int64(stmt, 0),
            .queue = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1)),
            .payload = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2)),
            .priority = sqlite3_column_int(stmt, 3),
            .status = static_cast<JobStatus>(sqlite3_column_int(stmt, 4)),
            .attempts = sqlite3_column_int(stmt, 5),
            .lease_expires_at = sqlite3_column_int64(stmt, 6),
            .run_after = sqlite3_column_int64(stmt, 7),
            .created_at = sqlite3_column_int64(stmt, 8)
        };
    } else if (rc != SQLITE_DONE) {
        std::string err = sqlite3_errmsg(db_);
        sqlite3_finalize(stmt);
        throw std::runtime_error("Failed to claim job: " + err);
    }

    sqlite3_finalize(stmt);
    return result;
}

bool Engine::ack(int64_t job_id) {
    // TODO: UPDATE jobs SET status = Done WHERE id = ? AND status = Leased
    // Check sqlite3_changes(db_) after step — 0 means this job wasn't
    // leased (already acked, or its lease already expired and got
    // reclaimed by someone else) — that's your `false` case.
    const char* sql = "UPDATE jobs SET status = ? WHERE id = ? AND status = ?;";
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare ack statement: " +
                                  std::string(sqlite3_errmsg(db_)));
    }

    sqlite3_bind_int(stmt, 1, static_cast<int>(JobStatus::Done));
    sqlite3_bind_int64(stmt, 2, job_id);
    sqlite3_bind_int(stmt, 3, static_cast<int>(JobStatus::Leased));
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        std::string err = sqlite3_errmsg(db_);
        sqlite3_finalize(stmt);
        throw std::runtime_error("Failed to ack job: " + err);
    }

    const bool changed = sqlite3_changes(db_) > 0;
    sqlite3_finalize(stmt);
    return changed;
}

bool Engine::nack(int64_t job_id) {
    // Step 1: read attempts, but only for a row this caller still owns
    // (status = Leased). This SELECT-then-UPDATE is safe here in a way it
    // was NOT safe in claim(): nobody else can be racing us for a job
    // that's already Leased to us specifically -- the only other actor
    // that could touch this row is the reaper, if our lease already
    // expired between claim() and now. The final UPDATE's own
    // "AND status = Leased" guard (checked via sqlite3_changes) catches
    // that case: if the reaper already moved it, our UPDATE matches 0
    // rows and we correctly report false, same as ack().
    const char* select_sql = "SELECT attempts FROM jobs WHERE id = ? AND status = ?;";
    sqlite3_stmt* select_stmt = nullptr;
    if (sqlite3_prepare_v2(db_, select_sql, -1, &select_stmt, nullptr) != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare nack select statement: " +
                                  std::string(sqlite3_errmsg(db_)));
    }
    sqlite3_bind_int64(select_stmt, 1, job_id);
    sqlite3_bind_int(select_stmt, 2, static_cast<int>(JobStatus::Leased));

    const int select_rc = sqlite3_step(select_stmt);
    if (select_rc == SQLITE_DONE) {
        sqlite3_finalize(select_stmt);
        return false;  // not currently leased to anyone -- nothing to nack
    }
    if (select_rc != SQLITE_ROW) {
        std::string err = sqlite3_errmsg(db_);
        sqlite3_finalize(select_stmt);
        throw std::runtime_error("Failed to read job for nack: " + err);
    }
    const int attempts = sqlite3_column_int(select_stmt, 0);
    sqlite3_finalize(select_stmt);

    // Branch on attempts (already incremented back in claim()) vs the
    // retry policy, and apply the matching UPDATE -- shared with
    // reap_expired_leases(), which faces the exact same decision for a
    // batch of rows instead of one.
    return transition_after_failure(job_id, attempts);
}

// Applies the Dead-vs-Ready+backoff decision to a single row that the
// caller has already confirmed is (or was) Leased -- shared by nack()
// (one ownership-checked row) and reap_expired_leases() (a batch of
// expired rows). Both branches keep the "AND status = Leased" guard, so a
// row that moved out of Leased between the caller's read and this UPDATE
// (e.g. raced by a late ack(), or by the reaper) safely no-ops instead of
// stomping on it -- caught via sqlite3_changes(), same as ack().
bool Engine::transition_after_failure(int64_t job_id, int current_attempts) {
    sqlite3_stmt* update_stmt = nullptr;
    if (current_attempts >= policy_.max_attempts) {
        const char* dead_sql = "UPDATE jobs SET status = ? WHERE id = ? AND status = ?;";
        if (sqlite3_prepare_v2(db_, dead_sql, -1, &update_stmt, nullptr) != SQLITE_OK) {
            throw std::runtime_error("Failed to prepare dead-transition statement: " +
                                      std::string(sqlite3_errmsg(db_)));
        }
        sqlite3_bind_int(update_stmt, 1, static_cast<int>(JobStatus::Dead));
        sqlite3_bind_int64(update_stmt, 2, job_id);
        sqlite3_bind_int(update_stmt, 3, static_cast<int>(JobStatus::Leased));
    } else {
        const char* retry_sql =
            "UPDATE jobs SET status = ?, run_after = ? WHERE id = ? AND status = ?;";
        if (sqlite3_prepare_v2(db_, retry_sql, -1, &update_stmt, nullptr) != SQLITE_OK) {
            throw std::runtime_error("Failed to prepare retry-transition statement: " +
                                      std::string(sqlite3_errmsg(db_)));
        }
        sqlite3_bind_int(update_stmt, 1, static_cast<int>(JobStatus::Ready));
        sqlite3_bind_int64(update_stmt, 2, now_ms() + backoff_for_attempt(current_attempts));
        sqlite3_bind_int64(update_stmt, 3, job_id);
        sqlite3_bind_int(update_stmt, 4, static_cast<int>(JobStatus::Leased));
    }

    if (sqlite3_step(update_stmt) != SQLITE_DONE) {
        std::string err = sqlite3_errmsg(db_);
        sqlite3_finalize(update_stmt);
        throw std::runtime_error("Failed to apply failure transition: " + err);
    }

    const bool changed = sqlite3_changes(db_) > 0;
    sqlite3_finalize(update_stmt);
    return changed;
}

int Engine::reap_expired_leases() {
    // Step 1: find every row whose lease has expired, but only collect the
    // ids/attempts -- don't act on them yet. We finalize this SELECT before
    // running any UPDATE against the same connection, so we're never
    // mutating the table while a cursor is still open over it.
    static const char* select_sql = "SELECT id, attempts FROM jobs WHERE status = ? AND lease_expires_at < ?;";
    sqlite3_stmt* select_stmt = nullptr;
    if (sqlite3_prepare_v2(db_, select_sql, -1, &select_stmt, nullptr) != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare reap select statement: " +
                                  std::string(sqlite3_errmsg(db_)));
    }
    sqlite3_bind_int(select_stmt, 1, static_cast<int>(JobStatus::Leased));
    sqlite3_bind_int64(select_stmt, 2, now_ms());

    std::vector<std::pair<int64_t, int>> expired;
    while (sqlite3_step(select_stmt) == SQLITE_ROW) {
        int64_t id = sqlite3_column_int64(select_stmt, 0);
        int attempts = sqlite3_column_int(select_stmt, 1);
        expired.emplace_back(id, attempts);
    }
    sqlite3_finalize(select_stmt);

    // Step 2: apply the same Dead-vs-Ready+backoff branch as nack(), one
    // row at a time, via the shared transition_after_failure() helper --
    // still guarded by "AND status = Leased" in case the original
    // worker's late ack()/nack() beat us to a given row.
    int count = 0;
    for (const auto& [id, attempts] : expired) {
        if (transition_after_failure(id, attempts)) {
            ++count;
        }
    }

    return count;
}

int64_t Engine::backoff_for_attempt(int attempt) const {
    // TODO: exponential backoff — e.g.
    // min(base_backoff_ms * 2^(attempt-1), max_backoff_ms). Watch out for
    // integer overflow if attempt gets large; policy_.max_attempts bounds
    // it in practice, but don't rely on the caller respecting that.
    if (attempt <= 0) {
        return policy_.base_backoff_ms;
    }
    int64_t backoff = policy_.base_backoff_ms;
    for (int i = 1; i < attempt; ++i) {
        if (backoff > policy_.max_backoff_ms / 2) {
            backoff = policy_.max_backoff_ms;
            break;
        }
        backoff *= 2;
    }
    return std::min(backoff, policy_.max_backoff_ms);
}

Engine::Stats Engine::stats(const std::string& queue) {
    const char* sql =
        "SELECT status, COUNT(*) FROM jobs WHERE queue = ? GROUP BY status;";
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr) != SQLITE_OK) {
        throw std::runtime_error("Failed to prepare stats statement: " +
                                  std::string(sqlite3_errmsg(db_)));
    }
    sqlite3_bind_text(stmt, 1, queue.c_str(), -1, SQLITE_TRANSIENT);

    Stats result{};
    int rc;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const auto status = static_cast<JobStatus>(sqlite3_column_int(stmt, 0));
        const int64_t n = sqlite3_column_int64(stmt, 1);
        switch (status) {
            case JobStatus::Ready:  result.ready = n; break;
            case JobStatus::Leased: result.leased = n; break;
            case JobStatus::Done:   result.done = n; break;
            case JobStatus::Dead:   result.dead = n; break;
        }
    }
    if (rc != SQLITE_DONE) {
        std::string err = sqlite3_errmsg(db_);
        sqlite3_finalize(stmt);
        throw std::runtime_error("Failed to read stats: " + err);
    }

    sqlite3_finalize(stmt);
    return result;
}

}  // namespace jobqueue