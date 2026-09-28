// Raw SQLite benchmark harness -- no network, no jobqueue_engine library
// link. Uses the SQLite C API directly, with the EXACT pragma sequence
// and EXACT SQL text copied verbatim from src/engine.cpp (Engine's
// constructor, enqueue, claim, ack), so these numbers measure what the
// engine itself does, not a reimplementation of it.

#include <sqlite3.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <sys/utsname.h>
#include <unistd.h>

namespace {

using Clock = std::chrono::steady_clock;

constexpr auto kWarmup = std::chrono::seconds(2);
constexpr auto kMeasure = std::chrono::seconds(10);
constexpr int kRepeats = 3;

// ---- Exact copies from src/engine.cpp -- do not rewrite these strings ----

const char* kPragmaBusyTimeout = "PRAGMA busy_timeout=5000;";
const char* kPragmaJournalMode = "PRAGMA journal_mode=WAL;";
// synchronous is the experimental variable in measurements 2-5, so it's
// passed in per-call below rather than hardcoded -- but the PRAGMA
// SEQUENCE (busy_timeout, then journal_mode, then synchronous) matches
// Engine::Engine exactly, including the reason: busy_timeout must be set
// before the other two, or they have no retry/wait behavior yet.

const char* kSchemaSql =
    "CREATE TABLE IF NOT EXISTS jobs ("
    "id INTEGER PRIMARY KEY, queue TEXT, payload TEXT, priority INTEGER, "
    "status INTEGER, attempts INTEGER, lease_expires_at INTEGER, "
    "run_after INTEGER, created_at INTEGER);"
    "CREATE INDEX IF NOT EXISTS idx_claim ON jobs(queue, status, run_after, priority);";

const char* kEnqueueSql =
    "INSERT INTO jobs (queue, payload, priority, status, attempts, "
    "lease_expires_at, run_after, created_at) "
    "VALUES (?, ?, ?, ?, 0, 0, ?, ?);";

const char* kClaimSql =
    "UPDATE jobs SET status = ?, attempts = attempts + 1, lease_expires_at = ? "
    "WHERE id = (SELECT id FROM jobs WHERE queue = ? AND status = ? AND run_after <= ? "
    "ORDER BY priority DESC, id ASC LIMIT 1) "
    "RETURNING id, queue, payload, priority, status, attempts, lease_expires_at, run_after, created_at;";

const char* kAckSql = "UPDATE jobs SET status = ? WHERE id = ? AND status = ? AND attempts = ?;";

// JobStatus enum values, from include/jobqueue/engine.hpp
// (enum class JobStatus { Ready, Leased, Done, Dead };)
constexpr int kStatusReady = 0;
constexpr int kStatusLeased = 1;
constexpr int kStatusDone = 2;

// Copy of Engine::now_ms()'s logic -- used to supply realistic bind
// values, not part of the SQL text itself.
int64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

// ---- CSV / env output ----

std::ofstream g_csv;

void csv_row(const std::string& measurement, const std::string& configuration,
             const std::string& stat, double value) {
    g_csv << measurement << ',' << configuration << ',' << stat << ',' << value << '\n';
    g_csv.flush();
}

// ---- stats helpers ----

struct Stats { double median, min, max; };

Stats compute_stats(std::vector<double> values) {
    std::sort(values.begin(), values.end());
    double lo = values.front();
    double hi = values.back();
    size_t n = values.size();
    double median = (n % 2 == 0) ? (values[n / 2 - 1] + values[n / 2]) / 2.0 : values[n / 2];
    return {median, lo, hi};
}

double percentile(std::vector<double> values, double p) {
    std::sort(values.begin(), values.end());
    size_t idx = static_cast<size_t>(p * (values.size() - 1));
    return values[idx];
}

// ---- SQLite plumbing ----

std::atomic<long> g_busy_retries{0};

// sqlite3_step, but retries on SQLITE_BUSY/SQLITE_LOCKED instead of
// treating it as fatal -- PRAGMA busy_timeout already absorbs brief
// contention internally (retrying for up to 5s before SQLite itself
// returns SQLITE_BUSY to us), so seeing it here means that was already
// exceeded. Counted so measurement 5 can report how often that happened.
int step_with_busy_retry(sqlite3_stmt* stmt) {
    for (;;) {
        int rc = sqlite3_step(stmt);
        if (rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
            g_busy_retries.fetch_add(1, std::memory_order_relaxed);
            sqlite3_reset(stmt);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        return rc;
    }
}

void exec_or_throw(sqlite3* db, const char* sql) {
    for (;;) {
        char* errmsg = nullptr;
        int rc = sqlite3_exec(db, sql, nullptr, nullptr, &errmsg);
        if (rc == SQLITE_OK) return;
        if (rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
            g_busy_retries.fetch_add(1, std::memory_order_relaxed);
            sqlite3_free(errmsg);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        std::string err = errmsg ? errmsg : sqlite3_errmsg(db);
        sqlite3_free(errmsg);
        throw std::runtime_error(std::string("exec failed: ") + err);
    }
}

sqlite3* open_and_configure(const std::string& path, const char* synchronous_pragma) {
    sqlite3* db = nullptr;
    if (sqlite3_open(path.c_str(), &db) != SQLITE_OK) {
        std::string err = sqlite3_errmsg(db);
        sqlite3_close(db);
        throw std::runtime_error("open failed: " + err);
    }
    exec_or_throw(db, kPragmaBusyTimeout);
    exec_or_throw(db, kPragmaJournalMode);
    exec_or_throw(db, synchronous_pragma);
    exec_or_throw(db, kSchemaSql);
    return db;
}

void remove_db_files(const std::filesystem::path& db_path) {
    for (auto suffix : {"", "-wal", "-shm"}) {
        std::filesystem::path p = db_path;
        p += suffix;
        std::error_code ec;
        std::filesystem::remove(p, ec);
    }
}

// ---- engine-call wrappers: same prepare/bind/step/finalize shape as
// the real Engine methods, using the SQL text constants above verbatim ----

int64_t do_enqueue(sqlite3* db, int64_t counter) {
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, kEnqueueSql, -1, &stmt, nullptr) != SQLITE_OK) {
        throw std::runtime_error(std::string("prepare enqueue failed: ") + sqlite3_errmsg(db));
    }
    std::string queue = "default";
    std::string payload = "bench-" + std::to_string(counter);
    int64_t now = now_ms();
    sqlite3_bind_text(stmt, 1, queue.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, payload.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 3, 0);
    sqlite3_bind_int(stmt, 4, kStatusReady);
    sqlite3_bind_int64(stmt, 5, now);
    sqlite3_bind_int64(stmt, 6, now);

    if (step_with_busy_retry(stmt) != SQLITE_DONE) {
        std::string err = sqlite3_errmsg(db);
        sqlite3_finalize(stmt);
        throw std::runtime_error("enqueue failed: " + err);
    }
    int64_t id = sqlite3_last_insert_rowid(db);
    sqlite3_finalize(stmt);
    return id;
}

struct ClaimedJob { int64_t id = 0; int attempts = 0; bool found = false; };

ClaimedJob do_claim(sqlite3* db, const std::string& queue, int64_t lease_ms) {
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, kClaimSql, -1, &stmt, nullptr) != SQLITE_OK) {
        throw std::runtime_error(std::string("prepare claim failed: ") + sqlite3_errmsg(db));
    }
    int64_t now = now_ms();
    sqlite3_bind_int(stmt, 1, kStatusLeased);
    sqlite3_bind_int64(stmt, 2, now + lease_ms);
    sqlite3_bind_text(stmt, 3, queue.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 4, kStatusReady);
    sqlite3_bind_int64(stmt, 5, now);

    ClaimedJob result;
    int rc = step_with_busy_retry(stmt);
    if (rc == SQLITE_ROW) {
        result.id = sqlite3_column_int64(stmt, 0);
        result.attempts = sqlite3_column_int(stmt, 5);
        result.found = true;
    } else if (rc != SQLITE_DONE) {
        std::string err = sqlite3_errmsg(db);
        sqlite3_finalize(stmt);
        throw std::runtime_error("claim failed: " + err);
    }
    sqlite3_finalize(stmt);
    return result;
}

bool do_ack(sqlite3* db, int64_t job_id, int attempt) {
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(db, kAckSql, -1, &stmt, nullptr) != SQLITE_OK) {
        throw std::runtime_error(std::string("prepare ack failed: ") + sqlite3_errmsg(db));
    }
    sqlite3_bind_int(stmt, 1, kStatusDone);
    sqlite3_bind_int64(stmt, 2, job_id);
    sqlite3_bind_int(stmt, 3, kStatusLeased);
    sqlite3_bind_int(stmt, 4, attempt);

    if (step_with_busy_retry(stmt) != SQLITE_DONE) {
        std::string err = sqlite3_errmsg(db);
        sqlite3_finalize(stmt);
        throw std::runtime_error("ack failed: " + err);
    }
    bool changed = sqlite3_changes(db) > 0;
    sqlite3_finalize(stmt);
    return changed;
}

// ---- timed-configuration driver: op() returns how many "units" (jobs
// or commits) it processed; runs for `duration`, returns total units ----

template <typename Op>
long run_for_units(Clock::duration duration, Op&& op) {
    auto deadline = Clock::now() + duration;
    long units = 0;
    while (Clock::now() < deadline) {
        units += op();
    }
    return units;
}

template <typename Op>
Stats benchmark_configuration(Op&& op) {
    std::vector<double> rates;
    for (int r = 0; r < kRepeats; ++r) {
        run_for_units(kWarmup, op);
        long units = run_for_units(kMeasure, op);
        rates.push_back(static_cast<double>(units) / std::chrono::duration<double>(kMeasure).count());
    }
    return compute_stats(rates);
}

void report(const std::string& measurement, const std::string& configuration, const Stats& s,
            const char* unit_label) {
    std::cout << "  " << configuration << ": median=" << s.median << " " << unit_label
              << " (min=" << s.min << ", max=" << s.max << ")\n";
    csv_row(measurement, configuration, std::string("median_") + unit_label, s.median);
    csv_row(measurement, configuration, std::string("min_") + unit_label, s.min);
    csv_row(measurement, configuration, std::string("max_") + unit_label, s.max);
}

// ---- Measurement 1: raw fsync latency ----

void measurement1_raw_fsync(const std::filesystem::path& bench_dir) {
    std::cout << "\n=== 1. Raw fsync latency ===\n";
    std::filesystem::path path = bench_dir / "raw_fsync.bin";
    std::vector<char> buf(4096, 'x');

    int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) throw std::runtime_error("open failed for raw fsync test");

    constexpr int kIterations = 2000;
    std::vector<double> latencies_ms;
    latencies_ms.reserve(kIterations);
    for (int i = 0; i < kIterations; ++i) {
        auto t0 = Clock::now();
        if (::write(fd, buf.data(), buf.size()) != static_cast<ssize_t>(buf.size())) {
            ::close(fd);
            throw std::runtime_error("short write in raw fsync test");
        }
        if (::fsync(fd) != 0) {
            ::close(fd);
            throw std::runtime_error("fsync failed in raw fsync test");
        }
        auto t1 = Clock::now();
        latencies_ms.push_back(std::chrono::duration<double, std::milli>(t1 - t0).count());
    }
    ::close(fd);
    std::filesystem::remove(path);

    double p50 = percentile(latencies_ms, 0.50);
    double p99 = percentile(latencies_ms, 0.99);
    std::cout << "  4KB write+fsync x" << kIterations << ": p50=" << p50 << "ms p99=" << p99 << "ms\n";
    csv_row("raw_fsync", "4KB_write_fsync", "p50_ms", p50);
    csv_row("raw_fsync", "4KB_write_fsync", "p99_ms", p99);
}

// ---- Measurement 2: commit loop ----

void measurement2_commit_loop(const std::filesystem::path& bench_dir) {
    std::cout << "\n=== 2. Commit loop (one INSERT per transaction) ===\n";
    for (const char* sync : {"PRAGMA synchronous=FULL;", "PRAGMA synchronous=NORMAL;",
                              "PRAGMA synchronous=OFF;"}) {
        std::filesystem::path db_path = bench_dir / "m2.db";
        remove_db_files(db_path);
        sqlite3* db = open_and_configure(db_path.string(), sync);

        int64_t counter = 0;
        auto op = [&]() -> long { do_enqueue(db, counter++); return 1; };
        Stats s = benchmark_configuration(op);
        report("commit_loop", sync, s, "commits_per_sec");

        sqlite3_close(db);
    }
}

// ---- Measurement 3: job lifecycle, unbatched (3 separate commits) ----

void measurement3_unbatched(const std::filesystem::path& bench_dir) {
    std::cout << "\n=== 3. Job lifecycle, unbatched (enqueue/claim/ack = 3 commits) ===\n";
    for (const char* sync : {"PRAGMA synchronous=FULL;", "PRAGMA synchronous=NORMAL;"}) {
        std::filesystem::path db_path = bench_dir / "m3.db";
        remove_db_files(db_path);
        sqlite3* db = open_and_configure(db_path.string(), sync);

        int64_t counter = 0;
        auto op = [&]() -> long {
            do_enqueue(db, counter++);
            auto job = do_claim(db, "default", 30000);
            if (job.found) {
                do_ack(db, job.id, job.attempts);
                return 1;
            }
            return 0;
        };
        Stats s = benchmark_configuration(op);
        report("lifecycle_unbatched", sync, s, "jobs_per_sec");

        sqlite3_close(db);
    }
}

// ---- Measurement 4: job lifecycle, batched (K per transaction) ----

void measurement4_batched(const std::filesystem::path& bench_dir) {
    std::cout << "\n=== 4. Job lifecycle, batched (K jobs per transaction) ===\n";
    for (const char* sync : {"PRAGMA synchronous=FULL;", "PRAGMA synchronous=NORMAL;"}) {
        for (int k : {1, 10, 50}) {
            std::filesystem::path db_path = bench_dir / "m4.db";
            remove_db_files(db_path);
            sqlite3* db = open_and_configure(db_path.string(), sync);

            int64_t counter = 0;
            auto op = [&]() -> long {
                exec_or_throw(db, "BEGIN;");
                for (int i = 0; i < k; ++i) do_enqueue(db, counter++);
                exec_or_throw(db, "COMMIT;");

                std::vector<ClaimedJob> claimed;
                claimed.reserve(k);
                exec_or_throw(db, "BEGIN;");
                for (int i = 0; i < k; ++i) {
                    auto job = do_claim(db, "default", 30000);
                    if (job.found) claimed.push_back(job);
                }
                exec_or_throw(db, "COMMIT;");

                exec_or_throw(db, "BEGIN;");
                for (auto& job : claimed) do_ack(db, job.id, job.attempts);
                exec_or_throw(db, "COMMIT;");

                return static_cast<long>(claimed.size());
            };
            Stats s = benchmark_configuration(op);
            std::string config = std::string(sync) + " K=" + std::to_string(k);
            report("lifecycle_batched", config, s, "jobs_per_sec");

            sqlite3_close(db);
        }
    }
}

// ---- Measurement 5: writer concurrency ----

void measurement5_concurrency(const std::filesystem::path& bench_dir) {
    std::cout << "\n=== 5. Writer concurrency (measurement 3 shape, synchronous=FULL) ===\n";
    for (int num_threads : {1, 4, 8}) {
        std::filesystem::path db_path = bench_dir / "m5.db";
        remove_db_files(db_path);
        // Schema/pragmas created once up front so threads don't race to
        // create the table -- each thread then opens its own connection.
        sqlite3* setup_db = open_and_configure(db_path.string(), "PRAGMA synchronous=FULL;");
        sqlite3_close(setup_db);

        g_busy_retries.store(0, std::memory_order_relaxed);
        std::vector<double> per_thread_rates(num_threads);
        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, t]() {
                sqlite3* db = open_and_configure(db_path.string(), "PRAGMA synchronous=FULL;");
                int64_t counter = static_cast<int64_t>(t) << 32;  // disjoint id space per thread
                auto op = [&]() -> long {
                    do_enqueue(db, counter++);
                    auto job = do_claim(db, "default", 30000);
                    if (job.found) {
                        do_ack(db, job.id, job.attempts);
                        return 1;
                    }
                    return 0;
                };
                run_for_units(kWarmup, op);
                long units = run_for_units(kMeasure, op);
                per_thread_rates[t] = static_cast<double>(units) /
                                       std::chrono::duration<double>(kMeasure).count();
                sqlite3_close(db);
            });
        }
        for (auto& th : threads) th.join();

        double total_rate = 0.0;
        for (double r : per_thread_rates) total_rate += r;
        long busy_retries = g_busy_retries.load(std::memory_order_relaxed);

        std::string config = "threads=" + std::to_string(num_threads);
        std::cout << "  " << config << ": total=" << total_rate << " jobs/sec, "
                  << busy_retries << " SQLITE_BUSY retries\n";
        csv_row("concurrency", config, "total_jobs_per_sec", total_rate);
        csv_row("concurrency", config, "busy_retries", static_cast<double>(busy_retries));
    }
}

// ---- environment info ----

std::string shell_capture(const std::string& cmd) {
    std::array<char, 256> buf{};
    std::string result;
    FILE* pipe = popen(cmd.c_str(), "r");
    if (!pipe) return "unknown";
    while (fgets(buf.data(), buf.size(), pipe) != nullptr) {
        result += buf.data();
    }
    pclose(pipe);
    while (!result.empty() && (result.back() == '\n' || result.back() == '\r')) result.pop_back();
    return result;
}

void write_env_file(const std::filesystem::path& path, const std::filesystem::path& bench_dir) {
    std::ofstream f(path);
    f << "nproc: " << std::thread::hardware_concurrency() << "\n";

    utsname uts{};
    if (uname(&uts) == 0) {
        f << "kernel: " << uts.sysname << " " << uts.release << " " << uts.machine << "\n";
    } else {
        f << "kernel: unknown\n";
    }

    f << "sqlite_version: " << sqlite3_libversion() << "\n";

    std::string fstype = shell_capture("findmnt -no FSTYPE --target " + bench_dir.string());
    f << "db_filesystem: " << fstype << " (" << bench_dir.string() << ")\n";
}

}  // namespace

int main() {
    const char* home = std::getenv("HOME");
    std::filesystem::path bench_dir = std::filesystem::path(home ? home : ".") / "bench";
    std::filesystem::create_directories(bench_dir);

    std::filesystem::path results_dir = std::filesystem::path(__FILE__).parent_path() / "results";
    std::filesystem::create_directories(results_dir);

    g_csv.open(results_dir / "raw_sqlite.csv");
    g_csv << "measurement,configuration,stat,value\n";

    write_env_file(results_dir / "env.txt", bench_dir);

    std::cout << "bench dir: " << bench_dir << "\n";
    std::cout << "results:   " << results_dir << "\n";

    measurement1_raw_fsync(bench_dir);
    measurement2_commit_loop(bench_dir);
    measurement3_unbatched(bench_dir);
    measurement4_batched(bench_dir);
    measurement5_concurrency(bench_dir);

    g_csv.close();
    std::cout << "\ndone. CSV: " << (results_dir / "raw_sqlite.csv") << "\n";
    return 0;
}
