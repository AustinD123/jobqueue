#include "jobqueue/engine.hpp"

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <unordered_set>

namespace {

// RAII helper: gives this run a fresh, unique SQLite file and deletes it
// (plus any WAL/SHM sidecars) when the test finishes.
class TempDbFile {
public:
    TempDbFile()
        : path_((std::filesystem::temp_directory_path() /
                 "jobqueue_claim_race_test.db")
                    .string()) {
        std::filesystem::remove(path_);
        std::filesystem::remove(path_ + "-wal");
        std::filesystem::remove(path_ + "-shm");
    }
    ~TempDbFile() {
        std::filesystem::remove(path_);
        std::filesystem::remove(path_ + "-wal");
        std::filesystem::remove(path_ + "-shm");
    }
    const std::string& path() const { return path_; }

private:
    std::string path_;
};

}  // namespace

int main(int argc, char** argv) {
    // Parametrized via optional CLI args; ctest runs with the defaults.
    const int num_threads = (argc > 1) ? std::atoi(argv[1]) : 8;   // N
    const int num_jobs = (argc > 2) ? std::atoi(argv[2]) : 1000;   // M
    const int64_t lease_ms = 30000;

    TempDbFile db;

    // Seed M jobs before any worker thread starts claiming.
    {
        jobqueue::Engine seeder(db.path());
        for (int i = 0; i < num_jobs; ++i) {
            seeder.enqueue("default", "payload-" + std::to_string(i));
        }
    }

    std::mutex claimed_mutex;
    std::vector<int64_t> claimed_ids;
    std::vector<std::string> errors;

    auto worker = [&]() {
        // Each thread gets its own Engine (its own SQLite connection) over
        // the same db file -- do not share one Engine across threads.
        // Exceptions are caught here (not asserted on) purely so one
        // thread hitting an error doesn't std::terminate the whole
        // process via an uncaught exception escaping a thread function --
        // that would silently discard every other thread's results too.
        try {
            jobqueue::Engine engine(db.path());
            for (;;) {
                auto job = engine.claim("default", lease_ms);
                if (!job.has_value()) {
                    break;  // queue drained, from this thread's point of view
                }
                std::lock_guard<std::mutex> lock(claimed_mutex);
                claimed_ids.push_back(job->id);
            }
        } catch (const std::exception& ex) {
            std::lock_guard<std::mutex> lock(claimed_mutex);
            errors.push_back(ex.what());
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker);
    }
    for (auto& t : threads) {
        t.join();
    }

    std::cout << "spawned " << num_threads << " threads, seeded " << num_jobs
              << " jobs, claimed " << claimed_ids.size() << " total, "
              << errors.size() << " thread(s) hit an exception\n";
    for (const auto& e : errors) {
        std::cout << "  - " << e << "\n";
    }

    // TODO (mine to fill in):
    // - assert claimed_ids.size() == static_cast<size_t>(num_jobs)
    // - assert no duplicate ids in claimed_ids (sort + adjacent_find, or a
    //   std::set/unordered_set insert-and-check) -- this is the actual
    //   double-claim property claim() is supposed to guarantee.
    // - decide what you want to assert about `errors` -- e.g. should this
    //   test require errors.empty(), or is that a separate bug to track?
    if (claimed_ids.size() != static_cast<size_t>(num_jobs)) {
        std::cerr << "FAIL: expected " << num_jobs << " claims, got " << claimed_ids.size() << "\n";
        return 1;
    }

    std::unordered_set<int64_t> seen;
    for (int64_t id : claimed_ids) {
        if (!seen.insert(id).second) {
            std::cerr << "FAIL: job " << id << " was claimed more than once\n";
            return 1;
        }
    }

    std::cout << "OK: " << claimed_ids.size() << " jobs claimed, all unique\n";

    return 0;
}
