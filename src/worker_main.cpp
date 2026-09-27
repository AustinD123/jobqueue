#include "jobqueue/worker.hpp"

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

struct Args {
    std::string host = "127.0.0.1";
    uint16_t port = 0;
    bool port_set = false;
    std::string queue = "default";
    int concurrency = 1;
    int64_t job_ms = 10;
    int64_t lease_ms = 30000;
};

Args parse_args(int argc, char** argv) {
    Args args;
    for (int i = 1; i < argc; ++i) {
        std::string key = argv[i];
        auto next_value = [&]() -> std::string {
            if (i + 1 >= argc) {
                throw std::runtime_error("missing value for " + key);
            }
            return argv[++i];
        };

        if (key == "--host") {
            args.host = next_value();
        } else if (key == "--port") {
            args.port = static_cast<uint16_t>(std::atoi(next_value().c_str()));
            args.port_set = true;
        } else if (key == "--queue") {
            args.queue = next_value();
        } else if (key == "--concurrency") {
            args.concurrency = std::atoi(next_value().c_str());
        } else if (key == "--job-ms") {
            args.job_ms = std::atoll(next_value().c_str());
        } else if (key == "--lease-ms") {
            args.lease_ms = std::atoll(next_value().c_str());
        } else {
            throw std::runtime_error("unknown argument: " + key);
        }
    }

    if (!args.port_set) {
        throw std::runtime_error("--port is required");
    }
    return args;
}

void print_usage(const char* prog) {
    std::cerr << "usage: " << prog
              << " --port PORT [--host 127.0.0.1] [--queue default]"
                 " [--concurrency 1] [--job-ms 10] [--lease-ms 30000]\n";
}

}  // namespace

int main(int argc, char** argv) {
    Args args;
    try {
        args = parse_args(argc, argv);
    } catch (const std::exception& ex) {
        std::cerr << "error: " << ex.what() << "\n";
        print_usage(argv[0]);
        return 1;
    }

    std::vector<std::thread> threads;
    threads.reserve(args.concurrency);
    for (int i = 0; i < args.concurrency; ++i) {
        // worker_loop() is unimplemented right now and always throws --
        // catching here (rather than letting it escape the thread
        // function) is what keeps that from taking down every other
        // worker thread via std::terminate, same reasoning as the
        // broker's per-connection try/catch. Once worker_loop() is a
        // real long-running loop, this becomes "one worker's fatal error
        // doesn't kill the whole fleet."
        threads.emplace_back([&args, i]() {
            try {
                jobqueue::worker_loop(args.host, args.port, args.queue, args.job_ms,
                                       args.lease_ms);
            } catch (const std::exception& ex) {
                std::cerr << "worker thread " << i << " exited: " << ex.what() << "\n";
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }

    return 0;
}
