#pragma once

#include "jobqueue/engine.hpp"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>

namespace jobqueue {

class LeaseReaper {
public:
    LeaseReaper(std::string db_path, std::chrono::milliseconds interval,
                RetryPolicy policy = {});
    ~LeaseReaper();

    LeaseReaper(const LeaseReaper&) = delete;
    LeaseReaper& operator=(const LeaseReaper&) = delete;

    void start();
    void stop();  // safe to call from the destructor; joins the thread

private:
    void run();

    std::string db_path_;
    std::chrono::milliseconds interval_;
    RetryPolicy policy_;

    std::thread thread_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool running_ = false;
    bool stop_requested_ = false;
};

}  // namespace jobqueue
