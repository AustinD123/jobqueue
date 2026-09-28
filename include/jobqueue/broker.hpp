#pragma once

#include "jobqueue/engine.hpp"
#include "jobqueue/reaper.hpp"

#include <atomic>
#include <cstdint>
#include <string>

namespace jobqueue {

class Broker {
public:
    Broker(std::string db_path, uint16_t port, RetryPolicy policy = {});
    ~Broker();

    void run();   // blocks: bind, listen, accept loop
    void stop();  // NOT purely signal-safe anymore -- see .cpp

private:
    void handle_connection(int client_fd);

    std::string db_path_;
    uint16_t port_;
    RetryPolicy policy_;
    int listen_fd_;
    std::atomic<bool> running_;
    LeaseReaper reaper_;
};

}  // namespace jobqueue
