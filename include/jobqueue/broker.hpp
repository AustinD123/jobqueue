#pragma once

#include "jobqueue/engine.hpp"

#include <atomic>
#include <cstdint>
#include <string>

namespace jobqueue {

class Broker {
public:
    Broker(std::string db_path, uint16_t port, RetryPolicy policy = {});

    void run();   // blocks: bind, listen, accept loop
    void stop();  // signal-safe shutdown, closes listening socket

private:
    void handle_connection(int client_fd);

    std::string db_path_;
    uint16_t port_;
    RetryPolicy policy_;
    int listen_fd_;
    std::atomic<bool> running_;
};

}  // namespace jobqueue
