#pragma once

#include <cstdint>
#include <string>

#include <nlohmann/json.hpp>

namespace jobqueue {

// Client-side mirror of the broker's per-connection loop: one send, one
// blocking read of a full newline-terminated line, parsed as JSON.
class ClientConnection {
public:
    ClientConnection(const std::string& host, uint16_t port);
    ~ClientConnection();

    ClientConnection(const ClientConnection&) = delete;
    ClientConnection& operator=(const ClientConnection&) = delete;

    nlohmann::json send_request(nlohmann::json req);

private:
    int fd_;
};

}  // namespace jobqueue
