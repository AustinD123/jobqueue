#include "jobqueue/client_conn.hpp"

#include <cerrno>
#include <cstring>
#include <stdexcept>

#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

namespace jobqueue {

ClientConnection::ClientConnection(const std::string& host, uint16_t port) : fd_(-1) {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo* result = nullptr;
    // getaddrinfo (not gethostbyname) since worker_main spawns multiple
    // concurrent threads, each constructing their own ClientConnection --
    // gethostbyname isn't thread-safe, getaddrinfo is.
    int rc = getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &result);
    if (rc != 0) {
        throw std::runtime_error(std::string("getaddrinfo() failed: ") + gai_strerror(rc));
    }

    fd_ = socket(result->ai_family, result->ai_socktype, result->ai_protocol);
    if (fd_ < 0) {
        std::string err = std::strerror(errno);
        freeaddrinfo(result);
        throw std::runtime_error("socket() failed: " + err);
    }

    if (connect(fd_, result->ai_addr, result->ai_addrlen) < 0) {
        std::string err = std::strerror(errno);
        freeaddrinfo(result);
        close(fd_);
        throw std::runtime_error("connect() failed: " + err);
    }

    freeaddrinfo(result);
}

ClientConnection::~ClientConnection() {
    if (fd_ >= 0) {
        close(fd_);
    }
}

nlohmann::json ClientConnection::send_request(nlohmann::json req) {
    std::string out = req.dump() + "\n";
    size_t sent_total = 0;
    while (sent_total < out.size()) {
        ssize_t n = send(fd_, out.data() + sent_total, out.size() - sent_total, 0);
        if (n <= 0) {
            throw std::runtime_error(std::string("send() failed: ") + std::strerror(errno));
        }
        sent_total += static_cast<size_t>(n);
    }

    std::string buffer;
    char chunk[4096];
    while (buffer.find('\n') == std::string::npos) {
        ssize_t n = recv(fd_, chunk, sizeof(chunk), 0);
        if (n < 0) {
            throw std::runtime_error(std::string("recv() failed: ") + std::strerror(errno));
        }
        if (n == 0) {
            throw std::runtime_error("connection closed before a full response line was received");
        }
        buffer.append(chunk, static_cast<size_t>(n));
    }

    std::string line = buffer.substr(0, buffer.find('\n'));
    return nlohmann::json::parse(line);  // throws nlohmann::json::parse_error on malformed input
}

}  // namespace jobqueue
