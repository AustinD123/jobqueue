#include "jobqueue/broker.hpp"
#include "jobqueue/dispatch.hpp"

#include <cerrno>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace jobqueue {

Broker::Broker(std::string db_path, uint16_t port, RetryPolicy policy)
    : db_path_(std::move(db_path)),
      port_(port),
      policy_(policy),
      listen_fd_(-1),
      running_(false) {}

void Broker::run() {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        throw std::runtime_error(std::string("socket() failed: ") + std::strerror(errno));
    }

    int opt = 1;
    if (setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        throw std::runtime_error(std::string("setsockopt(SO_REUSEADDR) failed: ") +
                                  std::strerror(errno));
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);

    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        throw std::runtime_error(std::string("bind() failed: ") + std::strerror(errno));
    }

    if (listen(listen_fd_, 16) < 0) {
        throw std::runtime_error(std::string("listen() failed: ") + std::strerror(errno));
    }

    running_ = true;
    while (running_) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
        if (client_fd < 0) {
            if (!running_) {
                break;  // stop() closed listen_fd_ out from under us -- expected
            }
            continue;  // transient accept() error, keep serving
        }
        std::thread(&Broker::handle_connection, this, client_fd).detach();
    }
}

void Broker::stop() {
    // Both operations here (an atomic store and close()) are
    // async-signal-safe, so this is safe to call directly from a signal
    // handler (e.g. SIGINT/SIGTERM), not just from another thread.
    running_ = false;
    close(listen_fd_);
}

void Broker::handle_connection(int client_fd) {
    // Own connection, per our connection-per-thread design -- not shared
    // with the accept loop's thread or any other connection's thread.
    // Constructing it can throw (e.g. the SQLite busy-lock race under
    // concurrent connection setup) -- same reasoning as the dispatch()
    // try/catch below: an exception escaping a std::thread's entry
    // function is fatal to the whole process, not just this connection,
    // so this needs its own guard, not just the request loop's.
    std::unique_ptr<Engine> engine;
    try {
        engine = std::make_unique<Engine>(db_path_, policy_);
    } catch (const std::exception& ex) {
        nlohmann::json response = {{"ok", false}, {"error", ex.what()}};
        std::string out = response.dump() + "\n";
        send(client_fd, out.c_str(), out.size(), 0);
        close(client_fd);
        return;
    }

    std::string buffer;
    char chunk[4096];

    for (;;) {
        ssize_t n = recv(client_fd, chunk, sizeof(chunk), 0);
        if (n <= 0) {
            break;  // 0 = peer closed, <0 = error -- either way, we're done
        }
        buffer.append(chunk, static_cast<size_t>(n));

        size_t newline;
        while ((newline = buffer.find('\n')) != std::string::npos) {
            std::string line = buffer.substr(0, newline);
            buffer.erase(0, newline + 1);

            // dispatch() can throw on bad input (missing fields, unknown
            // command, etc.) -- an exception escaping a std::thread's
            // entry function calls std::terminate and takes down the
            // whole broker process, not just this connection, so every
            // request is answered with either a real response or an
            // error response, never left to propagate.
            nlohmann::json response;
            try {
                nlohmann::json request = nlohmann::json::parse(line);
                response = dispatch(request, *engine);
            } catch (const std::exception& ex) {
                response = {{"ok", false}, {"error", ex.what()}};
            }

            std::string out = response.dump() + "\n";
            send(client_fd, out.c_str(), out.size(), 0);
        }
    }

    close(client_fd);
}

}  // namespace jobqueue
