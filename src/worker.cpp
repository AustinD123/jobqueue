#include "jobqueue/worker.hpp"
#include "jobqueue/client_conn.hpp"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>

namespace jobqueue {

namespace {

constexpr auto kInitialReconnectBackoff = std::chrono::milliseconds(100);
constexpr auto kMaxReconnectBackoff = std::chrono::milliseconds(5000);

}  // namespace

void worker_loop(const std::string& host, uint16_t port, const std::string& queue,
                  int64_t job_ms, int64_t lease_ms) {
    auto reconnect_backoff = kInitialReconnectBackoff;

    // Retries until a connection succeeds -- a single failed attempt
    // (broker still restarting, not back up yet) must not give up after
    // one try, or the doubling-backoff schedule below would be pointless.
    auto reconnect = [&]() -> std::unique_ptr<ClientConnection> {
        for (;;) {
            try {
                auto new_conn = std::make_unique<ClientConnection>(host, port);
                reconnect_backoff = kInitialReconnectBackoff;  // reset on success
                return new_conn;
            } catch (const std::exception& ex) {
                std::cerr << "worker: reconnect attempt failed (" << ex.what()
                          << "), retrying in " << reconnect_backoff.count() << "ms\n";
                std::this_thread::sleep_for(reconnect_backoff);
                reconnect_backoff = std::min(reconnect_backoff * 2, kMaxReconnectBackoff);
            }
        }
    };

    auto conn = reconnect();

    for (;;) {
        nlohmann::json claim_resp;
        try {
            claim_resp =
                conn->send_request({{"cmd", "claim"}, {"queue", queue}, {"lease_ms", lease_ms}});
        } catch (const std::exception& ex) {
            // Connection died before we held anything -- nothing to drop,
            // just reconnect and try again.
            std::cerr << "worker: connection error during claim (" << ex.what()
                      << "), reconnecting\n";
            conn = reconnect();
            continue;
        }

        if (!claim_resp.at("ok").get<bool>()) {
            throw std::runtime_error("claim failed");
        }
        if (claim_resp.at("job").is_null()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }

        int64_t job_id = claim_resp.at("job").at("id").get<int64_t>();
        int attempt = claim_resp.at("job").at("attempts").get<int>();
        std::this_thread::sleep_for(std::chrono::milliseconds(job_ms));

        nlohmann::json ack_resp;
        try {
            ack_resp = conn->send_request(
                {{"cmd", "ack"}, {"queue", queue}, {"job_id", job_id}, {"attempt", attempt}});
        } catch (const std::exception& ex) {
            // Connection died with a job still held. Do NOT retry the
            // ack/nack over the rebuilt connection -- we can't tell
            // whether the original ack actually landed before the
            // connection broke, so retrying risks double-acking, or
            // acking a job someone else has since reclaimed after its
            // lease expired. Drop it and move on; the lease reaper will
            // eventually reclaim it if the ack never went through.
            std::cerr << "worker: connection error while acking job " << job_id << " ("
                      << ex.what() << "), dropping it and reconnecting\n";
            conn = reconnect();
            continue;
        }

        if (!ack_resp.at("ok").get<bool>()) {
            throw std::runtime_error("ack failed");
        }
    }
}

}  // namespace jobqueue
