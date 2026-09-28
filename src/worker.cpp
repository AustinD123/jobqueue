#include "jobqueue/worker.hpp"
#include "jobqueue/client_conn.hpp"

#include <chrono>
#include <stdexcept>
#include <thread>

namespace jobqueue {

void worker_loop(const std::string& host, uint16_t port, const std::string& queue,
                  int64_t job_ms, int64_t lease_ms) {
    ClientConnection conn(host, port);

    for (;;) {
        auto claim_resp = conn.send_request({{"cmd", "claim"}, {"queue", queue}, {"lease_ms", lease_ms}});
        // TODO: check claim_resp.at("ok") -- what should happen if it's false?
        if (!claim_resp.at("ok").get<bool>()) {
            throw std::runtime_error("claim failed");
        }
        if (claim_resp.at("job").is_null()) {
            // TODO: sleep some poll interval, then `continue;`
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;  // Example poll interval
        }

        int64_t job_id = claim_resp.at("job").at("id").get<int64_t>();
        int attempt = claim_resp.at("job").at("attempts").get<int>();
        // TODO: "do the work" -- sleep for job_ms
        std::this_thread::sleep_for(std::chrono::milliseconds(job_ms));

        // TODO: ack the job, check the response
        auto ack_resp = conn.send_request(
            {{"cmd", "ack"}, {"queue", queue}, {"job_id", job_id}, {"attempt", attempt}});
        if (!ack_resp.at("ok").get<bool>()) {
            throw std::runtime_error("ack failed");
        }
    }
}


}  // namespace jobqueue
