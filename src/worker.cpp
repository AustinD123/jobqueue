#include "jobqueue/worker.hpp"

#include <stdexcept>

namespace jobqueue {

void worker_loop(const std::string& host, uint16_t port, const std::string& queue,
                  int64_t job_ms, int64_t lease_ms) {
    (void)host;
    (void)port;
    (void)queue;
    (void)job_ms;
    (void)lease_ms;
    throw std::logic_error("not implemented");
}

}  // namespace jobqueue
