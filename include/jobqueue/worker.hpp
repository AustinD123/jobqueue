#pragma once

#include <cstdint>
#include <string>

namespace jobqueue {

// Implemented by hand elsewhere -- this declaration exists so
// worker_main.cpp can call it and link, while the real claim/process/ack
// loop is written separately.
void worker_loop(const std::string& host, uint16_t port, const std::string& queue,
                  int64_t job_ms, int64_t lease_ms);

}  // namespace jobqueue
