#pragma once

#include <chrono>
#include <cstdint>

namespace jobqueue {

// Lease = 30s, reap interval = 5s.
// Worst case: a job abandoned by a dead worker sits unavailable for up to
// 35s (30s lease + 5s until the next reaper check) before being requeued.
// Acceptable for this system's throughput profile.
constexpr int64_t kLeaseMs = 30000;
constexpr auto kReapInterval = std::chrono::milliseconds(5000);

}  // namespace jobqueue
