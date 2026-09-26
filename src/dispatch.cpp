#include "jobqueue/dispatch.hpp"
#include "jobqueue/engine.hpp"
#include <nlohmann/json.hpp>

namespace jobqueue {

// ---- helpers first ----

static nlohmann::json job_to_json(const Job& job) {
    return {
        {"id", job.id},
        {"queue", job.queue},
        {"payload", job.payload},
        {"priority", job.priority},
        {"attempts", job.attempts},
        {"status", to_string(job.status)},
        {"run_after", job.run_after},
        {"created_at", job.created_at}
    };
}

static nlohmann::json ack_nack_response(bool success) {
    if (success) return {{"ok", true}};
    return {{"ok", false}, {"error", "job not leased or already resolved"}};
}

// ---- dispatch ----

nlohmann::json dispatch(const nlohmann::json& req, Engine& engine) {
    std::string cmd = req.at("cmd");

    if (cmd == "enqueue") {
        int64_t id = engine.enqueue(req.at("queue").get<std::string>(),
                                     req.at("payload").get<std::string>(),
                                     req.value("priority", 0));
        return {{"ok", true}, {"id", id}};
    }

    if (cmd == "claim") {
        auto job = engine.claim(req.at("queue").get<std::string>(),
                                 req.at("lease_ms").get<int64_t>());
        if (!job) return {{"ok", true}, {"job", nullptr}};
        return {{"ok", true}, {"job", job_to_json(*job)}};
    }

    if (cmd == "ack") {
        return ack_nack_response(engine.ack(req.at("job_id").get<int64_t>()));
    }

    if (cmd == "nack") {
        return ack_nack_response(engine.nack(req.at("job_id").get<int64_t>()));
    }

    if (cmd == "stats") {
        auto s = engine.stats(req.at("queue").get<std::string>());
        return {{"ok", true}, {"ready", s.ready}, {"leased", s.leased},
                 {"dead", s.dead}, {"done", s.done}};
    }

    return {{"ok", false}, {"error", "unknown command: " + cmd}};
}

}  // namespace jobqueue