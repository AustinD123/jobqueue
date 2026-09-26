#pragma once

#include "jobqueue/engine.hpp"

#include <nlohmann/json.hpp>

namespace jobqueue {

// Implemented by hand elsewhere -- this declaration exists so broker.cpp
// can call it and link, while the real request-handling logic is written
// separately.
nlohmann::json dispatch(const nlohmann::json& request, Engine& engine);

}  // namespace jobqueue
