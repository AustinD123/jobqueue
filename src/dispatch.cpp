#include "jobqueue/dispatch.hpp"

#include <stdexcept>

namespace jobqueue {

nlohmann::json dispatch(const nlohmann::json& request, Engine& engine) {
    (void)request;
    (void)engine;
    throw std::logic_error("not implemented");
}

}  // namespace jobqueue
