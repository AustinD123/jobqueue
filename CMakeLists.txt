cmake_minimum_required(VERSION 3.20)
project(jobqueue LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(PkgConfig REQUIRED)
pkg_check_modules(SQLITE3 REQUIRED sqlite3>=3.35)
find_package(Threads REQUIRED)

add_library(jobqueue_engine STATIC
    src/engine.cpp
    src/reaper.cpp
)
target_include_directories(jobqueue_engine PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
target_include_directories(jobqueue_engine PRIVATE
    ${SQLITE3_INCLUDE_DIRS}
)
target_link_libraries(jobqueue_engine PUBLIC
    ${SQLITE3_LIBRARIES}
    Threads::Threads
)
target_compile_options(jobqueue_engine PRIVATE
    ${SQLITE3_CFLAGS_OTHER}
)

enable_testing()

add_executable(engine_tests
    tests/engine_tests.cpp
)
target_link_libraries(engine_tests PRIVATE
    jobqueue_engine
)

add_test(NAME engine_tests COMMAND engine_tests)

add_executable(claim_race_test
    tests/claim_race_test.cpp
)
target_link_libraries(claim_race_test PRIVATE
    jobqueue_engine
    Threads::Threads
)

add_test(NAME claim_race_test COMMAND claim_race_test)

# Broker: plain POSIX sockets, Linux-only scope by design (see
# src/broker.cpp) -- guarded so `cmake configure` still succeeds on
# non-Unix dev machines that don't have nlohmann_json installed and
# aren't building this target anyway.
if(UNIX)
    find_package(nlohmann_json REQUIRED)

    add_executable(broker
        src/broker.cpp
        src/broker_main.cpp
        src/dispatch.cpp
    )
    target_link_libraries(broker PRIVATE
        jobqueue_engine
        nlohmann_json::nlohmann_json
    )

    add_executable(worker
        src/worker_main.cpp
        src/worker.cpp
        src/client_conn.cpp
    )
    target_include_directories(worker PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    )
    target_link_libraries(worker PRIVATE
        nlohmann_json::nlohmann_json
        Threads::Threads
    )
endif()
