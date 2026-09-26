#include "jobqueue/broker.hpp"

#include <cstdlib>
#include <iostream>

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: " << argv[0] << " <port> [db_path]\n";
        return 1;
    }
    uint16_t port = static_cast<uint16_t>(std::atoi(argv[1]));
    std::string db_path = argc > 2 ? argv[2] : "jobqueue.db";

    jobqueue::Broker broker(db_path, port);
    broker.run();
    return 0;
}
