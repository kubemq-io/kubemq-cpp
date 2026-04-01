#pragma once

#include <memory>
#include <string>

#include <httplib.h>

#include "engine/engine.h"

namespace burnin {

// HttpServer wraps cpp-httplib and sets up all REST routes.
class HttpServer {
public:
    HttpServer(int port, Engine& engine);
    ~HttpServer();

    // Start listening (blocking). Call from a dedicated thread.
    void Start();

    // Stop the server gracefully.
    void Stop();

private:
    void SetupRoutes();

    int port_;
    Engine& engine_;
    httplib::Server server_;
};

}  // namespace burnin
