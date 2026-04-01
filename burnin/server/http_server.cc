#include "server/http_server.h"
#include "server/handlers.h"

#include <iostream>

namespace burnin {

HttpServer::HttpServer(int port, Engine& engine)
    : port_(port), engine_(engine) {
    SetupRoutes();
}

HttpServer::~HttpServer() {
    Stop();
}

void HttpServer::SetupRoutes() {
    // Handle OPTIONS preflight for all routes
    server_.Options(".*", [](const httplib::Request&, httplib::Response& res) {
        SetCorsHeaders(res);
        res.status = 204;
    });

    // Health & readiness
    server_.Get("/health", [this](const httplib::Request& req, httplib::Response& res) {
        HandleHealth(engine_, req, res);
    });

    server_.Get("/ready", [this](const httplib::Request& req, httplib::Response& res) {
        HandleReady(engine_, req, res);
    });

    // Metrics (Prometheus-style text)
    server_.Get("/metrics", [this](const httplib::Request& req, httplib::Response& res) {
        HandleMetrics(engine_, req, res);
    });

    // Info
    server_.Get("/info", [this](const httplib::Request& req, httplib::Response& res) {
        HandleInfo(engine_, req, res);
    });

    // Broker status
    server_.Get("/broker/status", [this](const httplib::Request& req, httplib::Response& res) {
        HandleBrokerStatus(engine_, req, res);
    });

    // Run lifecycle
    server_.Post("/run/start", [this](const httplib::Request& req, httplib::Response& res) {
        HandleRunStart(engine_, req, res);
    });

    server_.Post("/run/stop", [this](const httplib::Request& req, httplib::Response& res) {
        HandleRunStop(engine_, req, res);
    });

    server_.Get("/run", [this](const httplib::Request& req, httplib::Response& res) {
        HandleRun(engine_, req, res);
    });

    server_.Get("/run/status", [this](const httplib::Request& req, httplib::Response& res) {
        HandleRunStatus(engine_, req, res);
    });

    server_.Get("/run/config", [this](const httplib::Request& req, httplib::Response& res) {
        HandleRunConfig(engine_, req, res);
    });

    server_.Get("/run/report", [this](const httplib::Request& req, httplib::Response& res) {
        HandleRunReport(engine_, req, res);
    });

    // Cleanup
    server_.Post("/cleanup", [this](const httplib::Request& req, httplib::Response& res) {
        HandleCleanup(engine_, req, res);
    });

    // Legacy aliases
    server_.Get("/status", [this](const httplib::Request& req, httplib::Response& res) {
        HandleLegacyStatus(engine_, req, res);
    });

    server_.Get("/summary", [this](const httplib::Request& req, httplib::Response& res) {
        HandleLegacySummary(engine_, req, res);
    });
}

void HttpServer::Start() {
    std::cout << "[HttpServer] Listening on port " << port_ << "\n";
    server_.listen("0.0.0.0", port_);
}

void HttpServer::Stop() {
    server_.stop();
}

}  // namespace burnin
