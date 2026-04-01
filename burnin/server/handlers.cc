#include "server/handlers.h"

#include <sstream>

namespace burnin {

void SetCorsHeaders(httplib::Response& res) {
    res.set_header("Access-Control-Allow-Origin", "*");
    res.set_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.set_header("Access-Control-Allow-Headers", "Content-Type");
}

void JsonResponse(httplib::Response& res, int status_code, const nlohmann::json& body) {
    SetCorsHeaders(res);
    res.status = status_code;
    res.set_content(body.dump(), "application/json");
}

// GET /health
void HandleHealth(Engine& /*engine*/, const httplib::Request& /*req*/, httplib::Response& res) {
    JsonResponse(res, 200, {{"status", "alive"}});
}

// GET /ready
void HandleReady(Engine& engine, const httplib::Request& /*req*/, httplib::Response& res) {
    std::string state = engine.State();
    bool ready = (state == kStateIdle || state == kStateRunning ||
                  state == kStateStopped);
    JsonResponse(res, 200, {{"status", ready ? "ready" : "not_ready"}, {"state", state}});
}

// GET /metrics
void HandleMetrics(Engine& engine, const httplib::Request& /*req*/, httplib::Response& res) {
    SetCorsHeaders(res);

    // Simple Prometheus-compatible text format
    auto snapshot = engine.GetRunStatus();
    std::ostringstream ss;

    ss << "# HELP kubemq_burnin_state Current state of the burn-in engine\n";
    ss << "# TYPE kubemq_burnin_state gauge\n";
    ss << "kubemq_burnin_state{state=\"" << engine.State() << "\"} 1\n";

    if (snapshot.contains("metrics") && snapshot["metrics"].contains("_totals")) {
        auto& totals = snapshot["metrics"]["_totals"];
        ss << "# HELP kubemq_burnin_sent_total Total messages sent\n";
        ss << "# TYPE kubemq_burnin_sent_total counter\n";
        ss << "kubemq_burnin_sent_total " << totals.value("sent", 0) << "\n";

        ss << "# HELP kubemq_burnin_received_total Total messages received\n";
        ss << "# TYPE kubemq_burnin_received_total counter\n";
        ss << "kubemq_burnin_received_total " << totals.value("received", 0) << "\n";

        ss << "# HELP kubemq_burnin_errors_total Total errors\n";
        ss << "# TYPE kubemq_burnin_errors_total counter\n";
        ss << "kubemq_burnin_errors_total " << totals.value("errors", 0) << "\n";
    }

    res.status = 200;
    res.set_content(ss.str(), "text/plain; charset=utf-8");
}

// GET /info
void HandleInfo(Engine& engine, const httplib::Request& /*req*/, httplib::Response& res) {
    JsonResponse(res, 200, engine.GetInfo());
}

// GET /broker/status
void HandleBrokerStatus(Engine& engine, const httplib::Request& /*req*/, httplib::Response& res) {
    JsonResponse(res, 200, engine.GetBrokerStatus());
}

// POST /run/start
void HandleRunStart(Engine& engine, const httplib::Request& req, httplib::Response& res) {
    Config config;
    if (!req.body.empty()) {
        try {
            config = Config::FromJson(req.body);
        } catch (const std::exception& e) {
            JsonResponse(res, 400, {{"error", std::string("invalid config: ") + e.what()}});
            return;
        }
    } else {
        config.SetDefaults();
    }

    auto result = engine.StartRun(config);
    if (!result.error.empty()) {
        JsonResponse(res, 409, {{"error", result.error}});
        return;
    }

    JsonResponse(res, 202, {
        {"run_id", result.run_id},
        {"enabled_patterns", result.enabled_count},
        {"status", "starting"},
    });
}

// POST /run/stop
void HandleRunStop(Engine& engine, const httplib::Request& /*req*/, httplib::Response& res) {
    auto error = engine.StopRun();
    if (!error.empty()) {
        JsonResponse(res, 409, {{"error", error}});
        return;
    }
    JsonResponse(res, 202, {{"status", "stopping"}});
}

// GET /run
void HandleRun(Engine& engine, const httplib::Request& /*req*/, httplib::Response& res) {
    JsonResponse(res, 200, engine.GetRunFull());
}

// GET /run/status
void HandleRunStatus(Engine& engine, const httplib::Request& /*req*/, httplib::Response& res) {
    JsonResponse(res, 200, engine.GetRunStatus());
}

// GET /run/config
void HandleRunConfig(Engine& engine, const httplib::Request& /*req*/, httplib::Response& res) {
    auto [config, found] = engine.GetRunConfig();
    if (!found) {
        JsonResponse(res, 404, {{"error", "no run configuration available"}});
        return;
    }
    JsonResponse(res, 200, config);
}

// GET /run/report
void HandleRunReport(Engine& engine, const httplib::Request& /*req*/, httplib::Response& res) {
    auto [report, found] = engine.GetRunReport();
    if (!found) {
        JsonResponse(res, 404, {{"error", "no run report available"}});
        return;
    }
    JsonResponse(res, 200, report);
}

// POST /cleanup
void HandleCleanup(Engine& engine, const httplib::Request& /*req*/, httplib::Response& res) {
    auto result = engine.CleanupChannels();
    if (result.contains("error")) {
        JsonResponse(res, 409, result);
        return;
    }
    JsonResponse(res, 200, result);
}

// GET /status (legacy)
void HandleLegacyStatus(Engine& engine, const httplib::Request& req, httplib::Response& res) {
    HandleRunStatus(engine, req, res);
}

// GET /summary (legacy)
void HandleLegacySummary(Engine& engine, const httplib::Request& req, httplib::Response& res) {
    HandleRunReport(engine, req, res);
}

}  // namespace burnin
