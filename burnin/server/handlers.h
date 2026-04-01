#pragma once

#include <httplib.h>

#include "engine/engine.h"

namespace burnin {

// Set CORS headers on response.
void SetCorsHeaders(httplib::Response& res);

// Send JSON response with CORS headers.
void JsonResponse(httplib::Response& res, int status_code, const nlohmann::json& body);

// --- Handler functions for all 13 REST endpoints ---

// GET /health - Returns {"status": "alive"}
void HandleHealth(Engine& engine, const httplib::Request& req, httplib::Response& res);

// GET /ready - Returns {"status": "ready"/"not_ready"}
void HandleReady(Engine& engine, const httplib::Request& req, httplib::Response& res);

// GET /metrics - Prometheus text format
void HandleMetrics(Engine& engine, const httplib::Request& req, httplib::Response& res);

// GET /info - SDK version, broker info, uptime
void HandleInfo(Engine& engine, const httplib::Request& req, httplib::Response& res);

// GET /broker/status - Broker connectivity check
void HandleBrokerStatus(Engine& engine, const httplib::Request& req, httplib::Response& res);

// POST /run/start - Start run with JSON config (202 Accepted)
void HandleRunStart(Engine& engine, const httplib::Request& req, httplib::Response& res);

// POST /run/stop - Graceful stop (202 Accepted)
void HandleRunStop(Engine& engine, const httplib::Request& req, httplib::Response& res);

// GET /run - Full run status + config
void HandleRun(Engine& engine, const httplib::Request& req, httplib::Response& res);

// GET /run/status - Run metrics snapshot
void HandleRunStatus(Engine& engine, const httplib::Request& req, httplib::Response& res);

// GET /run/config - Run configuration (or 404)
void HandleRunConfig(Engine& engine, const httplib::Request& req, httplib::Response& res);

// GET /run/report - Final test report (or 404)
void HandleRunReport(Engine& engine, const httplib::Request& req, httplib::Response& res);

// POST /cleanup - Delete channels (idle only)
void HandleCleanup(Engine& engine, const httplib::Request& req, httplib::Response& res);

// GET /status - Legacy alias for /run/status
void HandleLegacyStatus(Engine& engine, const httplib::Request& req, httplib::Response& res);

// GET /summary - Legacy alias for /run/report
void HandleLegacySummary(Engine& engine, const httplib::Request& req, httplib::Response& res);

}  // namespace burnin
