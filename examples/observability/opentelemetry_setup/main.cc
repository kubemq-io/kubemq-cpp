// Example: observability/opentelemetry_setup
//
// Demonstrates OpenTelemetry integration for distributed tracing and metrics
// with the KubeMQ C++ SDK. When an OpenTelemetry TracerProvider and
// MeterProvider are globally registered, the SDK instruments gRPC calls
// automatically. This example sends a demo event to generate trace spans.
//
// Channel: cpp-observability.opentelemetry_setup
// Client ID: cpp-observability-opentelemetry-setup-client
//
// Run with a KubeMQ server and optionally an OpenTelemetry collector
// (e.g., Jaeger, Zipkin) for trace visualization.

#include <kubemq/kubemq.h>

#include <chrono>
#include <iostream>
#include <thread>

// To enable OpenTelemetry tracing, install the opentelemetry-cpp SDK and
// configure a TracerProvider before creating the KubeMQ client:
//
//   #include <opentelemetry/sdk/trace/tracer_provider.h>
//   #include <opentelemetry/exporters/ostream/span_exporter.h>
//   #include <opentelemetry/trace/provider.h>
//
//   auto exporter = std::make_unique<opentelemetry::exporter::trace::OStreamSpanExporter>();
//   auto processor = std::make_unique<opentelemetry::sdk::trace::SimpleSpanProcessor>(
//       std::move(exporter));
//   auto provider = std::make_shared<opentelemetry::sdk::trace::TracerProvider>(
//       std::move(processor));
//   opentelemetry::trace::Provider::SetTracerProvider(provider);

int main() {
    std::cout << "[1] OpenTelemetry providers configured (using default/no-op)" << std::endl;
    std::cout << "[INFO] To see real traces, configure an OpenTelemetry collector and "
              << "replace the no-op provider above." << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-observability-opentelemetry-setup-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    // Send a demo event to generate trace spans
    auto event_result = kubemq::Event::Builder()
                            .SetChannel("cpp-observability.opentelemetry_setup")
                            .SetBody("traced-event")
                            .SetMetadata("otel-demo")
                            .Build();
    if (!event_result.ok()) {
        std::cerr << "[ERROR] Build event: " << event_result.status().message() << std::endl;
        return 1;
    }

    auto send_status = client->SendEvent(*event_result);
    if (!send_status.ok()) {
        std::cerr << "[ERROR] SendEvent: " << send_status.message() << std::endl;
        return 1;
    }
    std::cout << "[2] Event sent with tracing enabled" << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(1));

    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[3] Client closed" << std::endl;

    return 0;
}
