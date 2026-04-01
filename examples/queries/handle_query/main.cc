// Example: queries/handle_query
//
// Demonstrates subscribing to queries and handling them with business logic.
// The handler processes incoming queries and returns data in the response,
// with emphasis on metadata propagation in both query and reply.
//
// Channel: cpp-queries.handle_query
// Client ID: cpp-queries-handle-query-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).

#include <kubemq/kubemq.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

int main() {
    std::cout << "[1] Connecting to localhost:50000" << std::endl;

    kubemq::ClientOptions options;
    options.set_address("localhost", 50000);
    options.set_client_id("cpp-queries-handle-query-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-queries.handle_query";
    std::atomic<bool> query_handled{false};

    // Register a query handler that logs all fields and returns data.
    std::cout << "[2] Subscribing to queries on channel " << channel << std::endl;
    auto sub_result = client->SubscribeToQueries(
        channel, "",
        [&client, &query_handled](const kubemq::QueryReceive& q) {
            std::cout << "[4] Handling query: id=" << q.id << " channel=" << q.channel
                      << " body=" << q.body << " metadata=" << q.metadata << std::endl;

            // Process the query and prepare a response with JSON body.
            auto now_epoch = std::chrono::duration_cast<std::chrono::seconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
            auto reply_result = kubemq::QueryReply::Builder()
                                    .SetRequestId(q.id)
                                    .SetResponseTo(q.response_to)
                                    .SetBody(R"({"users":["alice","bob"]})")
                                    .SetMetadata("success")
                                    .SetExecuted(true)
                                    .SetExecutedAt(now_epoch)
                                    .Build();
            if (!reply_result.ok()) {
                std::cerr << "[ERROR] Build reply: " << reply_result.status().message()
                          << std::endl;
                return;
            }
            auto send_status = client->SendQueryResponse(*reply_result);
            if (!send_status.ok()) {
                std::cerr << "[ERROR] SendQueryResponse: " << send_status.message() << std::endl;
                return;
            }
            query_handled.store(true);
        },
        [](const kubemq::Status& err) {
            std::cerr << "[ERROR] Handler error: " << err.message() << std::endl;
        });
    if (!sub_result.ok()) {
        std::cerr << "[ERROR] SubscribeToQueries: " << sub_result.status().message() << std::endl;
        return 1;
    }
    auto& sub = *sub_result;

    // Allow subscription to fully establish before sending.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // Send a query with metadata.
    std::cout << "[3] Sending query to channel " << channel << std::endl;
    auto query_build = kubemq::Query::Builder()
                           .SetChannel(channel)
                           .SetBody("list-users")
                           .SetMetadata("request-meta")
                           .SetTimeout(std::chrono::seconds(10))
                           .Build();
    if (!query_build.ok()) {
        std::cerr << "[ERROR] Build query: " << query_build.status().message() << std::endl;
        return 1;
    }
    auto resp_result = client->SendQuery(*query_build);
    if (!resp_result.ok()) {
        std::cerr << "[ERROR] SendQuery: " << resp_result.status().message() << std::endl;
        return 1;
    }
    std::cout << "[5] Response: executed=" << std::boolalpha << resp_result->executed
              << " body=" << resp_result->body << " metadata=" << resp_result->metadata
              << std::endl;

    // Wait for the handler to finish processing.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Cancel the subscription explicitly.
    // Note: The Subscription destructor also calls Cancel(), but explicit
    // cleanup is shown here for clarity and to match Go's defer pattern.
    sub->Cancel();
    std::cout << "[6] Subscription cancelled" << std::endl;

    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[7] Client closed" << std::endl;

    return 0;
}
