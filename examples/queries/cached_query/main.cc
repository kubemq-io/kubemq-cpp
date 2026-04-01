// Example: queries/cached_query
//
// Demonstrates query caching with CacheKey and CacheTTL.
// The first query triggers the handler; subsequent queries with the same
// cache key are served from cache (cache_hit=true) without calling the handler.
//
// Channel: cpp-queries.cached_query
// Client ID: cpp-queries-cached-query-client
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
    options.set_client_id("cpp-queries-cached-query-client");

    auto client_result = kubemq::Client::Create(options);
    if (!client_result.ok()) {
        std::cerr << "[ERROR] Failed to create client: " << client_result.status().message()
                  << std::endl;
        return 1;
    }
    auto& client = *client_result;

    std::string channel = "cpp-queries.cached_query";
    std::atomic<int> handler_calls{0};

    // Register a query handler.
    std::cout << "[2] Subscribing to queries on channel " << channel << std::endl;
    auto sub_result = client->SubscribeToQueries(
        channel, "",
        [&client, &handler_calls](const kubemq::QueryReceive& q) {
            int call_num = handler_calls.fetch_add(1) + 1;
            std::cout << "[*] Handler called (call #" << call_num << "): body=" << q.body
                      << std::endl;

            auto now_epoch = std::chrono::duration_cast<std::chrono::seconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
            auto reply_result = kubemq::QueryReply::Builder()
                                    .SetRequestId(q.id)
                                    .SetResponseTo(q.response_to)
                                    .SetBody("cached-result")
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
            }
        },
        [](const kubemq::Status& err) {
            std::cerr << "[ERROR] Subscription error: " << err.message() << std::endl;
        });
    if (!sub_result.ok()) {
        std::cerr << "[ERROR] SubscribeToQueries: " << sub_result.status().message() << std::endl;
        return 1;
    }
    auto& sub = *sub_result;

    // Allow subscription to fully establish before sending.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // First query: handler is called, result is cached for 60 seconds.
    std::cout << "[3] Sending first query (cache miss expected)" << std::endl;
    auto q1_build = kubemq::Query::Builder()
                        .SetChannel(channel)
                        .SetBody("cacheable-query")
                        .SetTimeout(std::chrono::seconds(10))
                        .SetCacheKey("my-cache-key")
                        .SetCacheTTL(std::chrono::milliseconds(60000))
                        .Build();
    if (!q1_build.ok()) {
        std::cerr << "[ERROR] Build query 1: " << q1_build.status().message() << std::endl;
        return 1;
    }
    auto q1_resp = client->SendQuery(*q1_build);
    if (!q1_resp.ok()) {
        std::cerr << "[ERROR] SendQuery 1: " << q1_resp.status().message() << std::endl;
        return 1;
    }
    std::cout << "[4] First query:  executed=" << std::boolalpha << q1_resp->executed
              << " cache_hit=" << q1_resp->cache_hit << " body=" << q1_resp->body << std::endl;

    // Wait for handler to complete.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Second query with same cache key: served from cache (no handler call).
    std::cout << "[5] Sending second query (cache hit expected)" << std::endl;
    auto q2_build = kubemq::Query::Builder()
                        .SetChannel(channel)
                        .SetBody("cacheable-query")
                        .SetTimeout(std::chrono::seconds(10))
                        .SetCacheKey("my-cache-key")
                        .SetCacheTTL(std::chrono::milliseconds(60000))
                        .Build();
    if (!q2_build.ok()) {
        std::cerr << "[ERROR] Build query 2: " << q2_build.status().message() << std::endl;
        return 1;
    }
    auto q2_resp = client->SendQuery(*q2_build);
    if (!q2_resp.ok()) {
        std::cerr << "[ERROR] SendQuery 2: " << q2_resp.status().message() << std::endl;
        return 1;
    }
    std::cout << "[6] Second query: executed=" << std::boolalpha << q2_resp->executed
              << " cache_hit=" << q2_resp->cache_hit << " body=" << q2_resp->body << std::endl;

    // Cancel the subscription explicitly.
    // Note: The Subscription destructor also calls Cancel(), but explicit
    // cleanup is shown here for clarity and to match Go's defer pattern.
    sub->Cancel();
    std::cout << "[7] Subscription cancelled" << std::endl;

    auto close_status = client->Close();
    if (!close_status.ok()) {
        std::cerr << "[ERROR] Close failed: " << close_status.message() << std::endl;
        return 1;
    }
    std::cout << "[8] Client closed" << std::endl;

    return 0;
}
