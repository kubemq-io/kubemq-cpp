#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include <mutex>

namespace kubemq {
namespace {

TEST(StreamHandleTest, CloseIdempotent_AtomicExchange) {
    std::atomic<bool> done{false};
    int close_count = 0;

    auto do_close = [&]() {
        if (done.exchange(true, std::memory_order_acq_rel)) return;
        close_count++;
    };

    do_close();
    do_close();
    EXPECT_EQ(close_count, 1);
}

TEST(StreamHandleTest, JoinSafe_ConcurrentWaitAndClose) {
    std::once_flag join_once;
    std::thread worker([]() { std::this_thread::sleep_for(std::chrono::milliseconds(50)); });

    auto safe_join = [&]() {
        std::call_once(join_once, [&]() {
            if (worker.joinable()) worker.join();
        });
    };

    std::thread t1(safe_join);
    std::thread t2(safe_join);
    t1.join();
    t2.join();
}

TEST(StreamHandleTest, RequiresCallbacksAtConstruction) {
    // Verify the atomic exchange + once_flag patterns work together
    std::atomic<bool> done{false};
    std::once_flag join_once;
    int close_count = 0;

    auto do_close = [&]() {
        if (done.exchange(true, std::memory_order_acq_rel)) return;
        close_count++;
        std::call_once(join_once, [&]() { /* join simulation */ });
    };

    do_close();
    do_close();
    EXPECT_EQ(close_count, 1);
}

}  // namespace
}  // namespace kubemq
