#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "internal/transport/message_buffer.h"

namespace kubemq {
namespace internal {
namespace {

// --- Push ---

TEST(MessageBufferTest, PushSingleMessage) {
    MessageBuffer buf(10);
    int evicted = buf.Push("message1");
    EXPECT_EQ(evicted, 0);
    EXPECT_EQ(buf.Size(), 1);
    EXPECT_FALSE(buf.Empty());
}

TEST(MessageBufferTest, PushMultipleMessages) {
    MessageBuffer buf(10);
    buf.Push("msg1");
    buf.Push("msg2");
    buf.Push("msg3");
    EXPECT_EQ(buf.Size(), 3);
}

TEST(MessageBufferTest, PushUpToCapacity) {
    MessageBuffer buf(3);
    buf.Push("msg1");
    buf.Push("msg2");
    int evicted = buf.Push("msg3");
    EXPECT_EQ(evicted, 0);
    EXPECT_EQ(buf.Size(), 3);
}

// --- Push at capacity (eviction) ---

TEST(MessageBufferTest, PushEvictsOldest) {
    MessageBuffer buf(3);
    buf.Push("msg1");
    buf.Push("msg2");
    buf.Push("msg3");

    int evicted = buf.Push("msg4");
    EXPECT_EQ(evicted, 1);
    EXPECT_EQ(buf.Size(), 3);
}

TEST(MessageBufferTest, PushEvictsMultiple) {
    MessageBuffer buf(2);
    buf.Push("msg1");
    buf.Push("msg2");
    buf.Push("msg3");  // evicts msg1
    buf.Push("msg4");  // evicts msg2
    EXPECT_EQ(buf.Size(), 2);
}

// --- Drain (order preserved) ---

TEST(MessageBufferTest, DrainOrderPreserved) {
    MessageBuffer buf(10);
    buf.Push("first");
    buf.Push("second");
    buf.Push("third");

    std::vector<std::string> drained;
    int count = buf.Drain([&](std::string&& msg) {
        drained.push_back(std::move(msg));
        return true;
    });

    EXPECT_EQ(count, 3);
    ASSERT_EQ(drained.size(), 3);
    EXPECT_EQ(drained[0], "first");
    EXPECT_EQ(drained[1], "second");
    EXPECT_EQ(drained[2], "third");
    EXPECT_TRUE(buf.Empty());
}

TEST(MessageBufferTest, DrainAfterEviction) {
    MessageBuffer buf(3);
    buf.Push("msg1");
    buf.Push("msg2");
    buf.Push("msg3");
    buf.Push("msg4");  // evicts msg1

    std::vector<std::string> drained;
    buf.Drain([&](std::string&& msg) {
        drained.push_back(std::move(msg));
        return true;
    });

    ASSERT_EQ(drained.size(), 3);
    EXPECT_EQ(drained[0], "msg2");
    EXPECT_EQ(drained[1], "msg3");
    EXPECT_EQ(drained[2], "msg4");
}

TEST(MessageBufferTest, DrainEmptyBuffer) {
    MessageBuffer buf(10);
    std::vector<std::string> drained;
    int count = buf.Drain([&](std::string&& msg) {
        drained.push_back(std::move(msg));
        return true;
    });
    EXPECT_EQ(count, 0);
    EXPECT_TRUE(drained.empty());
}

TEST(MessageBufferTest, DrainStopsOnFalse) {
    MessageBuffer buf(10);
    buf.Push("msg1");
    buf.Push("msg2");
    buf.Push("msg3");

    std::vector<std::string> drained;
    int count = buf.Drain([&](std::string&& msg) {
        drained.push_back(std::move(msg));
        return drained.size() < 2;  // stop after 2
    });

    EXPECT_EQ(count, 2);
    EXPECT_EQ(drained.size(), 2);
}

// --- Discard ---

TEST(MessageBufferTest, DiscardAll) {
    MessageBuffer buf(10);
    buf.Push("msg1");
    buf.Push("msg2");
    buf.Push("msg3");

    int discarded = buf.Discard();
    EXPECT_EQ(discarded, 3);
    EXPECT_TRUE(buf.Empty());
    EXPECT_EQ(buf.Size(), 0);
}

TEST(MessageBufferTest, DiscardEmptyBuffer) {
    MessageBuffer buf(10);
    int discarded = buf.Discard();
    EXPECT_EQ(discarded, 0);
}

// --- Size / Capacity / Empty ---

TEST(MessageBufferTest, Capacity) {
    MessageBuffer buf(100);
    EXPECT_EQ(buf.Capacity(), 100);
}

TEST(MessageBufferTest, CapacitySmall) {
    MessageBuffer buf(1);
    EXPECT_EQ(buf.Capacity(), 1);
}

TEST(MessageBufferTest, EmptyOnCreation) {
    MessageBuffer buf(10);
    EXPECT_TRUE(buf.Empty());
    EXPECT_EQ(buf.Size(), 0);
}

TEST(MessageBufferTest, NotEmptyAfterPush) {
    MessageBuffer buf(10);
    buf.Push("msg");
    EXPECT_FALSE(buf.Empty());
}

TEST(MessageBufferTest, EmptyAfterDrainAll) {
    MessageBuffer buf(10);
    buf.Push("msg1");
    buf.Push("msg2");
    buf.Drain([](std::string&&) { return true; });
    EXPECT_TRUE(buf.Empty());
}

TEST(MessageBufferTest, EmptyAfterDiscard) {
    MessageBuffer buf(10);
    buf.Push("msg1");
    buf.Discard();
    EXPECT_TRUE(buf.Empty());
}

TEST(MessageBufferTest, SizeAfterPushAndDrain) {
    MessageBuffer buf(10);
    buf.Push("msg1");
    buf.Push("msg2");
    EXPECT_EQ(buf.Size(), 2);

    buf.Drain([](std::string&&) { return true; });
    EXPECT_EQ(buf.Size(), 0);

    buf.Push("msg3");
    EXPECT_EQ(buf.Size(), 1);
}

// --- Stress test: wrap-around ---

TEST(MessageBufferTest, WrapAround) {
    MessageBuffer buf(3);
    // Fill and drain multiple times to test wrap-around
    for (int round = 0; round < 5; ++round) {
        buf.Push("a" + std::to_string(round));
        buf.Push("b" + std::to_string(round));
        buf.Push("c" + std::to_string(round));

        std::vector<std::string> drained;
        buf.Drain([&](std::string&& msg) {
            drained.push_back(std::move(msg));
            return true;
        });
        ASSERT_EQ(drained.size(), 3) << "Round " << round;
    }
}

}  // namespace
}  // namespace internal
}  // namespace kubemq
