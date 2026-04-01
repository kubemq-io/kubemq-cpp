#include <gtest/gtest.h>

#include <set>
#include <string>

#include "internal/uuid.h"

namespace kubemq {
namespace internal {
namespace {

TEST(UUIDTest, FormatLength) {
    std::string uuid = GenerateUUID();
    EXPECT_EQ(uuid.size(), 36)
        << "UUID should be 36 characters, got: " << uuid;
}

TEST(UUIDTest, HyphenPositions) {
    std::string uuid = GenerateUUID();
    ASSERT_EQ(uuid.size(), 36);
    EXPECT_EQ(uuid[8], '-') << "Expected hyphen at position 8, got: " << uuid;
    EXPECT_EQ(uuid[13], '-') << "Expected hyphen at position 13, got: " << uuid;
    EXPECT_EQ(uuid[18], '-') << "Expected hyphen at position 18, got: " << uuid;
    EXPECT_EQ(uuid[23], '-') << "Expected hyphen at position 23, got: " << uuid;
}

TEST(UUIDTest, AllHexCharacters) {
    std::string uuid = GenerateUUID();
    for (size_t i = 0; i < uuid.size(); ++i) {
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            continue;  // skip hyphens
        }
        EXPECT_TRUE(std::isxdigit(uuid[i]))
            << "Non-hex character at position " << i << ": " << uuid[i]
            << " in UUID: " << uuid;
    }
}

TEST(UUIDTest, Version4Bit) {
    // UUID v4: character at position 14 should be '4'
    // Format: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
    std::string uuid = GenerateUUID();
    ASSERT_EQ(uuid.size(), 36);
    EXPECT_EQ(uuid[14], '4')
        << "Version nibble should be '4', got: " << uuid[14]
        << " in UUID: " << uuid;
}

TEST(UUIDTest, VariantBits) {
    // UUID v4 variant: character at position 19 should be 8, 9, a, or b
    // Format: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx (y = 8,9,a,b)
    std::string uuid = GenerateUUID();
    ASSERT_EQ(uuid.size(), 36);
    char variant = uuid[19];
    EXPECT_TRUE(variant == '8' || variant == '9' ||
                variant == 'a' || variant == 'b')
        << "Variant character should be 8/9/a/b, got: " << variant
        << " in UUID: " << uuid;
}

TEST(UUIDTest, Uniqueness) {
    constexpr int kNumUUIDs = 1000;
    std::set<std::string> uuids;

    for (int i = 0; i < kNumUUIDs; ++i) {
        std::string uuid = GenerateUUID();
        auto [it, inserted] = uuids.insert(uuid);
        EXPECT_TRUE(inserted)
            << "Duplicate UUID found: " << uuid << " at iteration " << i;
    }
    EXPECT_EQ(uuids.size(), kNumUUIDs);
}

TEST(UUIDTest, MultipleCallsReturnDifferentValues) {
    std::string uuid1 = GenerateUUID();
    std::string uuid2 = GenerateUUID();
    std::string uuid3 = GenerateUUID();

    EXPECT_NE(uuid1, uuid2);
    EXPECT_NE(uuid2, uuid3);
    EXPECT_NE(uuid1, uuid3);
}

TEST(UUIDTest, ConsistentFormat) {
    // Generate multiple UUIDs and verify they all have the same format
    for (int i = 0; i < 100; ++i) {
        std::string uuid = GenerateUUID();
        ASSERT_EQ(uuid.size(), 36) << "Iteration " << i;
        ASSERT_EQ(uuid[8], '-') << "Iteration " << i;
        ASSERT_EQ(uuid[13], '-') << "Iteration " << i;
        ASSERT_EQ(uuid[18], '-') << "Iteration " << i;
        ASSERT_EQ(uuid[23], '-') << "Iteration " << i;
        ASSERT_EQ(uuid[14], '4') << "Iteration " << i;
    }
}

}  // namespace
}  // namespace internal
}  // namespace kubemq
