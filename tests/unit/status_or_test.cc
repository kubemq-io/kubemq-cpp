#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "kubemq/status.h"

namespace kubemq {
namespace {

// --- Construct from value ---

TEST(StatusOrTest, ConstructFromValue) {
    StatusOr<int> result(42);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.value(), 42);
}

TEST(StatusOrTest, ConstructFromStringValue) {
    StatusOr<std::string> result(std::string("hello"));
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.value(), "hello");
}

// --- Construct from Status ---

TEST(StatusOrTest, ConstructFromStatus) {
    Status err(ErrorCode::kValidation, "bad input");
    StatusOr<int> result(err);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), ErrorCode::kValidation);
    EXPECT_EQ(result.status().message(), "bad input");
}

TEST(StatusOrTest, ConstructFromStatusPreservesMessage) {
    Status err(ErrorCode::kTransient, "connection lost", "SendEvent", "ch1", "req1");
    StatusOr<std::string> result(err);
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.status().operation(), "SendEvent");
    EXPECT_EQ(result.status().channel(), "ch1");
}

// --- ok() ---

TEST(StatusOrTest, OkTrueForValue) {
    StatusOr<double> result(3.14);
    EXPECT_TRUE(result.ok());
}

TEST(StatusOrTest, OkFalseForError) {
    StatusOr<double> result(Status(ErrorCode::kFatal, "error"));
    EXPECT_FALSE(result.ok());
}

// --- value() ---

TEST(StatusOrTest, ValueReturnsStoredValue) {
    StatusOr<int> result(100);
    EXPECT_EQ(result.value(), 100);
}

TEST(StatusOrTest, ValueThrowsOnError) {
    StatusOr<int> result(Status(ErrorCode::kFatal, "fail"));
    EXPECT_THROW(result.value(), BadStatusAccess);
}

TEST(StatusOrTest, MutableValue) {
    StatusOr<std::string> result(std::string("original"));
    result.value() = "modified";
    EXPECT_EQ(result.value(), "modified");
}

// --- operator* ---

TEST(StatusOrTest, DereferenceConst) {
    const StatusOr<std::string> result(std::string("test"));
    EXPECT_EQ(*result, "test");
}

TEST(StatusOrTest, DereferenceMutable) {
    StatusOr<std::string> result(std::string("test"));
    *result = "changed";
    EXPECT_EQ(*result, "changed");
}

TEST(StatusOrTest, DereferenceRvalue) {
    StatusOr<std::string> result(std::string("moveme"));
    std::string moved = *std::move(result);
    EXPECT_EQ(moved, "moveme");
}

// --- operator-> ---

TEST(StatusOrTest, ArrowOperatorConst) {
    const StatusOr<std::string> result(std::string("hello world"));
    EXPECT_EQ(result->size(), 11);
}

TEST(StatusOrTest, ArrowOperatorMutable) {
    StatusOr<std::string> result(std::string("hello"));
    result->append(" world");
    EXPECT_EQ(*result, "hello world");
}

// --- operator bool ---

TEST(StatusOrTest, BoolConversionTrue) {
    StatusOr<int> result(42);
    EXPECT_TRUE(static_cast<bool>(result));
    if (result) {
        EXPECT_EQ(*result, 42);
    } else {
        FAIL() << "Expected StatusOr to be truthy";
    }
}

TEST(StatusOrTest, BoolConversionFalse) {
    StatusOr<int> result(Status(ErrorCode::kFatal, "error"));
    EXPECT_FALSE(static_cast<bool>(result));
}

// --- status() on success ---

TEST(StatusOrTest, StatusOnSuccessReturnsOk) {
    StatusOr<int> result(42);
    EXPECT_TRUE(result.status().ok());
    EXPECT_EQ(result.status().code(), ErrorCode::kOk);
}

// --- Move semantics ---

TEST(StatusOrTest, MoveConstructFromValue) {
    StatusOr<std::string> original(std::string("data"));
    StatusOr<std::string> moved(std::move(original));
    EXPECT_TRUE(moved.ok());
    EXPECT_EQ(*moved, "data");
}

TEST(StatusOrTest, MoveConstructFromError) {
    StatusOr<std::string> original(Status(ErrorCode::kTimeout, "slow"));
    StatusOr<std::string> moved(std::move(original));
    EXPECT_FALSE(moved.ok());
    EXPECT_EQ(moved.status().code(), ErrorCode::kTimeout);
}

TEST(StatusOrTest, MoveAssignValue) {
    StatusOr<int> a(10);
    StatusOr<int> b(20);
    b = std::move(a);
    EXPECT_TRUE(b.ok());
    EXPECT_EQ(*b, 10);
}

TEST(StatusOrTest, MoveValueOut) {
    StatusOr<std::string> result(std::string("big string data for move test"));
    std::string extracted = std::move(result).value();
    EXPECT_EQ(extracted, "big string data for move test");
}

// --- Complex types ---

struct TestStruct {
    int x;
    std::string name;
};

TEST(StatusOrTest, WorksWithStruct) {
    StatusOr<TestStruct> result(TestStruct{42, "test"});
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result->x, 42);
    EXPECT_EQ(result->name, "test");
}

TEST(StatusOrTest, WorksWithVector) {
    std::vector<int> v = {1, 2, 3, 4, 5};
    StatusOr<std::vector<int>> result(std::move(v));
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result->size(), 5);
    EXPECT_EQ((*result)[0], 1);
}

// --- BadStatusAccess ---

TEST(StatusOrTest, BadStatusAccess_ThrowsWithContext) {
    Status err(ErrorCode::kValidation, "bad input", "Op", "ch", "req1");
    StatusOr<int> result(err);

    try {
        result.value();
        FAIL() << "Expected BadStatusAccess";
    } catch (const BadStatusAccess& e) {
        EXPECT_EQ(e.status().code(), ErrorCode::kValidation);
        EXPECT_EQ(e.status().message(), "bad input");
        EXPECT_NE(std::string(e.what()).find("BadStatusAccess"), std::string::npos);
    }
}

TEST(StatusOrTest, BadStatusAccess_CarriesAllStatusFields) {
    Status err(ErrorCode::kTransient, "connection lost", "SendEvent", "ch1", "req42");
    StatusOr<std::string> result(err);

    try {
        result.value();
        FAIL() << "Expected BadStatusAccess";
    } catch (const BadStatusAccess& e) {
        EXPECT_EQ(e.status().code(), ErrorCode::kTransient);
        EXPECT_EQ(e.status().message(), "connection lost");
        EXPECT_EQ(e.status().operation(), "SendEvent");
        EXPECT_EQ(e.status().channel(), "ch1");
        EXPECT_EQ(e.status().request_id(), "req42");
    }
}

// Compile-time test: StatusOr<Status> must fail to compile.
// Uncomment the next line to verify (it should cause a static_assert failure):
// static_assert(!std::is_constructible_v<StatusOr<Status>, Status>, "should not compile");
// This is tested by the static_assert inside StatusOr itself.

}  // namespace
}  // namespace kubemq
