#include <gtest/gtest.h>

#include <grpcpp/grpcpp.h>

#include "kubemq/error_code.h"
#include "kubemq/status.h"
#include "internal/middleware/error_mapper.h"

namespace kubemq {
namespace internal {
namespace {

// --- All 17 gRPC codes mapped correctly ---

TEST(ErrorMapperTest, OkMapsToOk) {
    EXPECT_EQ(MapGrpcCode(grpc::StatusCode::OK), ErrorCode::kOk);
}

TEST(ErrorMapperTest, CancelledMapsToCancellation) {
    EXPECT_EQ(MapGrpcCode(grpc::StatusCode::CANCELLED), ErrorCode::kCancellation);
}

TEST(ErrorMapperTest, UnknownMapsToFatal) {
    EXPECT_EQ(MapGrpcCode(grpc::StatusCode::UNKNOWN), ErrorCode::kFatal);
}

TEST(ErrorMapperTest, InvalidArgumentMapsToValidation) {
    EXPECT_EQ(MapGrpcCode(grpc::StatusCode::INVALID_ARGUMENT), ErrorCode::kValidation);
}

TEST(ErrorMapperTest, DeadlineExceededMapsToTimeout) {
    EXPECT_EQ(MapGrpcCode(grpc::StatusCode::DEADLINE_EXCEEDED), ErrorCode::kTimeout);
}

TEST(ErrorMapperTest, NotFoundMapsToNotFound) {
    EXPECT_EQ(MapGrpcCode(grpc::StatusCode::NOT_FOUND), ErrorCode::kNotFound);
}

TEST(ErrorMapperTest, AlreadyExistsMapsToValidation) {
    EXPECT_EQ(MapGrpcCode(grpc::StatusCode::ALREADY_EXISTS), ErrorCode::kValidation);
}

TEST(ErrorMapperTest, PermissionDeniedMapsToAuthorization) {
    EXPECT_EQ(MapGrpcCode(grpc::StatusCode::PERMISSION_DENIED), ErrorCode::kAuthorization);
}

TEST(ErrorMapperTest, ResourceExhaustedMapsToThrottling) {
    EXPECT_EQ(MapGrpcCode(grpc::StatusCode::RESOURCE_EXHAUSTED), ErrorCode::kThrottling);
}

TEST(ErrorMapperTest, FailedPreconditionMapsToValidation) {
    EXPECT_EQ(MapGrpcCode(grpc::StatusCode::FAILED_PRECONDITION), ErrorCode::kValidation);
}

TEST(ErrorMapperTest, AbortedMapsToTransient) {
    EXPECT_EQ(MapGrpcCode(grpc::StatusCode::ABORTED), ErrorCode::kTransient);
}

TEST(ErrorMapperTest, OutOfRangeMapsToValidation) {
    EXPECT_EQ(MapGrpcCode(grpc::StatusCode::OUT_OF_RANGE), ErrorCode::kValidation);
}

TEST(ErrorMapperTest, UnimplementedMapsToFatal) {
    EXPECT_EQ(MapGrpcCode(grpc::StatusCode::UNIMPLEMENTED), ErrorCode::kFatal);
}

TEST(ErrorMapperTest, InternalMapsToFatal) {
    EXPECT_EQ(MapGrpcCode(grpc::StatusCode::INTERNAL), ErrorCode::kFatal);
}

TEST(ErrorMapperTest, UnavailableMapsToTransient) {
    EXPECT_EQ(MapGrpcCode(grpc::StatusCode::UNAVAILABLE), ErrorCode::kTransient);
}

TEST(ErrorMapperTest, DataLossMapsToFatal) {
    EXPECT_EQ(MapGrpcCode(grpc::StatusCode::DATA_LOSS), ErrorCode::kFatal);
}

TEST(ErrorMapperTest, UnauthenticatedMapsToAuthentication) {
    EXPECT_EQ(MapGrpcCode(grpc::StatusCode::UNAUTHENTICATED), ErrorCode::kAuthentication);
}

// --- FromGrpcStatus conversion ---

TEST(ErrorMapperTest, FromGrpcStatusOk) {
    grpc::Status grpc_ok = grpc::Status::OK;
    auto status = FromGrpcStatus(grpc_ok);
    EXPECT_TRUE(status.ok());
}

TEST(ErrorMapperTest, FromGrpcStatusUnavailable) {
    grpc::Status grpc_err(grpc::StatusCode::UNAVAILABLE, "service unavailable");
    auto status = FromGrpcStatus(grpc_err);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::kTransient);
    EXPECT_NE(status.message().find("service unavailable"), std::string::npos);
}

TEST(ErrorMapperTest, FromGrpcStatusWithOperation) {
    grpc::Status grpc_err(grpc::StatusCode::NOT_FOUND, "channel not found");
    auto status = FromGrpcStatus(grpc_err, "SendEvent", "test-channel", "req-123");
    EXPECT_EQ(status.operation(), "SendEvent");
    EXPECT_EQ(status.channel(), "test-channel");
    EXPECT_EQ(status.request_id(), "req-123");
}

TEST(ErrorMapperTest, FromGrpcStatusDeadlineExceeded) {
    grpc::Status grpc_err(grpc::StatusCode::DEADLINE_EXCEEDED, "deadline");
    auto status = FromGrpcStatus(grpc_err);
    EXPECT_EQ(status.code(), ErrorCode::kTimeout);
    EXPECT_TRUE(status.is_retryable());
}

TEST(ErrorMapperTest, FromGrpcStatusPermissionDenied) {
    grpc::Status grpc_err(grpc::StatusCode::PERMISSION_DENIED, "not authorized");
    auto status = FromGrpcStatus(grpc_err);
    EXPECT_EQ(status.code(), ErrorCode::kAuthorization);
    EXPECT_FALSE(status.is_retryable());
}

// --- IsConnectionError ---

TEST(ErrorMapperTest, IsConnectionErrorUnavailable) {
    grpc::Status grpc_err(grpc::StatusCode::UNAVAILABLE, "connection refused");
    EXPECT_TRUE(IsConnectionError(grpc_err));
}

TEST(ErrorMapperTest, IsConnectionErrorOk) {
    EXPECT_FALSE(IsConnectionError(grpc::Status::OK));
}

TEST(ErrorMapperTest, IsConnectionErrorNotFound) {
    grpc::Status grpc_err(grpc::StatusCode::NOT_FOUND, "not found");
    EXPECT_FALSE(IsConnectionError(grpc_err));
}

TEST(ErrorMapperTest, IsConnectionErrorInternal) {
    grpc::Status grpc_err(grpc::StatusCode::INTERNAL, "internal");
    EXPECT_FALSE(IsConnectionError(grpc_err));
}

// --- FromGrpcStatus context preservation ---

TEST(ErrorMapperTest, FromGrpcStatus_PreservesContext) {
    grpc::Status grpc_err(grpc::StatusCode::UNAVAILABLE, "connection refused");
    auto status = FromGrpcStatus(grpc_err, "GrpcTransport::Ping", "my-channel", "req-123");

    EXPECT_EQ(status.code(), ErrorCode::kTransient);
    EXPECT_EQ(status.operation(), "GrpcTransport::Ping");
    EXPECT_EQ(status.channel(), "my-channel");
    EXPECT_EQ(status.request_id(), "req-123");
    EXPECT_NE(status.message().find("connection refused"), std::string::npos);
}

}  // namespace
}  // namespace internal
}  // namespace kubemq
