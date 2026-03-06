#include <gtest/gtest.h>

#include <string_view>

#include "services/otlp-gateway/export_handler.h"

namespace {

TEST(HandleExportTests, SerializationFailureReturnsInternalAndSkipsPublish) {
  bool publish_called = false;
  const grpc::Status status = otlp_gateway::HandleExport(
      "trace", "otel.traces", "Failed to publish traces to NATS",
      [](std::string*) { return false; },
      [&publish_called](std::string_view, const void*, size_t) {
        publish_called = true;
        return true;
      });

  EXPECT_EQ(status.error_code(), grpc::StatusCode::INTERNAL);
  EXPECT_FALSE(publish_called);
}

TEST(HandleExportTests, PublishFailureReturnsUnavailable) {
  const grpc::Status status = otlp_gateway::HandleExport(
      "metrics", "otel.metrics", "Failed to publish metrics to NATS",
      [](std::string* payload) {
        payload->assign("serialized");
        return true;
      },
      [](std::string_view, const void*, size_t) { return false; });

  EXPECT_EQ(status.error_code(), grpc::StatusCode::UNAVAILABLE);
}

TEST(HandleExportTests, SuccessPathReturnsOk) {
  bool publish_called = false;
  const grpc::Status status = otlp_gateway::HandleExport(
      "logs", "otel.logs", "Failed to publish logs to NATS",
      [](std::string* payload) {
        payload->assign("serialized");
        return true;
      },
      [&publish_called](std::string_view subject, const void* data, size_t size) {
        publish_called = true;
        EXPECT_EQ(subject, "otel.logs");
        EXPECT_EQ(std::string(static_cast<const char*>(data), size), "serialized");
        return true;
      });

  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(publish_called);
}

}  // namespace
