#pragma once

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>

#include <grpcpp/grpcpp.h>
#include <spdlog/spdlog.h>

namespace otlp_gateway {

inline grpc::Status HandleExport(const char* request_type, const char* subject,
                                 const char* publish_failure_message,
                                 const std::function<bool(std::string*)>& serialize,
                                 const std::function<bool(std::string_view, const void*, size_t)>&
                                     publish) {
  std::string payload;
  if (!serialize(&payload)) {
    spdlog::error("Failed to serialize OTLP request type={} subject={}", request_type, subject);
    return grpc::Status(grpc::StatusCode::INTERNAL,
                        "Failed to serialize OTLP request payload");
  }

  if (!publish(subject, payload.data(), payload.size())) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, publish_failure_message);
  }

  return grpc::Status::OK;
}

}  // namespace otlp_gateway
