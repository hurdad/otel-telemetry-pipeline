#include "telemetry/tracer.h"

#include <iostream>

namespace telemetry {

ScopedSpan::ScopedSpan(std::string name)
    : name_(std::move(name)), started_at_(std::chrono::steady_clock::now()) {}

ScopedSpan::~ScopedSpan() {
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - started_at_);
  std::clog << "[span] " << name_ << " duration_ms=" << elapsed.count() << '\n';
}

void InitTelemetry() {
  const char* endpoint = std::getenv("OTEL_EXPORTER_OTLP_ENDPOINT");
  const char* service_name = std::getenv("OTEL_SERVICE_NAME");
  std::clog << "Telemetry init endpoint=" << (endpoint ? endpoint : "")
            << " service=" << (service_name ? service_name : "otel-pipeline") << '\n';
}

std::unique_ptr<ScopedSpan> StartSpan(const std::string& name) {
  return std::make_unique<ScopedSpan>(name);
}

}  // namespace telemetry
