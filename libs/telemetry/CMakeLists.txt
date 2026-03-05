add_library(telemetry STATIC
  src/tracer.cpp)

target_include_directories(telemetry PUBLIC include)

target_link_libraries(telemetry PUBLIC
  otel_proto
  opentelemetry_trace
  opentelemetry_metrics
  opentelemetry_logs
  opentelemetry_resources
  opentelemetry_exporter_otlp_grpc
  opentelemetry_exporter_otlp_grpc_log
  opentelemetry_exporter_otlp_grpc_metrics
  spdlog::spdlog)
