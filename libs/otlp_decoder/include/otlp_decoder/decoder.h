#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace otlp_decoder {

struct TraceRow {
  uint64_t timestamp_ns;
  std::string trace_id;
  std::string span_id;
  std::string parent_span_id;
  std::string service_name;
  std::string operation_name;
  uint64_t duration_ns;
};

struct MetricRow {
  uint64_t timestamp_ns;
  std::string service_name;
  std::string metric_name;
  double value;
};

struct LogRow {
  uint64_t timestamp_ns;
  std::string service_name;
  std::string severity_text;
  std::string body;
};

std::vector<TraceRow> DecodeTraces(const std::string& payload);
std::vector<MetricRow> DecodeMetrics(const std::string& payload);
std::vector<LogRow> DecodeLogs(const std::string& payload);

}  // namespace otlp_decoder
