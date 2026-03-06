#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace otlp_decoder {

struct TraceRow {
  uint64_t timestamp_ns;
  std::string trace_id;
  std::string span_id;
  std::string parent_span_id;
  std::string trace_state;
  std::string span_name;
  std::string span_kind;
  std::string service_name;
  std::map<std::string, std::string> resource_attributes;
  std::string scope_name;
  std::string scope_version;
  std::map<std::string, std::string> span_attributes;
  uint64_t duration_ns;
  std::string status_code;
  std::string status_message;
  std::vector<uint64_t> event_timestamps_ns;
  std::vector<std::string> event_names;
  std::vector<std::map<std::string, std::string>> event_attributes;
  std::vector<std::string> link_trace_ids;
  std::vector<std::string> link_span_ids;
  std::vector<std::string> link_trace_states;
  std::vector<std::map<std::string, std::string>> link_attributes;
};

enum class MetricType {
  Gauge,
  Sum,
  Histogram,
  ExponentialHistogram,
  Summary,
};

struct MetricRow {
  std::map<std::string, std::string> resource_attributes;
  std::string resource_schema_url;
  std::string scope_name;
  std::string scope_version;
  std::map<std::string, std::string> scope_attributes;
  uint32_t scope_dropped_attr_count;
  std::string scope_schema_url;

  std::string metric_description;
  std::string metric_unit;
  std::map<std::string, std::string> attributes;

  uint64_t start_timestamp_ns;
  uint64_t timestamp_ns;
  std::string service_name;
  std::string metric_name;
  double value;
  uint64_t count;
  std::vector<uint64_t> bucket_counts;
  std::vector<double> explicit_bounds;

  int32_t scale;
  uint64_t zero_count;
  int32_t positive_offset;
  std::vector<uint64_t> positive_bucket_counts;
  int32_t negative_offset;
  std::vector<uint64_t> negative_bucket_counts;

  std::vector<double> quantile_values;
  std::vector<double> quantile_percentages;

  uint32_t flags;
  double min;
  double max;
  int32_t aggregation_temporality;
  bool is_monotonic;

  std::vector<std::map<std::string, std::string>> exemplar_filtered_attributes;
  std::vector<uint64_t> exemplar_timestamps_ns;
  std::vector<double> exemplar_values;
  std::vector<std::string> exemplar_span_ids;
  std::vector<std::string> exemplar_trace_ids;

  MetricType metric_type;
};

struct LogRow {
  uint64_t timestamp_ns;
  std::string trace_id;
  std::string span_id;
  uint8_t trace_flags;
  std::string service_name;
  std::string severity_text;
  uint8_t severity_number;
  std::string body;
  std::string resource_schema_url;
  std::map<std::string, std::string> resource_attributes;
  std::string scope_schema_url;
  std::string scope_name;
  std::string scope_version;
  std::map<std::string, std::string> scope_attributes;
  std::map<std::string, std::string> log_attributes;
};

std::vector<TraceRow> DecodeTraces(const std::string& payload);
std::vector<MetricRow> DecodeMetrics(const std::string& payload);
std::vector<LogRow> DecodeLogs(const std::string& payload);

}  // namespace otlp_decoder
