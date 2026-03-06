#include "otlp_decoder/decoder.h"

#include <sstream>

#include "opentelemetry/proto/collector/logs/v1/logs_service.pb.h"
#include "opentelemetry/proto/collector/metrics/v1/metrics_service.pb.h"
#include "opentelemetry/proto/collector/trace/v1/trace_service.pb.h"

namespace otlp_decoder {

namespace {

std::string SpanKindToString(opentelemetry::proto::trace::v1::Span::SpanKind span_kind) {
  using SpanKind = opentelemetry::proto::trace::v1::Span::SpanKind;
  switch (span_kind) {
    case SpanKind::Span_SpanKind_SPAN_KIND_INTERNAL:
      return "SPAN_KIND_INTERNAL";
    case SpanKind::Span_SpanKind_SPAN_KIND_SERVER:
      return "SPAN_KIND_SERVER";
    case SpanKind::Span_SpanKind_SPAN_KIND_CLIENT:
      return "SPAN_KIND_CLIENT";
    case SpanKind::Span_SpanKind_SPAN_KIND_PRODUCER:
      return "SPAN_KIND_PRODUCER";
    case SpanKind::Span_SpanKind_SPAN_KIND_CONSUMER:
      return "SPAN_KIND_CONSUMER";
    case SpanKind::Span_SpanKind_SPAN_KIND_UNSPECIFIED:
      return "SPAN_KIND_UNSPECIFIED";
  }
  return "SPAN_KIND_UNSPECIFIED";
}

std::string StatusCodeToString(opentelemetry::proto::trace::v1::Status::StatusCode status_code) {
  using StatusCode = opentelemetry::proto::trace::v1::Status::StatusCode;
  switch (status_code) {
    case StatusCode::Status_StatusCode_STATUS_CODE_OK:
      return "STATUS_CODE_OK";
    case StatusCode::Status_StatusCode_STATUS_CODE_ERROR:
      return "STATUS_CODE_ERROR";
    case StatusCode::Status_StatusCode_STATUS_CODE_UNSET:
      return "STATUS_CODE_UNSET";
  }
  return "STATUS_CODE_UNSET";
}

std::string BytesToHex(const std::string& bytes) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string out;
  out.reserve(bytes.size() * 2);
  for (unsigned char c : bytes) {
    out.push_back(kHex[c >> 4]);
    out.push_back(kHex[c & 0x0F]);
  }
  return out;
}

std::string AnyValueToString(const opentelemetry::proto::common::v1::AnyValue& value) {
  using AnyValue = opentelemetry::proto::common::v1::AnyValue;
  switch (value.value_case()) {
    case AnyValue::kStringValue:
      return value.string_value();
    case AnyValue::kBoolValue:
      return value.bool_value() ? "true" : "false";
    case AnyValue::kIntValue:
      return std::to_string(value.int_value());
    case AnyValue::kDoubleValue:
      return std::to_string(value.double_value());
    case AnyValue::kBytesValue:
      return BytesToHex(value.bytes_value());
    case AnyValue::kArrayValue: {
      std::ostringstream os;
      os << '[';
      bool first = true;
      for (const auto& item : value.array_value().values()) {
        if (!first) {
          os << ',';
        }
        first = false;
        os << AnyValueToString(item);
      }
      os << ']';
      return os.str();
    }
    case AnyValue::kKvlistValue: {
      std::ostringstream os;
      os << '{';
      bool first = true;
      for (const auto& kv : value.kvlist_value().values()) {
        if (!first) {
          os << ',';
        }
        first = false;
        os << kv.key() << ':' << AnyValueToString(kv.value());
      }
      os << '}';
      return os.str();
    }
    case AnyValue::VALUE_NOT_SET:
      return "";
  }
  return "";
}

std::map<std::string, std::string> AttributesToMap(
    const google::protobuf::RepeatedPtrField<opentelemetry::proto::common::v1::KeyValue>& attrs) {
  std::map<std::string, std::string> out;
  for (const auto& attr : attrs) {
    out[attr.key()] = AnyValueToString(attr.value());
  }
  return out;
}

std::string ServiceNameFromMap(const std::map<std::string, std::string>& attrs) {
  const auto it = attrs.find("service.name");
  return it != attrs.end() ? it->second : "unknown";
}

}  // namespace

std::vector<TraceRow> DecodeTraces(const std::string& payload) {
  opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest req;
  if (!req.ParseFromString(payload)) {
    return {};
  }
  std::vector<TraceRow> rows;
  for (const auto& rs : req.resource_spans()) {
    auto resource_attributes = AttributesToMap(rs.resource().attributes());
    const std::string service_name = ServiceNameFromMap(resource_attributes);
    for (const auto& ss : rs.scope_spans()) {
      for (const auto& span : ss.spans()) {
        std::vector<uint64_t> event_timestamps_ns;
        std::vector<std::string> event_names;
        std::vector<std::map<std::string, std::string>> event_attributes;
        event_timestamps_ns.reserve(span.events_size());
        event_names.reserve(span.events_size());
        event_attributes.reserve(span.events_size());
        for (const auto& event : span.events()) {
          event_timestamps_ns.push_back(static_cast<uint64_t>(event.time_unix_nano()));
          event_names.push_back(event.name());
          event_attributes.push_back(AttributesToMap(event.attributes()));
        }

        std::vector<std::string> link_trace_ids;
        std::vector<std::string> link_span_ids;
        std::vector<std::string> link_trace_states;
        std::vector<std::map<std::string, std::string>> link_attributes;
        link_trace_ids.reserve(span.links_size());
        link_span_ids.reserve(span.links_size());
        link_trace_states.reserve(span.links_size());
        link_attributes.reserve(span.links_size());
        for (const auto& link : span.links()) {
          link_trace_ids.push_back(BytesToHex(link.trace_id()));
          link_span_ids.push_back(BytesToHex(link.span_id()));
          link_trace_states.push_back(link.trace_state());
          link_attributes.push_back(AttributesToMap(link.attributes()));
        }

        const auto start_ns = static_cast<uint64_t>(span.start_time_unix_nano());
        const auto end_ns = static_cast<uint64_t>(span.end_time_unix_nano());
        const uint64_t duration_ns = end_ns > start_ns ? end_ns - start_ns : 0;

        rows.push_back({
            start_ns,
            BytesToHex(span.trace_id()),
            BytesToHex(span.span_id()),
            BytesToHex(span.parent_span_id()),
            span.trace_state(),
            span.name(),
            SpanKindToString(span.kind()),
            service_name,
            resource_attributes,
            ss.scope().name(),
            ss.scope().version(),
            AttributesToMap(span.attributes()),
            duration_ns,
            StatusCodeToString(span.status().code()),
            span.status().message(),
            std::move(event_timestamps_ns),
            std::move(event_names),
            std::move(event_attributes),
            std::move(link_trace_ids),
            std::move(link_span_ids),
            std::move(link_trace_states),
            std::move(link_attributes)});
      }
    }
  }
  return rows;
}

std::vector<MetricRow> DecodeMetrics(const std::string& payload) {
  opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest req;
  if (!req.ParseFromString(payload)) {
    return {};
  }

  struct ExemplarArrays {
    std::vector<std::map<std::string, std::string>> filtered_attributes;
    std::vector<uint64_t> timestamps_ns;
    std::vector<double> values;
    std::vector<std::string> span_ids;
    std::vector<std::string> trace_ids;
  };

  const auto decode_exemplars = [](const auto& exemplars) {
    ExemplarArrays arrays;
    for (const auto& exemplar : exemplars) {
      arrays.filtered_attributes.push_back(AttributesToMap(exemplar.filtered_attributes()));
      arrays.timestamps_ns.push_back(static_cast<uint64_t>(exemplar.time_unix_nano()));
      arrays.values.push_back(exemplar.value_case() == exemplar.kAsInt
                                  ? static_cast<double>(exemplar.as_int())
                                  : exemplar.as_double());
      arrays.span_ids.push_back(BytesToHex(exemplar.span_id()));
      arrays.trace_ids.push_back(BytesToHex(exemplar.trace_id()));
    }
    return arrays;
  };

  using NDP = opentelemetry::proto::metrics::v1::NumberDataPoint;
  std::vector<MetricRow> rows;
  for (const auto& rm : req.resource_metrics()) {
    const auto resource_attributes = AttributesToMap(rm.resource().attributes());
    const std::string service_name = ServiceNameFromMap(resource_attributes);
    for (const auto& sm : rm.scope_metrics()) {
      const auto scope_attributes = AttributesToMap(sm.scope().attributes());
      for (const auto& metric : sm.metrics()) {
        const auto add = [&](MetricRow row) {
          row.resource_attributes = resource_attributes;
          row.resource_schema_url = rm.schema_url();
          row.scope_name = sm.scope().name();
          row.scope_version = sm.scope().version();
          row.scope_attributes = scope_attributes;
          row.scope_dropped_attr_count = sm.scope().dropped_attributes_count();
          row.scope_schema_url = sm.schema_url();
          row.service_name = service_name;
          row.metric_name = metric.name();
          row.metric_description = metric.description();
          row.metric_unit = metric.unit();
          rows.push_back(std::move(row));
        };
        const auto ndp_value = [](const NDP& dp) -> double {
          return dp.value_case() == NDP::kAsInt ? static_cast<double>(dp.as_int())
                                                : dp.as_double();
        };

        if (metric.has_gauge()) {
          for (const auto& dp : metric.gauge().data_points()) {
            const auto exemplars = decode_exemplars(dp.exemplars());
            add(MetricRow{
                .attributes = AttributesToMap(dp.attributes()),
                .start_timestamp_ns = static_cast<uint64_t>(dp.start_time_unix_nano()),
                .timestamp_ns = static_cast<uint64_t>(dp.time_unix_nano()),
                .value = ndp_value(dp),
                .count = 0,
                .flags = dp.flags(),
                .min = 0,
                .max = 0,
                .aggregation_temporality = 0,
                .is_monotonic = false,
                .exemplar_filtered_attributes = std::move(exemplars.filtered_attributes),
                .exemplar_timestamps_ns = std::move(exemplars.timestamps_ns),
                .exemplar_values = std::move(exemplars.values),
                .exemplar_span_ids = std::move(exemplars.span_ids),
                .exemplar_trace_ids = std::move(exemplars.trace_ids),
                .metric_type = MetricType::Gauge,
            });
          }
        } else if (metric.has_sum()) {
          for (const auto& dp : metric.sum().data_points()) {
            const auto exemplars = decode_exemplars(dp.exemplars());
            add(MetricRow{
                .attributes = AttributesToMap(dp.attributes()),
                .start_timestamp_ns = static_cast<uint64_t>(dp.start_time_unix_nano()),
                .timestamp_ns = static_cast<uint64_t>(dp.time_unix_nano()),
                .value = ndp_value(dp),
                .count = 0,
                .flags = dp.flags(),
                .min = 0,
                .max = 0,
                .aggregation_temporality = metric.sum().aggregation_temporality(),
                .is_monotonic = metric.sum().is_monotonic(),
                .exemplar_filtered_attributes = std::move(exemplars.filtered_attributes),
                .exemplar_timestamps_ns = std::move(exemplars.timestamps_ns),
                .exemplar_values = std::move(exemplars.values),
                .exemplar_span_ids = std::move(exemplars.span_ids),
                .exemplar_trace_ids = std::move(exemplars.trace_ids),
                .metric_type = MetricType::Sum,
            });
          }
        } else if (metric.has_histogram()) {
          for (const auto& dp : metric.histogram().data_points()) {
            std::vector<uint64_t> bucket_counts(dp.bucket_counts().begin(), dp.bucket_counts().end());
            std::vector<double> explicit_bounds(dp.explicit_bounds().begin(),
                                                dp.explicit_bounds().end());
            const auto exemplars = decode_exemplars(dp.exemplars());
            add(MetricRow{
                .attributes = AttributesToMap(dp.attributes()),
                .start_timestamp_ns = static_cast<uint64_t>(dp.start_time_unix_nano()),
                .timestamp_ns = static_cast<uint64_t>(dp.time_unix_nano()),
                .value = dp.sum(),
                .count = dp.count(),
                .bucket_counts = std::move(bucket_counts),
                .explicit_bounds = std::move(explicit_bounds),
                .flags = dp.flags(),
                .min = dp.has_min() ? dp.min() : 0,
                .max = dp.has_max() ? dp.max() : 0,
                .aggregation_temporality = metric.histogram().aggregation_temporality(),
                .is_monotonic = false,
                .exemplar_filtered_attributes = std::move(exemplars.filtered_attributes),
                .exemplar_timestamps_ns = std::move(exemplars.timestamps_ns),
                .exemplar_values = std::move(exemplars.values),
                .exemplar_span_ids = std::move(exemplars.span_ids),
                .exemplar_trace_ids = std::move(exemplars.trace_ids),
                .metric_type = MetricType::Histogram,
            });
          }
        } else if (metric.has_exponential_histogram()) {
          for (const auto& dp : metric.exponential_histogram().data_points()) {
            std::vector<uint64_t> positive_bucket_counts(
                dp.positive().bucket_counts().begin(), dp.positive().bucket_counts().end());
            std::vector<uint64_t> negative_bucket_counts(
                dp.negative().bucket_counts().begin(), dp.negative().bucket_counts().end());
            const auto exemplars = decode_exemplars(dp.exemplars());
            add(MetricRow{
                .attributes = AttributesToMap(dp.attributes()),
                .start_timestamp_ns = static_cast<uint64_t>(dp.start_time_unix_nano()),
                .timestamp_ns = static_cast<uint64_t>(dp.time_unix_nano()),
                .value = dp.sum(),
                .count = dp.count(),
                .scale = dp.scale(),
                .zero_count = dp.zero_count(),
                .positive_offset = dp.positive().offset(),
                .positive_bucket_counts = std::move(positive_bucket_counts),
                .negative_offset = dp.negative().offset(),
                .negative_bucket_counts = std::move(negative_bucket_counts),
                .flags = dp.flags(),
                .min = dp.has_min() ? dp.min() : 0,
                .max = dp.has_max() ? dp.max() : 0,
                .aggregation_temporality = metric.exponential_histogram().aggregation_temporality(),
                .is_monotonic = false,
                .exemplar_filtered_attributes = std::move(exemplars.filtered_attributes),
                .exemplar_timestamps_ns = std::move(exemplars.timestamps_ns),
                .exemplar_values = std::move(exemplars.values),
                .exemplar_span_ids = std::move(exemplars.span_ids),
                .exemplar_trace_ids = std::move(exemplars.trace_ids),
                .metric_type = MetricType::ExponentialHistogram,
            });
          }
        } else if (metric.has_summary()) {
          for (const auto& dp : metric.summary().data_points()) {
            std::vector<double> quantile_percentages;
            std::vector<double> quantile_values;
            quantile_percentages.reserve(dp.quantile_values_size());
            quantile_values.reserve(dp.quantile_values_size());
            for (const auto& quantile : dp.quantile_values()) {
              quantile_percentages.push_back(quantile.quantile());
              quantile_values.push_back(quantile.value());
            }
            add(MetricRow{
                .attributes = AttributesToMap(dp.attributes()),
                .start_timestamp_ns = static_cast<uint64_t>(dp.start_time_unix_nano()),
                .timestamp_ns = static_cast<uint64_t>(dp.time_unix_nano()),
                .value = dp.sum(),
                .count = dp.count(),
                .quantile_values = std::move(quantile_values),
                .quantile_percentages = std::move(quantile_percentages),
                .flags = dp.flags(),
                .min = 0,
                .max = 0,
                .aggregation_temporality = 0,
                .is_monotonic = false,
                .metric_type = MetricType::Summary,
            });
          }
        }
      }
    }
  }
  return rows;
}

std::vector<LogRow> DecodeLogs(const std::string& payload) {
  opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest req;
  if (!req.ParseFromString(payload)) {
    return {};
  }
  std::vector<LogRow> rows;
  for (const auto& rl : req.resource_logs()) {
    auto resource_attributes = AttributesToMap(rl.resource().attributes());
    const std::string service_name = ServiceNameFromMap(resource_attributes);
    for (const auto& sl : rl.scope_logs()) {
      auto scope_attributes = AttributesToMap(sl.scope().attributes());
      for (const auto& rec : sl.log_records()) {
        rows.push_back({
            static_cast<uint64_t>(rec.time_unix_nano()),
            BytesToHex(rec.trace_id()),
            BytesToHex(rec.span_id()),
            static_cast<uint8_t>(rec.flags()),
            service_name,
            rec.severity_text(),
            static_cast<uint8_t>(rec.severity_number()),
            AnyValueToString(rec.body()),
            rl.schema_url(),
            resource_attributes,
            sl.schema_url(),
            sl.scope().name(),
            sl.scope().version(),
            scope_attributes,
            AttributesToMap(rec.attributes())});
      }
    }
  }
  return rows;
}

}  // namespace otlp_decoder
