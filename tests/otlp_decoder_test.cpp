#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "opentelemetry/proto/collector/logs/v1/logs_service.pb.h"
#include "opentelemetry/proto/collector/metrics/v1/metrics_service.pb.h"
#include "opentelemetry/proto/collector/trace/v1/trace_service.pb.h"
#include "otlp_decoder/decoder.h"

namespace {

std::string SerializeTraceRequestWithServiceName(const std::string &service_name) {
  opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest req;
  auto *resource_spans = req.add_resource_spans();
  auto *attribute = resource_spans->mutable_resource()->add_attributes();
  attribute->set_key("service.name");
  attribute->mutable_value()->set_string_value(service_name);

  auto *scope_spans = resource_spans->add_scope_spans();
  auto *span = scope_spans->add_spans();
  span->set_trace_id("trace-1");
  span->set_span_id("span-1");
  span->set_parent_span_id("parent-1");
  span->set_name("operation-1");
  span->set_start_time_unix_nano(100);
  span->set_end_time_unix_nano(180);

  std::string payload;
  EXPECT_TRUE(req.SerializeToString(&payload));
  return payload;
}

std::string SerializeTraceRequestWithoutServiceName() {
  opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest req;
  auto *resource_spans = req.add_resource_spans();
  auto *scope_spans = resource_spans->add_scope_spans();
  auto *span = scope_spans->add_spans();
  span->set_trace_id("trace-2");
  span->set_span_id("span-2");
  span->set_name("operation-2");
  span->set_start_time_unix_nano(10);
  span->set_end_time_unix_nano(20);

  std::string payload;
  EXPECT_TRUE(req.SerializeToString(&payload));
  return payload;
}

std::string SerializeMetricsRequest(const std::string &service_name,
                                    const std::vector<std::string> &metric_names) {
  opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest req;
  auto *resource_metrics = req.add_resource_metrics();
  if (!service_name.empty()) {
    auto *attribute = resource_metrics->mutable_resource()->add_attributes();
    attribute->set_key("service.name");
    attribute->mutable_value()->set_string_value(service_name);
  }

  auto *scope_metrics = resource_metrics->add_scope_metrics();
  for (const auto &metric_name : metric_names) {
    auto *metric = scope_metrics->add_metrics();
    metric->set_name(metric_name);
    auto *dp = metric->mutable_gauge()->add_data_points();
    dp->set_time_unix_nano(1000);
    dp->set_as_double(1.5);
  }

  std::string payload;
  EXPECT_TRUE(req.SerializeToString(&payload));
  return payload;
}



std::string SerializeMetricsRequestWithMultipleDataPointTypes() {
  opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest req;
  auto *resource_metrics = req.add_resource_metrics();
  resource_metrics->set_schema_url("resource-schema");
  auto *attribute = resource_metrics->mutable_resource()->add_attributes();
  attribute->set_key("service.name");
  attribute->mutable_value()->set_string_value("analytics");
  auto *res_region = resource_metrics->mutable_resource()->add_attributes();
  res_region->set_key("cloud.region");
  res_region->mutable_value()->set_string_value("us-east-1");

  auto *scope_metrics = resource_metrics->add_scope_metrics();
  scope_metrics->set_schema_url("scope-schema");
  scope_metrics->mutable_scope()->set_name("scope-a");
  scope_metrics->mutable_scope()->set_version("v1");
  scope_metrics->mutable_scope()->set_dropped_attributes_count(4);
  auto *scope_attr = scope_metrics->mutable_scope()->add_attributes();
  scope_attr->set_key("library");
  scope_attr->mutable_value()->set_string_value("core");

  auto *gauge_metric = scope_metrics->add_metrics();
  gauge_metric->set_name("requests.inflight");
  gauge_metric->set_description("inflight requests");
  gauge_metric->set_unit("1");
  auto *gauge_dp = gauge_metric->mutable_gauge()->add_data_points();
  gauge_dp->set_start_time_unix_nano(5);
  gauge_dp->set_time_unix_nano(10);
  gauge_dp->set_as_int(7);
  gauge_dp->set_flags(3);
  auto *gauge_attr = gauge_dp->add_attributes();
  gauge_attr->set_key("host.name");
  gauge_attr->mutable_value()->set_string_value("api-1");
  auto *gauge_ex = gauge_dp->add_exemplars();
  gauge_ex->set_time_unix_nano(9);
  gauge_ex->set_as_double(6.5);
  gauge_ex->set_span_id("span");
  gauge_ex->set_trace_id("trace");

  auto *sum_metric = scope_metrics->add_metrics();
  sum_metric->set_name("requests.total");
  sum_metric->set_description("total requests");
  sum_metric->set_unit("count");
  sum_metric->mutable_sum()->set_aggregation_temporality(
      opentelemetry::proto::metrics::v1::AggregationTemporality::AGGREGATION_TEMPORALITY_CUMULATIVE);
  sum_metric->mutable_sum()->set_is_monotonic(true);
  auto *sum_dp = sum_metric->mutable_sum()->add_data_points();
  sum_dp->set_start_time_unix_nano(6);
  sum_dp->set_time_unix_nano(11);
  sum_dp->set_as_double(11.5);

  auto *histogram_metric = scope_metrics->add_metrics();
  histogram_metric->set_name("latency.histogram");
  histogram_metric->mutable_histogram()->set_aggregation_temporality(
      opentelemetry::proto::metrics::v1::AggregationTemporality::AGGREGATION_TEMPORALITY_DELTA);
  auto *histogram_dp = histogram_metric->mutable_histogram()->add_data_points();
  histogram_dp->set_start_time_unix_nano(7);
  histogram_dp->set_time_unix_nano(12);
  histogram_dp->set_count(2);
  histogram_dp->set_sum(15.25);
  histogram_dp->add_bucket_counts(1);
  histogram_dp->add_bucket_counts(1);
  histogram_dp->add_explicit_bounds(10.0);
  histogram_dp->set_min(1.0);
  histogram_dp->set_max(14.0);

  auto *exp_metric = scope_metrics->add_metrics();
  exp_metric->set_name("latency.exp");
  exp_metric->mutable_exponential_histogram()->set_aggregation_temporality(
      opentelemetry::proto::metrics::v1::AggregationTemporality::AGGREGATION_TEMPORALITY_CUMULATIVE);
  auto *exp_dp = exp_metric->mutable_exponential_histogram()->add_data_points();
  exp_dp->set_start_time_unix_nano(8);
  exp_dp->set_time_unix_nano(13);
  exp_dp->set_count(3);
  exp_dp->set_sum(20.0);
  exp_dp->set_scale(2);
  exp_dp->set_zero_count(1);
  exp_dp->mutable_positive()->set_offset(4);
  exp_dp->mutable_positive()->add_bucket_counts(2);
  exp_dp->mutable_negative()->set_offset(-2);
  exp_dp->mutable_negative()->add_bucket_counts(1);
  exp_dp->set_min(0.5);
  exp_dp->set_max(18.0);

  auto *summary_metric = scope_metrics->add_metrics();
  summary_metric->set_name("latency.summary");
  auto *summary_dp = summary_metric->mutable_summary()->add_data_points();
  summary_dp->set_start_time_unix_nano(9);
  summary_dp->set_time_unix_nano(14);
  summary_dp->set_count(4);
  summary_dp->set_sum(2.5);
  auto *qv = summary_dp->add_quantile_values();
  qv->set_quantile(0.5);
  qv->set_value(2.0);

  std::string payload;
  EXPECT_TRUE(req.SerializeToString(&payload));
  return payload;
}

std::string SerializeLogsRequestWithNonStringBody() {
  opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest req;
  auto *resource_logs = req.add_resource_logs();
  auto *scope_logs = resource_logs->add_scope_logs();
  auto *record = scope_logs->add_log_records();
  record->set_time_unix_nano(777);
  record->set_severity_text("DEBUG");
  record->mutable_body()->set_bool_value(true);

  std::string payload;
  EXPECT_TRUE(req.SerializeToString(&payload));
  return payload;
}
std::string SerializeLogsRequest(const std::string &service_name,
                                 uint64_t timestamp_ns,
                                 const std::string &severity_text,
                                 const std::string &body) {
  opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest req;
  auto *resource_logs = req.add_resource_logs();
  if (!service_name.empty()) {
    auto *attribute = resource_logs->mutable_resource()->add_attributes();
    attribute->set_key("service.name");
    attribute->mutable_value()->set_string_value(service_name);
  }

  auto *scope_logs = resource_logs->add_scope_logs();
  auto *record = scope_logs->add_log_records();
  record->set_time_unix_nano(timestamp_ns);
  record->set_severity_text(severity_text);
  record->mutable_body()->set_string_value(body);

  std::string payload;
  EXPECT_TRUE(req.SerializeToString(&payload));
  return payload;
}

TEST(OtlpDecoderTest, InvalidNonProtobufPayloadReturnsEmptyRowsForAllDecoders) {
  const std::string invalid_payload = "not-a-protobuf";

  EXPECT_TRUE(otlp_decoder::DecodeTraces(invalid_payload).empty());
  EXPECT_TRUE(otlp_decoder::DecodeMetrics(invalid_payload).empty());
  EXPECT_TRUE(otlp_decoder::DecodeLogs(invalid_payload).empty());
}

TEST(OtlpDecoderTest, MissingServiceNameDefaultsToUnknown) {
  auto traces = otlp_decoder::DecodeTraces(SerializeTraceRequestWithoutServiceName());
  ASSERT_EQ(traces.size(), 1);
  EXPECT_EQ(traces[0].service_name, "unknown");

  auto metrics = otlp_decoder::DecodeMetrics(SerializeMetricsRequest("", {"requests_total"}));
  ASSERT_EQ(metrics.size(), 1);
  EXPECT_EQ(metrics[0].service_name, "unknown");

  auto logs = otlp_decoder::DecodeLogs(SerializeLogsRequest("", 123, "INFO", "hello"));
  ASSERT_EQ(logs.size(), 1);
  EXPECT_EQ(logs[0].service_name, "unknown");
}

TEST(OtlpDecoderTest, DecodeTracesComputesDurationFromStartAndEndTime) {
  auto rows = otlp_decoder::DecodeTraces(SerializeTraceRequestWithServiceName("checkout"));

  ASSERT_EQ(rows.size(), 1);
  EXPECT_EQ(rows[0].timestamp_ns, 100u);
  EXPECT_EQ(rows[0].duration_ns, 80u);
  EXPECT_EQ(rows[0].service_name, "checkout");
  EXPECT_EQ(rows[0].span_name, "operation-1");
}

TEST(OtlpDecoderTest, DecodeMetricsMapsMetricNamesAndValues) {
  auto rows = otlp_decoder::DecodeMetrics(
      SerializeMetricsRequest("billing", {"cpu.usage", "memory.usage"}));

  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0].service_name, "billing");
  EXPECT_EQ(rows[0].metric_name, "cpu.usage");
  EXPECT_EQ(rows[0].timestamp_ns, 1000u);
  EXPECT_DOUBLE_EQ(rows[0].value, 1.5);
  EXPECT_EQ(rows[0].metric_type, otlp_decoder::MetricType::Gauge);

  EXPECT_EQ(rows[1].service_name, "billing");
  EXPECT_EQ(rows[1].metric_name, "memory.usage");
  EXPECT_EQ(rows[1].timestamp_ns, 1000u);
  EXPECT_DOUBLE_EQ(rows[1].value, 1.5);
  EXPECT_EQ(rows[1].metric_type, otlp_decoder::MetricType::Gauge);
}

TEST(OtlpDecoderTest, DecodeLogsMapsTimestampSeverityAndBodyStringValue) {
  auto rows = otlp_decoder::DecodeLogs(
      SerializeLogsRequest("frontend", 4242, "WARN", "cache miss"));

  ASSERT_EQ(rows.size(), 1);
  EXPECT_EQ(rows[0].timestamp_ns, 4242u);
  EXPECT_EQ(rows[0].service_name, "frontend");
  EXPECT_EQ(rows[0].severity_text, "WARN");
  EXPECT_EQ(rows[0].body, "cache miss");
}


TEST(OtlpDecoderTest, DecodeMetricsSupportsAllDataPointTypesAndMetadata) {
  auto rows = otlp_decoder::DecodeMetrics(SerializeMetricsRequestWithMultipleDataPointTypes());

  ASSERT_EQ(rows.size(), 5);
  EXPECT_EQ(rows[0].service_name, "analytics");
  EXPECT_EQ(rows[0].metric_name, "requests.inflight");
  EXPECT_EQ(rows[0].timestamp_ns, 10u);
  EXPECT_DOUBLE_EQ(rows[0].value, 7.0);
  EXPECT_EQ(rows[0].metric_type, otlp_decoder::MetricType::Gauge);

  EXPECT_EQ(rows[1].metric_name, "requests.total");
  EXPECT_EQ(rows[1].timestamp_ns, 11u);
  EXPECT_DOUBLE_EQ(rows[1].value, 11.5);
  EXPECT_EQ(rows[1].metric_type, otlp_decoder::MetricType::Sum);

  EXPECT_EQ(rows[2].metric_name, "latency.histogram");
  EXPECT_EQ(rows[2].timestamp_ns, 12u);
  EXPECT_DOUBLE_EQ(rows[2].value, 15.25);
  EXPECT_EQ(rows[2].metric_type, otlp_decoder::MetricType::Histogram);

  EXPECT_EQ(rows[2].aggregation_temporality,
            opentelemetry::proto::metrics::v1::AggregationTemporality::AGGREGATION_TEMPORALITY_DELTA);

  EXPECT_EQ(rows[3].metric_name, "latency.exp");
  EXPECT_EQ(rows[3].timestamp_ns, 13u);
  EXPECT_EQ(rows[3].scale, 2);
  EXPECT_EQ(rows[3].positive_offset, 4);
  EXPECT_EQ(rows[3].negative_offset, -2);
  EXPECT_EQ(rows[3].metric_type, otlp_decoder::MetricType::ExponentialHistogram);

  EXPECT_EQ(rows[4].metric_name, "latency.summary");
  EXPECT_EQ(rows[4].timestamp_ns, 14u);
  EXPECT_DOUBLE_EQ(rows[4].value, 2.5);
  EXPECT_EQ(rows[4].quantile_percentages[0], 0.5);
  EXPECT_EQ(rows[4].quantile_values[0], 2.0);
  EXPECT_EQ(rows[4].metric_type, otlp_decoder::MetricType::Summary);

  EXPECT_EQ(rows[0].resource_schema_url, "resource-schema");
  EXPECT_EQ(rows[0].scope_schema_url, "scope-schema");
  EXPECT_EQ(rows[0].scope_name, "scope-a");
  EXPECT_EQ(rows[0].scope_version, "v1");
  EXPECT_EQ(rows[0].scope_dropped_attr_count, 4u);
  EXPECT_EQ(rows[0].metric_description, "inflight requests");
  EXPECT_EQ(rows[0].metric_unit, "1");
  EXPECT_EQ(rows[0].attributes.at("host.name"), "api-1");
  EXPECT_EQ(rows[0].exemplar_timestamps_ns[0], 9u);
}

TEST(OtlpDecoderTest, DecodeLogsUsesEmptyBodyForNonStringValueTypes) {
  auto rows = otlp_decoder::DecodeLogs(SerializeLogsRequestWithNonStringBody());

  ASSERT_EQ(rows.size(), 1);
  EXPECT_EQ(rows[0].service_name, "unknown");
  EXPECT_EQ(rows[0].severity_text, "DEBUG");
  EXPECT_EQ(rows[0].body, "true");
}

}  // namespace
