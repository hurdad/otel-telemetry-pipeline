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
  }

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
  EXPECT_EQ(rows[0].operation_name, "operation-1");
}

TEST(OtlpDecoderTest, DecodeMetricsMapsMetricNamesAndCurrentHardcodedFields) {
  auto rows = otlp_decoder::DecodeMetrics(
      SerializeMetricsRequest("billing", {"cpu.usage", "memory.usage"}));

  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0].service_name, "billing");
  EXPECT_EQ(rows[0].metric_name, "cpu.usage");
  EXPECT_EQ(rows[0].timestamp_ns, 0u);
  EXPECT_DOUBLE_EQ(rows[0].value, 0.0);

  EXPECT_EQ(rows[1].service_name, "billing");
  EXPECT_EQ(rows[1].metric_name, "memory.usage");
  EXPECT_EQ(rows[1].timestamp_ns, 0u);
  EXPECT_DOUBLE_EQ(rows[1].value, 0.0);
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

}  // namespace
