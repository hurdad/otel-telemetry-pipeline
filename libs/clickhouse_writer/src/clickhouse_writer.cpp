#include "clickhouse_writer/clickhouse_writer.h"

#include <cstdint>
#include <iostream>

#include "clickhouse/client.h"
#include "clickhouse/columns/array.h"
#include "clickhouse/columns/date.h"
#include "clickhouse/columns/lowcardinality.h"
#include "clickhouse/columns/map.h"
#include "clickhouse/columns/numeric.h"
#include "clickhouse/columns/string.h"
#include "telemetry/tracer.h"

namespace clickhouse_writer {

struct ClickHouseWriter::Impl {
  clickhouse::ClientOptions opts;
  std::unique_ptr<clickhouse::Client> client;

  Impl(const std::string& host, uint16_t port, const std::string& database,
       const std::string& user, const std::string& password) {
    opts.SetHost(host).SetPort(port).SetDefaultDatabase(database)
        .SetUser(user).SetPassword(password);
  }

  clickhouse::Client& GetClient() {
    if (!client) {
      client = std::make_unique<clickhouse::Client>(opts);
    }
    return *client;
  }

  void ResetClient() { client.reset(); }
};

ClickHouseWriter::ClickHouseWriter(std::string host, uint16_t port, std::string database,
                                   std::string user, std::string password)
    : impl_(std::make_unique<Impl>(host, port, database, user, password)) {}

ClickHouseWriter::~ClickHouseWriter() = default;

void ClickHouseWriter::InsertTraces(const std::vector<otlp_decoder::TraceRow>& rows) {
  if (rows.empty()) return;
  auto span = telemetry::StartSpan("clickhouse_insert");
  try {
    auto& client = impl_->GetClient();

    auto ts_col = std::make_shared<clickhouse::ColumnDateTime64>(9);
    auto trace_id_col = std::make_shared<clickhouse::ColumnString>();
    auto span_id_col = std::make_shared<clickhouse::ColumnString>();
    auto parent_id_col = std::make_shared<clickhouse::ColumnString>();
    auto trace_state_col = std::make_shared<clickhouse::ColumnString>();
    auto span_name_col =
        std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>();
    auto span_kind_col =
        std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>();
    auto service_name_col =
        std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>();
    auto resource_attributes_col =
        std::make_shared<clickhouse::ColumnMapT<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>,
                                                 clickhouse::ColumnString>>(
            std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>(),
            std::make_shared<clickhouse::ColumnString>());
    auto scope_name_col = std::make_shared<clickhouse::ColumnString>();
    auto scope_version_col = std::make_shared<clickhouse::ColumnString>();
    auto span_attributes_col =
        std::make_shared<clickhouse::ColumnMapT<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>,
                                                 clickhouse::ColumnString>>(
            std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>(),
            std::make_shared<clickhouse::ColumnString>());
    auto duration_col = std::make_shared<clickhouse::ColumnUInt64>();
    auto status_code_col =
        std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>();
    auto status_message_col = std::make_shared<clickhouse::ColumnString>();
    auto event_timestamps_col = std::make_shared<clickhouse::ColumnArrayT<clickhouse::ColumnDateTime64>>(
        std::make_shared<clickhouse::ColumnDateTime64>(9));
    auto event_names_col = std::make_shared<
        clickhouse::ColumnArrayT<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>>(
        std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>());
    auto event_attributes_col =
        std::make_shared<clickhouse::ColumnArrayT<
            clickhouse::ColumnMapT<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>,
                                   clickhouse::ColumnString>>>(
            std::make_shared<clickhouse::ColumnMapT<
                clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>, clickhouse::ColumnString>>(
                std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>(),
                std::make_shared<clickhouse::ColumnString>()));
    auto link_trace_ids_col = std::make_shared<clickhouse::ColumnArrayT<clickhouse::ColumnString>>(
        std::make_shared<clickhouse::ColumnString>());
    auto link_span_ids_col = std::make_shared<clickhouse::ColumnArrayT<clickhouse::ColumnString>>(
        std::make_shared<clickhouse::ColumnString>());
    auto link_trace_states_col =
        std::make_shared<clickhouse::ColumnArrayT<clickhouse::ColumnString>>(
            std::make_shared<clickhouse::ColumnString>());
    auto link_attributes_col =
        std::make_shared<clickhouse::ColumnArrayT<
            clickhouse::ColumnMapT<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>,
                                   clickhouse::ColumnString>>>(
            std::make_shared<clickhouse::ColumnMapT<
                clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>, clickhouse::ColumnString>>(
                std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>(),
                std::make_shared<clickhouse::ColumnString>()));

    for (const auto& row : rows) {
      ts_col->Append(static_cast<int64_t>(row.timestamp_ns));
      trace_id_col->Append(row.trace_id);
      span_id_col->Append(row.span_id);
      parent_id_col->Append(row.parent_span_id);
      trace_state_col->Append(row.trace_state);
      span_name_col->Append(std::string_view(row.span_name));
      span_kind_col->Append(std::string_view(row.span_kind));
      service_name_col->Append(std::string_view(row.service_name));
      resource_attributes_col->Append(row.resource_attributes);
      scope_name_col->Append(row.scope_name);
      scope_version_col->Append(row.scope_version);
      span_attributes_col->Append(row.span_attributes);
      duration_col->Append(row.duration_ns);
      status_code_col->Append(std::string_view(row.status_code));
      status_message_col->Append(row.status_message);
      event_timestamps_col->Append(row.event_timestamps_ns);
      event_names_col->Append(row.event_names);
      event_attributes_col->Append(row.event_attributes);
      link_trace_ids_col->Append(row.link_trace_ids);
      link_span_ids_col->Append(row.link_span_ids);
      link_trace_states_col->Append(row.link_trace_states);
      link_attributes_col->Append(row.link_attributes);
    }

    clickhouse::Block block;
    block.AppendColumn("Timestamp", ts_col);
    block.AppendColumn("TraceId", trace_id_col);
    block.AppendColumn("SpanId", span_id_col);
    block.AppendColumn("ParentSpanId", parent_id_col);
    block.AppendColumn("TraceState", trace_state_col);
    block.AppendColumn("SpanName", span_name_col);
    block.AppendColumn("SpanKind", span_kind_col);
    block.AppendColumn("ServiceName", service_name_col);
    block.AppendColumn("ResourceAttributes", resource_attributes_col);
    block.AppendColumn("ScopeName", scope_name_col);
    block.AppendColumn("ScopeVersion", scope_version_col);
    block.AppendColumn("SpanAttributes", span_attributes_col);
    block.AppendColumn("Duration", duration_col);
    block.AppendColumn("StatusCode", status_code_col);
    block.AppendColumn("StatusMessage", status_message_col);
    block.AppendColumn("Events.Timestamp", event_timestamps_col);
    block.AppendColumn("Events.Name", event_names_col);
    block.AppendColumn("Events.Attributes", event_attributes_col);
    block.AppendColumn("Links.TraceId", link_trace_ids_col);
    block.AppendColumn("Links.SpanId", link_span_ids_col);
    block.AppendColumn("Links.TraceState", link_trace_states_col);
    block.AppendColumn("Links.Attributes", link_attributes_col);

    client.Insert("otel_traces", block);
    telemetry::RecordClickHouseRowsInserted(static_cast<uint64_t>(rows.size()));
    std::clog << "inserted traces rows=" << rows.size() << '\n';
  } catch (const std::exception& e) {
    impl_->ResetClient();
    telemetry::RecordClickHouseInsertError();
    std::clog << "ClickHouse InsertTraces error: " << e.what() << '\n';
  }
}

void ClickHouseWriter::InsertMetrics(const std::vector<otlp_decoder::MetricRow>& rows) {
  if (rows.empty()) return;
  auto span = telemetry::StartSpan("clickhouse_insert");
  try {
    auto& client = impl_->GetClient();

    struct MetricInsertBuffer {
      std::shared_ptr<clickhouse::ColumnDateTime64> start_time_col =
          std::make_shared<clickhouse::ColumnDateTime64>(9);
      std::shared_ptr<clickhouse::ColumnDateTime64> time_col =
          std::make_shared<clickhouse::ColumnDateTime64>(9);
      std::shared_ptr<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>> service_col =
          std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>();
      std::shared_ptr<clickhouse::ColumnString> metric_col = std::make_shared<clickhouse::ColumnString>();
      std::shared_ptr<clickhouse::ColumnFloat64> value_col = std::make_shared<clickhouse::ColumnFloat64>();
      std::shared_ptr<clickhouse::ColumnUInt64> count_col = std::make_shared<clickhouse::ColumnUInt64>();
      std::shared_ptr<clickhouse::ColumnUInt32> flags_col = std::make_shared<clickhouse::ColumnUInt32>();

      void Append(const otlp_decoder::MetricRow& row) {
        const auto ts = static_cast<int64_t>(row.timestamp_ns);
        start_time_col->Append(ts);
        time_col->Append(ts);
        service_col->Append(std::string_view(row.service_name));
        metric_col->Append(row.metric_name);
        value_col->Append(row.value);
        count_col->Append(row.count);
        flags_col->Append(0);
      }

      [[nodiscard]] size_t RowCount() const { return metric_col->Size(); }
    };

    MetricInsertBuffer gauge;
    MetricInsertBuffer sum;
    MetricInsertBuffer histogram;
    MetricInsertBuffer exponential_histogram;
    MetricInsertBuffer summary;

    for (const auto& row : rows) {
      switch (row.metric_type) {
        case otlp_decoder::MetricType::Gauge:
          gauge.Append(row);
          break;
        case otlp_decoder::MetricType::Sum:
          sum.Append(row);
          break;
        case otlp_decoder::MetricType::Histogram:
          histogram.Append(row);
          break;
        case otlp_decoder::MetricType::ExponentialHistogram:
          exponential_histogram.Append(row);
          break;
        case otlp_decoder::MetricType::Summary:
          summary.Append(row);
          break;
      }
    }

    const auto insert_gauge_or_sum = [&](const char* table, const MetricInsertBuffer& buffer) {
      if (buffer.RowCount() == 0) return;
      clickhouse::Block block;
      block.AppendColumn("StartTimeUnix", buffer.start_time_col);
      block.AppendColumn("TimeUnix", buffer.time_col);
      block.AppendColumn("ServiceName", buffer.service_col);
      block.AppendColumn("MetricName", buffer.metric_col);
      block.AppendColumn("Value", buffer.value_col);
      block.AppendColumn("Flags", buffer.flags_col);
      client.Insert(table, block);
    };

    const auto insert_histogram_or_summary = [&](const char* table, const MetricInsertBuffer& buffer) {
      if (buffer.RowCount() == 0) return;
      clickhouse::Block block;
      block.AppendColumn("StartTimeUnix", buffer.start_time_col);
      block.AppendColumn("TimeUnix", buffer.time_col);
      block.AppendColumn("ServiceName", buffer.service_col);
      block.AppendColumn("MetricName", buffer.metric_col);
      block.AppendColumn("Count", buffer.count_col);
      block.AppendColumn("Sum", buffer.value_col);
      block.AppendColumn("Flags", buffer.flags_col);
      client.Insert(table, block);
    };

    insert_gauge_or_sum("otel_metrics_gauge", gauge);
    insert_gauge_or_sum("otel_metrics_sum", sum);
    insert_histogram_or_summary("otel_metrics_histogram", histogram);
    insert_histogram_or_summary("otel_metrics_exponentialhistogram", exponential_histogram);
    insert_histogram_or_summary("otel_metrics_summary", summary);

    telemetry::RecordClickHouseRowsInserted(static_cast<uint64_t>(rows.size()));
    std::clog << "inserted metrics rows=" << rows.size() << '\n';
  } catch (const std::exception& e) {
    impl_->ResetClient();
    telemetry::RecordClickHouseInsertError();
    std::clog << "ClickHouse InsertMetrics error: " << e.what() << '\n';
  }
}

void ClickHouseWriter::InsertLogs(const std::vector<otlp_decoder::LogRow>& rows) {
  if (rows.empty()) return;
  auto span = telemetry::StartSpan("clickhouse_insert");
  try {
    auto& client = impl_->GetClient();

    auto ts_col = std::make_shared<clickhouse::ColumnDateTime64>(9);
    auto trace_id_col = std::make_shared<clickhouse::ColumnString>();
    auto span_id_col = std::make_shared<clickhouse::ColumnString>();
    auto trace_flags_col = std::make_shared<clickhouse::ColumnUInt8>();
    auto severity_text_col =
        std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>();
    auto severity_number_col = std::make_shared<clickhouse::ColumnUInt8>();
    auto svc_col = std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>();
    auto body_col = std::make_shared<clickhouse::ColumnString>();
    auto resource_schema_url_col =
        std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>();
    auto resource_attributes_col =
        std::make_shared<clickhouse::ColumnMapT<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>,
                                                 clickhouse::ColumnString>>(
            std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>(),
            std::make_shared<clickhouse::ColumnString>());
    auto scope_schema_url_col =
        std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>();
    auto scope_name_col = std::make_shared<clickhouse::ColumnString>();
    auto scope_version_col =
        std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>();
    auto scope_attributes_col =
        std::make_shared<clickhouse::ColumnMapT<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>,
                                                 clickhouse::ColumnString>>(
            std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>(),
            std::make_shared<clickhouse::ColumnString>());
    auto log_attributes_col =
        std::make_shared<clickhouse::ColumnMapT<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>,
                                                 clickhouse::ColumnString>>(
            std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>(),
            std::make_shared<clickhouse::ColumnString>());

    for (const auto& row : rows) {
      ts_col->Append(static_cast<int64_t>(row.timestamp_ns));
      trace_id_col->Append(row.trace_id);
      span_id_col->Append(row.span_id);
      trace_flags_col->Append(row.trace_flags);
      severity_text_col->Append(std::string_view(row.severity_text));
      severity_number_col->Append(row.severity_number);
      svc_col->Append(std::string_view(row.service_name));
      body_col->Append(row.body);
      resource_schema_url_col->Append(std::string_view(row.resource_schema_url));
      resource_attributes_col->Append(row.resource_attributes);
      scope_schema_url_col->Append(std::string_view(row.scope_schema_url));
      scope_name_col->Append(row.scope_name);
      scope_version_col->Append(std::string_view(row.scope_version));
      scope_attributes_col->Append(row.scope_attributes);
      log_attributes_col->Append(row.log_attributes);
    }

    clickhouse::Block block;
    block.AppendColumn("Timestamp", ts_col);
    block.AppendColumn("TraceId", trace_id_col);
    block.AppendColumn("SpanId", span_id_col);
    block.AppendColumn("TraceFlags", trace_flags_col);
    block.AppendColumn("SeverityText", severity_text_col);
    block.AppendColumn("SeverityNumber", severity_number_col);
    block.AppendColumn("ServiceName", svc_col);
    block.AppendColumn("Body", body_col);
    block.AppendColumn("ResourceSchemaUrl", resource_schema_url_col);
    block.AppendColumn("ResourceAttributes", resource_attributes_col);
    block.AppendColumn("ScopeSchemaUrl", scope_schema_url_col);
    block.AppendColumn("ScopeName", scope_name_col);
    block.AppendColumn("ScopeVersion", scope_version_col);
    block.AppendColumn("ScopeAttributes", scope_attributes_col);
    block.AppendColumn("LogAttributes", log_attributes_col);

    client.Insert("otel_logs", block);
    telemetry::RecordClickHouseRowsInserted(static_cast<uint64_t>(rows.size()));
    std::clog << "inserted logs rows=" << rows.size() << '\n';
  } catch (const std::exception& e) {
    impl_->ResetClient();
    telemetry::RecordClickHouseInsertError();
    std::clog << "ClickHouse InsertLogs error: " << e.what() << '\n';
  }
}

}  // namespace clickhouse_writer
