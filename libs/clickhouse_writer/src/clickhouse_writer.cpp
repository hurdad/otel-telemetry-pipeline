#include "clickhouse_writer/clickhouse_writer.h"

#include <iostream>

#include "clickhouse/client.h"
#include "clickhouse/columns/date.h"
#include "clickhouse/columns/lowcardinality.h"
#include "clickhouse/columns/numeric.h"
#include "clickhouse/columns/string.h"
#include "telemetry/tracer.h"

namespace clickhouse_writer {

struct ClickHouseWriter::Impl {
  clickhouse::ClientOptions opts;

  explicit Impl(const std::string& host, uint16_t port, const std::string& database) {
    opts.SetHost(host).SetPort(port).SetDefaultDatabase(database);
  }

  clickhouse::Client connect() { return clickhouse::Client(opts); }
};

ClickHouseWriter::ClickHouseWriter(std::string host, uint16_t port, std::string database)
    : host_(std::move(host)), port_(port), database_(std::move(database)) {
  try {
    impl_ = std::make_unique<Impl>(host_, port_, database_);
  } catch (const std::exception& e) {
    std::clog << "ClickHouse writer init failed host=" << host_ << " port=" << port_
              << " database=" << database_ << " error=" << e.what() << '\n';
  }
}

ClickHouseWriter::~ClickHouseWriter() = default;

void ClickHouseWriter::InsertTraces(const std::vector<otlp_decoder::TraceRow>& rows) {
  if (!impl_) {
    std::clog << "ClickHouse InsertTraces skipped: writer not initialized\n";
    return;
  }
  auto span = telemetry::StartSpan("clickhouse_insert");
  try {
    auto client = impl_->connect();

    auto ts_col        = std::make_shared<clickhouse::ColumnDateTime64>(9);
    auto trace_id_col  = std::make_shared<clickhouse::ColumnString>();
    auto span_id_col   = std::make_shared<clickhouse::ColumnString>();
    auto parent_id_col = std::make_shared<clickhouse::ColumnString>();
    auto svc_col       = std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>();
    auto op_col        = std::make_shared<clickhouse::ColumnString>();
    auto dur_col       = std::make_shared<clickhouse::ColumnUInt64>();

    for (const auto& row : rows) {
      ts_col->Append(static_cast<int64_t>(row.timestamp_ns));
      trace_id_col->Append(row.trace_id);
      span_id_col->Append(row.span_id);
      parent_id_col->Append(row.parent_span_id);
      svc_col->Append(std::string_view(row.service_name));
      op_col->Append(row.operation_name);
      dur_col->Append(row.duration_ns);
    }

    clickhouse::Block block;
    block.AppendColumn("timestamp",      ts_col);
    block.AppendColumn("trace_id",       trace_id_col);
    block.AppendColumn("span_id",        span_id_col);
    block.AppendColumn("parent_span_id", parent_id_col);
    block.AppendColumn("service_name",   svc_col);
    block.AppendColumn("operation_name", op_col);
    block.AppendColumn("duration",       dur_col);

    client.Insert("otel_traces", block);
    std::clog << "inserted traces rows=" << rows.size() << '\n';
  } catch (const std::exception& e) {
    std::clog << "ClickHouse InsertTraces error: " << e.what() << '\n';
  }
}

void ClickHouseWriter::InsertMetrics(const std::vector<otlp_decoder::MetricRow>& rows) {
  if (!impl_) {
    std::clog << "ClickHouse InsertMetrics skipped: writer not initialized\n";
    return;
  }
  auto span = telemetry::StartSpan("clickhouse_insert");
  try {
    auto client = impl_->connect();

    auto ts_col     = std::make_shared<clickhouse::ColumnDateTime64>(9);
    auto svc_col    = std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>();
    auto metric_col = std::make_shared<clickhouse::ColumnString>();
    auto value_col  = std::make_shared<clickhouse::ColumnFloat64>();

    for (const auto& row : rows) {
      ts_col->Append(static_cast<int64_t>(row.timestamp_ns));
      svc_col->Append(std::string_view(row.service_name));
      metric_col->Append(row.metric_name);
      value_col->Append(row.value);
    }

    clickhouse::Block block;
    block.AppendColumn("timestamp",   ts_col);
    block.AppendColumn("service_name", svc_col);
    block.AppendColumn("metric_name", metric_col);
    block.AppendColumn("value",       value_col);

    client.Insert("otel_metrics", block);
    std::clog << "inserted metrics rows=" << rows.size() << '\n';
  } catch (const std::exception& e) {
    std::clog << "ClickHouse InsertMetrics error: " << e.what() << '\n';
  }
}

void ClickHouseWriter::InsertLogs(const std::vector<otlp_decoder::LogRow>& rows) {
  if (!impl_) {
    std::clog << "ClickHouse InsertLogs skipped: writer not initialized\n";
    return;
  }
  auto span = telemetry::StartSpan("clickhouse_insert");
  try {
    auto client = impl_->connect();

    auto ts_col       = std::make_shared<clickhouse::ColumnDateTime64>(9);
    auto svc_col      = std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>();
    auto severity_col = std::make_shared<clickhouse::ColumnLowCardinalityT<clickhouse::ColumnString>>();
    auto body_col     = std::make_shared<clickhouse::ColumnString>();

    for (const auto& row : rows) {
      ts_col->Append(static_cast<int64_t>(row.timestamp_ns));
      svc_col->Append(std::string_view(row.service_name));
      severity_col->Append(std::string_view(row.severity_text));
      body_col->Append(row.body);
    }

    clickhouse::Block block;
    block.AppendColumn("timestamp",     ts_col);
    block.AppendColumn("service_name",  svc_col);
    block.AppendColumn("severity_text", severity_col);
    block.AppendColumn("body",          body_col);

    client.Insert("otel_logs", block);
    std::clog << "inserted logs rows=" << rows.size() << '\n';
  } catch (const std::exception& e) {
    std::clog << "ClickHouse InsertLogs error: " << e.what() << '\n';
  }
}

}  // namespace clickhouse_writer
