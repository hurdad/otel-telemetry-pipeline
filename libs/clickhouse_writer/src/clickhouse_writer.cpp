#include "clickhouse_writer/clickhouse_writer.h"

#include <iostream>

#include "telemetry/tracer.h"

namespace clickhouse_writer {

ClickHouseWriter::ClickHouseWriter(std::string host, uint16_t port, std::string database)
    : host_(std::move(host)), port_(port), database_(std::move(database)) {}

void ClickHouseWriter::InsertTraces(const std::vector<otlp_decoder::TraceRow>& rows) {
  auto span = telemetry::StartSpan("clickhouse_insert");
  std::clog << "insert traces rows=" << rows.size() << " into " << host_ << ':' << port_ << '/'
            << database_ << '\n';
}

void ClickHouseWriter::InsertMetrics(const std::vector<otlp_decoder::MetricRow>& rows) {
  auto span = telemetry::StartSpan("clickhouse_insert");
  std::clog << "insert metrics rows=" << rows.size() << '\n';
}

void ClickHouseWriter::InsertLogs(const std::vector<otlp_decoder::LogRow>& rows) {
  auto span = telemetry::StartSpan("clickhouse_insert");
  std::clog << "insert logs rows=" << rows.size() << '\n';
}

}  // namespace clickhouse_writer
