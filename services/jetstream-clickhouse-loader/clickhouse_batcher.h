#pragma once

#include "clickhouse_writer/clickhouse_writer.h"
#include "otlp_decoder/decoder.h"
#include "telemetry/tracer.h"

#include <chrono>

class ClickHouseBatcher {
 public:
  ClickHouseBatcher(const std::string& host = "localhost", uint16_t port = 9000,
                    const std::string& database = "default")
      : writer_(host, port, database),
        trace_batch_(50000, std::chrono::seconds(2)),
        metric_batch_(50000, std::chrono::seconds(2)),
        log_batch_(50000, std::chrono::seconds(2)) {}

  void ProcessTraces(const std::string& payload) {
    for (auto& row : otlp_decoder::DecodeTraces(payload)) {
      trace_batch_.Add(std::move(row), [this](const auto& rows) { writer_.InsertTraces(rows); });
    }
  }

  void ProcessMetrics(const std::string& payload) {
    for (auto& row : otlp_decoder::DecodeMetrics(payload)) {
      metric_batch_.Add(std::move(row), [this](const auto& rows) { writer_.InsertMetrics(rows); });
    }
  }

  void ProcessLogs(const std::string& payload) {
    for (auto& row : otlp_decoder::DecodeLogs(payload)) {
      log_batch_.Add(std::move(row), [this](const auto& rows) { writer_.InsertLogs(rows); });
    }
  }

  void FlushAll() {
    auto span = telemetry::StartSpan("batch_flush");
    trace_batch_.Flush([this](const auto& rows) { writer_.InsertTraces(rows); });
    metric_batch_.Flush([this](const auto& rows) { writer_.InsertMetrics(rows); });
    log_batch_.Flush([this](const auto& rows) { writer_.InsertLogs(rows); });
  }

 private:
  clickhouse_writer::ClickHouseWriter writer_;
  clickhouse_writer::BatchInsert<otlp_decoder::TraceRow> trace_batch_;
  clickhouse_writer::BatchInsert<otlp_decoder::MetricRow> metric_batch_;
  clickhouse_writer::BatchInsert<otlp_decoder::LogRow> log_batch_;
};
