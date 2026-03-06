#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "otlp_decoder/decoder.h"

namespace clickhouse_writer {


std::vector<std::string> RequiredMetricColumnsForTable(const std::string& table_name);

class ClickHouseWriter {
 public:
  ClickHouseWriter(std::string host, uint16_t port, std::string database,
                   std::string user = "default", std::string password = "");
  ~ClickHouseWriter();

  void InsertTraces(const std::vector<otlp_decoder::TraceRow>& rows);
  void InsertMetrics(const std::vector<otlp_decoder::MetricRow>& rows);
  void InsertLogs(const std::vector<otlp_decoder::LogRow>& rows);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

template <typename T>
class BatchInsert {
 public:
  BatchInsert(size_t max_rows, std::chrono::milliseconds flush_interval)
      : max_rows_(max_rows), flush_interval_(flush_interval) {}

  template <typename Inserter>
  void Add(T row, const Inserter& inserter) {
    rows_.emplace_back(std::move(row));
    const auto now = std::chrono::steady_clock::now();
    if (rows_.size() >= max_rows_ || now - last_flush_ >= flush_interval_) {
      inserter(rows_);
      rows_.clear();
      last_flush_ = now;
    }
  }

  template <typename Inserter>
  void Flush(const Inserter& inserter) {
    if (!rows_.empty()) {
      inserter(rows_);
      rows_.clear();
      last_flush_ = std::chrono::steady_clock::now();
    }
  }

 private:
  size_t max_rows_;
  std::chrono::milliseconds flush_interval_;
  std::chrono::steady_clock::time_point last_flush_ = std::chrono::steady_clock::now();
  std::vector<T> rows_;
};

}  // namespace clickhouse_writer
