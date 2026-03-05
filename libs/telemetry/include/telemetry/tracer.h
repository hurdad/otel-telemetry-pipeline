#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

namespace telemetry {

class ScopedSpan {
 public:
  explicit ScopedSpan(std::string name);
  ~ScopedSpan();

 private:
  std::string name_;
  std::chrono::steady_clock::time_point started_at_;
  class Impl;
  std::unique_ptr<Impl> impl_;
};

void InitTelemetry();
std::unique_ptr<ScopedSpan> StartSpan(const std::string& name);
void RecordClickHouseRowsInserted(uint64_t rows);
void RecordClickHouseInsertError();

}  // namespace telemetry
