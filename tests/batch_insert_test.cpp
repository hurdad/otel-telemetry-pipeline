#include <gtest/gtest.h>

#include <chrono>
#include <thread>
#include <vector>

#include "clickhouse_writer/clickhouse_writer.h"

namespace {

using clickhouse_writer::BatchInsert;
using otlp_decoder::TraceRow;

TEST(BatchInsertTest, CallsInserterWhenMaxRowsReached) {
  BatchInsert<TraceRow> batcher(2, std::chrono::seconds(1));
  int inserter_calls = 0;
  std::vector<std::vector<TraceRow>> snapshots;

  auto inserter = [&](const std::vector<TraceRow>& rows) {
    ++inserter_calls;
    snapshots.push_back(rows);
    return true;
  };

  const TraceRow first{1, "trace-1", "span-1"};
  const TraceRow second{2, "trace-2", "span-2"};

  batcher.Add(first, inserter);
  EXPECT_EQ(inserter_calls, 0);

  batcher.Add(second, inserter);

  ASSERT_EQ(inserter_calls, 1);
  ASSERT_EQ(snapshots.size(), 1U);
  ASSERT_EQ(snapshots[0].size(), 2U);
  EXPECT_EQ(snapshots[0][0].trace_id, first.trace_id);
  EXPECT_EQ(snapshots[0][1].trace_id, second.trace_id);
}

TEST(BatchInsertTest, FlushCallsInserterOnceAndClearsBuffer) {
  BatchInsert<TraceRow> batcher(10, std::chrono::seconds(1));
  int inserter_calls = 0;
  std::vector<std::vector<TraceRow>> snapshots;

  auto inserter = [&](const std::vector<TraceRow>& rows) {
    ++inserter_calls;
    snapshots.push_back(rows);
    return true;
  };

  const TraceRow pending{3, "trace-3", "span-3"};
  batcher.Add(pending, inserter);

  batcher.Flush(inserter);

  ASSERT_EQ(inserter_calls, 1);
  ASSERT_EQ(snapshots.size(), 1U);
  ASSERT_EQ(snapshots[0].size(), 1U);
  EXPECT_EQ(snapshots[0][0].trace_id, pending.trace_id);

  batcher.Flush(inserter);
  EXPECT_EQ(inserter_calls, 1);
}

TEST(BatchInsertTest, FlushNoOpWhenBufferEmpty) {
  BatchInsert<TraceRow> batcher(10, std::chrono::seconds(1));
  int inserter_calls = 0;
  std::vector<std::vector<TraceRow>> snapshots;

  auto inserter = [&](const std::vector<TraceRow>& rows) {
    ++inserter_calls;
    snapshots.push_back(rows);
    return true;
  };

  batcher.Flush(inserter);

  EXPECT_EQ(inserter_calls, 0);
  EXPECT_TRUE(snapshots.empty());
}

TEST(BatchInsertTest, AddTriggersFlushWhenIntervalElapsed) {
  BatchInsert<TraceRow> batcher(100, std::chrono::milliseconds(1));
  int inserter_calls = 0;
  std::vector<std::vector<TraceRow>> snapshots;

  auto inserter = [&](const std::vector<TraceRow>& rows) {
    ++inserter_calls;
    snapshots.push_back(rows);
    return true;
  };

  const TraceRow first{4, "trace-4", "span-4"};
  const TraceRow second{5, "trace-5", "span-5"};

  batcher.Add(first, inserter);
  EXPECT_EQ(inserter_calls, 0);

  std::this_thread::sleep_for(std::chrono::milliseconds(3));
  batcher.Add(second, inserter);

  ASSERT_EQ(inserter_calls, 1);
  ASSERT_EQ(snapshots.size(), 1U);
  ASSERT_EQ(snapshots[0].size(), 2U);
  EXPECT_EQ(snapshots[0][0].trace_id, first.trace_id);
  EXPECT_EQ(snapshots[0][1].trace_id, second.trace_id);
}

TEST(BatchInsertTest, FailedFlushRetainsRowsUntilSuccessfulRetry) {
  BatchInsert<TraceRow> batcher(2, std::chrono::seconds(1));
  int inserter_calls = 0;
  int failures_remaining = 1;
  std::vector<std::vector<TraceRow>> snapshots;

  auto inserter = [&](const std::vector<TraceRow>& rows) {
    ++inserter_calls;
    snapshots.push_back(rows);
    if (failures_remaining > 0) {
      --failures_remaining;
      return false;
    }
    return true;
  };

  const TraceRow first{6, "trace-6", "span-6"};
  const TraceRow second{7, "trace-7", "span-7"};

  batcher.Add(first, inserter);
  batcher.Add(second, inserter);

  ASSERT_EQ(inserter_calls, 1);
  ASSERT_EQ(snapshots.size(), 1U);
  ASSERT_EQ(snapshots[0].size(), 2U);

  // Initial flush failed, so rows should still be buffered and retried on explicit Flush.
  batcher.Flush(inserter);

  ASSERT_EQ(inserter_calls, 2);
  ASSERT_EQ(snapshots.size(), 2U);
  ASSERT_EQ(snapshots[1].size(), 2U);
  EXPECT_EQ(snapshots[1][0].trace_id, first.trace_id);
  EXPECT_EQ(snapshots[1][1].trace_id, second.trace_id);

  // Successful retry clears buffer.
  batcher.Flush(inserter);
  EXPECT_EQ(inserter_calls, 2);
}

TEST(ClickHouseWriterSchemaTest, MetricInsertBlocksContainAllSchemaColumns) {
  EXPECT_EQ(clickhouse_writer::RequiredMetricColumnsForTable("otel_metrics_gauge"),
            (std::vector<std::string>{"ResourceAttributes", "ResourceSchemaUrl", "ScopeName",
                                      "ScopeVersion", "ScopeAttributes", "ScopeDroppedAttrCount",
                                      "ScopeSchemaUrl", "ServiceName", "MetricName",
                                      "MetricDescription", "MetricUnit", "Attributes",
                                      "StartTimeUnix", "TimeUnix", "Value", "Flags",
                                      "Exemplars.FilteredAttributes", "Exemplars.TimeUnix",
                                      "Exemplars.Value", "Exemplars.SpanId", "Exemplars.TraceId"}));

  EXPECT_EQ(clickhouse_writer::RequiredMetricColumnsForTable("otel_metrics_sum"),
            (std::vector<std::string>{"ResourceAttributes", "ResourceSchemaUrl", "ScopeName",
                                      "ScopeVersion", "ScopeAttributes", "ScopeDroppedAttrCount",
                                      "ScopeSchemaUrl", "ServiceName", "MetricName",
                                      "MetricDescription", "MetricUnit", "Attributes",
                                      "StartTimeUnix", "TimeUnix", "Value", "Flags",
                                      "Exemplars.FilteredAttributes", "Exemplars.TimeUnix",
                                      "Exemplars.Value", "Exemplars.SpanId", "Exemplars.TraceId",
                                      "AggregationTemporality", "IsMonotonic"}));

  EXPECT_EQ(clickhouse_writer::RequiredMetricColumnsForTable("otel_metrics_histogram"),
            (std::vector<std::string>{"ResourceAttributes", "ResourceSchemaUrl", "ScopeName",
                                      "ScopeVersion", "ScopeAttributes", "ScopeDroppedAttrCount",
                                      "ScopeSchemaUrl", "ServiceName", "MetricName",
                                      "MetricDescription", "MetricUnit", "Attributes",
                                      "StartTimeUnix", "TimeUnix", "Count", "Sum",
                                      "BucketCounts", "ExplicitBounds",
                                      "Exemplars.FilteredAttributes", "Exemplars.TimeUnix",
                                      "Exemplars.Value", "Exemplars.SpanId", "Exemplars.TraceId",
                                      "Flags", "Min", "Max", "AggregationTemporality"}));

  EXPECT_EQ(clickhouse_writer::RequiredMetricColumnsForTable("otel_metrics_exponentialhistogram"),
            (std::vector<std::string>{"ResourceAttributes", "ResourceSchemaUrl", "ScopeName",
                                      "ScopeVersion", "ScopeAttributes", "ScopeDroppedAttrCount",
                                      "ScopeSchemaUrl", "ServiceName", "MetricName",
                                      "MetricDescription", "MetricUnit", "Attributes",
                                      "StartTimeUnix", "TimeUnix", "Count", "Sum", "Scale",
                                      "ZeroCount", "PositiveOffset", "PositiveBucketCounts",
                                      "NegativeOffset", "NegativeBucketCounts",
                                      "Exemplars.FilteredAttributes", "Exemplars.TimeUnix",
                                      "Exemplars.Value", "Exemplars.SpanId", "Exemplars.TraceId",
                                      "Flags", "Min", "Max", "AggregationTemporality"}));

  EXPECT_EQ(clickhouse_writer::RequiredMetricColumnsForTable("otel_metrics_summary"),
            (std::vector<std::string>{"ResourceAttributes", "ResourceSchemaUrl", "ScopeName",
                                      "ScopeVersion", "ScopeAttributes", "ScopeDroppedAttrCount",
                                      "ScopeSchemaUrl", "ServiceName", "MetricName",
                                      "MetricDescription", "MetricUnit", "Attributes",
                                      "StartTimeUnix", "TimeUnix", "Count", "Sum",
                                      "ValueAtQuantiles.Quantile", "ValueAtQuantiles.Value",
                                      "Flags"}));
}

}  // namespace
