#include <gtest/gtest.h>

#include <chrono>
#include <vector>

#include "clickhouse_writer/clickhouse_writer.h"

namespace {

using clickhouse_writer::BatchInsert;
using otlp_decoder::TraceRow;

TEST(BatchInsertTest, FlushesWhenMaxRowsReached) {
  BatchInsert<TraceRow> batcher(2, std::chrono::seconds(10));
  int flush_count = 0;
  size_t flushed_rows = 0;

  auto inserter = [&](const std::vector<TraceRow>& rows) {
    ++flush_count;
    flushed_rows = rows.size();
  };

  batcher.Add(TraceRow{1, "t1", "s1"}, inserter);
  EXPECT_EQ(flush_count, 0);

  batcher.Add(TraceRow{2, "t2", "s2"}, inserter);
  EXPECT_EQ(flush_count, 1);
  EXPECT_EQ(flushed_rows, 2U);
}

TEST(BatchInsertTest, ExplicitFlushWritesPendingRows) {
  BatchInsert<TraceRow> batcher(10, std::chrono::seconds(10));
  int flush_count = 0;
  size_t flushed_rows = 0;

  auto inserter = [&](const std::vector<TraceRow>& rows) {
    ++flush_count;
    flushed_rows = rows.size();
  };

  batcher.Add(TraceRow{1, "t1", "s1"}, inserter);
  batcher.Flush(inserter);

  EXPECT_EQ(flush_count, 1);
  EXPECT_EQ(flushed_rows, 1U);
}


TEST(ClickHouseWriterTest, MetricInsertBlocksContainAllSchemaColumns) {
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
