#include <gtest/gtest.h>

#include <chrono>
#include <thread>
#include <vector>

#include "clickhouse_writer/clickhouse_writer.h"

namespace {

using clickhouse_writer::BatchInsert;
using otlp_decoder::TraceRow;

TEST(BatchInsertFocusedTest, CallsInserterWhenMaxRowsReached) {
  BatchInsert<TraceRow> batcher(2, std::chrono::seconds(1));
  int inserter_calls = 0;
  std::vector<std::vector<TraceRow>> snapshots;

  auto inserter = [&](const std::vector<TraceRow>& rows) {
    ++inserter_calls;
    snapshots.push_back(rows);
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

TEST(BatchInsertFocusedTest, FlushCallsInserterOnceAndClearsBuffer) {
  BatchInsert<TraceRow> batcher(10, std::chrono::seconds(1));
  int inserter_calls = 0;
  std::vector<std::vector<TraceRow>> snapshots;

  auto inserter = [&](const std::vector<TraceRow>& rows) {
    ++inserter_calls;
    snapshots.push_back(rows);
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

TEST(BatchInsertFocusedTest, FlushNoOpWhenBufferEmpty) {
  BatchInsert<TraceRow> batcher(10, std::chrono::seconds(1));
  int inserter_calls = 0;
  std::vector<std::vector<TraceRow>> snapshots;

  auto inserter = [&](const std::vector<TraceRow>& rows) {
    ++inserter_calls;
    snapshots.push_back(rows);
  };

  batcher.Flush(inserter);

  EXPECT_EQ(inserter_calls, 0);
  EXPECT_TRUE(snapshots.empty());
}

TEST(BatchInsertFocusedTest, AddTriggersFlushWhenIntervalElapsed) {
  BatchInsert<TraceRow> batcher(100, std::chrono::milliseconds(1));
  int inserter_calls = 0;
  std::vector<std::vector<TraceRow>> snapshots;

  auto inserter = [&](const std::vector<TraceRow>& rows) {
    ++inserter_calls;
    snapshots.push_back(rows);
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

}  // namespace
