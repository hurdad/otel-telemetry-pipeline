#include "jetstream_client/jetstream_client.h"

#include <gtest/gtest.h>

#include <stdexcept>

namespace {

TEST(JetStreamPublisherInitTest, InitializesWhenForcedSuccess) {
  jetstream_client::testing::ForceInitializationSuccessForTests();
  EXPECT_NO_THROW({
    jetstream_client::JetStreamPublisher publisher("nats://invalid:4222",
                                                   "otel", {"otel.traces"});
  });
  jetstream_client::testing::ResetInitializationBehaviorForTests();
}

TEST(JetStreamPublisherInitTest, ThrowsWhenInitialConnectFails) {
  jetstream_client::testing::ForceInitializationFailureForTests();
  EXPECT_THROW(
      {
        jetstream_client::JetStreamPublisher publisher("nats://invalid:4222",
                                                       "otel", {"otel.traces"});
      },
      std::runtime_error);
  jetstream_client::testing::ResetInitializationBehaviorForTests();
}

TEST(JetStreamConsumerInitTest, InitializesWhenForcedSuccess) {
  jetstream_client::testing::ForceInitializationSuccessForTests();
  EXPECT_NO_THROW({
    jetstream_client::JetStreamConsumer consumer(
        "nats://invalid:4222", "otel",
        {"otel.traces", "otel.metrics", "otel.logs"});
  });
  jetstream_client::testing::ResetInitializationBehaviorForTests();
}

TEST(JetStreamConsumerInitTest, ThrowsWhenInitialConnectFails) {
  jetstream_client::testing::ForceInitializationFailureForTests();
  EXPECT_THROW(
      {
        jetstream_client::JetStreamConsumer consumer(
            "nats://invalid:4222", "otel",
            {"otel.traces", "otel.metrics", "otel.logs"});
      },
      std::runtime_error);
  jetstream_client::testing::ResetInitializationBehaviorForTests();
}

} // namespace
