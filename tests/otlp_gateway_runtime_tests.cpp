#include "services/otlp-gateway/runtime.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <exception>
#include <pthread.h>
#include <stdexcept>
#include <thread>

namespace {

class SignalMaskGuard {
public:
  explicit SignalMaskGuard(const sigset_t &set) {
    if (pthread_sigmask(SIG_BLOCK, &set, &old_set_) != 0) {
      throw std::runtime_error("failed to block signals");
    }
  }

  ~SignalMaskGuard() { pthread_sigmask(SIG_SETMASK, &old_set_, nullptr); }

private:
  sigset_t old_set_{};
};

} // namespace

TEST(OtlpGatewayRuntimeTest, RethrowsServerRunFailureAfterShutdown) {
  sigset_t signal_set;
  sigemptyset(&signal_set);
  sigaddset(&signal_set, SIGTERM);
  SignalMaskGuard guard(signal_set);

  std::atomic<bool> shutdown_called = false;

  EXPECT_THROW(
      RunServerUntilSignalOrFailure(
          signal_set, []() { throw std::runtime_error("run failure"); },
          [&shutdown_called]() { shutdown_called.store(true); },
          std::chrono::milliseconds(10)),
      std::runtime_error);

  EXPECT_TRUE(shutdown_called.load());
}

TEST(OtlpGatewayRuntimeTest, ReturnsSignalOnTermination) {
  sigset_t signal_set;
  sigemptyset(&signal_set);
  sigaddset(&signal_set, SIGTERM);
  SignalMaskGuard guard(signal_set);

  std::atomic<bool> stop_requested = false;
  std::atomic<bool> shutdown_called = false;

  ASSERT_EQ(raise(SIGTERM), 0);

  const int received_signal = RunServerUntilSignalOrFailure(
      signal_set,
      [&stop_requested]() {
        while (!stop_requested.load()) {
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
      },
      [&stop_requested, &shutdown_called]() {
        shutdown_called.store(true);
        stop_requested.store(true);
      },
      std::chrono::milliseconds(10));

  EXPECT_EQ(received_signal, SIGTERM);
  EXPECT_TRUE(shutdown_called.load());
}
