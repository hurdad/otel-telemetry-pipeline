#include "runtime.h"

#include <atomic>
#include <cerrno>
#include <cstring>
#include <exception>
#include <mutex>
#include <stdexcept>
#include <thread>

int RunServerUntilSignalOrFailure(
    const sigset_t &signal_set, const std::function<void()> &run_server,
    const std::function<void()> &shutdown_server,
    std::chrono::milliseconds poll_interval) {
  std::atomic<bool> server_failed = false;
  std::mutex exception_mutex;
  std::exception_ptr server_exception;

  std::thread server_thread([&]() {
    try {
      run_server();
    } catch (...) {
      {
        std::lock_guard<std::mutex> lock(exception_mutex);
        server_exception = std::current_exception();
      }
      server_failed.store(true, std::memory_order_release);
    }
  });

  int received_signal = 0;
  while (!server_failed.load(std::memory_order_acquire)) {
    timespec timeout{};
    timeout.tv_sec = std::chrono::duration_cast<std::chrono::seconds>(poll_interval).count();
    timeout.tv_nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(
                          poll_interval % std::chrono::seconds(1))
                          .count();

    received_signal = sigtimedwait(&signal_set, nullptr, &timeout);
    if (received_signal == SIGINT || received_signal == SIGTERM) {
      break;
    }
    if (received_signal == -1 && (errno == EAGAIN || errno == EINTR)) {
      continue;
    }
    if (received_signal == -1) {
      throw std::runtime_error(std::string("sigtimedwait failed: ") +
                               std::strerror(errno));
    }
  }

  shutdown_server();

  if (server_thread.joinable()) {
    server_thread.join();
  }

  if (server_failed.load(std::memory_order_acquire)) {
    std::exception_ptr exception_copy;
    {
      std::lock_guard<std::mutex> lock(exception_mutex);
      exception_copy = server_exception;
    }
    if (exception_copy) {
      std::rethrow_exception(exception_copy);
    }
    throw std::runtime_error("server failed without an exception");
  }

  return received_signal;
}
