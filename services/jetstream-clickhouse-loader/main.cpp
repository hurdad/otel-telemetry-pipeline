#include "clickhouse_batcher.h"
#include "config.h"
#include "jetstream_client/jetstream_client.h"
#include "telemetry/tracer.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <limits>
#include <mutex>
#include <pthread.h>
#include <string>
#include <thread>

#include "spdlog/spdlog.h"

namespace {

std::string GetEnvOrDefault(const char *key, const char *fallback) {
  const char *value = std::getenv(key);
  if (value == nullptr || value[0] == '\0') {
    return fallback;
  }
  return value;
}

uint16_t ParsePortOrDefault(const char *key, uint16_t fallback) {
  const std::string raw = GetEnvOrDefault(key, "");
  if (raw.empty()) {
    return fallback;
  }

  try {
    const unsigned long value = std::stoul(raw);
    if (value > std::numeric_limits<uint16_t>::max()) {
      throw std::out_of_range("port exceeds uint16_t");
    }
    return static_cast<uint16_t>(value);
  } catch (const std::exception &e) {
    spdlog::warn("Invalid {}='{}', using default {} error={}", key, raw,
                 fallback, e.what());
    return fallback;
  }
}

} // namespace

int main() {
  try {
    telemetry::InitTelemetry();

    // Load config file (default path can be overridden via LOADER_CONFIG env
    // var).
    const std::string config_path =
        GetEnvOrDefault("LOADER_CONFIG", "/etc/otel-pipeline/loader.yaml");
    LoaderConfig cfg = LoadConfig(config_path);

    // Environment variables override config file values when explicitly set.
    if (const char *v = std::getenv("NATS_URL"); v && v[0])
      cfg.nats_url = v;
    if (const char *v = std::getenv("NATS_STREAM"); v && v[0])
      cfg.nats_stream = v;
    if (const char *v = std::getenv("CLICKHOUSE_HOST"); v && v[0])
      cfg.clickhouse_host = v;
    if (const char *v = std::getenv("CLICKHOUSE_PORT"); v && v[0])
      cfg.clickhouse_port = static_cast<uint16_t>(
          ParsePortOrDefault("CLICKHOUSE_PORT", cfg.clickhouse_port));
    if (const char *v = std::getenv("CLICKHOUSE_DATABASE"); v && v[0])
      cfg.clickhouse_database = v;
    if (const char *v = std::getenv("CLICKHOUSE_USER"); v && v[0])
      cfg.clickhouse_user = v;
    if (const char *v = std::getenv("CLICKHOUSE_PASSWORD"); v && v[0])
      cfg.clickhouse_password = v;
    if (const char *v = std::getenv("TRACE_SUBJECT"); v && v[0])
      cfg.trace_subject = v;
    if (const char *v = std::getenv("METRIC_SUBJECT"); v && v[0])
      cfg.metric_subject = v;
    if (const char *v = std::getenv("LOG_SUBJECT"); v && v[0])
      cfg.log_subject = v;

    spdlog::info("Starting jetstream-clickhouse-loader (config={})",
                 config_path);
    spdlog::info("  NATS: {} stream={}", cfg.nats_url, cfg.nats_stream);
    spdlog::info("  ClickHouse: {}:{}/{} user={}", cfg.clickhouse_host,
                 cfg.clickhouse_port, cfg.clickhouse_database,
                 cfg.clickhouse_user);
    spdlog::info("  Batch: max_rows={} flush_interval={}s", cfg.batch_max_rows,
                 cfg.flush_interval.count());
    spdlog::info("  Subjects: traces={} metrics={} logs={}", cfg.trace_subject,
                 cfg.metric_subject, cfg.log_subject);

    // Block signals before spawning the consumer thread so they are delivered
    // only to the main thread's sigwait call.
    sigset_t signal_set;
    sigemptyset(&signal_set);
    sigaddset(&signal_set, SIGINT);
    sigaddset(&signal_set, SIGTERM);
    if (pthread_sigmask(SIG_BLOCK, &signal_set, nullptr) != 0) {
      spdlog::error("Failed to block termination signals, exiting without "
                    "graceful shutdown");
      return 1;
    }

    ClickHouseBatcher batcher(cfg.clickhouse_host, cfg.clickhouse_port,
                              cfg.clickhouse_database, cfg.clickhouse_user,
                              cfg.clickhouse_password, cfg.batch_max_rows,
                              cfg.flush_interval);
    jetstream_client::JetStreamConsumer consumer(
        cfg.nats_url, cfg.nats_stream,
        {cfg.trace_subject, cfg.metric_subject, cfg.log_subject});

    std::atomic<bool> running{true};
    std::atomic<bool> consumer_failed{false};
    std::mutex consumer_exception_mutex;
    std::exception_ptr consumer_exception;

    std::thread consumer_thread([&]() {
      try {
        while (running.load(std::memory_order_relaxed)) {
          consumer.Poll([&batcher, &cfg](const jetstream_client::Message &msg) {
            if (msg.subject == cfg.trace_subject) {
              batcher.ProcessTraces(msg.payload);
            } else if (msg.subject == cfg.metric_subject) {
              batcher.ProcessMetrics(msg.payload);
            } else if (msg.subject == cfg.log_subject) {
              batcher.ProcessLogs(msg.payload);
            }
          });
          batcher.FlushAll();
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        batcher.FlushAll();
      } catch (...) {
        {
          std::lock_guard<std::mutex> lock(consumer_exception_mutex);
          consumer_exception = std::current_exception();
        }
        consumer_failed.store(true, std::memory_order_release);
      }
    });

    // Poll for signals with a short timeout so we also detect consumer failures.
    int received_signal = 0;
    while (!consumer_failed.load(std::memory_order_acquire)) {
      timespec timeout{};
      timeout.tv_nsec = 100'000'000;  // 100 ms
      const int sig = sigtimedwait(&signal_set, nullptr, &timeout);
      if (sig == SIGINT || sig == SIGTERM) {
        received_signal = sig;
        break;
      }
      if (sig != -1) continue;
      if (errno == EAGAIN || errno == EINTR) continue;
      spdlog::error("sigtimedwait failed: {}", std::strerror(errno));
      break;
    }

    running.store(false, std::memory_order_relaxed);
    if (consumer_thread.joinable()) {
      consumer_thread.join();
    }

    if (consumer_failed.load(std::memory_order_acquire)) {
      std::exception_ptr ep;
      {
        std::lock_guard<std::mutex> lock(consumer_exception_mutex);
        ep = consumer_exception;
      }
      if (ep) std::rethrow_exception(ep);
      throw std::runtime_error("consumer loop failed without an exception");
    }

    spdlog::info("Received signal {}, shutting down loader", received_signal);
    return 0;
  } catch (const std::exception &e) {
    spdlog::critical("Fatal startup/runtime error: {}", e.what());
    return 1;
  }
}
