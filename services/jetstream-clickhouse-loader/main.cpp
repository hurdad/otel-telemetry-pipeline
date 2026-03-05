#include "clickhouse_batcher.h"
#include "jetstream_client/jetstream_client.h"
#include "telemetry/tracer.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <pthread.h>
#include <string>
#include <thread>

namespace {

std::string GetEnvOrDefault(const char* key, const char* fallback) {
  const char* value = std::getenv(key);
  if (value == nullptr || value[0] == '\0') {
    return fallback;
  }
  return value;
}

uint16_t ParsePortOrDefault(const char* key, uint16_t fallback) {
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
  } catch (const std::exception& e) {
    std::clog << "Invalid " << key << "='" << raw << "', using default " << fallback
              << " error=" << e.what() << '\n';
    return fallback;
  }
}

}  // namespace

int main() {
  telemetry::InitTelemetry();

  const std::string nats_url    = GetEnvOrDefault("NATS_URL", "nats://localhost:4222");
  const std::string nats_stream = GetEnvOrDefault("NATS_STREAM", "OTEL_TELEMETRY");
  const std::string ch_host     = GetEnvOrDefault("CLICKHOUSE_HOST", "localhost");
  const uint16_t    ch_port     = ParsePortOrDefault("CLICKHOUSE_PORT", 9000);
  const std::string ch_database = GetEnvOrDefault("CLICKHOUSE_DATABASE", "default");

  std::clog << "Starting jetstream-clickhouse-loader\n"
            << "  NATS: " << nats_url << " stream=" << nats_stream << '\n'
            << "  ClickHouse: " << ch_host << ':' << ch_port << '/' << ch_database << '\n';

  // Block signals before spawning the consumer thread so they are delivered
  // only to the main thread's sigwait call.
  sigset_t signal_set;
  sigemptyset(&signal_set);
  sigaddset(&signal_set, SIGINT);
  sigaddset(&signal_set, SIGTERM);
  if (pthread_sigmask(SIG_BLOCK, &signal_set, nullptr) != 0) {
    std::clog << "Failed to block termination signals, exiting without graceful shutdown\n";
    return 1;
  }

  ClickHouseBatcher batcher(ch_host, ch_port, ch_database);
  jetstream_client::JetStreamConsumer consumer(
      nats_url, nats_stream, {"otel.traces", "otel.metrics", "otel.logs"});

  std::atomic<bool> running{true};
  std::thread consumer_thread([&]() {
    while (running.load(std::memory_order_relaxed)) {
      consumer.Poll([&batcher](const jetstream_client::Message& msg) {
        if (msg.subject == "otel.traces") {
          batcher.ProcessTraces(msg.payload);
        } else if (msg.subject == "otel.metrics") {
          batcher.ProcessMetrics(msg.payload);
        } else if (msg.subject == "otel.logs") {
          batcher.ProcessLogs(msg.payload);
        }
      });
      batcher.FlushAll();
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    batcher.FlushAll();
  });

  int received_signal = 0;
  if (sigwait(&signal_set, &received_signal) != 0) {
    std::clog << "sigwait failed, forcing shutdown\n";
    received_signal = SIGTERM;
  }
  std::clog << "Received signal " << received_signal << ", shutting down loader\n";

  running.store(false, std::memory_order_relaxed);
  if (consumer_thread.joinable()) {
    consumer_thread.join();
  }

  return 0;
}
