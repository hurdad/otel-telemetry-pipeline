#include "clickhouse_batcher.cpp"
#include "jetstream_client/jetstream_client.h"
#include "telemetry/tracer.h"

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <limits>
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

  ClickHouseBatcher batcher(ch_host, ch_port, ch_database);

  jetstream_client::JetStreamConsumer consumer(
      nats_url, nats_stream, {"otel.traces", "otel.metrics", "otel.logs"});

  while (true) {
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

  return 0;
}
