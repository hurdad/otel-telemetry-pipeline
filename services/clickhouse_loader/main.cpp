#include "jetstream_client/jetstream_client.h"
#include "telemetry/tracer.h"

#include <chrono>
#include <cstdlib>
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

}  // namespace

int main() {
  telemetry::InitTelemetry();

  jetstream_client::JetStreamConsumer consumer(GetEnvOrDefault("NATS_URL", "nats://localhost:4222"),
                                                GetEnvOrDefault("NATS_STREAM", "OTEL_TELEMETRY"),
                                                {"otel.traces", "otel.metrics", "otel.logs"});

  while (true) {
    consumer.Poll([](const jetstream_client::Message&) {});
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  return 0;
}
