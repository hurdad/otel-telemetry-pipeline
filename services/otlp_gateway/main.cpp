#include "grpc_server.h"

#include "telemetry/tracer.h"

#include <cstdlib>
#include <string>

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
  OtlpGrpcServer server(GetEnvOrDefault("GATEWAY_LISTEN_ADDR", "0.0.0.0:4317"),
                        GetEnvOrDefault("NATS_URL", "nats://localhost:4222"));
  server.Run();
  return 0;
}
