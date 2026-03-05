#include "config.h"
#include "grpc_server.h"

#include "telemetry/tracer.h"

#include <csignal>
#include <cstdlib>
#include <pthread.h>
#include <string>
#include <thread>

#include "spdlog/spdlog.h"

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

  // Load config file (default path can be overridden via GATEWAY_CONFIG env var).
  const std::string config_path =
      GetEnvOrDefault("GATEWAY_CONFIG", "/etc/otel-pipeline/gateway.yaml");
  GatewayConfig cfg = LoadConfig(config_path);

  // Environment variables override config file values when explicitly set.
  if (const char* v = std::getenv("GATEWAY_LISTEN_ADDR"); v && v[0]) cfg.listen_addr = v;
  if (const char* v = std::getenv("NATS_URL"); v && v[0]) cfg.nats_url = v;
  if (const char* v = std::getenv("NATS_STREAM"); v && v[0]) cfg.nats_stream = v;

  spdlog::info("Starting otlp-gateway (config={})", config_path);
  spdlog::info("  Listen: {}", cfg.listen_addr);
  spdlog::info("  NATS: {} stream={}", cfg.nats_url, cfg.nats_stream);

  OtlpGrpcServer server(cfg.listen_addr, cfg.nats_url, cfg.nats_stream);

  // Block signals before spawning the server thread so they are delivered only
  // to the main thread's sigwait call, regardless of which thread they arrive on.
  sigset_t signal_set;
  sigemptyset(&signal_set);
  sigaddset(&signal_set, SIGINT);
  sigaddset(&signal_set, SIGTERM);
  if (pthread_sigmask(SIG_BLOCK, &signal_set, nullptr) != 0) {
    spdlog::error("Failed to block termination signals, exiting");
    return 1;
  }

  std::thread server_thread([&server]() { server.Run(); });

  int received_signal = 0;
  if (sigwait(&signal_set, &received_signal) != 0) {
    spdlog::error("sigwait failed, forcing shutdown");
    received_signal = SIGTERM;
  }
  spdlog::info("Received signal {}, shutting down OTLP gateway", received_signal);

  server.Shutdown();

  if (server_thread.joinable()) {
    server_thread.join();
  }

  return 0;
}
