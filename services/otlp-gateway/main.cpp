#include "config.h"
#include "grpc_server.h"

#include "telemetry/tracer.h"

#include <csignal>
#include <cstdlib>
#include <exception>
#include <pthread.h>
#include <string>
#include <thread>

#include "runtime.h"

#include "spdlog/spdlog.h"

namespace {

std::string GetEnvOrDefault(const char *key, const char *fallback) {
  const char *value = std::getenv(key);
  if (value == nullptr || value[0] == '\0') {
    return fallback;
  }
  return value;
}

} // namespace

int main() {
  try {
    telemetry::InitTelemetry();

    // Load config file (default path can be overridden via GATEWAY_CONFIG env
    // var).
    const std::string config_path =
        GetEnvOrDefault("GATEWAY_CONFIG", "/etc/otel-pipeline/gateway.yaml");
    GatewayConfig cfg = LoadConfig(config_path);

    // Environment variables override config file values when explicitly set.
    if (const char *v = std::getenv("GATEWAY_LISTEN_ADDR"); v && v[0])
      cfg.listen_addr = v;
    if (const char *v = std::getenv("NATS_URL"); v && v[0])
      cfg.nats_url = v;
    if (const char *v = std::getenv("NATS_STREAM"); v && v[0])
      cfg.nats_stream = v;
    if (const char *v = std::getenv("TRACE_SUBJECT"); v && v[0])
      cfg.trace_subject = v;
    if (const char *v = std::getenv("METRIC_SUBJECT"); v && v[0])
      cfg.metric_subject = v;
    if (const char *v = std::getenv("LOG_SUBJECT"); v && v[0])
      cfg.log_subject = v;
    if (const char *v = std::getenv("GATEWAY_TLS_ENABLED"); v && v[0])
      cfg.tls_enabled = std::string(v) == "true" || std::string(v) == "1";
    if (const char *v = std::getenv("GATEWAY_TLS_CERT_FILE"); v && v[0])
      cfg.tls_cert_file = v;
    if (const char *v = std::getenv("GATEWAY_TLS_KEY_FILE"); v && v[0])
      cfg.tls_key_file = v;
    if (const char *v = std::getenv("GATEWAY_TLS_CA_FILE"); v && v[0])
      cfg.tls_ca_file = v;

    spdlog::info("Starting otlp-gateway (config={})", config_path);
    spdlog::info("  Listen: {}", cfg.listen_addr);
    spdlog::info("  NATS: {} stream={}", cfg.nats_url, cfg.nats_stream);
    spdlog::info("  Subjects: traces={} metrics={} logs={}", cfg.trace_subject,
                 cfg.metric_subject, cfg.log_subject);
    spdlog::info("  TLS: enabled={}", cfg.tls_enabled);

    OtlpGrpcServer server(cfg);

    // Block signals before spawning the server thread so they are delivered
    // only to the main thread's sigwait call, regardless of which thread they
    // arrive on.
    sigset_t signal_set;
    sigemptyset(&signal_set);
    sigaddset(&signal_set, SIGINT);
    sigaddset(&signal_set, SIGTERM);
    if (pthread_sigmask(SIG_BLOCK, &signal_set, nullptr) != 0) {
      spdlog::error("Failed to block termination signals, exiting");
      return 1;
    }

    const int received_signal = RunServerUntilSignalOrFailure(
        signal_set, [&server]() { server.Run(); }, [&server]() { server.Shutdown(); });

    spdlog::info("Received signal {}, shutting down OTLP gateway",
                 received_signal);

    return 0;
  } catch (const std::exception &e) {
    spdlog::critical("Fatal startup/runtime error: {}", e.what());
    return 1;
  }
}
