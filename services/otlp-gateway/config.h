#pragma once

#include <iostream>
#include <string>

#include <yaml-cpp/yaml.h>

struct GatewayConfig {
  std::string listen_addr = "0.0.0.0:4317";
  std::string nats_url    = "nats://localhost:4222";
  std::string nats_stream = "OTEL_TELEMETRY";
  std::string trace_subject  = "otel.traces";
  std::string metric_subject = "otel.metrics";
  std::string log_subject    = "otel.logs";
  bool tls_enabled = false;
  std::string tls_cert_file;
  std::string tls_key_file;
  std::string tls_ca_file;
};

inline GatewayConfig LoadConfig(const std::string& path) {
  GatewayConfig cfg;
  try {
    const YAML::Node root = YAML::LoadFile(path);

    if (const auto server = root["server"]) {
      if (server["listen"]) cfg.listen_addr = server["listen"].as<std::string>();
    }
    if (const auto nats = root["nats"]) {
      if (nats["url"])    cfg.nats_url    = nats["url"].as<std::string>();
      if (nats["stream"]) cfg.nats_stream = nats["stream"].as<std::string>();
    }
    if (const auto subjects = root["subjects"]) {
      if (subjects["traces"]) {
        cfg.trace_subject = subjects["traces"].as<std::string>();
      }
      if (subjects["metrics"]) {
        cfg.metric_subject = subjects["metrics"].as<std::string>();
      }
      if (subjects["logs"]) {
        cfg.log_subject = subjects["logs"].as<std::string>();
      }
    }
    if (const auto server = root["server"]) {
      if (const auto tls = server["tls"]) {
        if (tls["enabled"]) {
          cfg.tls_enabled = tls["enabled"].as<bool>();
        }
        if (tls["cert_file"]) {
          cfg.tls_cert_file = tls["cert_file"].as<std::string>();
        }
        if (tls["key_file"]) {
          cfg.tls_key_file = tls["key_file"].as<std::string>();
        }
        if (tls["ca_file"]) {
          cfg.tls_ca_file = tls["ca_file"].as<std::string>();
        }
      }
    }
  } catch (const YAML::Exception& e) {
    std::clog << "Warning: failed to load config from " << path << ": " << e.what() << '\n';
  }
  return cfg;
}
