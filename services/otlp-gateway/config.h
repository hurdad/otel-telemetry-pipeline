#pragma once

#include <iostream>
#include <string>

#include <yaml-cpp/yaml.h>

struct GatewayConfig {
  std::string listen_addr = "0.0.0.0:4317";
  std::string nats_url    = "nats://localhost:4222";
  std::string nats_stream = "OTEL_TELEMETRY";
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
  } catch (const YAML::Exception& e) {
    std::clog << "Warning: failed to load config from " << path << ": " << e.what() << '\n';
  }
  return cfg;
}
