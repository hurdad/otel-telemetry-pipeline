#pragma once

#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>

#include <yaml-cpp/yaml.h>

struct LoaderConfig {
  std::string nats_url            = "nats://localhost:4222";
  std::string nats_stream         = "OTEL_TELEMETRY";
  std::string clickhouse_host     = "localhost";
  uint16_t    clickhouse_port     = 9000;
  std::string clickhouse_database = "default";
  std::string clickhouse_user     = "default";
  std::string clickhouse_password = "";
  uint32_t    batch_max_rows      = 50000;
  std::chrono::seconds flush_interval{2};
  std::string trace_subject  = "otel.traces";
  std::string metric_subject = "otel.metrics";
  std::string log_subject    = "otel.logs";
};

inline LoaderConfig LoadConfig(const std::string& path) {
  LoaderConfig cfg;
  try {
    const YAML::Node root = YAML::LoadFile(path);

    if (const auto nats = root["nats"]) {
      if (nats["url"])    cfg.nats_url    = nats["url"].as<std::string>();
      if (nats["stream"]) cfg.nats_stream = nats["stream"].as<std::string>();
      if (const auto subjects = nats["subjects"]) {
        if (subjects.IsSequence() && subjects.size() >= 3) {
          cfg.trace_subject = subjects[0].as<std::string>();
          cfg.metric_subject = subjects[1].as<std::string>();
          cfg.log_subject = subjects[2].as<std::string>();
        }
      }
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
    if (const auto ch = root["clickhouse"]) {
      if (ch["host"])     cfg.clickhouse_host     = ch["host"].as<std::string>();
      if (ch["port"])     cfg.clickhouse_port     = ch["port"].as<uint16_t>();
      if (ch["database"]) cfg.clickhouse_database = ch["database"].as<std::string>();
      if (ch["user"])     cfg.clickhouse_user     = ch["user"].as<std::string>();
      if (ch["password"]) cfg.clickhouse_password = ch["password"].as<std::string>();
    }
    if (const auto batch = root["batch"]) {
      if (batch["max_batch_rows"])
        cfg.batch_max_rows = batch["max_batch_rows"].as<uint32_t>();
      if (batch["flush_interval_seconds"])
        cfg.flush_interval =
            std::chrono::seconds(batch["flush_interval_seconds"].as<uint32_t>());
    }
  } catch (const YAML::Exception& e) {
    std::clog << "Warning: failed to load config from " << path << ": " << e.what() << '\n';
  }
  return cfg;
}
