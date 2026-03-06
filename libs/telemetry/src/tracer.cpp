#include "telemetry/tracer.h"

#include <cstdlib>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "opentelemetry/exporters/otlp/otlp_grpc_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_grpc_exporter_options.h"
#include "opentelemetry/exporters/otlp/otlp_grpc_log_record_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_grpc_log_record_exporter_options.h"
#include "opentelemetry/exporters/otlp/otlp_grpc_metric_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_grpc_metric_exporter_options.h"
#include "opentelemetry/context/context.h"
#include "opentelemetry/logs/logger.h"
#include "opentelemetry/logs/provider.h"
#include "opentelemetry/metrics/meter.h"
#include "opentelemetry/metrics/provider.h"
#include "opentelemetry/sdk/logs/batch_log_record_processor_factory.h"
#include "opentelemetry/sdk/logs/logger_provider.h"
#include "opentelemetry/sdk/logs/logger_provider_factory.h"
#include "opentelemetry/sdk/logs/provider.h"
#include "opentelemetry/sdk/common/global_log_handler.h"
#include "opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_factory.h"
#include "opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader_options.h"
#include "opentelemetry/sdk/metrics/meter_context_factory.h"
#include "opentelemetry/sdk/metrics/meter_provider.h"
#include "opentelemetry/sdk/metrics/view/view_registry.h"
#include "opentelemetry/sdk/metrics/meter_provider_factory.h"
#include "opentelemetry/sdk/metrics/provider.h"
#include "opentelemetry/sdk/resource/resource.h"
#include "opentelemetry/sdk/trace/batch_span_processor_factory.h"
#include "opentelemetry/sdk/trace/provider.h"
#include "opentelemetry/sdk/trace/tracer_provider.h"
#include "opentelemetry/sdk/trace/tracer_provider_factory.h"
#include "opentelemetry/trace/provider.h"
#include "opentelemetry/trace/scope.h"
#include "opentelemetry/trace/span.h"

#include "spdlog/logger.h"
#include "spdlog/pattern_formatter.h"
#include "spdlog/sinks/base_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

namespace trace_api = opentelemetry::trace;
namespace trace_sdk = opentelemetry::sdk::trace;
namespace logs_api = opentelemetry::logs;
namespace logs_sdk = opentelemetry::sdk::logs;
namespace metrics_api = opentelemetry::metrics;
namespace metrics_sdk = opentelemetry::sdk::metrics;
namespace resource_sdk = opentelemetry::sdk::resource;
namespace otlp = opentelemetry::exporter::otlp;

namespace telemetry {

namespace {

class OtlpSpdlogSink final : public spdlog::sinks::base_sink<std::mutex> {
 public:
  explicit OtlpSpdlogSink(opentelemetry::nostd::shared_ptr<logs_api::Logger> logger)
      : logger_(std::move(logger)) {}

 protected:
  void sink_it_(const spdlog::details::log_msg& msg) override {
    if (!logger_) {
      return;
    }

    static thread_local bool in_export = false;
    if (in_export) {
      return;
    }

    spdlog::memory_buf_t formatted;
    formatter_->format(msg, formatted);
    const std::string payload(formatted.data(), formatted.size());

    in_export = true;
    switch (msg.level) {
      case spdlog::level::critical:
      case spdlog::level::err:
        logger_->Error(payload);
        break;
      case spdlog::level::warn:
        logger_->Warn(payload);
        break;
      case spdlog::level::info:
        logger_->Info(payload);
        break;
      case spdlog::level::debug:
      case spdlog::level::trace:
        logger_->Debug(payload);
        break;
      default:
        break;
    }
    in_export = false;
  }

  void flush_() override {}

 private:
  opentelemetry::nostd::shared_ptr<logs_api::Logger> logger_;
};

class SpdlogOtelInternalLogHandler final : public opentelemetry::sdk::common::internal_log::LogHandler {
 public:
  explicit SpdlogOtelInternalLogHandler(std::shared_ptr<spdlog::logger> logger)
      : logger_(std::move(logger)) {}

  void Handle(opentelemetry::sdk::common::internal_log::LogLevel level,
              const char* file,
              int line,
              const char* msg,
              const opentelemetry::sdk::common::AttributeMap&) noexcept override {
    (void)file;
    if (!logger_) {
      return;
    }

    const char* text = msg == nullptr ? "" : msg;
    switch (level) {
      case opentelemetry::sdk::common::internal_log::LogLevel::Error:
        logger_->error("[otel-cpp:{}] {}", line, text);
        break;
      case opentelemetry::sdk::common::internal_log::LogLevel::Warning:
        logger_->warn("[otel-cpp:{}] {}", line, text);
        break;
      case opentelemetry::sdk::common::internal_log::LogLevel::Info:
        logger_->info("[otel-cpp:{}] {}", line, text);
        break;
      case opentelemetry::sdk::common::internal_log::LogLevel::Debug:
        logger_->debug("[otel-cpp:{}] {}", line, text);
        break;
      default:
        logger_->trace("[otel-cpp:{}] {}", line, text);
        break;
    }
  }

 private:
  std::shared_ptr<spdlog::logger> logger_;
};

resource_sdk::ResourceAttributes ParseResourceAttributes(const char* raw) {
  resource_sdk::ResourceAttributes out;
  if (raw == nullptr || raw[0] == '\0') {
    return out;
  }

  std::stringstream stream(raw);
  std::string pair;
  while (std::getline(stream, pair, ',')) {
    const auto equals = pair.find('=');
    if (equals == std::string::npos) {
      continue;
    }
    const std::string key = pair.substr(0, equals);
    const std::string value = pair.substr(equals + 1);
    if (!key.empty()) {
      out[key] = value;
    }
  }
  return out;
}

class TelemetryRuntime {
 public:
  static TelemetryRuntime& Instance() {
    static TelemetryRuntime instance;
    return instance;
  }

  void Init() {
    std::lock_guard<std::mutex> lock(mu_);
    if (initialized_) {
      return;
    }

    const std::string endpoint = GetEnvOrDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "");
    const std::string service_name = GetEnvOrDefault("OTEL_SERVICE_NAME", "otel-pipeline");
    const std::string resource_attributes = GetEnvOrDefault("OTEL_RESOURCE_ATTRIBUTES", "");

    if (endpoint.empty()) {
      spdlog::warn("Telemetry disabled: OTEL_EXPORTER_OTLP_ENDPOINT is empty");
      initialized_ = true;
      return;
    }

    auto resource_kvs = ParseResourceAttributes(resource_attributes.c_str());
    resource_kvs["service.name"] = service_name;
    resource_ = resource_sdk::Resource::Create(resource_kvs);

    InitTracing(endpoint);
    InitMetrics(endpoint);
    InitLogs(endpoint);

    tracer_ = trace_api::Provider::GetTracerProvider()->GetTracer("otel-pipeline-self", "1.0.0");
    meter_ = metrics_api::Provider::GetMeterProvider()->GetMeter("otel-pipeline-self", "1.0.0");
    logger_ = logs_api::Provider::GetLoggerProvider()->GetLogger("otel-pipeline-self-logger",
                                                                  "otel-pipeline-self");

    ConfigureSpdlog();

    span_counter_ = meter_->CreateUInt64Counter("self_telemetry.spans_total", "spans", "");
    span_duration_ms_ =
        meter_->CreateDoubleHistogram("self_telemetry.span_duration_ms", "ms", "");
    clickhouse_rows_inserted_counter_ = meter_->CreateUInt64Counter(
        "self_telemetry.clickhouse_writer.rows_inserted_total", "rows", "");
    clickhouse_insert_errors_counter_ =
        meter_->CreateUInt64Counter("self_telemetry.clickhouse_writer.insert_errors_total",
                                    "errors", "");

    logger_->Info("Self telemetry initialized");
    initialized_ = true;
    spdlog::info("Telemetry initialized endpoint={} service={}", endpoint, service_name);
  }

  opentelemetry::nostd::shared_ptr<trace_api::Span> StartSpan(const std::string& name) {
    if (!tracer_) {
      return {};
    }
    return tracer_->StartSpan(name);
  }

  void RecordSpanMetrics(double duration_ms) {
    if (span_counter_) {
      span_counter_->Add(1);
    }
    if (span_duration_ms_) {
      span_duration_ms_->Record(duration_ms, opentelemetry::context::Context{});
    }
  }

  void LogSpan(const std::string& name, uint64_t duration_ms) {
    if (logger_) {
      const std::string msg =
          "span_completed name=" + name + " duration_ms=" + std::to_string(duration_ms);
      logger_->Debug(msg);
    }
  }

  void RecordClickHouseRowsInserted(uint64_t rows) {
    if (clickhouse_rows_inserted_counter_) {
      clickhouse_rows_inserted_counter_->Add(rows);
    }
  }

  void RecordClickHouseInsertError() {
    if (clickhouse_insert_errors_counter_) {
      clickhouse_insert_errors_counter_->Add(1);
    }
  }

 private:
  static std::string GetEnvOrDefault(const char* key, const char* fallback) {
    const char* value = std::getenv(key);
    if (value == nullptr || value[0] == '\0') {
      return fallback;
    }
    return value;
  }

  void InitTracing(const std::string& endpoint) {
    otlp::OtlpGrpcExporterOptions options;
    options.endpoint = endpoint;
    options.use_ssl_credentials = false;
    auto exporter = otlp::OtlpGrpcExporterFactory::Create(options);
    auto processor = trace_sdk::BatchSpanProcessorFactory::Create(
        std::move(exporter), trace_sdk::BatchSpanProcessorOptions{});
    auto provider = trace_sdk::TracerProviderFactory::Create(std::move(processor), resource_);
    tracer_provider_ = std::shared_ptr<trace_sdk::TracerProvider>(std::move(provider));

    std::shared_ptr<trace_api::TracerProvider> api_provider = tracer_provider_;
    trace_sdk::Provider::SetTracerProvider(api_provider);
  }

  void InitMetrics(const std::string& endpoint) {
    otlp::OtlpGrpcMetricExporterOptions options;
    options.endpoint = endpoint;
    options.use_ssl_credentials = false;
    auto exporter = otlp::OtlpGrpcMetricExporterFactory::Create(options);

    metrics_sdk::PeriodicExportingMetricReaderOptions reader_options;
    reader_options.export_interval_millis = std::chrono::milliseconds(1000);
    reader_options.export_timeout_millis = std::chrono::milliseconds(500);

    auto reader = metrics_sdk::PeriodicExportingMetricReaderFactory::Create(std::move(exporter),
                                                                             reader_options);

    auto context = metrics_sdk::MeterContextFactory::Create(
        std::unique_ptr<metrics_sdk::ViewRegistry>(new metrics_sdk::ViewRegistry()), resource_);
    context->AddMetricReader(std::move(reader));
    auto provider = metrics_sdk::MeterProviderFactory::Create(std::move(context));
    meter_provider_ = std::shared_ptr<metrics_sdk::MeterProvider>(std::move(provider));

    std::shared_ptr<metrics_api::MeterProvider> api_provider = meter_provider_;
    metrics_sdk::Provider::SetMeterProvider(api_provider);
  }

  void InitLogs(const std::string& endpoint) {
    otlp::OtlpGrpcLogRecordExporterOptions options;
    options.endpoint = endpoint;
    options.use_ssl_credentials = false;
    auto exporter = otlp::OtlpGrpcLogRecordExporterFactory::Create(options);
    auto processor = logs_sdk::BatchLogRecordProcessorFactory::Create(
        std::move(exporter), logs_sdk::BatchLogRecordProcessorOptions{});
    auto provider = logs_sdk::LoggerProviderFactory::Create(std::move(processor), resource_);
    logger_provider_ = std::shared_ptr<logs_sdk::LoggerProvider>(std::move(provider));

    std::shared_ptr<logs_api::LoggerProvider> api_provider = logger_provider_;
    logs_sdk::Provider::SetLoggerProvider(api_provider);
  }

  void ConfigureSpdlog() {
    auto stdout_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    stdout_sink->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%n] %v");

    auto otlp_sink = std::make_shared<OtlpSpdlogSink>(logger_);
    otlp_sink->set_pattern("%v");

    std::vector<spdlog::sink_ptr> sinks{stdout_sink, otlp_sink};
    auto app_logger = std::make_shared<spdlog::logger>("otel-pipeline", sinks.begin(), sinks.end());
    app_logger->set_level(spdlog::level::info);
    app_logger->flush_on(spdlog::level::warn);
    spdlog::set_default_logger(app_logger);

    // Route otel-cpp internal logs to stdout only to prevent recursive log exporting.
    auto internal_logger = std::make_shared<spdlog::logger>("otel-cpp", stdout_sink);
    internal_logger->set_level(spdlog::level::warn);
    auto handler = opentelemetry::nostd::shared_ptr<opentelemetry::sdk::common::internal_log::LogHandler>(
        new SpdlogOtelInternalLogHandler(internal_logger));
    opentelemetry::sdk::common::internal_log::GlobalLogHandler::SetLogHandler(handler);
    opentelemetry::sdk::common::internal_log::GlobalLogHandler::SetLogLevel(
        opentelemetry::sdk::common::internal_log::LogLevel::Warning);
  }

  std::mutex mu_;
  bool initialized_ = false;

  resource_sdk::Resource resource_;
  std::shared_ptr<trace_sdk::TracerProvider> tracer_provider_;
  std::shared_ptr<metrics_sdk::MeterProvider> meter_provider_;
  std::shared_ptr<logs_sdk::LoggerProvider> logger_provider_;

  opentelemetry::nostd::shared_ptr<trace_api::Tracer> tracer_;
  opentelemetry::nostd::shared_ptr<metrics_api::Meter> meter_;
  opentelemetry::nostd::shared_ptr<logs_api::Logger> logger_;

  opentelemetry::nostd::unique_ptr<metrics_api::Counter<uint64_t>> span_counter_;
  opentelemetry::nostd::unique_ptr<metrics_api::Histogram<double>> span_duration_ms_;
  opentelemetry::nostd::unique_ptr<metrics_api::Counter<uint64_t>> clickhouse_rows_inserted_counter_;
  opentelemetry::nostd::unique_ptr<metrics_api::Counter<uint64_t>> clickhouse_insert_errors_counter_;
};

}  // namespace

class ScopedSpan::Impl {
 public:
  explicit Impl(opentelemetry::nostd::shared_ptr<trace_api::Span> span)
      : span_(std::move(span)), scope_(span_) {}

  void End() {
    if (span_) {
      span_->End();
    }
  }

 private:
  opentelemetry::nostd::shared_ptr<trace_api::Span> span_;
  trace_api::Scope scope_;
};

ScopedSpan::ScopedSpan(std::string name)
    : name_(std::move(name)),
      started_at_(std::chrono::steady_clock::now()),
      impl_(std::make_unique<Impl>(TelemetryRuntime::Instance().StartSpan(name_))) {}

ScopedSpan::~ScopedSpan() {
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - started_at_);

  if (impl_) {
    impl_->End();
  }

  TelemetryRuntime::Instance().RecordSpanMetrics(static_cast<double>(elapsed.count()));
  TelemetryRuntime::Instance().LogSpan(name_, static_cast<uint64_t>(elapsed.count()));

  spdlog::debug("[span] {} duration_ms={}", name_, elapsed.count());
}

void InitTelemetry() { TelemetryRuntime::Instance().Init(); }

std::unique_ptr<ScopedSpan> StartSpan(const std::string& name) {
  return std::make_unique<ScopedSpan>(name);
}

void RecordClickHouseRowsInserted(uint64_t rows) {
  TelemetryRuntime::Instance().RecordClickHouseRowsInserted(rows);
}

void RecordClickHouseInsertError() { TelemetryRuntime::Instance().RecordClickHouseInsertError(); }

}  // namespace telemetry
