#include "grpc_server.h"

#include <fstream>
#include <stdexcept>

#include <grpcpp/grpcpp.h>

#include "export_handler.h"
#include "telemetry/tracer.h"

namespace {

std::string ReadFileOrThrow(const std::string& path) {
  std::ifstream file(path);
  if (!file.is_open()) {
    throw std::runtime_error("unable to open TLS file: " + path);
  }
  return std::string((std::istreambuf_iterator<char>(file)),
                     std::istreambuf_iterator<char>());
}

}  // namespace

OtlpGrpcServer::TraceServiceImpl::TraceServiceImpl(
    jetstream_client::JetStreamPublisher& publisher, std::string subject)
    : publisher_(publisher), subject_(std::move(subject)) {}

grpc::Status OtlpGrpcServer::TraceServiceImpl::Export(
    grpc::ServerContext*,
    const opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest* request,
    opentelemetry::proto::collector::trace::v1::ExportTraceServiceResponse*) {
  auto span = telemetry::StartSpan("grpc_export_traces");
  return otlp_gateway::HandleExport(
      "trace", subject_.c_str(), "Failed to publish traces to NATS",
      [request](std::string* payload) { return request->SerializeToString(payload); },
      [this](std::string_view subject, const void* data, size_t size) {
        return publisher_.Publish(std::string(subject), data, size);
      });
}

OtlpGrpcServer::MetricsServiceImpl::MetricsServiceImpl(
    jetstream_client::JetStreamPublisher& publisher, std::string subject)
    : publisher_(publisher), subject_(std::move(subject)) {}

grpc::Status OtlpGrpcServer::MetricsServiceImpl::Export(
    grpc::ServerContext*,
    const opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest* request,
    opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceResponse*) {
  auto span = telemetry::StartSpan("grpc_export_metrics");
  return otlp_gateway::HandleExport(
      "metrics", subject_.c_str(), "Failed to publish metrics to NATS",
      [request](std::string* payload) { return request->SerializeToString(payload); },
      [this](std::string_view subject, const void* data, size_t size) {
        return publisher_.Publish(std::string(subject), data, size);
      });
}

OtlpGrpcServer::LogsServiceImpl::LogsServiceImpl(
    jetstream_client::JetStreamPublisher& publisher, std::string subject)
    : publisher_(publisher), subject_(std::move(subject)) {}

grpc::Status OtlpGrpcServer::LogsServiceImpl::Export(
    grpc::ServerContext*,
    const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest* request,
    opentelemetry::proto::collector::logs::v1::ExportLogsServiceResponse*) {
  auto span = telemetry::StartSpan("grpc_export_logs");
  return otlp_gateway::HandleExport(
      "logs", subject_.c_str(), "Failed to publish logs to NATS",
      [request](std::string* payload) { return request->SerializeToString(payload); },
      [this](std::string_view subject, const void* data, size_t size) {
        return publisher_.Publish(std::string(subject), data, size);
      });
}

OtlpGrpcServer::OtlpGrpcServer(const GatewayConfig& config)
    : address_(config.listen_addr),
      tls_enabled_(config.tls_enabled),
      tls_cert_file_(config.tls_cert_file),
      tls_key_file_(config.tls_key_file),
      tls_ca_file_(config.tls_ca_file),
      publisher_(config.nats_url, config.nats_stream,
                 {config.trace_subject, config.metric_subject,
                  config.log_subject}),
      trace_service_(publisher_, config.trace_subject),
      metrics_service_(publisher_, config.metric_subject),
      logs_service_(publisher_, config.log_subject) {}

void OtlpGrpcServer::Run() {
  grpc::ServerBuilder builder;
  std::shared_ptr<grpc::ServerCredentials> credentials;
  if (tls_enabled_) {
    grpc::SslServerCredentialsOptions ssl_options;
    ssl_options.pem_root_certs = tls_ca_file_.empty() ? "" : ReadFileOrThrow(tls_ca_file_);
    ssl_options.pem_key_cert_pairs.push_back(
        grpc::SslServerCredentialsOptions::PemKeyCertPair{
            ReadFileOrThrow(tls_key_file_), ReadFileOrThrow(tls_cert_file_)});
    ssl_options.client_certificate_request = GRPC_SSL_DONT_REQUEST_CLIENT_CERTIFICATE;
    credentials = grpc::SslServerCredentials(ssl_options);
  } else {
    credentials = grpc::InsecureServerCredentials();
  }

  builder.AddListeningPort(address_, credentials);
  builder.RegisterService(&trace_service_);
  builder.RegisterService(&metrics_service_);
  builder.RegisterService(&logs_service_);
  server_ = builder.BuildAndStart();
  if (!server_) {
    throw std::runtime_error("Failed to start OTLP gRPC server on " + address_);
  }
  server_->Wait();
}

void OtlpGrpcServer::Shutdown() {
  if (server_) {
    server_->Shutdown();
  }
}
