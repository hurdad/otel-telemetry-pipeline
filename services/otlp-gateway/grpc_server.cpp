#include "grpc_server.h"

#include <grpcpp/grpcpp.h>

#include "export_handler.h"
#include "telemetry/tracer.h"

namespace {
constexpr char kTraceSubject[] = "otel.traces";
constexpr char kMetricSubject[] = "otel.metrics";
constexpr char kLogSubject[] = "otel.logs";
}

OtlpGrpcServer::TraceServiceImpl::TraceServiceImpl(jetstream_client::JetStreamPublisher& publisher)
    : publisher_(publisher) {}

grpc::Status OtlpGrpcServer::TraceServiceImpl::Export(
    grpc::ServerContext*,
    const opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest* request,
    opentelemetry::proto::collector::trace::v1::ExportTraceServiceResponse*) {
  auto span = telemetry::StartSpan("grpc_export_traces");
  return otlp_gateway::HandleExport(
      "trace", kTraceSubject, "Failed to publish traces to NATS",
      [request](std::string* payload) { return request->SerializeToString(payload); },
      [this](std::string_view subject, const void* data, size_t size) {
        return publisher_.Publish(std::string(subject), data, size);
      });
}

OtlpGrpcServer::MetricsServiceImpl::MetricsServiceImpl(jetstream_client::JetStreamPublisher& publisher)
    : publisher_(publisher) {}

grpc::Status OtlpGrpcServer::MetricsServiceImpl::Export(
    grpc::ServerContext*,
    const opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest* request,
    opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceResponse*) {
  auto span = telemetry::StartSpan("grpc_export_metrics");
  return otlp_gateway::HandleExport(
      "metrics", kMetricSubject, "Failed to publish metrics to NATS",
      [request](std::string* payload) { return request->SerializeToString(payload); },
      [this](std::string_view subject, const void* data, size_t size) {
        return publisher_.Publish(std::string(subject), data, size);
      });
}

OtlpGrpcServer::LogsServiceImpl::LogsServiceImpl(jetstream_client::JetStreamPublisher& publisher)
    : publisher_(publisher) {}

grpc::Status OtlpGrpcServer::LogsServiceImpl::Export(
    grpc::ServerContext*,
    const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest* request,
    opentelemetry::proto::collector::logs::v1::ExportLogsServiceResponse*) {
  auto span = telemetry::StartSpan("grpc_export_logs");
  return otlp_gateway::HandleExport(
      "logs", kLogSubject, "Failed to publish logs to NATS",
      [request](std::string* payload) { return request->SerializeToString(payload); },
      [this](std::string_view subject, const void* data, size_t size) {
        return publisher_.Publish(std::string(subject), data, size);
      });
}

OtlpGrpcServer::OtlpGrpcServer(std::string address, std::string nats_url, std::string stream_name)
    : address_(std::move(address)),
      publisher_(std::move(nats_url), std::move(stream_name),
                 {kTraceSubject, kMetricSubject, kLogSubject}),
      trace_service_(publisher_),
      metrics_service_(publisher_),
      logs_service_(publisher_) {}

void OtlpGrpcServer::Run() {
  grpc::ServerBuilder builder;
  builder.AddListeningPort(address_, grpc::InsecureServerCredentials());
  builder.RegisterService(&trace_service_);
  builder.RegisterService(&metrics_service_);
  builder.RegisterService(&logs_service_);
  server_ = builder.BuildAndStart();
  server_->Wait();
}

void OtlpGrpcServer::Shutdown() {
  if (server_) {
    server_->Shutdown();
  }
}
