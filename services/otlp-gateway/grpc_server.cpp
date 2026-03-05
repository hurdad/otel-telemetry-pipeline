#include "grpc_server.h"

#include <grpcpp/grpcpp.h>

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
  std::string payload;
  request->SerializeToString(&payload);
  if (!publisher_.Publish(kTraceSubject, payload.data(), payload.size())) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Failed to publish traces to NATS");
  }
  return grpc::Status::OK;
}

OtlpGrpcServer::MetricsServiceImpl::MetricsServiceImpl(jetstream_client::JetStreamPublisher& publisher)
    : publisher_(publisher) {}

grpc::Status OtlpGrpcServer::MetricsServiceImpl::Export(
    grpc::ServerContext*,
    const opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest* request,
    opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceResponse*) {
  auto span = telemetry::StartSpan("grpc_export_metrics");
  std::string payload;
  request->SerializeToString(&payload);
  if (!publisher_.Publish(kMetricSubject, payload.data(), payload.size())) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Failed to publish metrics to NATS");
  }
  return grpc::Status::OK;
}

OtlpGrpcServer::LogsServiceImpl::LogsServiceImpl(jetstream_client::JetStreamPublisher& publisher)
    : publisher_(publisher) {}

grpc::Status OtlpGrpcServer::LogsServiceImpl::Export(
    grpc::ServerContext*,
    const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest* request,
    opentelemetry::proto::collector::logs::v1::ExportLogsServiceResponse*) {
  auto span = telemetry::StartSpan("grpc_export_logs");
  std::string payload;
  request->SerializeToString(&payload);
  if (!publisher_.Publish(kLogSubject, payload.data(), payload.size())) {
    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Failed to publish logs to NATS");
  }
  return grpc::Status::OK;
}

OtlpGrpcServer::OtlpGrpcServer(std::string address, std::string nats_url)
    : address_(std::move(address)),
      publisher_(std::move(nats_url)),
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
