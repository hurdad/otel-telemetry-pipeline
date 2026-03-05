#pragma once

#include <memory>
#include <string>

#include <grpcpp/server.h>

#include "jetstream_client/jetstream_client.h"
#include "opentelemetry/proto/collector/logs/v1/logs_service.grpc.pb.h"
#include "opentelemetry/proto/collector/metrics/v1/metrics_service.grpc.pb.h"
#include "opentelemetry/proto/collector/trace/v1/trace_service.grpc.pb.h"

class OtlpGrpcServer final {
 public:
  OtlpGrpcServer(std::string address, std::string nats_url);
  void Run();
  void Shutdown();

 private:
  class TraceServiceImpl final
      : public opentelemetry::proto::collector::trace::v1::TraceService::Service {
   public:
    explicit TraceServiceImpl(jetstream_client::JetStreamPublisher& publisher);
    grpc::Status Export(
        grpc::ServerContext* context,
        const opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest* request,
        opentelemetry::proto::collector::trace::v1::ExportTraceServiceResponse* response) override;

   private:
    jetstream_client::JetStreamPublisher& publisher_;
  };

  class MetricsServiceImpl final
      : public opentelemetry::proto::collector::metrics::v1::MetricsService::Service {
   public:
    explicit MetricsServiceImpl(jetstream_client::JetStreamPublisher& publisher);
    grpc::Status Export(
        grpc::ServerContext* context,
        const opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest* request,
        opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceResponse* response) override;

   private:
    jetstream_client::JetStreamPublisher& publisher_;
  };

  class LogsServiceImpl final
      : public opentelemetry::proto::collector::logs::v1::LogsService::Service {
   public:
    explicit LogsServiceImpl(jetstream_client::JetStreamPublisher& publisher);
    grpc::Status Export(
        grpc::ServerContext* context,
        const opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest* request,
        opentelemetry::proto::collector::logs::v1::ExportLogsServiceResponse* response) override;

   private:
    jetstream_client::JetStreamPublisher& publisher_;
  };

  std::string address_;
  jetstream_client::JetStreamPublisher publisher_;
  TraceServiceImpl trace_service_;
  MetricsServiceImpl metrics_service_;
  LogsServiceImpl logs_service_;
  std::unique_ptr<grpc::Server> server_;
};
