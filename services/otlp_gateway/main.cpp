#include "grpc_server.h"

#include "telemetry/tracer.h"

int main() {
  telemetry::InitTelemetry();
  OtlpGrpcServer server("0.0.0.0:4317", "nats://localhost:4222");
  server.Run();
  return 0;
}
