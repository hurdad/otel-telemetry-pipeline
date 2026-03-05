add_executable(otel-otlp-gateway
  main.cpp
  grpc_server.cpp)

target_include_directories(otel-otlp-gateway PRIVATE .)
target_link_libraries(otel-otlp-gateway
  PRIVATE telemetry jetstream_client otel_proto)

install(TARGETS otel-otlp-gateway
  RUNTIME DESTINATION bin)
