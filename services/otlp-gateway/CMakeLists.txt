add_executable(otlp-gateway
  main.cpp
  grpc_server.cpp
  runtime.cpp)

target_include_directories(otlp-gateway PRIVATE .)
target_link_libraries(otlp-gateway
  PRIVATE telemetry jetstream_client otel_proto yaml-cpp otel_pipeline_jemalloc)

install(TARGETS otlp-gateway
  RUNTIME DESTINATION bin)
