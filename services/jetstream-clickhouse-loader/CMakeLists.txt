add_executable(jetstream-clickhouse-loader
  main.cpp)

target_include_directories(jetstream-clickhouse-loader PRIVATE .)
target_link_libraries(jetstream-clickhouse-loader
  PRIVATE telemetry jetstream_client otlp_decoder clickhouse_writer otel_pipeline_jemalloc)

install(TARGETS jetstream-clickhouse-loader
  RUNTIME DESTINATION bin)
