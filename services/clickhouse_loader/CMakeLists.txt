add_executable(jetstream-clickhouse-loader
  main.cpp
  jetstream_consumer.cpp
  clickhouse_batcher.cpp)

target_include_directories(jetstream-clickhouse-loader PRIVATE .)
target_link_libraries(jetstream-clickhouse-loader
  PRIVATE telemetry jetstream_client otlp_decoder clickhouse_writer)

install(TARGETS jetstream-clickhouse-loader
  RUNTIME DESTINATION bin)
