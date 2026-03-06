find_package(GTest REQUIRED)
find_package(Threads REQUIRED)

add_executable(otlp_decoder_tests
  otlp_decoder_test.cpp)
target_link_libraries(otlp_decoder_tests PRIVATE
  otlp_decoder
  otel_proto
  GTest::gtest
  GTest::gtest_main)
add_test(NAME otlp_decoder_tests COMMAND otlp_decoder_tests)

add_executable(clickhouse_writer_tests
  batch_insert_test.cpp)
target_link_libraries(clickhouse_writer_tests PRIVATE clickhouse_writer GTest::gtest GTest::gtest_main)
add_test(NAME clickhouse_writer_tests COMMAND clickhouse_writer_tests)

if(TARGET telemetry)
  add_executable(telemetry_tests
    telemetry_tests.cpp)
  target_link_libraries(telemetry_tests PRIVATE telemetry GTest::gtest GTest::gtest_main)
  add_test(NAME telemetry_tests COMMAND telemetry_tests)
endif()

add_executable(grpc_export_handler_tests
  grpc_export_handler_tests.cpp)
target_include_directories(grpc_export_handler_tests PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(grpc_export_handler_tests PRIVATE
  GTest::gtest
  GTest::gtest_main
  gRPC::grpc++
  spdlog::spdlog)
add_test(NAME grpc_export_handler_tests COMMAND grpc_export_handler_tests)
add_executable(jetstream_client_init_tests
  jetstream_client_init_tests.cpp)
target_link_libraries(jetstream_client_init_tests PRIVATE
  jetstream_client
  GTest::gtest
  GTest::gtest_main)
add_test(NAME jetstream_client_init_tests COMMAND jetstream_client_init_tests)

add_executable(otlp_gateway_runtime_tests
  otlp_gateway_runtime_tests.cpp
  ${CMAKE_SOURCE_DIR}/services/otlp-gateway/runtime.cpp)
target_include_directories(otlp_gateway_runtime_tests PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(otlp_gateway_runtime_tests PRIVATE
  GTest::gtest
  GTest::gtest_main
  Threads::Threads)
add_test(NAME otlp_gateway_runtime_tests COMMAND otlp_gateway_runtime_tests)
