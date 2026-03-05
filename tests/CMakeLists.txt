find_package(GTest REQUIRED)

add_executable(otlp_decoder_tests
  otlp_decoder_test.cpp)
target_link_libraries(otlp_decoder_tests PRIVATE
  otlp_decoder
  otel_proto
  GTest::gtest
  GTest::gtest_main)
add_test(NAME otlp_decoder_tests COMMAND otlp_decoder_tests)

add_executable(clickhouse_writer_tests
  clickhouse_writer_tests.cpp)
target_link_libraries(clickhouse_writer_tests PRIVATE clickhouse_writer GTest::gtest GTest::gtest_main)
add_test(NAME clickhouse_writer_tests COMMAND clickhouse_writer_tests)

if(TARGET telemetry)
  add_executable(telemetry_tests
    telemetry_tests.cpp)
  target_link_libraries(telemetry_tests PRIVATE telemetry GTest::gtest GTest::gtest_main)
  add_test(NAME telemetry_tests COMMAND telemetry_tests)
endif()
