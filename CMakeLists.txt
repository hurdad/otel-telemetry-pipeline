cmake_minimum_required(VERSION 3.24)
project(otel_telemetry_pipeline VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(OTEL_PIPELINE_BUILD_TESTS "Build tests" OFF)

include(cmake/dependencies.cmake)

add_subdirectory(libs/telemetry)
add_subdirectory(libs/jetstream_client)
add_subdirectory(libs/otlp_decoder)
add_subdirectory(libs/clickhouse_writer)

add_subdirectory(services/otlp_gateway)
add_subdirectory(services/clickhouse_loader)
