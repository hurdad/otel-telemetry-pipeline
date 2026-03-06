cmake_minimum_required(VERSION 3.24)
project(otel_telemetry_pipeline VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(OTEL_PIPELINE_BUILD_TESTS "Build tests" OFF)
set(BUILD_TESTING OFF CACHE BOOL "Disable third-party tests by default" FORCE)

find_library(OTEL_PIPELINE_JEMALLOC_LIB NAMES jemalloc REQUIRED)
add_library(otel_pipeline_jemalloc INTERFACE)
target_link_libraries(otel_pipeline_jemalloc INTERFACE ${OTEL_PIPELINE_JEMALLOC_LIB})

include(cmake/dependencies.cmake)

add_subdirectory(libs/telemetry)
add_subdirectory(libs/jetstream_client)
add_subdirectory(libs/otlp_decoder)
add_subdirectory(libs/clickhouse_writer)

add_subdirectory(services/otlp-gateway)
add_subdirectory(services/jetstream-clickhouse-loader)

if(OTEL_PIPELINE_BUILD_TESTS)
  set(BUILD_TESTING ON CACHE BOOL "Enable first-party tests" FORCE)
  enable_testing()
  add_subdirectory(tests)
endif()
