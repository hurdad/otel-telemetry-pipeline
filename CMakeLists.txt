cmake_minimum_required(VERSION 3.24)
project(otel_telemetry_pipeline VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(OTEL_PIPELINE_BUILD_TESTS "Build tests" OFF)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  set(OTEL_PIPELINE_USE_JEMALLOC_DEFAULT ON)
else()
  set(OTEL_PIPELINE_USE_JEMALLOC_DEFAULT OFF)
endif()

option(OTEL_PIPELINE_USE_JEMALLOC "Link services with jemalloc allocator" ${OTEL_PIPELINE_USE_JEMALLOC_DEFAULT})
set(BUILD_TESTING OFF CACHE BOOL "Disable third-party tests by default" FORCE)

add_library(otel_pipeline_jemalloc INTERFACE)
if(OTEL_PIPELINE_USE_JEMALLOC)
  find_library(OTEL_PIPELINE_JEMALLOC_LIB NAMES jemalloc)
  if(NOT OTEL_PIPELINE_JEMALLOC_LIB)
    message(FATAL_ERROR "OTEL_PIPELINE_USE_JEMALLOC is ON, but jemalloc was not found")
  endif()
  target_link_libraries(otel_pipeline_jemalloc INTERFACE ${OTEL_PIPELINE_JEMALLOC_LIB})
endif()

include(cmake/dependencies.cmake)

add_subdirectory(libs/telemetry)
add_subdirectory(libs/jetstream_client)
add_subdirectory(libs/otlp_decoder)
add_subdirectory(libs/clickhouse_writer)

add_subdirectory(services/otlp_gateway)
add_subdirectory(services/clickhouse_loader)

if(OTEL_PIPELINE_BUILD_TESTS)
  set(BUILD_TESTING ON CACHE BOOL "Enable first-party tests" FORCE)
  enable_testing()
  add_subdirectory(tests)
endif()
