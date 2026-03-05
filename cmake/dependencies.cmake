find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(ZLIB REQUIRED)
find_package(yaml-cpp REQUIRED)

if(EXISTS "${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp/CMakeLists.txt")
  set(WITH_OTLP_GRPC ON CACHE BOOL "Enable OTLP gRPC exporter" FORCE)
  set(WITH_OTLP_HTTP OFF CACHE BOOL "Disable OTLP HTTP exporter" FORCE)
  set(WITH_OTLP_FILE OFF CACHE BOOL "Disable OTLP file exporter" FORCE)
  set(WITH_EXAMPLES OFF CACHE BOOL "Disable OpenTelemetry examples" FORCE)
  set(WITH_FUNC_TESTS OFF CACHE BOOL "Disable OpenTelemetry functional tests" FORCE)
  set(BUILD_TESTING OFF CACHE BOOL "Disable OpenTelemetry tests" FORCE)
  set(WITH_BENCHMARK OFF CACHE BOOL "Disable OpenTelemetry benchmarks" FORCE)
  add_subdirectory("${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp"
                   third_party/opentelemetry-cpp EXCLUDE_FROM_ALL)
endif()

set(OTEL_PROTO_ROOT "${CMAKE_SOURCE_DIR}/third_party/opentelemetry-cpp/third_party/opentelemetry-proto")
if(NOT EXISTS "${OTEL_PROTO_ROOT}/opentelemetry/proto")
  message(FATAL_ERROR "OpenTelemetry proto sources were not found at ${OTEL_PROTO_ROOT}")
endif()
file(GLOB_RECURSE OTEL_PROTO_FILES CONFIGURE_DEPENDS
  "${OTEL_PROTO_ROOT}/opentelemetry/proto/*.proto")

set(OTEL_PROTO_GEN_DIR "${CMAKE_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${OTEL_PROTO_GEN_DIR}")

set(OTEL_PROTO_GENERATED_SRCS)
set(OTEL_PROTO_GENERATED_HDRS)
foreach(PROTO_FILE IN LISTS OTEL_PROTO_FILES)
  file(RELATIVE_PATH REL_PATH "${OTEL_PROTO_ROOT}" "${PROTO_FILE}")
  get_filename_component(REL_DIR "${REL_PATH}" DIRECTORY)
  get_filename_component(BASE "${REL_PATH}" NAME_WE)

  list(APPEND OTEL_PROTO_GENERATED_SRCS
    "${OTEL_PROTO_GEN_DIR}/${REL_DIR}/${BASE}.pb.cc"
    "${OTEL_PROTO_GEN_DIR}/${REL_DIR}/${BASE}.grpc.pb.cc")
  list(APPEND OTEL_PROTO_GENERATED_HDRS
    "${OTEL_PROTO_GEN_DIR}/${REL_DIR}/${BASE}.pb.h"
    "${OTEL_PROTO_GEN_DIR}/${REL_DIR}/${BASE}.grpc.pb.h")
endforeach()

add_custom_command(
  OUTPUT ${OTEL_PROTO_GENERATED_SRCS} ${OTEL_PROTO_GENERATED_HDRS}
  COMMAND ${Protobuf_PROTOC_EXECUTABLE}
  ARGS
    --proto_path=${OTEL_PROTO_ROOT}
    --cpp_out=${OTEL_PROTO_GEN_DIR}
    --grpc_out=${OTEL_PROTO_GEN_DIR}
    --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
    ${OTEL_PROTO_FILES}
  DEPENDS ${OTEL_PROTO_FILES}
  VERBATIM)

add_library(otel_proto STATIC ${OTEL_PROTO_GENERATED_SRCS})
target_include_directories(otel_proto PUBLIC "${OTEL_PROTO_GEN_DIR}")
target_link_libraries(otel_proto PUBLIC protobuf::libprotobuf gRPC::grpc++ ZLIB::ZLIB)

if(EXISTS "${CMAKE_SOURCE_DIR}/third_party/clickhouse-cpp/CMakeLists.txt")
  add_subdirectory("${CMAKE_SOURCE_DIR}/third_party/clickhouse-cpp" third_party/clickhouse-cpp EXCLUDE_FROM_ALL)
endif()

if(EXISTS "${CMAKE_SOURCE_DIR}/third_party/nats-cpp/CMakeLists.txt")
  set(_otel_pipeline_build_testing "${BUILD_TESTING}")
  set(BUILD_TESTING OFF)
  add_subdirectory("${CMAKE_SOURCE_DIR}/third_party/nats-cpp" third_party/nats-cpp EXCLUDE_FROM_ALL)
  set(BUILD_TESTING "${_otel_pipeline_build_testing}")
endif()
