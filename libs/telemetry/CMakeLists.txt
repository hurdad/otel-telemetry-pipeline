add_library(telemetry STATIC
  src/tracer.cpp)

target_include_directories(telemetry PUBLIC include)

target_link_libraries(telemetry PUBLIC otel_proto)
