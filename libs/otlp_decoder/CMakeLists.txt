add_library(otlp_decoder STATIC
  src/decoder.cpp)

target_include_directories(otlp_decoder PUBLIC include)
target_link_libraries(otlp_decoder PUBLIC otel_proto)
