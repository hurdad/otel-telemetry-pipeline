add_library(jetstream_client STATIC
  src/jetstream_client.cpp)

target_include_directories(jetstream_client PUBLIC include)
target_link_libraries(jetstream_client PUBLIC telemetry)
