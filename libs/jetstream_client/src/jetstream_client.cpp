#include "jetstream_client/jetstream_client.h"

#include <iostream>

#include "telemetry/tracer.h"

namespace jetstream_client {

JetStreamPublisher::JetStreamPublisher(std::string url) : url_(std::move(url)) {}

bool JetStreamPublisher::Publish(const std::string& subject, const void* data, size_t size) {
  auto span = telemetry::StartSpan("jetstream_publish");
  std::clog << "publishing to " << url_ << " subject=" << subject << " bytes=" << size << '\n';
  (void)data;
  return true;
}

JetStreamConsumer::JetStreamConsumer(std::string url, std::string stream,
                                     std::vector<std::string> subjects)
    : url_(std::move(url)), stream_(std::move(stream)), subjects_(std::move(subjects)) {}

void JetStreamConsumer::Poll(const Handler& handler) {
  auto span = telemetry::StartSpan("jetstream_consume");
  for (const auto& subject : subjects_) {
    handler(Message{subject, {}});
  }
}

}  // namespace jetstream_client
