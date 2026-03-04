#pragma once

#include <functional>
#include <string>
#include <vector>

namespace jetstream_client {

struct Message {
  std::string subject;
  std::string payload;
};

class JetStreamPublisher {
 public:
  explicit JetStreamPublisher(std::string url);
  bool Publish(const std::string& subject, const void* data, size_t size);

 private:
  std::string url_;
};

class JetStreamConsumer {
 public:
  using Handler = std::function<void(const Message&)>;

  JetStreamConsumer(std::string url, std::string stream, std::vector<std::string> subjects);
  void Poll(const Handler& handler);

 private:
  std::string url_;
  std::string stream_;
  std::vector<std::string> subjects_;
};

}  // namespace jetstream_client
