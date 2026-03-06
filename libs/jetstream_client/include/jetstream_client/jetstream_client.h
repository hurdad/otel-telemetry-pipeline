#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace jetstream_client {

namespace testing {

// Test seam used to simulate connection outcomes without requiring a live NATS
// server.
void ForceInitializationSuccessForTests();
void ForceInitializationFailureForTests();
void ResetInitializationBehaviorForTests();

} // namespace testing

struct Message {
  std::string subject;
  std::string payload;
};

class JetStreamPublisher {
public:
  // Connects and ensures the named stream exists with the given subjects.
  JetStreamPublisher(std::string url, std::string stream_name,
                     std::vector<std::string> subjects);
  ~JetStreamPublisher();
  bool Publish(const std::string &subject, const void *data, size_t size);

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
  std::string url_;
};

class JetStreamConsumer {
public:
  using Handler = std::function<void(const Message &)>;

  JetStreamConsumer(std::string url, std::string stream,
                    std::vector<std::string> subjects);
  ~JetStreamConsumer();
  void Poll(const Handler &handler);

private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
  std::string url_;
  std::string stream_;
  std::vector<std::string> subjects_;
};

} // namespace jetstream_client
