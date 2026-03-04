#include "jetstream_client/jetstream_client.h"

#include <functional>
#include <string>
#include <vector>

class LoaderConsumer {
 public:
  explicit LoaderConsumer(std::string nats_url)
      : consumer_(std::move(nats_url), "OTEL_TELEMETRY", {"otel.traces", "otel.metrics", "otel.logs"}) {}

  void Poll(const jetstream_client::JetStreamConsumer::Handler& handler) { consumer_.Poll(handler); }

 private:
  jetstream_client::JetStreamConsumer consumer_;
};
