#include "jetstream_client/jetstream_client.h"

#include <algorithm>
#include <iostream>
#include <stdexcept>

#include "telemetry/tracer.h"
#include <natscpp/natscpp.hpp>

namespace jetstream_client {

namespace {

enum class InitBehaviorOverride {
  kDefault,
  kForceSuccess,
  kForceFailure,
};

InitBehaviorOverride g_init_behavior_override = InitBehaviorOverride::kDefault;

// Creates the stream if it doesn't already exist.
void ensure_stream(natscpp::jetstream &js, const std::string &stream_name,
                   const std::vector<std::string> &subjects) {
  try {
    js.get_stream_info(stream_name);
  } catch (const natscpp::nats_error &e) {
    if (e.status() == NATS_NOT_FOUND) {
      js.create_stream({stream_name, subjects});
      std::clog << "JetStream stream created name=" << stream_name << '\n';
    } else {
      throw;
    }
  }
}

} // namespace

// ---------------------------------------------------------------------------
// JetStreamPublisher
// ---------------------------------------------------------------------------

struct JetStreamPublisher::Impl {
  natscpp::connection conn;
  natscpp::jetstream js;

  Impl(std::string_view url, const std::string &stream_name,
       const std::vector<std::string> &subjects)
      : conn(natscpp::connection::connect_to(url)), js(conn) {
    ensure_stream(js, stream_name, subjects);
  }
};

JetStreamPublisher::JetStreamPublisher(std::string url, std::string stream_name,
                                       std::vector<std::string> subjects)
    : url_(std::move(url)) {
  if (g_init_behavior_override == InitBehaviorOverride::kForceFailure) {
    throw std::runtime_error(
        "JetStream publisher forced init failure for tests");
  }
  if (g_init_behavior_override == InitBehaviorOverride::kForceSuccess) {
    return;
  }
  impl_ = std::make_unique<Impl>(url_, stream_name, subjects);
}

JetStreamPublisher::~JetStreamPublisher() = default;

bool JetStreamPublisher::Publish(const std::string &subject, const void *data,
                                 size_t size) {
  if (!impl_) {
    std::clog << "JetStream publisher not connected, dropping subject="
              << subject << '\n';
    return false;
  }
  auto span = telemetry::StartSpan("jetstream_publish");
  try {
    (void)impl_->js.publish(
        subject, std::string_view(static_cast<const char *>(data), size));
    return true;
  } catch (const natscpp::nats_error &e) {
    std::clog << "JetStream publish error subject=" << subject << ": "
              << e.what() << '\n';
    return false;
  }
}

// ---------------------------------------------------------------------------
// JetStreamConsumer
// ---------------------------------------------------------------------------

struct JetStreamConsumer::Impl {
  natscpp::connection conn;
  natscpp::jetstream js;
  std::vector<natscpp::js_pull_consumer> consumers;
  std::vector<std::string> subjects;

  Impl(std::string_view url, const std::string &stream_name,
       const std::vector<std::string> &subs)
      : conn(natscpp::connection::connect_to(url)), js(conn) {
    ensure_stream(js, stream_name, subs);
    for (const auto &subject : subs) {
      // Derive a durable consumer name from the subject (dots -> dashes).
      std::string durable = subject;
      std::replace(durable.begin(), durable.end(), '.', '-');
      consumers.push_back(js.pull_subscribe(subject, durable));
      subjects.push_back(subject);
    }
  }
};

JetStreamConsumer::JetStreamConsumer(std::string url, std::string stream,
                                     std::vector<std::string> subjects)
    : url_(std::move(url)), stream_(std::move(stream)),
      subjects_(std::move(subjects)) {
  if (g_init_behavior_override == InitBehaviorOverride::kForceFailure) {
    throw std::runtime_error(
        "JetStream consumer forced init failure for tests");
  }
  if (g_init_behavior_override == InitBehaviorOverride::kForceSuccess) {
    return;
  }
  impl_ = std::make_unique<Impl>(url_, stream_, subjects_);
}

JetStreamConsumer::~JetStreamConsumer() = default;

void JetStreamConsumer::Poll(const Handler &handler) {
  if (!impl_) {
    std::clog << "JetStream consumer not connected, skipping poll\n";
    return;
  }
  auto span = telemetry::StartSpan("jetstream_consume");
  for (size_t i = 0; i < impl_->consumers.size(); ++i) {
    // Drain all available messages for this subject.
    while (true) {
      try {
        auto msg = impl_->consumers[i].next(std::chrono::milliseconds(100));
        handler(Message{std::string(msg.subject()), std::string(msg.data())});
        msg.ack();
      } catch (const natscpp::nats_error &e) {
        if (e.status() != NATS_TIMEOUT) {
          std::clog << "JetStream consume error subject=" << impl_->subjects[i]
                    << ": " << e.what() << '\n';
        }
        break;
      }
    }
  }
}

namespace testing {

void ForceInitializationSuccessForTests() {
  g_init_behavior_override = InitBehaviorOverride::kForceSuccess;
}

void ForceInitializationFailureForTests() {
  g_init_behavior_override = InitBehaviorOverride::kForceFailure;
}

void ResetInitializationBehaviorForTests() {
  g_init_behavior_override = InitBehaviorOverride::kDefault;
}

} // namespace testing

} // namespace jetstream_client
