#pragma once

#include <csignal>
#include <chrono>
#include <functional>

int RunServerUntilSignalOrFailure(
    const sigset_t &signal_set, const std::function<void()> &run_server,
    const std::function<void()> &shutdown_server,
    std::chrono::milliseconds poll_interval = std::chrono::milliseconds(100));
