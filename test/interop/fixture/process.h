#pragma once

#include <chrono>
#include <cstddef>
#include <expected>
#include <string>
#include <string_view>
#include <vector>

#include "core/error/error.h"

namespace axc::interop
{

using core::Error;

struct ProcessResult
{
    int exit_code{-1};
    bool timed_out{false};
    std::string output;  // stdout and stderr combined

    [[nodiscard]] bool exited_normally() const noexcept
    {
        return !timed_out && exit_code >= 0;
    }
};

struct ProcessOptions
{
    std::vector<std::string> arguments;
    std::vector<std::string> environment;  // KEY=VALUE applied in the child
    std::chrono::milliseconds timeout{std::chrono::seconds(30)};
    std::size_t output_limit{1U << 20};
    /// Written to the child's stdin and then closed. Empty ⇒ the child sees
    /// EOF immediately (the archive tool's confirmation prompts use this).
    std::string input;
};

/// Runs a process to completion with a timeout, capturing stdout+stderr.
[[nodiscard]] std::expected<ProcessResult, Error> run_process(std::string_view executable,
                                                              const ProcessOptions& options) noexcept;

/// A long-running child (media driver, archive, cluster node).
class ChildProcess
{
public:
    ChildProcess() noexcept = default;
    ChildProcess(ChildProcess&& other) noexcept;
    ChildProcess& operator=(ChildProcess&& other) noexcept;
    ~ChildProcess();

    ChildProcess(const ChildProcess&) = delete;
    ChildProcess& operator=(const ChildProcess&) = delete;

    static std::expected<ChildProcess, Error> spawn(std::string_view executable,
                                                    const std::vector<std::string>& arguments,
                                                    const std::vector<std::string>& environment = {}) noexcept;

    [[nodiscard]] bool running() const noexcept;
    void terminate(std::chrono::milliseconds grace = std::chrono::seconds(5)) noexcept;

    [[nodiscard]] int exit_code() const noexcept
    {
        return exit_code_;
    }

    [[nodiscard]] int pid() const noexcept
    {
        return pid_;
    }

private:
    mutable int pid_{-1};
    mutable int exit_code_{-1};
};

}  // namespace axc::interop
