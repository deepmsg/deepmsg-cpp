#pragma once

#include <chrono>
#include <expected>
#include <string>
#include <vector>

#include "fixture/process.h"

namespace axc::interop
{

using core::Error;

/// Runs `java ...` with a timeout. Exit code 143 (SIGTERM) is the normal way a
/// JVM peer is stopped, not a failure.
class JavaProcess
{
public:
    static constexpr int SIGTERM_EXIT_CODE = 143;

    explicit JavaProcess(std::string executable = default_executable());

    [[nodiscard]] std::expected<ProcessResult, Error> run(
        const std::vector<std::string>& arguments,
        std::chrono::milliseconds timeout = std::chrono::seconds(30)) const noexcept;

    [[nodiscard]] const std::string& executable() const noexcept
    {
        return executable_;
    }

    /// `AXC_REAL_JAVA` if set, otherwise `java` from PATH.
    [[nodiscard]] static std::string default_executable();

private:
    std::string executable_;
};

}  // namespace axc::interop
