#pragma once

#include <expected>
#include <string>
#include <string_view>
#include <vector>

#include "core/error/error.h"
#include "fixture/process.h"

namespace axc::interop
{

using core::Error;

/// A C media driver (`aeronmd`) on a caller-owned directory.
///
/// The destructor stops the driver but **never deletes the directory**: it is
/// shared with the client/archive/cluster peers (the Rust track lost a day to a
/// fixture that cleaned it up under the other processes).
class CDriver
{
public:
    CDriver() = default;
    CDriver(CDriver&&) noexcept = default;
    CDriver& operator=(CDriver&&) noexcept = default;
    ~CDriver() = default;

    CDriver(const CDriver&) = delete;
    CDriver& operator=(const CDriver&) = delete;

    static std::expected<CDriver, Error> start(std::string_view aeronmd_path, std::string_view aeron_dir) noexcept;

    /// Same, with `KEY=VALUE` entries applied to the driver's environment
    /// (e.g. `AERON_SPIES_SIMULATE_CONNECTION=true`).
    static std::expected<CDriver, Error> start(std::string_view aeronmd_path, std::string_view aeron_dir,
                                               const std::vector<std::string>& environment) noexcept;

    [[nodiscard]] const std::string& aeron_dir() const noexcept
    {
        return aeron_dir_;
    }

    [[nodiscard]] bool running() const noexcept
    {
        return process_.running();
    }

    [[nodiscard]] int pid() const noexcept
    {
        return process_.pid();
    }

    void stop() noexcept
    {
        process_.terminate();
    }

private:
    ChildProcess process_;
    std::string aeron_dir_;
};

}  // namespace axc::interop
