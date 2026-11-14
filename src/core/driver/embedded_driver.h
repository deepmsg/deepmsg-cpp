// The one place a process starts the C media driver.
//
// Every binary that embeds the driver (the archive server's SHARED shape, the
// cluster node's `--embedded-driver` shape, and the media driver itself) uses
// this so that the order — context, media install, driver init, start — exists
// once and cannot drift between them.

#pragma once

#include <expected>

#include "core/config/property_source.h"
#include "core/error/error.h"
#include "core/media/dpdk/media.h"

namespace axc::core::driver
{

/// `aeron_driver_termination_hook_func_t`: the driver asks the process to stop.
using TerminationHook = void (*)(void* state);

class EmbeddedDriver
{
public:
    /// `aeron_driver_context_init` -> media install -> `aeron_driver_init` ->
    /// `aeron_driver_start`. On failure nothing is left running and the
    /// driver directory is not created (the C driver creates it in
    /// `aeron_driver_init`, `aeron_driver.c:917`).
    [[nodiscard]] static std::expected<EmbeddedDriver, Error> create(const PropertySource& properties,
                                                                     TerminationHook hook, void* state) noexcept;

    ~EmbeddedDriver();

    EmbeddedDriver(EmbeddedDriver&& other) noexcept;
    EmbeddedDriver& operator=(EmbeddedDriver&& other) noexcept;

    EmbeddedDriver(const EmbeddedDriver&) = delete;
    EmbeddedDriver& operator=(const EmbeddedDriver&) = delete;

    [[nodiscard]] aeron_driver_t* driver() const noexcept
    {
        return driver_;
    }

    /// `aeron_driver_close` -> media close -> `aeron_driver_context_close`.
    /// Idempotent; the destructor calls it.
    [[nodiscard]] std::expected<void, Error> close() noexcept;

private:
    EmbeddedDriver(aeron_driver_context_t* context, aeron_driver_t* driver, media::dpdk::Media media) noexcept;

    aeron_driver_context_t* context_{nullptr};
    aeron_driver_t* driver_{nullptr};
    media::dpdk::Media media_{};
};

}  // namespace axc::core::driver
