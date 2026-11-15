// The DPDK media, from the launcher's side.
//
// Nothing here includes a DPDK header in this slice: the media is configured by
// `deepmsg.dpdk.*` properties, validated, and installed into a media driver
// context. P1 ends at the seam: `install_if_configured` refuses an enabled
// media in a build without DPDK and reports that the implementation is not
// there yet when it is enabled in a DPDK build. The userspace stack and the
// port arrive in P2/P3.

#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "core/config/property_source.h"
#include "core/error/error.h"

extern "C"
{
#include "aeronmd.h"
}

namespace axc::core::media::dpdk
{

namespace stack
{
class Stack;
}

/// An IPv4 address in host byte order with its prefix length.
struct Ipv4Prefix
{
    std::uint32_t address{0};
    std::uint8_t prefix_length{0};
};

/// The `deepmsg.dpdk.*` settings, after validation.
struct Settings
{
    bool enabled{false};
    std::string eal_args;
    std::optional<std::string> port;
    std::optional<Ipv4Prefix> ipv4_address;
    std::optional<std::uint32_t> ipv4_gateway;  // host byte order
    std::string static_neighbours;
    std::uint16_t rx_descriptors{1024};
    std::uint16_t tx_descriptors{1024};
    std::optional<std::uint32_t> mempool_size;
    std::uint32_t mempool_cache{256};
    std::uint16_t rx_burst{32};
    std::uint32_t sender_ring{1024};
    std::uint16_t ephemeral_low{32768};
    std::uint16_t ephemeral_high{60999};
    std::uint32_t arp_timeout_ms{300000};
    std::uint32_t arp_retry_ms{1000};
};

/// Read and validate `deepmsg.dpdk.*`.
///
/// An unknown `deepmsg.dpdk.*` name is an error rather than a silent ignore, the
/// way the reference refuses a configuration name it does not serve.
[[nodiscard]] std::expected<Settings, Error> parse_settings(const PropertySource& properties) noexcept;

/// The EAL command line this media starts with: `argv[0]`, the operator's
/// `deepmsg.dpdk.eal.args`, and the arguments the media adds for itself.
///
/// The media owns two arguments because they are properties of the process
/// rather than of the operator's deployment: `--lcores=0@(<this process's
/// cpuset>)` keeps EAL inside the CPUs the process was given, and
/// `--no-telemetry` keeps it from opening a socket nobody reads. An operator
/// argument that conflicts with either is an error rather than an override
/// (`analysis/cpp/plan-dpdk-media.md` §4.7).
[[nodiscard]] std::expected<std::vector<std::string>, Error> eal_arguments(const Settings& settings) noexcept;

/// The media a driver context holds: the EAL, the pool, the port and the
/// userspace stack. Move-only because none of those can be shared.
class Media
{
public:
    Media() noexcept;
    Media(Media&& other) noexcept;
    Media& operator=(Media&& other) noexcept;
    ~Media();

    Media(const Media&) = delete;
    Media& operator=(const Media&) = delete;

    [[nodiscard]] bool enabled() const noexcept
    {
        return nullptr != impl_;
    }

    /// The userspace stack, when the media is open.
    [[nodiscard]] stack::Stack* stack() const noexcept;

    /// The port and the pool, when the media is open.
    [[nodiscard]] class Port* port() const noexcept;
    [[nodiscard]] class Mempool* pool() const noexcept;

    /// Stop and release what the media owns. Idempotent; the destructor calls it.
    [[nodiscard]] std::expected<void, Error> close() noexcept;

private:
    friend std::expected<Media, Error> open_media(const Settings& settings) noexcept;

    struct Impl;
    std::unique_ptr<Impl> impl_;
    bool eal_started_{false};
    bool closed_{false};
};

/// Open the media the settings describe (EAL, port, pool and the stack) without
/// installing anything into a driver context. `install_if_configured` uses it
/// once the bindings exist; tests use it directly.
[[nodiscard]] std::expected<Media, Error> open_media(const Settings& settings) noexcept;

/// Parse and open in one step.
[[nodiscard]] std::expected<Media, Error> open_media(const PropertySource& properties) noexcept;

/// Whether an idle strategy lets frames pile up in the NIC ring.
///
/// This media's reader is the driver's receiver thread and nobody else drains
/// the port, so a strategy that is not a busy spin turns the ring into the
/// queue: `eagain` and 84 (missed) are what the operator sees, without a hint
/// as to why. That includes the C driver's default `backoff`: it spins only
/// until it parks, and the park grows to 1 ms (`aeron_agent.h:45-46`) -- a
/// benchmark of the driver-level round trip measured 1.07 ms with the default
/// and 29.7 us with `spin`. A strategy outside `spin`/`noop` earns a line on
/// stdout at install time, not a refusal (design §4.12).
///
/// The answer comes from the function the context loaded, not from its
/// `..._idle_strategy_name`: `aeron_driver_context_init` loads the function
/// from `AERON_*_IDLE_STRATEGY` and leaves the name at its `backoff` default
/// (`aeron_driver_context.c:1204-1211`), so the name is not a reading of what
/// the thread will do.
[[nodiscard]] bool idle_strategy_can_starve_the_ring(void (*strategy)(void* state, int work_count)) noexcept;

/// The reference's media-selection name, honoured as an alias (P5).
///
/// `aeron.udp.channel.transport.bindings.media=dpdk` is what an operator of the
/// reference's closed DPDK media would write. The C driver resolves that name
/// during `aeron_driver_context_init` (`aeron_driver_context.c:1288`), before a
/// media can be installed, and knows only `default`; `dpdk` would be a
/// `dlsym(RTLD_DEFAULT, "dpdk")` of a symbol we do not export
/// (`aeron_symbol_table.c:59-95`). So the launcher answers `default` in the
/// driver's environment and turns `deepmsg.dpdk.enabled` on here, before the
/// context is built. `aeron.print.configuration` therefore shows `default`; that
/// is the documented deviation.
///
/// A conflicting `deepmsg.dpdk.enabled=false` is refused rather than silently
/// overridden. Any other name is left alone, so the driver refuses it exactly
/// as the reference would.
[[nodiscard]] std::expected<void, Error> apply_reference_media_alias(PropertySource& properties) noexcept;

/// Install the configured media into a driver context.
///
/// A build without DPDK refuses `deepmsg.dpdk.enabled=true` with a message that
/// names the build option; with the media disabled this is a successful no-op.
/// The bindings that put the stack in front of the driver's channels are P4b;
/// until they land an enabled media is refused rather than half-installed.
[[nodiscard]] std::expected<Media, Error> install_if_configured(aeron_driver_context_t* context,
                                                                const PropertySource& properties) noexcept;

}  // namespace axc::core::media::dpdk
