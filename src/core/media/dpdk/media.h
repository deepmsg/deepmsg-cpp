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

/// Install the configured media into a driver context.
///
/// A build without DPDK refuses `deepmsg.dpdk.enabled=true` with a message that
/// names the build option; with the media disabled this is a successful no-op.
/// The bindings that put the stack in front of the driver's channels are P4b;
/// until they land an enabled media is refused rather than half-installed.
[[nodiscard]] std::expected<Media, Error> install_if_configured(aeron_driver_context_t* context,
                                                                const PropertySource& properties) noexcept;

}  // namespace axc::core::media::dpdk
