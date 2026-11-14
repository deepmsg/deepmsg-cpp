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

/// The media a driver context holds. Empty until P3; move-only because what it
/// will own (EAL, port, pool) cannot be shared.
class Media
{
public:
    Media() = default;
    explicit Media(bool enabled) noexcept : enabled_{enabled} {}
    Media(Media&& other) noexcept = default;
    Media& operator=(Media&& other) noexcept = default;
    ~Media();

    Media(const Media&) = delete;
    Media& operator=(const Media&) = delete;

    [[nodiscard]] bool enabled() const noexcept
    {
        return enabled_;
    }

    /// Stop and release what the media owns. Idempotent; the destructor calls it.
    [[nodiscard]] std::expected<void, Error> close() noexcept;

private:
    bool enabled_{false};
    bool closed_{false};
};

/// Install the configured media into a driver context.
///
/// A build without DPDK refuses `deepmsg.dpdk.enabled=true` with a message that
/// names the build option; a DPDK build reports that the implementation lands
/// in P3. With the media disabled this is a successful no-op.
[[nodiscard]] std::expected<Media, Error> install_if_configured(aeron_driver_context_t* context,
                                                                const PropertySource& properties) noexcept;

}  // namespace axc::core::media::dpdk
