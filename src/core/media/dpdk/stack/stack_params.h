// The parameters every transport on one machine shares.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>
#include <vector>

namespace axc::core::media::dpdk::stack
{

using Mac = std::array<std::uint8_t, 6>;

/// What this port calls itself and how it behaves.
struct StackParams
{
    /// This port's IPv4 address, host byte order.
    std::uint32_t address{0};
    /// How many leading bits are the subnet; decides link vs gateway.
    std::uint8_t prefix_length{0};
    /// This port's MAC.
    Mac mac{};
    /// The next hop for destinations off this link, if one was configured.
    std::optional<std::uint32_t> gateway;
    /// The largest frame this port will send or accept.
    std::size_t mtu{1408};
    /// How many bytes of datagrams the transport believes it can hold.
    std::size_t receive_window{1u << 20};
    /// How many frames one pass takes off the port.
    std::size_t rx_burst{32};
    std::uint16_t ephemeral_low{32768};
    std::uint16_t ephemeral_high{60999};
    std::int64_t arp_interval_ns{1000LL * 1000 * 1000};
    std::int64_t arp_timeout_ns{300LL * 1000 * 1000 * 1000};
    /// Next hops that were configured rather than learned.
    std::vector<std::pair<std::uint32_t, Mac>> static_neighbours;

    /// The largest datagram this port can carry: its MTU less the IPv4 and UDP
    /// headers.
    [[nodiscard]] constexpr std::size_t largest_datagram() const noexcept
    {
        constexpr std::size_t overhead = 20 + 8;
        return mtu > overhead ? mtu - overhead : 0;
    }

    /// How many frames one transport's ring holds.
    [[nodiscard]] std::size_t ring_capacity() const noexcept
    {
        const std::size_t per_window = (receive_window + mtu - 1) / (mtu > 0 ? mtu : 1);
        std::size_t capacity = 2;
        const std::size_t wanted = per_window * 2;
        while (capacity < wanted)
        {
            capacity <<= 1;
        }
        return capacity < 2 ? 2 : capacity;
    }
};

}  // namespace axc::core::media::dpdk::stack
