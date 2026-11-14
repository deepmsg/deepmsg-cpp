// The DPDK media's counter type ids.
//
// The ids are the reference's (`aeron-client/src/main/c/aeron_counters.h:155-
// 173`); 80/81 are unused. The labels are this project's own, because the
// reference's DPDK media is Premium and closed (plan §4.9).

#pragma once

#include <cstdint>
#include <string_view>

namespace axc::core::media::dpdk::counters
{

inline constexpr std::int32_t PORT_INFO = 75;
inline constexpr std::int32_t TRANSPORT = 76;
inline constexpr std::int32_t NOBUFS = 77;
inline constexpr std::int32_t TX_EAGAIN = 78;
inline constexpr std::int32_t ERROR = 79;
inline constexpr std::int32_t PKTS = 82;
inline constexpr std::int32_t BYTES = 83;
inline constexpr std::int32_t MISSED_PACKETS = 84;
inline constexpr std::int32_t ARP_MISS = 85;
inline constexpr std::int32_t RX_SENDER_DISCARD = 86;
inline constexpr std::int32_t POLLER = 87;
inline constexpr std::int32_t QUEUE_DROP_COUNT = 88;
inline constexpr std::int32_t CHECKSUM_FAILURE = 89;
inline constexpr std::int32_t FRAGMENTED_PACKETS = 90;
inline constexpr std::int32_t MEMPOOL_AVAILABLE = 91;
inline constexpr std::int32_t EXTENDED_STATS = 92;
inline constexpr std::int32_t RX_UNSUPPORTED_ETHERNET_TYPE = 93;
inline constexpr std::int32_t RX_UNSUPPORTED_PROTOCOL = 94;
inline constexpr std::int32_t RX_RECEIVER_DISCARD = 95;

/// The label this build gives a type id.
[[nodiscard]] constexpr std::string_view label(std::int32_t type_id) noexcept
{
    switch (type_id)
    {
        case PORT_INFO:
            return "dpdk port";
        case TRANSPORT:
            return "dpdk transport";
        case NOBUFS:
            return "dpdk nobufs";
        case TX_EAGAIN:
            return "dpdk tx eagain";
        case ERROR:
            return "dpdk error";
        case PKTS:
            return "dpdk packets";
        case BYTES:
            return "dpdk bytes";
        case MISSED_PACKETS:
            return "dpdk missed packets";
        case ARP_MISS:
            return "dpdk arp miss";
        case RX_SENDER_DISCARD:
            return "dpdk rx sender discard";
        case POLLER:
            return "dpdk poller";
        case QUEUE_DROP_COUNT:
            return "dpdk queue drop";
        case CHECKSUM_FAILURE:
            return "dpdk checksum failure";
        case FRAGMENTED_PACKETS:
            return "dpdk fragmented packets";
        case MEMPOOL_AVAILABLE:
            return "dpdk mempool available";
        case EXTENDED_STATS:
            return "dpdk extended stats";
        case RX_UNSUPPORTED_ETHERNET_TYPE:
            return "dpdk rx unsupported ethernet type";
        case RX_UNSUPPORTED_PROTOCOL:
            return "dpdk rx unsupported protocol";
        case RX_RECEIVER_DISCARD:
            return "dpdk rx receiver discard";
        default:
            return "dpdk counter";
    }
}

}  // namespace axc::core::media::dpdk::counters
