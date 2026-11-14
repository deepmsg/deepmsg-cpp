#include "core/media/dpdk/stack/arp.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <utility>

namespace axc::core::media::dpdk::arp
{

std::optional<std::size_t> write_request(std::span<std::uint8_t> buffer, const stack::StackParams& params,
                                         std::uint32_t address) noexcept
{
    const frame::EthernetHeader ethernet{frame::BROADCAST, params.mac, frame::ETHERNET_TYPE_ARP};
    const frame::ArpPacket packet{frame::ARP_REQUEST, params.mac, params.address, std::array<std::uint8_t, 6>{},
                                  address};
    return frame::write_arp(buffer, ethernet, packet);
}

std::pair<frame::EthernetHeader, frame::ArpPacket> reply_to(const stack::StackParams& params,
                                                            const frame::ArpPacket& request) noexcept
{
    const frame::EthernetHeader ethernet{request.sender_hardware, params.mac, frame::ETHERNET_TYPE_ARP};
    const frame::ArpPacket packet{frame::ARP_REPLY, params.mac, params.address, request.sender_hardware,
                                  request.sender_protocol};
    return {ethernet, packet};
}

bool asks_about(const frame::ArpPacket& packet, std::uint32_t address) noexcept
{
    return packet.operation == frame::ARP_REQUEST && packet.target_protocol == address;
}

}  // namespace axc::core::media::dpdk::arp
