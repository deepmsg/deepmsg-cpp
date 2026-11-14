// ARP: the frames, and the two decisions that go with them.
//
// The neighbour table holds what is known and when it was learned; this module
// is the wire side of it — what a request looks like, what a reply to one
// looks like, and which arriving packets are worth learning from. Answering is
// not optional: with the port bound to a userspace stack there is no kernel to
// answer on its behalf.

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <utility>

#include "core/media/dpdk/stack/frame.h"
#include "core/media/dpdk/stack/stack_params.h"

namespace axc::core::media::dpdk::arp
{

/// Write the request that asks who holds `address`, addressed to the broadcast
/// MAC.
[[nodiscard]] std::optional<std::size_t> write_request(std::span<std::uint8_t> buffer, const stack::StackParams& params,
                                                       std::uint32_t address) noexcept;

/// The reply to a request this port is the target of: the two ends swap.
[[nodiscard]] std::pair<frame::EthernetHeader, frame::ArpPacket> reply_to(const stack::StackParams& params,
                                                                          const frame::ArpPacket& request) noexcept;

/// Whether a packet is a request for `address`, which is what this stack has
/// to answer.
[[nodiscard]] bool asks_about(const frame::ArpPacket& packet, std::uint32_t address) noexcept;

}  // namespace axc::core::media::dpdk::arp
