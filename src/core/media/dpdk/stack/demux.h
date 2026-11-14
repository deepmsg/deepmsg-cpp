// One frame -> one transport's ring, one answer, or one counter.
//
// Everything a received frame can be is decided here. A frame the stack cannot
// place is one the reference's DPDK transport cannot place either, and lands on
// the same counter. Transcribed from the Rust track's `media/userspace/demux.rs`.

#pragma once

#include <cstdint>
#include <vector>

#include "core/media/dpdk/stack/frame.h"
#include "core/media/dpdk/stack/packet_io.h"
#include "core/media/dpdk/stack/stack.h"

namespace axc::core::media::dpdk::demux
{

/// An answer the receiver owes, built after the receive burst rather than
/// inside the port's callback.
struct Deferred
{
    enum class Kind
    {
        arp_reply,
        icmp_reply,
    };

    Kind kind{Kind::arp_reply};
    frame::EthernetHeader ethernet{};
    frame::ArpPacket arp{};
    frame::Ipv4Write ipv4{};
    std::uint16_t identifier{0};
    std::uint16_t sequence{0};
    std::vector<std::uint8_t> payload;
};

/// Handle one received frame; answers what the port may do with it.
[[nodiscard]] packet_io::Disposition demux(stack::Stack& stack, const packet_io::RxFrame& frame,
                                           std::vector<Deferred>& deferred);

}  // namespace axc::core::media::dpdk::demux
