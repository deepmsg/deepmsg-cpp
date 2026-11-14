#include "core/media/dpdk/stack/demux.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <utility>
#include <variant>

#include "core/media/dpdk/stack/arp.h"

namespace axc::core::media::dpdk::demux
{

namespace
{

/// Aeron's frame type lives at offset 6, little-endian
/// (`crates/driver/src/protocol.rs`).
constexpr std::size_t FRAME_TYPE_OFFSET = 6;
constexpr std::int16_t FRAME_TYPE_PAD = 0x00;
constexpr std::int16_t FRAME_TYPE_DATA = 0x01;
constexpr std::int16_t FRAME_TYPE_SETUP = 0x05;

/// Whether an Aeron frame carries data, which is what says its datagram was
/// going to the receiver's side rather than the sender's. A payload too short
/// to hold a frame header counts as data.
[[nodiscard]] bool is_data_plane(std::span<const std::uint8_t> payload) noexcept
{
    if (payload.size() < FRAME_TYPE_OFFSET + 2)
    {
        return true;
    }
    const auto raw = static_cast<std::uint16_t>(payload[FRAME_TYPE_OFFSET] |
                                                (static_cast<std::uint16_t>(payload[FRAME_TYPE_OFFSET + 1]) << 8));
    const auto frame_type = static_cast<std::int16_t>(raw);
    return frame_type == FRAME_TYPE_DATA || frame_type == FRAME_TYPE_PAD || frame_type == FRAME_TYPE_SETUP;
}

}  // namespace

packet_io::Disposition demux(stack::Stack& target, const packet_io::RxFrame& frame, std::vector<Deferred>& deferred)
{
    const auto ethernet = frame::read_ethernet_header(frame.bytes);
    if (!ethernet || !ethernet->is_for(target.params().mac))
    {
        // Not this port's, and dropped silently: a port that is not in
        // promiscuous mode is never handed one.
        return packet_io::Disposition::consumed();
    }

    const auto received = frame::read(frame.bytes, frame.checksums);
    if (!received)
    {
        target.count_a_discard(received.error());
        return packet_io::Disposition::consumed();
    }

    if (const auto* packet = std::get_if<frame::ArpPacket>(&*received))
    {
        // Gleaning: any frame from an address on this link carries that
        // address's MAC, and learning it lets an answer go straight back.
        target.glean(packet->sender_protocol, packet->sender_hardware);

        if (arp::asks_about(*packet, target.params().address))
        {
            const auto [reply_ethernet, reply_packet] = arp::reply_to(target.params(), *packet);
            deferred.push_back(Deferred{Deferred::Kind::arp_reply, reply_ethernet, reply_packet, {}, 0, 0, {}});
        }
        return packet_io::Disposition::consumed();
    }

    if (const auto* echo = std::get_if<frame::EchoReceived>(&*received))
    {
        if (echo->echo.echo_type != frame::ICMP_ECHO_REQUEST)
        {
            return packet_io::Disposition::consumed();
        }
        const auto& params = target.params();
        Deferred reply;
        reply.kind = Deferred::Kind::icmp_reply;
        reply.ipv4 = frame::Ipv4Write{ethernet->source, params.mac, params.address, echo->ipv4.source,
                                      target.next_identification()};
        reply.identifier = echo->echo.identifier;
        reply.sequence = echo->echo.sequence;
        reply.payload.assign(echo->echo.payload.begin(), echo->echo.payload.end());
        deferred.push_back(std::move(reply));
        return packet_io::Disposition::consumed();
    }

    const auto* udp = std::get_if<frame::UdpReceived>(&*received);
    if (nullptr == udp)
    {
        return packet_io::Disposition::consumed();
    }

    target.glean(udp->ipv4.source, ethernet->source);
    const ports::Ipv4Endpoint source{udp->ipv4.source, udp->datagram.source_port};
    const auto binding = target.lookup(udp->ipv4.destination, udp->datagram.destination_port, source);
    if (binding)
    {
        (void)target.deliver(*binding, source, udp->datagram.payload);
        return packet_io::Disposition::consumed();
    }

    if (is_data_plane(udp->datagram.payload))
    {
        target.counters().rx_receiver_discard.fetch_add(1, std::memory_order_relaxed);
    }
    else
    {
        target.counters().rx_sender_discard.fetch_add(1, std::memory_order_relaxed);
    }
    return packet_io::Disposition::consumed();
}

}  // namespace axc::core::media::dpdk::demux
