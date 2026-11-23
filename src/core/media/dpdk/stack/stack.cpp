#include "core/media/dpdk/stack/stack.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "core/error/error.h"
#include "core/media/dpdk/stack/arp.h"
#include "core/media/dpdk/stack/demux.h"

namespace axc::core::media::dpdk::stack
{

namespace
{

struct DemuxContext
{
    Stack* stack;
    std::vector<demux::Deferred>* deferred;
};

packet_io::Disposition demux_callback(void* context, const packet_io::RxFrame& frame)
{
    auto& demux_context = *static_cast<DemuxContext*>(context);
    return demux::demux(*demux_context.stack, frame, *demux_context.deferred);
}

struct CopyFill
{
    std::span<const std::uint8_t> bytes;
};

std::pair<std::size_t, frame::TxOffloads> copy_fill(void* context, std::span<std::uint8_t> buffer)
{
    const auto& fill = *static_cast<CopyFill*>(context);
    if (fill.bytes.size() > buffer.size())
    {
        return {fill.bytes.size(), frame::TxOffloads::none()};
    }
    std::copy(fill.bytes.begin(), fill.bytes.end(), buffer.begin());
    return {fill.bytes.size(), frame::TxOffloads::none()};
}

struct UdpFill
{
    /// The port this datagram actually leaves by: the sender's or the
    /// receiver's, and the two may not always answer alike (2026-11-23, the
    /// checksum question a cloud NIC asks is the port's, not the stack's).
    packet_io::TxPort* port;
    frame::Ipv4Write ipv4;
    ports::Binding binding;
    ports::Ipv4Endpoint destination;
    std::span<const std::span<const std::uint8_t>> parts;
    std::size_t total;
};

std::pair<std::size_t, frame::TxOffloads> udp_fill(void* context, std::span<std::uint8_t> buffer)
{
    const auto& udp = *static_cast<UdpFill*>(context);
    if (buffer.size() < frame::IPV4_UDP_HEADER_LENGTH + udp.total)
    {
        return {0, frame::TxOffloads::none()};
    }
    std::size_t offset = frame::IPV4_UDP_HEADER_LENGTH;
    for (const auto part : udp.parts)
    {
        std::copy(part.begin(), part.end(), buffer.begin() + static_cast<std::ptrdiff_t>(offset));
        offset += part.size();
    }
    const frame::UdpWrite write{udp.ipv4, udp.binding.port, udp.destination.port, udp.total};
    const auto written = frame::write_ipv4_udp(buffer, write, udp.port->checksums());
    if (!written)
    {
        return {0, frame::TxOffloads::none()};
    }
    return *written;
}

/// The stack's clock: steady nanoseconds unless a test pinned one.
[[nodiscard]] std::int64_t steady_now_ns() noexcept
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

}  // namespace

Stack::Stack(StackParams params, std::unique_ptr<packet_io::RxPort> rx, std::unique_ptr<packet_io::TxPort> sender_tx,
             std::unique_ptr<packet_io::TxPort> receiver_tx)
    : params_{std::move(params)},
      ports_{ports::SLOT_CAPACITY, params_.ephemeral_low, params_.ephemeral_high},
      neighbours_{neighbour::NEIGHBOUR_CAPACITY, params_.arp_timeout_ns, params_.arp_interval_ns},
      rings_{std::make_unique<Ring[]>(ports::SLOT_CAPACITY)},
      rx_{std::move(rx)},
      sender_tx_{std::move(sender_tx)},
      receiver_tx_{std::move(receiver_tx)}
{
    // A frame is at most the port's MTU plus the Ethernet header that MTU
    // does not count.
    answer_buffer_.resize(params_.mtu + frame::ETHERNET_HEADER_LENGTH);
    for (std::size_t index = 0; index < ports::SLOT_CAPACITY; ++index)
    {
        rings_[index].capacity = params_.ring_capacity();
    }
    for (const auto& [address, hardware] : params_.static_neighbours)
    {
        neighbours_.insert_fixed(address, hardware);
    }
}

Stack::~Stack() = default;

std::expected<ports::Binding, ports::BindFailure> Stack::bind(std::optional<std::uint32_t> address, std::uint16_t port,
                                                              std::optional<ports::Ipv4Endpoint> peer,
                                                              ports::Ipv4Endpoint failure_at)
{
    // A parked frame holds a buffer until its transport drains it, so a ring
    // that the pool cannot cover is refused here rather than turning into
    // 88-shaped drops later.
    const std::size_t capacity = params_.ring_capacity();
    if (ring_mbufs_ + capacity > params_.ring_mbuf_budget)
    {
        return std::unexpected(ports::BindFailure{ENOBUFS, -1, failure_at});
    }

    auto binding = ports_.bind(address, port, peer, failure_at);
    if (binding)
    {
        reset_ring(*binding);
        ring_mbufs_ += capacity;
    }
    return binding;
}

void Stack::release(const ports::Binding& binding)
{
    ports_.release(binding);
    clear_ring(binding.slot);
}

void Stack::reconnect(const ports::Binding& binding, ports::Ipv4Endpoint peer)
{
    ports_.reconnect(binding, peer);
}

std::optional<ports::Binding> Stack::lookup(std::uint32_t destination, std::uint16_t port,
                                            ports::Ipv4Endpoint source) const
{
    return ports_.lookup(params_.address, destination, port, source);
}

std::optional<std::uint32_t> Stack::next_hop(std::uint32_t destination) const
{
    if (neighbour::on_the_same_link(params_.address, destination, params_.prefix_length))
    {
        return destination;
    }
    return params_.gateway;
}

std::optional<Mac> Stack::neighbour(std::uint32_t address) const
{
    SpinGuard guard{neighbours_lock_};
    return neighbours_.resolve(address, now_ns());
}

void Stack::glean(std::uint32_t address, Mac hardware)
{
    if (neighbour::on_the_same_link(params_.address, address, params_.prefix_length))
    {
        learn(address, hardware);
    }
}

void Stack::learn(std::uint32_t address, Mac hardware)
{
    SpinGuard guard{neighbours_lock_};
    neighbours_.learn(address, hardware, now_ns());
}

void Stack::insert_neighbour(std::uint32_t address, Mac hardware)
{
    SpinGuard guard{neighbours_lock_};
    neighbours_.insert_fixed(address, hardware);
}

std::size_t Stack::pump()
{
    counters_.poller.fetch_add(1, std::memory_order_relaxed);

    deferred_.clear();
    kept_frames_.clear();
    DemuxContext context{this, &deferred_};
    const std::size_t handed = rx_->rx_burst(params_.rx_burst, &demux_callback, &context, kept_frames_);

    for (auto& kept : kept_frames_)
    {
        (void)deliver_frame(kept.binding, kept.source, kept.payload_offset, std::move(kept.frame));
    }
    // A frame the ring could not take is dropped here, and its buffer goes back
    // to the pool now rather than at the next pass.
    kept_frames_.clear();

    for (auto& answer : deferred_)
    {
        std::optional<std::size_t> written;
        if (answer.kind == demux::Deferred::Kind::arp_reply)
        {
            written = frame::write_arp(answer_buffer_, answer.ethernet, answer.arp);
        }
        else
        {
            const frame::IcmpEcho echo{frame::ICMP_ECHO_REPLY, answer.identifier, answer.sequence, answer.payload};
            written = frame::write_icmp_echo_reply(answer_buffer_, answer.ipv4, echo);
        }
        if (written)
        {
            (void)send_frame(std::span<const std::uint8_t>{answer_buffer_}.first(*written), *receiver_tx_);
        }
    }

    return handed;
}

std::expected<std::size_t, Error> Stack::send(const ports::Binding& binding, ports::Ipv4Endpoint destination,
                                              std::span<const std::uint8_t> payload)
{
    const std::span<const std::uint8_t> parts[]{payload};
    return send(binding, destination, std::span<const std::span<const std::uint8_t>>{parts});
}

std::expected<std::size_t, Error> Stack::send(const ports::Binding& binding, ports::Ipv4Endpoint destination,
                                              std::span<const std::span<const std::uint8_t>> parts)
{
    return send_to(*sender_tx_, binding, destination, parts);
}

std::expected<std::size_t, Error> Stack::send_to(packet_io::TxPort& port, const ports::Binding& binding,
                                                 ports::Ipv4Endpoint destination,
                                                 std::span<const std::span<const std::uint8_t>> parts)
{
    std::size_t payload_size = 0;
    for (const auto part : parts)
    {
        payload_size += part.size();
    }

    // Not back pressure: no later pass makes a datagram this size fit, and the
    // caller has to hear about it (counter 79, `EMSGSIZE` at the seam). The
    // channel's MTU is the driver's and the port's is the card's, and nothing
    // reconciles them before a frame is built (plan §4.10).
    if (payload_size > params_.largest_datagram())
    {
        counters_.errors.fetch_add(1, std::memory_order_relaxed);
        return std::unexpected(make_error(ErrorCode::invalid_argument,
                                          "datagram is larger than the port allows: " + std::to_string(payload_size) +
                                              " > " + std::to_string(params_.largest_datagram())));
    }

    // A datagram this stack addresses to its own address cannot travel on the
    // wire: nobody out there answers an ARP for an address that is ours. The
    // kernel media loops it through the local route; a media that owns the
    // address does the same, and the port table decides who receives it.
    if (destination.address == params_.address)
    {
        const auto target = lookup(destination.address, destination.port, {params_.address, binding.port});
        if (!target)
        {
            return 0;
        }
        bool delivered = false;
        if (1 == parts.size())
        {
            delivered = deliver(*target, {params_.address, binding.port}, parts.front());
        }
        else
        {
            std::vector<std::uint8_t> joined;
            joined.reserve(payload_size);
            for (const auto part : parts)
            {
                joined.insert(joined.end(), part.begin(), part.end());
            }
            delivered = deliver(*target, {params_.address, binding.port}, joined);
        }
        return delivered ? payload_size : 0;
    }

    const auto next = next_hop(destination.address);
    if (!next)
    {
        counters_.arp_miss.fetch_add(1, std::memory_order_relaxed);
        return 0;
    }

    std::optional<Mac> hardware;
    bool ask = false;
    {
        SpinGuard guard{neighbours_lock_};
        hardware = neighbours_.resolve(*next, now_ns());
        if (!hardware)
        {
            ask = neighbours_.ask(*next, now_ns());
        }
    }

    if (!hardware)
    {
        if (ask)
        {
            std::vector<std::uint8_t> scratch(params_.mtu);
            if (const auto written = arp::write_request(scratch, params_, *next))
            {
                (void)send_frame(std::span<const std::uint8_t>{scratch}.first(*written), port);
            }
        }
        counters_.arp_miss.fetch_add(1, std::memory_order_relaxed);
        return 0;
    }

    const frame::Ipv4Write ipv4{*hardware, params_.mac, params_.address, destination.address, next_identification()};

    UdpFill fill_context{&port, ipv4, binding, destination, parts, payload_size};
    const packet_io::TxResult sent = port.tx_burst(1, &udp_fill, &fill_context);
    if (0 == sent.taken)
    {
        note_tx_result(sent);
        return 0;
    }
    return payload_size;
}

std::expected<Stack::DatagramsSent, Error> Stack::send_datagrams(
    const ports::Binding& binding, ports::Ipv4Endpoint destination,
    std::span<const std::span<const std::uint8_t>> datagrams, TxRole role)
{
    packet_io::TxPort& port = TxRole::receiver == role ? *receiver_tx_ : *sender_tx_;
    DatagramsSent sent;
    for (const auto datagram : datagrams)
    {
        const std::span<const std::uint8_t> parts[]{datagram};
        const auto result = send_to(port, binding, destination, std::span<const std::span<const std::uint8_t>>{parts});
        if (!result)
        {
            // A datagram that cannot travel at all is the caller's to hear
            // about, but only while nothing has gone: once one has, the run is
            // a short send and the reference answers with the count it took
            // (`sendv` returns `num_sent`, `aeron_udp_channel_transport.c:728`).
            if (0 == sent.datagrams)
            {
                return std::unexpected(result.error());
            }
            break;
        }
        if (0 == *result)
        {
            break;
        }
        sent.datagrams += 1;
        sent.bytes += static_cast<std::int64_t>(datagram.size());
    }
    return sent;
}

std::size_t Stack::receive(const ports::Binding& binding, std::span<std::uint8_t> buffer, ports::Ipv4Endpoint* source)
{
    Ring& ring = rings_[binding.slot];
    SpinGuard guard{ring.lock};
    if (ring.generation != binding.generation || 0 == ring.size)
    {
        return 0;
    }

    const Datagram& datagram = *ring.frames[ring.head];
    if (nullptr != source)
    {
        *source = datagram.source;
    }
    const auto frame_bytes = datagram.frame.as_bytes();
    const auto bytes = frame_bytes.subspan(std::min(datagram.payload_offset, frame_bytes.size()));
    const std::size_t copied = std::min(buffer.size(), bytes.size());
    std::copy(bytes.data(), bytes.data() + copied, buffer.begin());
    // The parked buffer goes back to the pool here, and not before: everything
    // above read through it.
    ring.frames[ring.head].reset();
    ring.head = (ring.head + 1) % ring.capacity;
    --ring.size;
    return copied;
}

std::uint16_t Stack::next_identification() noexcept
{
    return static_cast<std::uint16_t>(identification_.fetch_add(1, std::memory_order_relaxed) + 1);
}

std::int64_t Stack::now_ns() const noexcept
{
    if (manual_clock_)
    {
        return manual_now_.load(std::memory_order_relaxed);
    }
    return steady_now_ns();
}

void Stack::set_now_for_test(std::int64_t now_ns) noexcept
{
    manual_clock_ = true;
    manual_now_.store(now_ns, std::memory_order_relaxed);
}

void Stack::count_a_discard(frame::Discard discard) noexcept
{
    switch (discard)
    {
        case frame::Discard::checksum_failure:
            counters_.checksum_failure.fetch_add(1, std::memory_order_relaxed);
            break;
        case frame::Discard::fragmented:
        case frame::Discard::options:
            counters_.fragmented.fetch_add(1, std::memory_order_relaxed);
            break;
        case frame::Discard::unsupported_ethernet_type:
        case frame::Discard::malformed:
            counters_.unsupported_ethernet_type.fetch_add(1, std::memory_order_relaxed);
            break;
        case frame::Discard::unsupported_protocol:
            counters_.unsupported_protocol.fetch_add(1, std::memory_order_relaxed);
            break;
        case frame::Discard::short_frame:
            break;
    }
}

void Stack::count_unclaimed(std::span<const std::uint8_t> payload) noexcept
{
    (void)payload;
    counters_.rx_receiver_discard.fetch_add(1, std::memory_order_relaxed);
}

bool Stack::deliver(const ports::Binding& binding, ports::Ipv4Endpoint source, std::span<const std::uint8_t> payload)
{
    return deliver_frame(binding, source, 0,
                         packet_io::ReadyFrame::bytes(std::vector<std::uint8_t>(payload.begin(), payload.end())));
}

/// Put a frame (parked, in the DPDK build) in its transport's ring. The ring is
/// full or stale: the frame is dropped here and its buffer goes back with it.
bool Stack::deliver_frame(const ports::Binding& binding, ports::Ipv4Endpoint source, std::size_t payload_offset,
                          packet_io::ReadyFrame&& frame)
{
    Ring& ring = rings_[binding.slot];
    SpinGuard guard{ring.lock};
    if (ring.generation != binding.generation || ring.size >= ring.capacity)
    {
        counters_.queue_drop.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    const std::size_t tail = (ring.head + ring.size) % ring.capacity;
    ring.frames[tail] = Datagram{source, payload_offset, std::move(frame)};
    ++ring.size;
    return true;
}

void Stack::reset_ring(const ports::Binding& binding)
{
    Ring& ring = rings_[binding.slot];
    SpinGuard guard{ring.lock};
    ring.capacity = params_.ring_capacity();
    ring.frames = std::vector<std::optional<Datagram>>(ring.capacity);
    ring.head = 0;
    ring.size = 0;
    ring.generation = binding.generation;
}

void Stack::clear_ring(std::uint16_t slot)
{
    Ring& ring = rings_[slot];
    SpinGuard guard{ring.lock};
    if (ring.capacity <= ring_mbufs_)
    {
        ring_mbufs_ -= ring.capacity;
    }
    else
    {
        ring_mbufs_ = 0;
    }
    ring.frames.clear();
    ring.head = 0;
    ring.size = 0;
    ring.capacity = 0;
    ring.generation = 0;
}

std::size_t Stack::send_frame(std::span<const std::uint8_t> bytes, packet_io::TxPort& port)
{
    // `params_.mtu` is the port's MTU, which counts the IPv4 and UDP headers
    // but not the Ethernet one; `largest_datagram()` already holds the send
    // side to that, and this is the frame's own bound.
    if (bytes.size() > params_.mtu + frame::ETHERNET_HEADER_LENGTH)
    {
        counters_.errors.fetch_add(1, std::memory_order_relaxed);
        return 0;
    }
    CopyFill fill{bytes};
    const packet_io::TxResult sent = port.tx_burst(1, &copy_fill, &fill);
    if (0 == sent.taken)
    {
        note_tx_result(sent);
    }
    return sent.taken;
}

/// A burst that took nothing is either an empty pool (77) or a ring that would
/// not take the frame (78); only the port can tell the two apart, and it did.
void Stack::note_tx_result(const packet_io::TxResult& result) noexcept
{
    if (result.not_allocated > 0)
    {
        counters_.nobufs.fetch_add(result.not_allocated, std::memory_order_relaxed);
        return;
    }
    counters_.tx_eagain.fetch_add(1, std::memory_order_relaxed);
}

}  // namespace axc::core::media::dpdk::stack
