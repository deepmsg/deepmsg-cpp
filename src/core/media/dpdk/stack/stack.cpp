#include "core/media/dpdk/stack/stack.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>

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
    Stack* stack;
    frame::Ipv4Write ipv4;
    ports::Binding binding;
    ports::Ipv4Endpoint destination;
    std::span<const std::uint8_t> payload;
};

std::pair<std::size_t, frame::TxOffloads> udp_fill(void* context, std::span<std::uint8_t> buffer)
{
    const auto& udp = *static_cast<UdpFill*>(context);
    if (buffer.size() < frame::IPV4_UDP_HEADER_LENGTH + udp.payload.size())
    {
        return {0, frame::TxOffloads::none()};
    }
    std::copy(udp.payload.begin(), udp.payload.end(), buffer.begin() + frame::IPV4_UDP_HEADER_LENGTH);
    const frame::UdpWrite write{udp.ipv4, udp.binding.port, udp.destination.port, udp.payload.size()};
    const auto written = frame::write_ipv4_udp(buffer, write, udp.stack->sender_tx().checksums());
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
    auto binding = ports_.bind(address, port, peer, failure_at);
    if (binding)
    {
        reset_ring(*binding);
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

    std::vector<demux::Deferred> deferred;
    DemuxContext context{this, &deferred};
    std::vector<std::pair<ports::Binding, packet_io::ReadyFrame>> kept;
    const std::size_t handed = rx_->rx_burst(params_.rx_burst, &demux_callback, &context, kept);

    std::vector<std::uint8_t> scratch(params_.mtu);
    for (auto& answer : deferred)
    {
        std::optional<std::size_t> written;
        if (answer.kind == demux::Deferred::Kind::arp_reply)
        {
            written = frame::write_arp(scratch, answer.ethernet, answer.arp);
        }
        else
        {
            const frame::IcmpEcho echo{frame::ICMP_ECHO_REPLY, answer.identifier, answer.sequence, answer.payload};
            written = frame::write_icmp_echo_reply(scratch, answer.ipv4, echo);
        }
        if (written)
        {
            (void)send_frame(std::span<const std::uint8_t>{scratch}.first(*written), *receiver_tx_);
        }
    }

    return handed;
}

std::size_t Stack::send(const ports::Binding& binding, ports::Ipv4Endpoint destination,
                        std::span<const std::uint8_t> payload)
{
    if (payload.size() > params_.largest_datagram())
    {
        return 0;
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
                (void)send_frame(std::span<const std::uint8_t>{scratch}.first(*written), *sender_tx_);
            }
        }
        counters_.arp_miss.fetch_add(1, std::memory_order_relaxed);
        return 0;
    }

    const frame::Ipv4Write ipv4{*hardware, params_.mac, params_.address, destination.address, next_identification()};

    UdpFill fill_context{this, ipv4, binding, destination, payload};
    const std::size_t taken = sender_tx_->tx_burst(1, &udp_fill, &fill_context);
    if (0 == taken)
    {
        counters_.tx_eagain.fetch_add(1, std::memory_order_relaxed);
        return 0;
    }
    return payload.size();
}

std::size_t Stack::receive(const ports::Binding& binding, std::span<std::uint8_t> buffer)
{
    Ring& ring = rings_[binding.slot];
    SpinGuard guard{ring.lock};
    if (ring.generation != binding.generation || ring.frames.empty())
    {
        return 0;
    }

    const auto bytes = ring.frames.front().as_bytes();
    const std::size_t copied = std::min(buffer.size(), bytes.size());
    std::copy(bytes.data(), bytes.data() + copied, buffer.begin());
    ring.frames.pop_front();
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

bool Stack::deliver(const ports::Binding& binding, std::span<const std::uint8_t> payload)
{
    Ring& ring = rings_[binding.slot];
    SpinGuard guard{ring.lock};
    if (ring.generation != binding.generation || ring.frames.size() >= ring.capacity)
    {
        counters_.queue_drop.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    ring.frames.emplace_back(packet_io::ReadyFrame{std::vector<std::uint8_t>(payload.begin(), payload.end())});
    return true;
}

void Stack::reset_ring(const ports::Binding& binding)
{
    Ring& ring = rings_[binding.slot];
    SpinGuard guard{ring.lock};
    ring.frames.clear();
    ring.generation = binding.generation;
    ring.capacity = params_.ring_capacity();
}

void Stack::clear_ring(std::uint16_t slot)
{
    Ring& ring = rings_[slot];
    SpinGuard guard{ring.lock};
    ring.frames.clear();
    ring.generation = 0;
}

std::size_t Stack::send_frame(std::span<const std::uint8_t> bytes, packet_io::TxPort& port)
{
    if (bytes.size() > params_.mtu)
    {
        return 0;
    }
    CopyFill fill{bytes};
    const std::size_t taken = port.tx_burst(1, &copy_fill, &fill);
    if (0 == taken)
    {
        counters_.tx_eagain.fetch_add(1, std::memory_order_relaxed);
    }
    return taken;
}

}  // namespace axc::core::media::dpdk::stack
