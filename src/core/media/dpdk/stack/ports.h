// Which transports hold which `(address, port)`, and what a frame's
// destination has to match to reach one.
//
// The kernel answers these questions with a socket: a bind fails with
// `EADDRINUSE`, a connected socket ignores datagrams from anyone else, a
// wildcard socket hears every address the machine has. Transcribed from the
// Rust track's `media/userspace/ports.rs` (G1–G5, G12).

#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>

namespace axc::core::media::dpdk::ports
{

using SlotId = std::uint16_t;

/// How many `(address, port)` pairs a stack can have bound at once.
inline constexpr std::size_t SLOT_CAPACITY = 4096;

/// Linux's `ip_local_port_range`, because a peer cannot tell which stack chose
/// one of these.
inline constexpr std::uint16_t EPHEMERAL_PORTS_FIRST = 32768;
inline constexpr std::uint16_t EPHEMERAL_PORTS_LAST = 60999;

/// An IPv4 endpoint: the address in host byte order.
struct Ipv4Endpoint
{
    std::uint32_t address{0};
    std::uint16_t port{0};

    [[nodiscard]] friend constexpr bool operator==(const Ipv4Endpoint& left,
                                                   const Ipv4Endpoint& right) noexcept = default;
};

/// Which slot a port belongs to, and which binding of that slot it is.
struct Binding
{
    SlotId slot{0};
    std::uint16_t port{0};
    std::uint32_t generation{0};

    [[nodiscard]] friend constexpr bool operator==(const Binding& left, const Binding& right) noexcept = default;
};

/// A bind failure in the shape the endpoints compose their report from. The
/// descriptor is `-1` because there is none.
struct BindFailure
{
    int errcode{0};
    int descriptor{-1};
    Ipv4Endpoint address{};
};

/// The `(address, port)` pairs in use, and how a destination finds them.
class PortTable
{
public:
    /// A table with room for `capacity` bindings, allocating ephemeral ports
    /// from `[ephemeral_low, ephemeral_high]`.
    PortTable(std::size_t capacity, std::uint16_t ephemeral_low, std::uint16_t ephemeral_high);
    ~PortTable();

    PortTable(PortTable&& other) noexcept;
    PortTable& operator=(PortTable&& other) noexcept;
    PortTable(const PortTable&) = delete;
    PortTable& operator=(const PortTable&) = delete;

    [[nodiscard]] std::size_t capacity() const noexcept;
    [[nodiscard]] std::size_t in_use() const noexcept;

    /// Bind `(address, port)`, allocating a port from the ephemeral range when
    /// `port` is zero. `address` empty is a wildcard bind; `peer` is the far
    /// end a connected transport hears from.
    [[nodiscard]] std::expected<Binding, BindFailure> bind(std::optional<std::uint32_t> address, std::uint16_t port,
                                                           std::optional<Ipv4Endpoint> peer, Ipv4Endpoint failure_at);

    /// Give a port back. The binding that was handed out is dead from here.
    void release(const Binding& binding);

    /// The binding a datagram delivered to `destination` from `source`
    /// reaches, or nothing when no port is bound, the binding is for another
    /// address, or a connected transport is hearing from somebody else.
    [[nodiscard]] std::optional<Binding> lookup(std::uint32_t own, std::uint32_t destination, std::uint16_t port,
                                                Ipv4Endpoint source) const;

    /// Point a connected binding at a different peer.
    void reconnect(const Binding& binding, Ipv4Endpoint peer);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace axc::core::media::dpdk::ports
