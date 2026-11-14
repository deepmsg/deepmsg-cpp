// A port over a wire of queues, and the wire's own knobs.
//
// The whole userspace stack — two drivers talking to each other, the
// demultiplexer, the port table, ARP — runs over this in the default test
// build, with no privileges and no hardware. It is a wire and not a switch:
// what one end sends the other end is handed, in order, whatever it is
// addressed to. Splitting frames by destination is the stack's job.
//
// Transcribed from the Rust track's `media/userspace/fake_port.rs`.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <span>
#include <vector>

#include "core/media/dpdk/stack/frame.h"
#include "core/media/dpdk/stack/packet_io.h"

namespace axc::core::media::dpdk::fake_port
{

/// Which end of a wire a port is on.
enum class Side
{
    a,
    b,
};

/// The side frames sent from here arrive at.
[[nodiscard]] constexpr Side other_side(Side side) noexcept
{
    return Side::a == side ? Side::b : Side::a;
}

/// What a direction of the wire does to the frames that cross it. Every knob
/// counts the frames that direction has carried, so `every` is a period:
/// `drop_every = 3` loses the third, sixth, ninth.
struct Injection
{
    std::optional<std::uint64_t> drop_every;
    std::optional<std::uint64_t> duplicate_every;
    bool reorder{false};
    std::optional<std::uint64_t> corrupt_checksum_every;
    std::optional<std::size_t> capacity;
};

class Wire;
class FakePort;

/// A wire between two ends, and the ports on it.
///
/// The wire owns its state; a port holds a pointer to it, so the wire must
/// outlive every port made from it (tests declare the wire first).
class Wire
{
public:
    Wire();
    ~Wire();
    Wire(const Wire&) = delete;
    Wire& operator=(const Wire&) = delete;

    /// A port on `side` of this wire, with the MAC and maximum frame length it
    /// answers for.
    [[nodiscard]] FakePort port(Side side, std::array<std::uint8_t, 6> mac, std::size_t mtu);

    void set_injection(Side side, const Injection& injection);

    /// How many frames `side` has sent that the other end has not taken.
    [[nodiscard]] std::size_t queued(Side side) const;

    /// How many frames have crossed the direction leaving `side`, dropped and
    /// duplicated ones included.
    [[nodiscard]] std::uint64_t carried(Side side) const;

    /// Put a frame on the wire from `side`, as a port on it would.
    void send(Side side, std::span<const std::uint8_t> frame);

    /// Take up to `max` frames that crossed into `side`, in order.
    [[nodiscard]] std::vector<std::vector<std::uint8_t>> take(Side side, std::size_t max);

private:
    friend class FakePort;

    struct State;
    std::unique_ptr<State> state_;

    void enqueue(Side side, std::vector<std::uint8_t> frame);
    [[nodiscard]] std::optional<std::vector<std::uint8_t>> pop(Side side);
    [[nodiscard]] std::size_t delivery_capacity(Side side) const;
};

/// A port on a `Wire`.
class FakePort : public packet_io::RxPort, public packet_io::TxPort
{
public:
    FakePort(FakePort&& other) noexcept = default;
    FakePort& operator=(FakePort&& other) noexcept = default;
    ~FakePort() override = default;

    [[nodiscard]] Side side() const noexcept
    {
        return side_;
    }

    std::size_t rx_burst(std::size_t max, packet_io::RxCallback callback, void* context,
                         std::vector<std::pair<ports::Binding, packet_io::ReadyFrame>>& kept) override;

    [[nodiscard]] packet_io::Offloads offloads() const noexcept override;
    [[nodiscard]] std::array<std::uint8_t, 6> mac() const noexcept override;
    [[nodiscard]] std::size_t mtu() const noexcept override;

    std::size_t tx_burst(std::size_t count, packet_io::TxFill fill, void* context) override;
    [[nodiscard]] frame::L4Checksum checksums() const noexcept override;

private:
    friend class Wire;
    FakePort(Wire* wire, Side side, std::array<std::uint8_t, 6> mac, std::size_t mtu)
        : wire_{wire}, side_{side}, mac_{mac}, mtu_{mtu}, scratch_(mtu, 0)
    {
    }

    Wire* wire_;
    Side side_;
    std::array<std::uint8_t, 6> mac_;
    std::size_t mtu_;
    std::vector<std::uint8_t> scratch_;
};

inline FakePort Wire::port(Side side, std::array<std::uint8_t, 6> mac, std::size_t mtu)
{
    return FakePort{this, side, mac, mtu};
}

}  // namespace axc::core::media::dpdk::fake_port
