// What a port is: frames in, frames out, and what happens to a frame the
// reader keeps.
//
// The design's `PacketIo` cut in two, because in this stack's shape the two
// halves are held by different roles: the receiver thread owns the one receive
// queue and each agent thread owns the transmit queue that belongs to it.
// Transcribed from the Rust track's `media/userspace/packet_io.rs`.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "core/media/dpdk/stack/frame.h"
#include "core/media/dpdk/stack/ports.h"

namespace axc::core::media::dpdk::packet_io
{

/// A frame a port has handed over, borrowed for the length of the callback.
struct RxFrame
{
    std::span<const std::uint8_t> bytes;
    frame::Checksums checksums{frame::Checksums::software};
};

/// What the reader wants done with the frame it was just handed: nothing, or
/// it belongs to the transport holding this binding.
struct Disposition
{
    std::optional<ports::Binding> keep;

    static constexpr Disposition consumed() noexcept
    {
        return {};
    }

    static constexpr Disposition keep_for(ports::Binding binding) noexcept
    {
        return Disposition{binding};
    }
};

/// `f(context, frame)` answers each frame's fate. A function pointer rather
/// than `std::function` because this is a per-frame call on the receive path.
using RxCallback = Disposition (*)(void* context, const RxFrame& frame);

/// A frame that has left its port but not yet its transport. Owned bytes; the
/// DPDK build adds a frame still parked in the port's pool.
class ReadyFrame
{
public:
    explicit ReadyFrame(std::vector<std::uint8_t> bytes) noexcept : bytes_{std::move(bytes)} {}

    ReadyFrame(ReadyFrame&& other) noexcept = default;
    ReadyFrame& operator=(ReadyFrame&& other) noexcept = default;
    ~ReadyFrame() = default;

    ReadyFrame(const ReadyFrame&) = delete;
    ReadyFrame& operator=(const ReadyFrame&) = delete;

    [[nodiscard]] std::span<const std::uint8_t> as_bytes() const noexcept
    {
        return bytes_;
    }

private:
    std::vector<std::uint8_t> bytes_;
};

/// What a port's hardware did to a frame's checksums.
enum class Verdict
{
    /// The hardware checked them and they held.
    passed,
    /// It checked them and they did not.
    failed,
    /// It checked nothing.
    not_judged,
};

/// Which checksums a port's hardware judges for it.
struct Offloads
{
    bool ipv4_checksum{false};
    bool udp_checksum{false};

    /// A port that judges nothing, so every frame it hands over is the stack's
    /// to verify.
    static constexpr Offloads none() noexcept
    {
        return {};
    }

    [[nodiscard]] constexpr bool judges_both() const noexcept
    {
        return ipv4_checksum && udp_checksum;
    }

    /// What this stack may believe about a frame the port answered `verdict`
    /// about. `passed` is worth taking only when the port judges both; a frame
    /// the hardware rejected is the stack's to check, where counter 89 is
    /// counted.
    [[nodiscard]] constexpr frame::Checksums checksums(Verdict verdict) const noexcept
    {
        return (Verdict::passed == verdict && judges_both()) ? frame::Checksums::verified : frame::Checksums::software;
    }
};

/// A port's receive side. One thread owns it; never shared.
class RxPort
{
public:
    RxPort() = default;
    virtual ~RxPort() = default;
    RxPort(RxPort&&) noexcept = default;
    RxPort& operator=(RxPort&&) noexcept = default;
    RxPort(const RxPort&) = delete;
    RxPort& operator=(const RxPort&) = delete;

    /// Hand over up to `max` frames; `callback` decides each one's fate, and a
    /// kept frame is appended to `kept` under the binding it was routed under.
    virtual std::size_t rx_burst(std::size_t max, RxCallback callback, void* context,
                                 std::vector<std::pair<ports::Binding, ReadyFrame>>& kept) = 0;

    [[nodiscard]] virtual Offloads offloads() const noexcept = 0;
    [[nodiscard]] virtual std::array<std::uint8_t, 6> mac() const noexcept = 0;
    [[nodiscard]] virtual std::size_t mtu() const noexcept = 0;
};

/// `fill(context, buffer)` writes one frame and answers its length (zero for
/// nothing this time) and the offload it asks the port for.
using TxFill = std::pair<std::size_t, frame::TxOffloads> (*)(void* context, std::span<std::uint8_t> buffer);

/// A port's transmit side.
class TxPort
{
public:
    TxPort() = default;
    virtual ~TxPort() = default;
    TxPort(TxPort&&) noexcept = default;
    TxPort& operator=(TxPort&&) noexcept = default;
    TxPort(const TxPort&) = delete;
    TxPort& operator=(const TxPort&) = delete;

    /// Ask `fill` for up to `count` frames and hand them to the port. Answers
    /// how many were taken (G7).
    virtual std::size_t tx_burst(std::size_t count, TxFill fill, void* context) = 0;

    [[nodiscard]] virtual frame::L4Checksum checksums() const noexcept = 0;
    [[nodiscard]] virtual std::array<std::uint8_t, 6> mac() const noexcept = 0;
    [[nodiscard]] virtual std::size_t mtu() const noexcept = 0;
};

}  // namespace axc::core::media::dpdk::packet_io
