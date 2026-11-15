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
/// it belongs to the transport holding this binding (and to the peer it came
/// from, which the ring has to carry to `receive`).
struct Disposition
{
    std::optional<ports::Binding> keep;
    ports::Ipv4Endpoint source{};
    /// Where the datagram starts inside the frame (what `receive` answers).
    std::size_t payload_offset{0};

    static constexpr Disposition consumed() noexcept
    {
        return {};
    }

    static constexpr Disposition keep_for(ports::Binding binding, ports::Ipv4Endpoint from,
                                          std::size_t offset = 0) noexcept
    {
        return Disposition{binding, from, offset};
    }
};

/// `f(context, frame)` answers each frame's fate. A function pointer rather
/// than `std::function` because this is a per-frame call on the receive path.
using RxCallback = Disposition (*)(void* context, const RxFrame& frame);

/// A frame that has left its port but not yet its transport.
///
/// Two backings. `bytes` owns a copy -- the in-memory wire and the one path
/// that has no pool (a datagram this stack addresses to itself). `parked`
/// borrows a frame that is still in the port's pool and gives it back through
/// `release` when the frame is destroyed: that is what keeps the receive path
/// from copying and from allocating, and `release` is a plain function pointer
/// so the per-frame path never touches `std::function` or the heap.
///
/// The parked form must not outlive its port or pool: the ring owner frees
/// every parked frame before the port closes (`Stack::clear_ring`).
class ReadyFrame
{
public:
    ReadyFrame() noexcept = default;

    static ReadyFrame bytes(std::vector<std::uint8_t> data) noexcept
    {
        ReadyFrame frame;
        frame.owned_ = std::move(data);
        frame.data_ = frame.owned_.data();
        frame.length_ = frame.owned_.size();
        return frame;
    }

    static ReadyFrame parked(void* handle, const std::uint8_t* data, std::size_t length,
                             void (*release)(void* handle) noexcept) noexcept
    {
        ReadyFrame frame;
        frame.handle_ = handle;
        frame.data_ = data;
        frame.length_ = length;
        frame.release_ = release;
        return frame;
    }

    ReadyFrame(ReadyFrame&& other) noexcept
        : handle_{other.handle_},
          data_{other.data_},
          length_{other.length_},
          release_{other.release_},
          owned_{std::move(other.owned_)}
    {
        other.handle_ = nullptr;
        other.data_ = nullptr;
        other.length_ = 0;
        other.release_ = nullptr;
        if (!owned_.empty())
        {
            data_ = owned_.data();
            length_ = owned_.size();
        }
    }

    ReadyFrame& operator=(ReadyFrame&& other) noexcept
    {
        if (this != &other)
        {
            release();
            handle_ = other.handle_;
            data_ = other.data_;
            length_ = other.length_;
            release_ = other.release_;
            owned_ = std::move(other.owned_);
            other.handle_ = nullptr;
            other.data_ = nullptr;
            other.length_ = 0;
            other.release_ = nullptr;
            if (!owned_.empty())
            {
                data_ = owned_.data();
                length_ = owned_.size();
            }
        }
        return *this;
    }

    ~ReadyFrame()
    {
        release();
    }

    ReadyFrame(const ReadyFrame&) = delete;
    ReadyFrame& operator=(const ReadyFrame&) = delete;

    [[nodiscard]] std::span<const std::uint8_t> as_bytes() const noexcept
    {
        return std::span<const std::uint8_t>{data_, length_};
    }

    [[nodiscard]] bool valid() const noexcept
    {
        return nullptr != data_;
    }

private:
    void release() noexcept
    {
        if (nullptr != release_)
        {
            release_(handle_);
            release_ = nullptr;
        }
    }

    void* handle_{nullptr};
    const std::uint8_t* data_{nullptr};
    std::size_t length_{0};
    void (*release_)(void* handle) noexcept {nullptr};
    std::vector<std::uint8_t> owned_{};
};

/// A frame a port kept for a transport: the binding it was routed under, the
/// peer it came from, and the frame itself (parked, in the DPDK build).
struct KeptFrame
{
    ports::Binding binding;
    ports::Ipv4Endpoint source;
    std::size_t payload_offset{0};
    ReadyFrame frame;
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
    /// A kept frame is handed over without a copy where the port can (the DPDK
    /// build parks it); the rest are released before this returns.
    virtual std::size_t rx_burst(std::size_t max, RxCallback callback, void* context, std::vector<KeptFrame>& kept) = 0;

    [[nodiscard]] virtual Offloads offloads() const noexcept = 0;
    [[nodiscard]] virtual std::array<std::uint8_t, 6> mac() const noexcept = 0;
    [[nodiscard]] virtual std::size_t mtu() const noexcept = 0;
};

/// `fill(context, buffer)` writes one frame and answers its length (zero for
/// nothing this time) and the offload it asks the port for.
using TxFill = std::pair<std::size_t, frame::TxOffloads> (*)(void* context, std::span<std::uint8_t> buffer);

/// What one transmit burst did.
///
/// A full ring and an empty buffer pool both leave the caller with nothing
/// taken, and they are different counters: 78 (tx eagain) is the ring's
/// answer, 77 (nobufs) is the pool's. Only the port can tell the two apart, so
/// it says both.
struct TxResult
{
    std::size_t taken{0};
    std::size_t not_allocated{0};
};

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
    /// how many were taken (G7) and how many the port could not allocate.
    virtual TxResult tx_burst(std::size_t count, TxFill fill, void* context) = 0;

    [[nodiscard]] virtual frame::L4Checksum checksums() const noexcept = 0;
    [[nodiscard]] virtual std::array<std::uint8_t, 6> mac() const noexcept = 0;
    [[nodiscard]] virtual std::size_t mtu() const noexcept = 0;
};

}  // namespace axc::core::media::dpdk::packet_io
