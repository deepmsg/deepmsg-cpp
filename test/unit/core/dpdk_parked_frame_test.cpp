// The receive path without a copy and without an allocation.
//
// A port hands a kept frame over in the form the transport will read it in: the
// DPDK build parks the buffer it already has (the mbuf) and gives it back
// through a release hook when the transport has copied it out. These cases pin
// the properties that matter, using a port that parks frames out of static
// buffers, so they are judged without DPDK: the buffer is released exactly
// once, a full ring drops *and* frees, a ring the pool cannot cover is refused,
// and a pass over parked frames allocates nothing.
//
// Compiled in every build: the stack and the frame types are not DPDK-only.

#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <new>
#include <span>
#include <vector>

#include "core/media/dpdk/stack/frame.h"
#include "core/media/dpdk/stack/packet_io.h"
#include "core/media/dpdk/stack/stack.h"

// ---------------------------------------------------------------------------
// A counting allocator, so "no allocation on the receive path" is a reading and
// not a claim. It forwards to malloc/free and only counts.
//
// It is not installed under a sanitizer: ASan's interceptors own operator new
// and free, and a test-binary override makes every allocation it sees an
// alloc-dealloc mismatch. The zero-allocation case skips there instead of
// claiming a reading it did not take.
// ---------------------------------------------------------------------------
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
#define PARKED_FRAME_COUNTS_ALLOCATIONS 0
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer) || __has_feature(memory_sanitizer)
#define PARKED_FRAME_COUNTS_ALLOCATIONS 0
#else
#define PARKED_FRAME_COUNTS_ALLOCATIONS 1
#endif
#else
#define PARKED_FRAME_COUNTS_ALLOCATIONS 1
#endif

namespace
{
// `thread_local`: the counting `operator new` below is process-wide, and other
// threads in the same test binary allocate concurrently.
thread_local std::size_t g_allocations = 0;
}  // namespace

#if PARKED_FRAME_COUNTS_ALLOCATIONS
void* operator new(std::size_t size)
{
    ++g_allocations;
    if (void* const memory = std::malloc(size == 0 ? 1 : size))
    {
        return memory;
    }
    throw std::bad_alloc{};
}

void operator delete(void* memory) noexcept
{
    std::free(memory);
}

void operator delete(void* memory, std::size_t /*size*/) noexcept
{
    std::free(memory);
}

void* operator new[](std::size_t size)
{
    ++g_allocations;
    if (void* const memory = std::malloc(size == 0 ? 1 : size))
    {
        return memory;
    }
    throw std::bad_alloc{};
}

void operator delete[](void* memory) noexcept
{
    std::free(memory);
}

void operator delete[](void* memory, std::size_t /*size*/) noexcept
{
    std::free(memory);
}
#endif  // PARKED_FRAME_COUNTS_ALLOCATIONS

namespace
{

using axc::core::media::dpdk::frame::Checksums;
using axc::core::media::dpdk::frame::Ipv4Write;
using axc::core::media::dpdk::frame::L4Checksum;
using axc::core::media::dpdk::packet_io::KeptFrame;
using axc::core::media::dpdk::packet_io::Offloads;
using axc::core::media::dpdk::packet_io::ReadyFrame;
using axc::core::media::dpdk::packet_io::RxCallback;
using axc::core::media::dpdk::packet_io::RxFrame;
using axc::core::media::dpdk::packet_io::RxPort;
using axc::core::media::dpdk::packet_io::TxFill;
using axc::core::media::dpdk::packet_io::TxPort;
using axc::core::media::dpdk::packet_io::TxResult;
using axc::core::media::dpdk::ports::Binding;
using axc::core::media::dpdk::ports::Ipv4Endpoint;
using axc::core::media::dpdk::stack::Mac;
using axc::core::media::dpdk::stack::Stack;
using axc::core::media::dpdk::stack::StackParams;

constexpr std::uint32_t OWN = 0x0a0a0001;
constexpr std::uint32_t PEER = 0x0a0a0002;
constexpr std::uint16_t BOUND_PORT = 5000;
constexpr Mac MAC{0x02, 0, 0, 0, 0, 0x01};
constexpr std::size_t PAYLOAD_LENGTH = 8;
constexpr std::size_t IPV4_UDP_HEADER_LENGTH = 14 + 20 + 8;
constexpr std::size_t FRAME_LENGTH = IPV4_UDP_HEADER_LENGTH + PAYLOAD_LENGTH;

/// A port over static buffers holding one valid UDP datagram to `OWN:5000`.
/// Each hand-out parks its own buffer and counts the release.
class ParkingPort : public RxPort
{
public:
    explicit ParkingPort(std::size_t frames) : frames_{frames}
    {
        for (std::size_t index = 0; index < frames_ && index < MAX_FRAMES; ++index)
        {
            std::array<std::uint8_t, PAYLOAD_LENGTH> payload{};
            for (std::size_t byte = 0; byte < PAYLOAD_LENGTH; ++byte)
            {
                payload[byte] = static_cast<std::uint8_t>(index + byte);
            }
            const Ipv4Write ipv4{MAC, MAC, PEER, OWN, 1};
            // The writer takes the payload as already written after the
            // headers, and fills the headers and the checksum around it.
            std::copy(payload.begin(), payload.end(),
                      buffers_[index].begin() + static_cast<std::ptrdiff_t>(IPV4_UDP_HEADER_LENGTH));
            (void)axc::core::media::dpdk::frame::write_ipv4_udp_here(
                std::span<std::uint8_t>{buffers_[index]}.first(FRAME_LENGTH),
                axc::core::media::dpdk::frame::UdpWrite{ipv4, 5001, BOUND_PORT, PAYLOAD_LENGTH});
        }
    }

    static void release(void* handle) noexcept
    {
        ++g_released;
        (void)handle;
    }

    std::size_t rx_burst(std::size_t max, RxCallback callback, void* context, std::vector<KeptFrame>& kept) override
    {
        std::size_t handed = 0;
        while (handed < max && next_ < frames_)
        {
            auto& buffer = buffers_[next_];
            const RxFrame view{std::span<const std::uint8_t>{buffer.data(), FRAME_LENGTH}, Checksums::software};
            const auto disposition = callback(context, view);
            if (disposition.keep)
            {
                kept.push_back(KeptFrame{*disposition.keep, disposition.source, disposition.payload_offset,
                                         ReadyFrame::parked(buffer.data(), buffer.data(), FRAME_LENGTH, &release)});
            }
            ++next_;
            ++handed;
        }
        return handed;
    }

    [[nodiscard]] Offloads offloads() const noexcept override
    {
        return Offloads::none();
    }
    [[nodiscard]] std::array<std::uint8_t, 6> mac() const noexcept override
    {
        return MAC;
    }
    [[nodiscard]] std::size_t mtu() const noexcept override
    {
        return 1500;
    }

    static std::size_t released() noexcept
    {
        return g_released;
    }
    static void reset_released() noexcept
    {
        g_released = 0;
    }

private:
    static constexpr std::size_t MAX_FRAMES = 512;
    static inline std::size_t g_released = 0;

    std::array<std::array<std::uint8_t, FRAME_LENGTH>, MAX_FRAMES> buffers_{};
    std::size_t frames_{0};
    std::size_t next_{0};
};

class NullTxPort : public TxPort
{
public:
    TxResult tx_burst(std::size_t count, TxFill /*fill*/, void* /*context*/) override
    {
        return TxResult{count, 0};
    }
    [[nodiscard]] L4Checksum checksums() const noexcept override
    {
        return L4Checksum::software;
    }
    [[nodiscard]] std::array<std::uint8_t, 6> mac() const noexcept override
    {
        return MAC;
    }
    [[nodiscard]] std::size_t mtu() const noexcept override
    {
        return 1500;
    }
};

StackParams params(std::size_t ring_budget = std::numeric_limits<std::size_t>::max())
{
    StackParams result;
    result.address = OWN;
    result.prefix_length = 24;
    result.mac = MAC;
    result.mtu = 1500;
    // The window picks the ring: sixty-four frames in, `ring_capacity` is 128.
    result.receive_window = 64 * result.mtu;
    result.rx_burst = 64;
    result.ring_mbuf_budget = ring_budget;
    return result;
}

std::unique_ptr<Stack> make_stack(ParkingPort** port, std::size_t frames,
                                  std::size_t ring_budget = std::numeric_limits<std::size_t>::max())
{
    auto rx = std::make_unique<ParkingPort>(frames);
    *port = rx.get();
    return std::make_unique<Stack>(params(ring_budget), std::move(rx), std::make_unique<NullTxPort>(),
                                   std::make_unique<NullTxPort>());
}

}  // namespace

TEST(ParkedFrame, APumpedFrameIsHandedToTheRingAndReleasedWhenItIsRead)
{
    ParkingPort::reset_released();
    ParkingPort* port = nullptr;
    auto stack = make_stack(&port, 4);
    const auto binding = stack->bind(OWN, BOUND_PORT, std::nullopt, Ipv4Endpoint{OWN, BOUND_PORT});
    ASSERT_TRUE(binding.has_value());

    EXPECT_EQ(4u, stack->pump());
    EXPECT_EQ(0u, ParkingPort::released()) << "the ring holds the parked frames; nothing is freed yet";

    std::array<std::uint8_t, PAYLOAD_LENGTH + 16> payload{};
    for (std::size_t index = 0; index < 4; ++index)
    {
        const std::size_t received = stack->receive(*binding, payload);
        EXPECT_EQ(PAYLOAD_LENGTH, received);
        EXPECT_EQ(static_cast<std::uint8_t>(index), payload[0]) << "the datagram, not the frame";
        EXPECT_EQ(index + 1, ParkingPort::released());
    }
    EXPECT_EQ(4u, ParkingPort::released());
    (void)port;
}

TEST(ParkedFrame, AFullRingDropsTheFrameAndFreesIt)
{
    ParkingPort::reset_released();
    ParkingPort* port = nullptr;
    auto stack = make_stack(&port, 300);
    const auto binding = stack->bind(OWN, BOUND_PORT, std::nullopt, Ipv4Endpoint{OWN, BOUND_PORT});
    ASSERT_TRUE(binding.has_value());
    const std::size_t capacity = stack->params().ring_capacity();

    // Four pumps of 64: the ring holds `capacity`, the rest are dropped.
    std::size_t delivered = 0;
    for (std::size_t pass = 0; pass < 4; ++pass)
    {
        const std::size_t handed = stack->pump();
        delivered += handed;
    }
    EXPECT_EQ(256u, delivered);
    EXPECT_EQ(256u - capacity, stack->counters().queue_drop.load());
    EXPECT_EQ(256u - capacity, ParkingPort::released()) << "a dropped frame still goes back to the pool";

    // Drain: everything else comes back through `receive`.
    std::array<std::uint8_t, PAYLOAD_LENGTH + 16> payload{};
    for (std::size_t index = 0; index < capacity; ++index)
    {
        ASSERT_EQ(PAYLOAD_LENGTH, stack->receive(*binding, payload));
    }
    EXPECT_EQ(256u, ParkingPort::released());
    (void)port;
}

TEST(ParkedFrame, ASteadyPassAllocatesNothing)
{
#if !PARKED_FRAME_COUNTS_ALLOCATIONS
    GTEST_SKIP() << "the counting allocator is not installed under a sanitizer";
#endif
    ParkingPort::reset_released();
    ParkingPort* port = nullptr;
    auto stack = make_stack(&port, 256);
    const auto binding = stack->bind(OWN, BOUND_PORT, std::nullopt, Ipv4Endpoint{OWN, BOUND_PORT});
    ASSERT_TRUE(binding.has_value());

    // The first pass grows the stack's own member vectors: warm up on it.
    ASSERT_EQ(64u, stack->pump());

    const std::size_t before = g_allocations;
    ASSERT_EQ(64u, stack->pump());
    const std::size_t allocations = g_allocations - before;
    EXPECT_EQ(0u, allocations) << "a pass of parked frames copied nothing and allocated nothing";
    (void)port;
}

TEST(ParkedFrame, ARingThePoolCannotCoverIsRefused)
{
    ParkingPort* port = nullptr;
    const std::size_t capacity = params().ring_capacity();

    auto too_small = make_stack(&port, 1, capacity - 1);
    const auto refused = too_small->bind(OWN, BOUND_PORT, std::nullopt, Ipv4Endpoint{OWN, BOUND_PORT});
    ASSERT_FALSE(refused.has_value());
    EXPECT_EQ(ENOBUFS, refused.error().errcode);

    auto enough = make_stack(&port, 1, capacity);
    const auto accepted = enough->bind(OWN, BOUND_PORT, std::nullopt, Ipv4Endpoint{OWN, BOUND_PORT});
    ASSERT_TRUE(accepted.has_value());
    EXPECT_EQ(capacity, enough->ring_mbufs());
}
