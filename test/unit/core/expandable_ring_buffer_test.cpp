// C4c: `ExpandableRingBuffer` is Agrona 2.6.1's byte ring as the pending
// service message tracker uses it. The expectations below are the Rust track's
// `ring.rs` fixture, which was read out of `agrona-2.6.1.jar` with
// `javap -p -c` and pinned against a run of the jar.

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "core/collections/expandable_ring_buffer.h"

namespace
{

using axc::core::ExpandableRingBuffer;

[[nodiscard]] std::vector<std::uint8_t> payload(std::uint8_t start, std::size_t length)
{
    std::vector<std::uint8_t> bytes(length);
    for (std::size_t i = 0; i < length; i++)
    {
        bytes[i] = static_cast<std::uint8_t>(start + i);
    }
    return bytes;
}

TEST(ExpandableRingBufferTest, appendAndEachHandOverTheOffsetsTheJarHandsOver)
{
    ExpandableRingBuffer ring;
    EXPECT_TRUE(ring.append(payload(0xA0, 3)));
    EXPECT_TRUE(ring.append(payload(0xA0, 8)));
    EXPECT_TRUE(ring.append(payload(0xA0, 9)));
    EXPECT_EQ(56u, ring.size());
    EXPECT_EQ(64u, ring.capacity());

    std::vector<std::pair<std::size_t, std::size_t>> seen;
    ring.for_each(
        0,
        [&seen](std::span<const std::uint8_t> message, std::size_t head_offset)
        {
            seen.emplace_back(message.size(), head_offset);
            return true;
        },
        10);

    ASSERT_EQ(3u, seen.size());
    EXPECT_EQ(3u, seen[0].first);
    EXPECT_EQ(16u, seen[0].second);
    EXPECT_EQ(8u, seen[1].first);
    EXPECT_EQ(32u, seen[1].second);
    EXPECT_EQ(9u, seen[2].first);
    EXPECT_EQ(56u, seen[2].second);
}

TEST(ExpandableRingBufferTest, eachFromAMessageBoundaryResumesAfterIt)
{
    ExpandableRingBuffer ring;
    EXPECT_TRUE(ring.append(payload(0xA0, 3)));
    EXPECT_TRUE(ring.append(payload(0xA0, 8)));
    EXPECT_TRUE(ring.append(payload(0xA0, 9)));

    std::vector<std::pair<std::size_t, std::size_t>> seen;
    ring.for_each(
        16,
        [&seen](std::span<const std::uint8_t> message, std::size_t head_offset)
        {
            seen.emplace_back(message.size(), head_offset);
            return true;
        },
        10);

    ASSERT_EQ(2u, seen.size());
    EXPECT_EQ(8u, seen[0].first);
    EXPECT_EQ(32u, seen[0].second);
    EXPECT_EQ(9u, seen[1].first);
    EXPECT_EQ(56u, seen[1].second);
}

TEST(ExpandableRingBufferTest, consumeBacksUpWhenTheConsumerSaysNo)
{
    ExpandableRingBuffer ring;
    EXPECT_TRUE(ring.append(payload(0xA0, 3)));
    EXPECT_TRUE(ring.append(payload(0xA0, 8)));

    const std::size_t refused = ring.consume([](std::span<const std::uint8_t>, std::size_t) { return false; }, 10);
    EXPECT_EQ(0u, refused);
    EXPECT_EQ(32u, ring.size());

    const std::size_t consumed = ring.consume([](std::span<const std::uint8_t>, std::size_t) { return true; }, 1);
    EXPECT_EQ(16u, consumed);
    EXPECT_EQ(16u, ring.size());
}

TEST(ExpandableRingBufferTest, aFixedCapacityRefusesTheFrameItCannotHold)
{
    ExpandableRingBuffer ring(64, 64);
    EXPECT_TRUE(ring.append(payload(0xA0, 24)));
    EXPECT_TRUE(ring.append(payload(0xA0, 24)));
    EXPECT_FALSE(ring.append(payload(0xA0, 24)));
    EXPECT_EQ(64u, ring.size());
    EXPECT_EQ(64u, ring.capacity());
}

TEST(ExpandableRingBufferTest, aFrameThatWouldWrapIsPrecededByPadding)
{
    // Capacity 64. Two frames (32 + 16 bytes) fill 48; consuming the first
    // leaves head 32 and tail 48 with 16 bytes of data. A 24-byte frame does
    // not fit the 16 bytes to the end, but it does fit before head -- so the
    // ring writes a padding frame over [48, 64) and the message at 0.
    ExpandableRingBuffer ring(64, 64);
    EXPECT_TRUE(ring.append(payload(0, 24)));
    EXPECT_TRUE(ring.append(payload(0, 8)));
    const std::size_t consumed = ring.consume([](std::span<const std::uint8_t>, std::size_t) { return true; }, 1);
    EXPECT_EQ(32u, consumed);
    EXPECT_EQ(48u, ring.tail());
    EXPECT_TRUE(ring.append(payload(0, 16)));
    EXPECT_EQ(88u, ring.tail());

    std::vector<std::size_t> lengths;
    ring.for_each(
        0,
        [&lengths](std::span<const std::uint8_t> message, std::size_t)
        {
            lengths.push_back(message.size());
            return true;
        },
        10);
    ASSERT_EQ(2u, lengths.size());
    EXPECT_EQ(8u, lengths[0]);
    EXPECT_EQ(16u, lengths[1]);
}

TEST(ExpandableRingBufferTest, resetZeroStillAcceptsAnAppend)
{
    ExpandableRingBuffer ring;
    EXPECT_TRUE(ring.reset(0));
    EXPECT_EQ(1u, ring.capacity());
    EXPECT_TRUE(ring.append(std::vector<std::uint8_t>{'x'}));
    EXPECT_EQ(16u, ring.size());
}

TEST(ExpandableRingBufferTest, growthKeepsTheQueuedMessages)
{
    ExpandableRingBuffer ring;
    EXPECT_TRUE(ring.append(payload(1, 3)));
    EXPECT_TRUE(ring.append(payload(2, 8)));

    // Force a growth past the first power-of-two capacity.
    EXPECT_TRUE(ring.append(payload(3, 200)));
    EXPECT_GE(ring.capacity(), 256u);

    std::vector<std::uint8_t> starts;
    ring.for_each(
        0,
        [&starts](std::span<const std::uint8_t> message, std::size_t)
        {
            starts.push_back(message[0]);
            return true;
        },
        10);
    ASSERT_EQ(3u, starts.size());
    EXPECT_EQ(1u, starts[0]);
    EXPECT_EQ(2u, starts[1]);
    EXPECT_EQ(3u, starts[2]);
}

}  // namespace
