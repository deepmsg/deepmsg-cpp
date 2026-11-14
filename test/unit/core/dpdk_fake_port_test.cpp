#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <utility>
#include <vector>

#include "core/media/dpdk/stack/fake_port.h"
#include "core/media/dpdk/stack/packet_io.h"
#include "core/media/dpdk/stack/ports.h"

namespace
{

using axc::core::media::dpdk::fake_port::FakePort;
using axc::core::media::dpdk::fake_port::Injection;
using axc::core::media::dpdk::fake_port::Side;
using axc::core::media::dpdk::fake_port::Wire;
using axc::core::media::dpdk::frame::TxOffloads;
using axc::core::media::dpdk::packet_io::Disposition;
using axc::core::media::dpdk::packet_io::ReadyFrame;
using axc::core::media::dpdk::packet_io::RxFrame;
using axc::core::media::dpdk::packet_io::RxPort;
using axc::core::media::dpdk::packet_io::TxPort;
using axc::core::media::dpdk::ports::Binding;

constexpr std::array<std::uint8_t, 6> MAC_A{0x02, 0, 0, 0, 0, 0x01};
constexpr std::array<std::uint8_t, 6> MAC_B{0x02, 0, 0, 0, 0, 0x02};

Disposition consume_all(void* /*context*/, const RxFrame& /*frame*/)
{
    return Disposition::consumed();
}

Disposition keep_all(void* context, const RxFrame& /*frame*/)
{
    return Disposition::keep_for(*static_cast<Binding*>(context));
}

struct Fill
{
    std::vector<std::uint8_t> bytes;
};

std::pair<std::size_t, TxOffloads> fill_with(void* context, std::span<std::uint8_t> buffer)
{
    const auto& fill = *static_cast<Fill*>(context);
    if (fill.bytes.size() > buffer.size())
    {
        return {fill.bytes.size(), TxOffloads::none()};
    }
    std::copy(fill.bytes.begin(), fill.bytes.end(), buffer.begin());
    return {fill.bytes.size(), TxOffloads::none()};
}

TEST(FakePort, WhatOneSideSendsTheOtherSideTakes)
{
    Wire wire;
    const std::vector<std::uint8_t> frame{1, 2, 3};

    wire.send(Side::a, frame);
    EXPECT_EQ(1u, wire.queued(Side::a));
    EXPECT_EQ(1u, wire.carried(Side::a));

    const auto taken = wire.take(Side::b, 10);
    ASSERT_EQ(1u, taken.size());
    EXPECT_EQ(frame, taken.front());
    EXPECT_EQ(0u, wire.queued(Side::a));
}

TEST(FakePort, EveryNthFrameIsDropped)
{
    Wire wire;
    wire.set_injection(Side::a, Injection{2, std::nullopt, false, std::nullopt, std::nullopt});
    for (std::uint8_t index = 1; index <= 4; ++index)
    {
        wire.send(Side::a, std::vector<std::uint8_t>{index});
    }

    const auto taken = wire.take(Side::b, 10);
    ASSERT_EQ(2u, taken.size());
    EXPECT_EQ((std::vector<std::uint8_t>{1}), taken[0]);
    EXPECT_EQ((std::vector<std::uint8_t>{3}), taken[1]);
}

TEST(FakePort, EveryNthFrameIsDeliveredTwice)
{
    Wire wire;
    wire.set_injection(Side::a, Injection{std::nullopt, 2, false, std::nullopt, std::nullopt});
    wire.send(Side::a, std::vector<std::uint8_t>{1});
    wire.send(Side::a, std::vector<std::uint8_t>{2});

    const auto taken = wire.take(Side::b, 10);
    ASSERT_EQ(3u, taken.size());
    EXPECT_EQ((std::vector<std::uint8_t>{1}), taken[0]);
    EXPECT_EQ((std::vector<std::uint8_t>{2}), taken[1]);
    EXPECT_EQ((std::vector<std::uint8_t>{2}), taken[2]);
}

TEST(FakePort, ReorderingSwapsTwoQueuedFrames)
{
    Wire wire;
    wire.set_injection(Side::a, Injection{std::nullopt, std::nullopt, true, std::nullopt, std::nullopt});
    wire.send(Side::a, std::vector<std::uint8_t>{1});
    wire.send(Side::a, std::vector<std::uint8_t>{2});

    const auto taken = wire.take(Side::b, 10);
    ASSERT_EQ(2u, taken.size());
    EXPECT_EQ((std::vector<std::uint8_t>{2}), taken[0]);
    EXPECT_EQ((std::vector<std::uint8_t>{1}), taken[1]);
}

TEST(FakePort, CorruptionFlipsTheLastByte)
{
    Wire wire;
    wire.set_injection(Side::a, Injection{std::nullopt, std::nullopt, false, 1, std::nullopt});
    wire.send(Side::a, std::vector<std::uint8_t>{1, 2, 3});

    const auto taken = wire.take(Side::b, 10);
    ASSERT_EQ(1u, taken.size());
    EXPECT_EQ((std::vector<std::uint8_t>{1, 2, static_cast<std::uint8_t>(3 ^ 0xff)}), taken.front());
}

TEST(FakePort, ACapacityHoldsFramesBackRatherThanDroppingThem)
{
    Wire wire;
    wire.set_injection(Side::a, Injection{std::nullopt, std::nullopt, false, std::nullopt, 1});
    for (std::uint8_t index = 1; index <= 3; ++index)
    {
        wire.send(Side::a, std::vector<std::uint8_t>{index});
    }

    auto port = wire.port(Side::b, MAC_B, 1500);
    std::vector<std::pair<Binding, ReadyFrame>> kept;
    EXPECT_EQ(1u, port.rx_burst(10, consume_all, nullptr, kept));
    EXPECT_EQ(2u, wire.queued(Side::a));
    EXPECT_EQ(1u, port.rx_burst(10, consume_all, nullptr, kept));
    EXPECT_EQ(1u, wire.queued(Side::a));
}

TEST(FakePort, AKeptFrameCarriesItsBytesAndBinding)
{
    Wire wire;
    auto sender = wire.port(Side::a, MAC_A, 1500);
    auto receiver = wire.port(Side::b, MAC_B, 1500);
    Binding binding{3, 5000, 7};

    Fill fill{std::vector<std::uint8_t>{9, 8, 7, 6}};
    ASSERT_EQ(1u, sender.tx_burst(1, fill_with, &fill));

    std::vector<std::pair<Binding, ReadyFrame>> kept;
    EXPECT_EQ(1u, receiver.rx_burst(1, keep_all, &binding, kept));
    ASSERT_EQ(1u, kept.size());
    EXPECT_EQ(binding, kept.front().first);
    EXPECT_EQ(fill.bytes,
              std::vector<std::uint8_t>(kept.front().second.as_bytes().begin(), kept.front().second.as_bytes().end()));
}

TEST(FakePort, AConsumedFrameIsNotKept)
{
    Wire wire;
    auto sender = wire.port(Side::a, MAC_A, 1500);
    auto receiver = wire.port(Side::b, MAC_B, 1500);

    Fill fill{std::vector<std::uint8_t>{1, 2, 3}};
    ASSERT_EQ(1u, sender.tx_burst(1, fill_with, &fill));

    std::vector<std::pair<Binding, ReadyFrame>> kept;
    EXPECT_EQ(1u, receiver.rx_burst(1, consume_all, nullptr, kept));
    EXPECT_TRUE(kept.empty());
}

TEST(FakePort, AFrameLongerThanThePortIsNotTaken)
{
    Wire wire;
    auto sender = wire.port(Side::a, MAC_A, 8);
    Fill fill{std::vector<std::uint8_t>(9, 0xaa)};

    EXPECT_EQ(0u, sender.tx_burst(1, fill_with, &fill));
    EXPECT_EQ(0u, wire.queued(Side::a));
}

TEST(FakePort, AWireComputesNothing)
{
    Wire wire;
    auto port = wire.port(Side::a, MAC_A, 1500);
    EXPECT_EQ(axc::core::media::dpdk::frame::L4Checksum::software, port.checksums());
    EXPECT_FALSE(port.offloads().judges_both());
    EXPECT_EQ(MAC_A, port.mac());
    EXPECT_EQ(1500u, port.mtu());
}

}  // namespace
