#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <span>
#include <vector>

#include "core/media/dpdk/stack/arp.h"
#include "core/media/dpdk/stack/frame.h"
#include "core/media/dpdk/stack/stack_params.h"

namespace
{

using axc::core::media::dpdk::arp::asks_about;
using axc::core::media::dpdk::arp::reply_to;
using axc::core::media::dpdk::arp::write_request;
using axc::core::media::dpdk::frame::Checksums;
using axc::core::media::dpdk::frame::Received;
using axc::core::media::dpdk::stack::StackParams;

constexpr std::uint32_t OWN = 0x0a0a0001;
constexpr std::uint32_t PEER = 0x0a0a0002;
constexpr std::array<std::uint8_t, 6> MAC{0x02, 0, 0, 0, 0, 0x01};
constexpr std::array<std::uint8_t, 6> PEER_MAC{0x02, 0, 0, 0, 0, 0x02};

StackParams params()
{
    return StackParams{OWN, 24, MAC};
}

TEST(Arp, ARequestBroadcastsAndAsks)
{
    std::vector<std::uint8_t> bytes(64, 0);
    const auto written = write_request(bytes, params(), PEER);
    ASSERT_TRUE(written.has_value());

    const auto received =
        axc::core::media::dpdk::frame::read(std::span<const std::uint8_t>{bytes}.first(*written), Checksums::software);
    ASSERT_TRUE(received.has_value());
    const auto* packet = std::get_if<axc::core::media::dpdk::frame::ArpPacket>(&*received);
    ASSERT_NE(nullptr, packet);
    EXPECT_EQ(axc::core::media::dpdk::frame::ARP_REQUEST, packet->operation);
    EXPECT_EQ(MAC, packet->sender_hardware);
    EXPECT_EQ(OWN, packet->sender_protocol);
    EXPECT_EQ((std::array<std::uint8_t, 6>{}), packet->target_hardware);
    EXPECT_EQ(PEER, packet->target_protocol);
    EXPECT_FALSE(asks_about(*packet, OWN));
}

TEST(Arp, AReplySwapsTheTwoEnds)
{
    const axc::core::media::dpdk::frame::ArpPacket request{axc::core::media::dpdk::frame::ARP_REQUEST, PEER_MAC, PEER,
                                                           std::array<std::uint8_t, 6>{}, OWN};

    EXPECT_TRUE(asks_about(request, OWN));
    EXPECT_FALSE(asks_about(request, PEER));

    const auto [ethernet, reply] = reply_to(params(), request);
    EXPECT_EQ(PEER_MAC, ethernet.destination);
    EXPECT_EQ(MAC, ethernet.source);
    EXPECT_EQ(axc::core::media::dpdk::frame::ARP_REPLY, reply.operation);
    EXPECT_EQ(MAC, reply.sender_hardware);
    EXPECT_EQ(OWN, reply.sender_protocol);
    EXPECT_EQ(PEER_MAC, reply.target_hardware);
    EXPECT_EQ(PEER, reply.target_protocol);

    std::vector<std::uint8_t> bytes(64, 0);
    const auto written = axc::core::media::dpdk::frame::write_arp(bytes, ethernet, reply);
    ASSERT_TRUE(written.has_value());

    const auto received =
        axc::core::media::dpdk::frame::read(std::span<const std::uint8_t>{bytes}.first(*written), Checksums::software);
    ASSERT_TRUE(received.has_value());
    const auto* read_back = std::get_if<axc::core::media::dpdk::frame::ArpPacket>(&*received);
    ASSERT_NE(nullptr, read_back);
    EXPECT_EQ(reply.operation, read_back->operation);
    EXPECT_EQ(reply.sender_protocol, read_back->sender_protocol);
    EXPECT_FALSE(asks_about(*read_back, OWN));
}

TEST(Arp, ARequestForAnotherAddressIsNotOursToAnswer)
{
    const axc::core::media::dpdk::frame::ArpPacket request{axc::core::media::dpdk::frame::ARP_REQUEST, PEER_MAC, PEER,
                                                           std::array<std::uint8_t, 6>{}, 0x0a0a0009};
    EXPECT_FALSE(asks_about(request, OWN));
}

}  // namespace
