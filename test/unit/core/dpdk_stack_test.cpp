#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "core/media/dpdk/stack/fake_port.h"
#include "core/media/dpdk/stack/frame.h"
#include "core/media/dpdk/stack/stack.h"
#include "core/media/dpdk/stack/transport.h"

namespace
{

using axc::core::media::dpdk::fake_port::FakePort;
using axc::core::media::dpdk::fake_port::Injection;
using axc::core::media::dpdk::fake_port::Side;
using axc::core::media::dpdk::fake_port::Wire;
using axc::core::media::dpdk::frame::Checksums;
using axc::core::media::dpdk::frame::Ipv4Write;
using axc::core::media::dpdk::stack::Mac;
using axc::core::media::dpdk::stack::Stack;
using axc::core::media::dpdk::stack::StackParams;
using axc::core::media::dpdk::transport::Transport;

constexpr std::uint32_t OWN = 0x0a0a0001;
constexpr std::uint32_t PEER = 0x0a0a0002;
constexpr Mac MAC_A{0x02, 0, 0, 0, 0, 0x01};
constexpr Mac MAC_B{0x02, 0, 0, 0, 0, 0x02};

StackParams params(std::uint32_t address, Mac mac)
{
    StackParams result;
    result.address = address;
    result.prefix_length = 24;
    result.mac = mac;
    result.mtu = 1500;
    result.receive_window = 64 * 1024;
    result.rx_burst = 32;
    return result;
}

std::unique_ptr<Stack> make_stack(Wire& wire, Side side, StackParams settings)
{
    auto rx = std::make_unique<FakePort>(wire.port(side, settings.mac, settings.mtu));
    auto sender = std::make_unique<FakePort>(wire.port(side, settings.mac, settings.mtu));
    auto receiver = std::make_unique<FakePort>(wire.port(side, settings.mac, settings.mtu));
    return std::make_unique<Stack>(settings, std::move(rx), std::move(sender), std::move(receiver));
}

axc::core::media::dpdk::ports::Ipv4Endpoint endpoint(std::uint32_t address, std::uint16_t port)
{
    return axc::core::media::dpdk::ports::Ipv4Endpoint{address, port};
}

TEST(Stack, ArpResolvesAndAUdpDatagramArrives)
{
    Wire wire;
    auto a = make_stack(wire, Side::a, params(OWN, MAC_A));
    auto b = make_stack(wire, Side::b, params(PEER, MAC_B));

    const auto binding_a = a->bind(OWN, 5000, std::nullopt, endpoint(OWN, 5000));
    const auto binding_b = b->bind(PEER, 5001, std::nullopt, endpoint(PEER, 5001));
    ASSERT_TRUE(binding_a.has_value());
    ASSERT_TRUE(binding_b.has_value());

    const std::array<std::uint8_t, 4> payload{1, 2, 3, 4};
    EXPECT_EQ(0u, a->send(*binding_a, endpoint(PEER, 5001), payload));  // ARP request first
    EXPECT_EQ(1u, b->pump());
    EXPECT_EQ(1u, a->pump());

    EXPECT_EQ(4u, a->send(*binding_a, endpoint(PEER, 5001), payload));
    EXPECT_EQ(1u, b->pump());

    std::array<std::uint8_t, 64> received{};
    EXPECT_EQ(4u, b->receive(*binding_b, received));
    EXPECT_EQ(payload, (std::array<std::uint8_t, 4>{received[0], received[1], received[2], received[3]}));
}

TEST(Stack, AStaticNeighbourSkipsTheArpExchange)
{
    Wire wire;
    auto settings_a = params(OWN, MAC_A);
    settings_a.static_neighbours.emplace_back(PEER, MAC_B);
    auto a = make_stack(wire, Side::a, settings_a);
    auto b = make_stack(wire, Side::b, params(PEER, MAC_B));

    const auto binding_a = a->bind(OWN, 5000, std::nullopt, endpoint(OWN, 5000));
    const auto binding_b = b->bind(PEER, 5001, std::nullopt, endpoint(PEER, 5001));
    ASSERT_TRUE(binding_a.has_value() && binding_b.has_value());

    const std::array<std::uint8_t, 2> payload{9, 8};
    EXPECT_EQ(2u, a->send(*binding_a, endpoint(PEER, 5001), payload));
    EXPECT_EQ(1u, b->pump());

    std::array<std::uint8_t, 8> received{};
    EXPECT_EQ(2u, b->receive(*binding_b, received));
    EXPECT_EQ(9, received[0]);
    EXPECT_EQ(8, received[1]);
}

TEST(Stack, AnUnclaimedDatagramIsCounted)
{
    Wire wire;
    auto settings_a = params(OWN, MAC_A);
    settings_a.static_neighbours.emplace_back(PEER, MAC_B);
    auto a = make_stack(wire, Side::a, settings_a);
    auto b = make_stack(wire, Side::b, params(PEER, MAC_B));

    const auto binding_a = a->bind(OWN, 5000, std::nullopt, endpoint(OWN, 5000));
    ASSERT_TRUE(binding_a.has_value());

    const std::array<std::uint8_t, 8> payload{0, 0, 0, 0, 0, 0, 1, 0};
    ASSERT_EQ(payload.size(), a->send(*binding_a, endpoint(PEER, 5001), payload));
    EXPECT_EQ(1u, b->pump());
    EXPECT_EQ(1u, b->counters().rx_receiver_discard.load());
}

TEST(Stack, ACorruptFrameIsAChecksumFailure)
{
    Wire wire;
    auto settings_a = params(OWN, MAC_A);
    settings_a.static_neighbours.emplace_back(PEER, MAC_B);
    auto a = make_stack(wire, Side::a, settings_a);
    auto b = make_stack(wire, Side::b, params(PEER, MAC_B));

    const auto binding_a = a->bind(OWN, 5000, std::nullopt, endpoint(OWN, 5000));
    ASSERT_TRUE(binding_a.has_value());

    wire.set_injection(Side::a, Injection{std::nullopt, std::nullopt, false, 1, std::nullopt});
    const std::array<std::uint8_t, 4> payload{1, 2, 3, 4};
    ASSERT_EQ(4u, a->send(*binding_a, endpoint(PEER, 5001), payload));
    EXPECT_EQ(1u, b->pump());
    EXPECT_EQ(1u, b->counters().checksum_failure.load());
}

TEST(Stack, AConnectedTransportHearsOnlyItsPeer)
{
    Wire wire;
    auto settings_a = params(OWN, MAC_A);
    settings_a.static_neighbours.emplace_back(PEER, MAC_B);
    auto a = make_stack(wire, Side::a, settings_a);
    auto b = make_stack(wire, Side::b, params(PEER, MAC_B));

    const auto binding_a = a->bind(OWN, 5000, std::nullopt, endpoint(OWN, 5000));
    const auto binding_b = b->bind(PEER, 5001, endpoint(OWN, 5000), endpoint(PEER, 5001));
    ASSERT_TRUE(binding_a.has_value() && binding_b.has_value());

    const std::array<std::uint8_t, 3> payload{1, 2, 3};
    ASSERT_EQ(3u, a->send(*binding_a, endpoint(PEER, 5001), payload));
    EXPECT_EQ(1u, b->pump());
    std::array<std::uint8_t, 16> buffer{};
    EXPECT_EQ(3u, b->receive(*binding_b, buffer));

    // A datagram from another source port is somebody else's.
    const auto port_9999 = a->bind(OWN, 9999, std::nullopt, endpoint(OWN, 9999));
    ASSERT_TRUE(port_9999.has_value());
    ASSERT_EQ(3u, a->send(*port_9999, endpoint(PEER, 5001), payload));
    EXPECT_EQ(1u, b->pump());
    EXPECT_EQ(0u, b->receive(*binding_b, buffer));
}

TEST(Stack, AnEchoRequestIsAnswered)
{
    Wire wire;
    auto a = make_stack(wire, Side::a, params(OWN, MAC_A));

    const std::array<std::uint8_t, 4> icmp_payload{'p', 'i', 'n', 'g'};
    const axc::core::media::dpdk::frame::IcmpEcho request{axc::core::media::dpdk::frame::ICMP_ECHO_REQUEST, 0x1234, 7,
                                                          icmp_payload};
    const Ipv4Write from_peer{MAC_A, MAC_B, PEER, OWN, 3};
    std::vector<std::uint8_t> frame_bytes(14 + 20 + 8 + icmp_payload.size());
    const auto written = axc::core::media::dpdk::frame::write_icmp_echo(
        frame_bytes, from_peer, request, axc::core::media::dpdk::frame::ICMP_ECHO_REQUEST);
    ASSERT_TRUE(written.has_value());
    wire.send(Side::b, std::span<const std::uint8_t>{frame_bytes}.first(*written));

    EXPECT_EQ(1u, a->pump());
    const auto answers = wire.take(Side::b, 10);
    ASSERT_EQ(1u, answers.size());

    const auto parsed = axc::core::media::dpdk::frame::read(answers.front(), Checksums::software);
    ASSERT_TRUE(parsed.has_value());
    const auto* echo = std::get_if<axc::core::media::dpdk::frame::EchoReceived>(&*parsed);
    ASSERT_NE(nullptr, echo);
    EXPECT_EQ(axc::core::media::dpdk::frame::ICMP_ECHO_REPLY, echo->echo.echo_type);
    EXPECT_EQ(0x1234, echo->echo.identifier);
    EXPECT_EQ(7, echo->echo.sequence);
    EXPECT_EQ("ping",
              std::string_view(reinterpret_cast<const char*>(echo->echo.payload.data()), echo->echo.payload.size()));
}

TEST(Stack, AReleasedBindingDoesNotReceiveAgain)
{
    Wire wire;
    auto settings_a = params(OWN, MAC_A);
    settings_a.static_neighbours.emplace_back(PEER, MAC_B);
    auto a = make_stack(wire, Side::a, settings_a);

    const auto first = a->bind(OWN, 5000, std::nullopt, endpoint(OWN, 5000));
    ASSERT_TRUE(first.has_value());
    a->release(*first);
    std::array<std::uint8_t, 8> buffer{};
    EXPECT_EQ(0u, a->receive(*first, buffer));

    const auto second = a->bind(OWN, 5000, std::nullopt, endpoint(OWN, 5000));
    ASSERT_TRUE(second.has_value());
    EXPECT_GT(second->generation, first->generation);
}

TEST(Stack, ATransportSendsAndReceives)
{
    Wire wire;
    auto settings_a = params(OWN, MAC_A);
    settings_a.static_neighbours.emplace_back(PEER, MAC_B);
    auto a = make_stack(wire, Side::a, settings_a);
    auto b = make_stack(wire, Side::b, params(PEER, MAC_B));

    const auto binding_a = a->bind(OWN, 5000, std::nullopt, endpoint(OWN, 5000));
    const auto binding_b = b->bind(PEER, 5001, std::nullopt, endpoint(PEER, 5001));
    ASSERT_TRUE(binding_a.has_value() && binding_b.has_value());

    Transport sender{*a, *binding_a, endpoint(PEER, 5001)};
    Transport receiver{*b, *binding_b};

    const std::array<std::uint8_t, 4> payload{4, 3, 2, 1};
    EXPECT_EQ(4u, sender.send(payload));
    EXPECT_EQ(1u, b->pump());

    std::array<std::uint8_t, 16> buffer{};
    EXPECT_EQ(4u, receiver.receive(buffer));
    EXPECT_EQ(4, buffer[0]);
    EXPECT_EQ(1, buffer[3]);
    EXPECT_EQ(5000, sender.local_port());
    EXPECT_EQ(OWN, sender.local_address());
}

}  // namespace
