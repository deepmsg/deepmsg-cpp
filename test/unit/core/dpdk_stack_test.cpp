#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
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

/// A transmit port whose pool is always empty: the only way a test reaches
/// counter 77, because a wire allocates nothing and a real port starves only
/// under load.
class StarvedTxPort : public axc::core::media::dpdk::packet_io::TxPort
{
public:
    explicit StarvedTxPort(std::size_t mtu) : mtu_{mtu} {}

    axc::core::media::dpdk::packet_io::TxResult tx_burst(std::size_t count,
                                                         axc::core::media::dpdk::packet_io::TxFill /*fill*/,
                                                         void* /*context*/) override
    {
        return axc::core::media::dpdk::packet_io::TxResult{0, count};
    }

    [[nodiscard]] axc::core::media::dpdk::frame::L4Checksum checksums() const noexcept override
    {
        return axc::core::media::dpdk::frame::L4Checksum::software;
    }
    [[nodiscard]] Mac mac() const noexcept override
    {
        return MAC_A;
    }
    [[nodiscard]] std::size_t mtu() const noexcept override
    {
        return mtu_;
    }

private:
    std::size_t mtu_;
};

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
    // A real port's capacity is a frame's: the MTU plus the Ethernet header it
    // does not count. The wire is handed the frame capacity and the stack the
    // MTU, which is the same split a NIC makes.
    const std::size_t frame_capacity = settings.mtu + axc::core::media::dpdk::frame::ETHERNET_HEADER_LENGTH;
    auto rx = std::make_unique<FakePort>(wire.port(side, settings.mac, frame_capacity));
    auto sender = std::make_unique<FakePort>(wire.port(side, settings.mac, frame_capacity));
    auto receiver = std::make_unique<FakePort>(wire.port(side, settings.mac, frame_capacity));
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
    EXPECT_EQ(0u, *a->send(*binding_a, endpoint(PEER, 5001), payload));  // ARP request first
    EXPECT_EQ(1u, b->pump());
    EXPECT_EQ(1u, a->pump());

    EXPECT_EQ(4u, *a->send(*binding_a, endpoint(PEER, 5001), payload));
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
    EXPECT_EQ(2u, *a->send(*binding_a, endpoint(PEER, 5001), payload));
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
    ASSERT_EQ(payload.size(), *a->send(*binding_a, endpoint(PEER, 5001), payload));
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
    ASSERT_EQ(4u, *a->send(*binding_a, endpoint(PEER, 5001), payload));
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
    ASSERT_EQ(3u, *a->send(*binding_a, endpoint(PEER, 5001), payload));
    EXPECT_EQ(1u, b->pump());
    std::array<std::uint8_t, 16> buffer{};
    EXPECT_EQ(3u, b->receive(*binding_b, buffer));

    // A datagram from another source port is somebody else's.
    const auto port_9999 = a->bind(OWN, 9999, std::nullopt, endpoint(OWN, 9999));
    ASSERT_TRUE(port_9999.has_value());
    ASSERT_EQ(3u, *a->send(*port_9999, endpoint(PEER, 5001), payload));
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

TEST(Stack, ADatagramToThisStacksOwnAddressLoopsInsideIt)
{
    Wire wire;
    auto a = make_stack(wire, Side::a, params(OWN, MAC_A));

    const auto sender = a->bind(OWN, 5000, std::nullopt, endpoint(OWN, 5000));
    const auto receiver = a->bind(OWN, 5001, std::nullopt, endpoint(OWN, 5001));
    ASSERT_TRUE(sender.has_value() && receiver.has_value());

    const std::array<std::uint8_t, 4> payload{7, 6, 5, 4};
    EXPECT_EQ(4u, *a->send(*sender, endpoint(OWN, 5001), payload));

    // Nothing went on the wire: the receiver's ring already has it.
    EXPECT_TRUE(wire.take(Side::a, 10).empty());

    std::array<std::uint8_t, 8> received{};
    EXPECT_EQ(4u, a->receive(*receiver, received));
    EXPECT_EQ(payload, (std::array<std::uint8_t, 4>{received[0], received[1], received[2], received[3]}));
}

TEST(Stack, AnEmptyPoolIsCounter77AndNotCounter78)
{
    Wire wire;
    auto settings = params(OWN, MAC_A);
    settings.static_neighbours.emplace_back(PEER, MAC_B);

    const std::size_t frame_capacity = settings.mtu + axc::core::media::dpdk::frame::ETHERNET_HEADER_LENGTH;
    auto rx = std::make_unique<FakePort>(wire.port(Side::a, MAC_A, frame_capacity));
    auto sender = std::make_unique<StarvedTxPort>(settings.mtu);
    auto receiver = std::make_unique<FakePort>(wire.port(Side::a, MAC_A, frame_capacity));
    Stack a{settings, std::move(rx), std::move(sender), std::move(receiver)};

    const auto binding = a.bind(OWN, 5000, std::nullopt, endpoint(OWN, 5000));
    ASSERT_TRUE(binding.has_value());

    const std::array<std::uint8_t, 3> payload{1, 2, 3};
    const auto sent = a.send(*binding, endpoint(PEER, 5001), payload);
    ASSERT_TRUE(sent.has_value()) << sent.error().message.view();
    EXPECT_EQ(0u, *sent);
    EXPECT_EQ(1u, a.counters().nobufs.load());
    EXPECT_EQ(0u, a.counters().tx_eagain.load());
}

TEST(Stack, AScatteredPayloadLeavesAsTheSameFrame)
{
    // Two fresh stacks, so the only difference between the frames they send is
    // the shape the payload arrived in (the IPv4 identification counts up per
    // send, so the two sends have to start from the same state).
    const std::array<std::uint8_t, 7> payload{1, 2, 3, 4, 5, 6, 7};
    auto settings = params(OWN, MAC_A);
    settings.static_neighbours.emplace_back(PEER, MAC_B);

    Wire single_wire;
    auto single_stack = make_stack(single_wire, Side::a, settings);
    make_stack(single_wire, Side::b, params(PEER, MAC_B));
    const auto single_binding = single_stack->bind(OWN, 5000, std::nullopt, endpoint(OWN, 5000));
    ASSERT_TRUE(single_binding.has_value());
    ASSERT_EQ(7u, *single_stack->send(*single_binding, endpoint(PEER, 5001), payload));
    const auto single = single_wire.take(Side::b, 1);
    ASSERT_EQ(1u, single.size());

    Wire scattered_wire;
    auto scattered_stack = make_stack(scattered_wire, Side::a, settings);
    make_stack(scattered_wire, Side::b, params(PEER, MAC_B));
    const auto scattered_binding = scattered_stack->bind(OWN, 5000, std::nullopt, endpoint(OWN, 5000));
    ASSERT_TRUE(scattered_binding.has_value());
    const std::span<const std::uint8_t> parts[]{std::span<const std::uint8_t>{payload}.first(2),
                                                std::span<const std::uint8_t>{payload}.subspan(2, 3),
                                                std::span<const std::uint8_t>{payload}.subspan(5)};
    ASSERT_EQ(7u, *scattered_stack->send(*scattered_binding, endpoint(PEER, 5001), parts));
    const auto scattered = scattered_wire.take(Side::b, 1);
    ASSERT_EQ(1u, scattered.size());

    EXPECT_EQ(single.front(), scattered.front()) << "three pieces, one frame, byte for byte the same";
    EXPECT_EQ(0u, scattered_stack->counters().errors.load()) << "a scattered send is not a refusal";
}

TEST(Stack, TheLargestDatagramLeavesAndOneByteMoreDoesNot)
{
    Wire wire;
    auto settings_a = params(OWN, MAC_A);
    settings_a.static_neighbours.emplace_back(PEER, MAC_B);
    auto a = make_stack(wire, Side::a, settings_a);
    auto b = make_stack(wire, Side::b, params(PEER, MAC_B));

    const auto binding_a = a->bind(OWN, 5000, std::nullopt, endpoint(OWN, 5000));
    const auto binding_b = b->bind(PEER, 5001, std::nullopt, endpoint(PEER, 5001));
    ASSERT_TRUE(binding_a.has_value() && binding_b.has_value());

    // The MTU counts IPv4 and UDP but not Ethernet: 1500 - 28.
    ASSERT_EQ(1472u, a->params().largest_datagram());

    const std::vector<std::uint8_t> largest(1472, 0x5a);
    EXPECT_EQ(1472u, *a->send(*binding_a, endpoint(PEER, 5001), largest));
    EXPECT_EQ(1u, b->pump());

    std::vector<std::uint8_t> received(largest.size() + 1);
    EXPECT_EQ(1472u, b->receive(*binding_b, received));

    // One byte more is refused before a frame is built, and it is an error and
    // not back pressure: no later pass makes it fit.
    const std::vector<std::uint8_t> too_large(1473, 0x5a);
    const auto refused = a->send(*binding_a, endpoint(PEER, 5001), too_large);
    ASSERT_FALSE(refused.has_value());
    EXPECT_NE(std::string::npos, std::string{refused.error().message.view()}.find("larger than the port allows"));
    EXPECT_EQ(1u, a->counters().errors.load());
    EXPECT_EQ(0u, a->counters().tx_eagain.load());
    EXPECT_EQ(0u, b->pump());

    // The frame bound itself is the MTU plus the Ethernet header: a full-size
    // echo request has to be answered, and its answer is 1514 bytes.
    const std::vector<std::uint8_t> icmp_payload(1472, 'p');
    const axc::core::media::dpdk::frame::IcmpEcho request{axc::core::media::dpdk::frame::ICMP_ECHO_REQUEST, 0x1234, 9,
                                                          icmp_payload};
    const Ipv4Write from_peer{MAC_A, MAC_B, PEER, OWN, 4};
    std::vector<std::uint8_t> frame_bytes(1472 + 42);
    const auto written = axc::core::media::dpdk::frame::write_icmp_echo(
        frame_bytes, from_peer, request, axc::core::media::dpdk::frame::ICMP_ECHO_REQUEST);
    ASSERT_TRUE(written.has_value());
    EXPECT_EQ(1514u, *written);
    wire.send(Side::b, std::span<const std::uint8_t>{frame_bytes}.first(*written));
    EXPECT_EQ(1u, a->pump());
    EXPECT_EQ(1u, wire.take(Side::b, 10).size());
}

/// A transmit port with room for `budget` frames in all: what it takes comes
/// off the budget, and once that is gone it refuses, which is how a NIC answers
/// while its ring waits for room.
class PartlyRefusingTxPort : public axc::core::media::dpdk::packet_io::TxPort
{
public:
    PartlyRefusingTxPort(std::size_t budget, std::size_t mtu) : budget_{budget}, mtu_{mtu} {}

    axc::core::media::dpdk::packet_io::TxResult tx_burst(std::size_t count,
                                                         axc::core::media::dpdk::packet_io::TxFill fill,
                                                         void* context) override
    {
        std::size_t taken = 0;
        for (; taken < count && taken < budget_; ++taken)
        {
            std::array<std::uint8_t, 2048> frame{};
            const auto [length, offloads] = fill(context, frame);
            EXPECT_GT(length, 0u);
            EXPECT_FALSE(offloads.compute) << "the in-memory wire computes no checksums";
        }
        budget_ -= taken;
        return axc::core::media::dpdk::packet_io::TxResult{taken, 0};
    }

    [[nodiscard]] axc::core::media::dpdk::frame::L4Checksum checksums() const noexcept override
    {
        return axc::core::media::dpdk::frame::L4Checksum::software;
    }
    [[nodiscard]] Mac mac() const noexcept override
    {
        return MAC_A;
    }
    [[nodiscard]] std::size_t mtu() const noexcept override
    {
        return mtu_;
    }

private:
    std::size_t budget_;
    std::size_t mtu_;
};

// The reference's `send_func` contract: one iov is one datagram, and the return
// is how many of them went (`media/aeron_udp_channel_transport.c:728-773`). A
// batch flattened into one datagram would read back as a short send and hold
// the sender's position where it was (`aeron_network_publication.c:495-580`).
TEST(Stack, EveryDatagramOfASendIsItsOwnFrameOnTheWire)
{
    Wire wire;
    auto settings_a = params(OWN, MAC_A);
    settings_a.static_neighbours.emplace_back(PEER, MAC_B);
    auto a = make_stack(wire, Side::a, settings_a);
    auto b = make_stack(wire, Side::b, params(PEER, MAC_B));

    const auto binding_a = a->bind(OWN, 5000, std::nullopt, endpoint(OWN, 5000));
    const auto binding_b = b->bind(PEER, 5001, std::nullopt, endpoint(PEER, 5001));
    ASSERT_TRUE(binding_a.has_value());
    ASSERT_TRUE(binding_b.has_value());

    const std::array<std::uint8_t, 4> first{1, 2, 3, 4};
    const std::array<std::uint8_t, 3> second{5, 6, 7};
    const std::array<std::uint8_t, 2> third{8, 9};
    const std::span<const std::uint8_t> datagrams[]{first, second, third};

    const auto sent = a->send_datagrams(*binding_a, endpoint(PEER, 5001), datagrams);
    ASSERT_TRUE(sent.has_value());
    EXPECT_EQ(3u, sent->datagrams);
    EXPECT_EQ(9, sent->bytes);

    EXPECT_EQ(3u, b->pump());

    std::array<std::uint8_t, 64> received{};
    EXPECT_EQ(4u, b->receive(*binding_b, received));
    EXPECT_EQ(1u, received[0]);
    EXPECT_EQ(4u, received[3]);
    EXPECT_EQ(3u, b->receive(*binding_b, received));
    EXPECT_EQ(5u, received[0]);
    EXPECT_EQ(7u, received[2]);
    EXPECT_EQ(2u, b->receive(*binding_b, received));
    EXPECT_EQ(8u, received[0]);
    EXPECT_EQ(9u, received[1]);
    EXPECT_EQ(0u, b->receive(*binding_b, received));
}

// What the port refuses stays the caller's to send: the answer is the count it
// took, in order, and the bytes are those datagrams' only.
TEST(Stack, ASendAnswersWhatThePortTookAndStopsThere)
{
    Wire wire;
    auto settings = params(OWN, MAC_A);
    settings.static_neighbours.emplace_back(PEER, MAC_B);

    const std::size_t frame_capacity = settings.mtu + axc::core::media::dpdk::frame::ETHERNET_HEADER_LENGTH;
    auto rx = std::make_unique<FakePort>(wire.port(Side::a, MAC_A, frame_capacity));
    auto sender = std::make_unique<PartlyRefusingTxPort>(2, settings.mtu);
    auto receiver = std::make_unique<FakePort>(wire.port(Side::a, MAC_A, frame_capacity));
    Stack a{settings, std::move(rx), std::move(sender), std::move(receiver)};

    const auto binding_a = a.bind(OWN, 5000, std::nullopt, endpoint(OWN, 5000));
    ASSERT_TRUE(binding_a.has_value());

    const std::array<std::uint8_t, 4> first{1, 2, 3, 4};
    const std::array<std::uint8_t, 3> second{5, 6, 7};
    const std::array<std::uint8_t, 2> third{8, 9};
    const std::span<const std::uint8_t> datagrams[]{first, second, third};

    const auto sent = a.send_datagrams(*binding_a, endpoint(PEER, 5001), datagrams);
    ASSERT_TRUE(sent.has_value());
    EXPECT_EQ(2u, sent->datagrams);
    EXPECT_EQ(7, sent->bytes);
}

}  // namespace
