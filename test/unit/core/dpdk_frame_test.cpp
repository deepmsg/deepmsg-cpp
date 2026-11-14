#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>
#include <vector>

#include "core/media/dpdk/stack/frame.h"

namespace
{

using namespace axc::core::media::dpdk::frame;

constexpr std::string_view GOLDEN_PAYLOAD{"deepmsg golden ipv4/udp frame!"};

std::vector<std::uint8_t> load_golden(const char* name)
{
    const std::string path = std::string{AXC_GOLDEN_DIR} + "/dpdk/frame/" + name;
    std::ifstream file{path, std::ios::binary};
    if (!file)
    {
        return {};
    }
    const std::vector<char> raw{std::istreambuf_iterator<char>{file}, std::istreambuf_iterator<char>{}};
    return {raw.begin(), raw.end()};
}

std::string_view as_string(std::span<const std::uint8_t> bytes)
{
    return {reinterpret_cast<const char*>(bytes.data()), bytes.size()};
}

std::uint64_t test_partial_sum(std::span<const std::uint8_t> bytes)
{
    std::uint64_t sum = 0;
    std::size_t index = 0;
    for (; index + 1 < bytes.size(); index += 2)
    {
        sum += (static_cast<std::uint64_t>(bytes[index]) << 8) | bytes[index + 1];
    }
    if (index < bytes.size())
    {
        sum += static_cast<std::uint64_t>(bytes[index]) << 8;
    }
    return sum;
}

std::uint16_t test_folded(std::uint64_t sum)
{
    while (sum > 0xffff)
    {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    return static_cast<std::uint16_t>(sum);
}

UdpWrite golden_udp_write(std::size_t payload_length, std::uint16_t identification)
{
    return UdpWrite{
        Ipv4Write{{0x02, 0, 0, 0, 0, 0x02}, {0x02, 0, 0, 0, 0, 0x01}, 0x0a4d0001, 0x0a4d0002, identification}, 12001,
        10999, payload_length};
}

}  // namespace

TEST(Frame, InternetChecksumMatchesRfc1071sExample)
{
    const std::array<std::uint8_t, 8> bytes{0x00, 0x01, 0xf2, 0x03, 0xf4, 0xf5, 0xf6, 0xf7};
    EXPECT_EQ(0x220d, internet_checksum(bytes));
}

TEST(Frame, OddTrailingByteIsTheHighHalfOfItsWord)
{
    const std::array<std::uint8_t, 3> odd{0x11, 0x22, 0x33};
    const std::array<std::uint8_t, 4> padded{0x11, 0x22, 0x33, 0x00};
    const std::array<std::uint8_t, 4> wrong{0x11, 0x22, 0x00, 0x33};

    EXPECT_EQ(internet_checksum(padded), internet_checksum(odd));
    EXPECT_NE(internet_checksum(wrong), internet_checksum(odd));
}

TEST(Frame, GoldenIsReproducedByteForByte)
{
    auto golden = load_golden("ipv4-udp.bin");
    ASSERT_EQ(IPV4_UDP_HEADER_LENGTH + GOLDEN_PAYLOAD.size(), golden.size());

    const auto received = read(golden, Checksums::software);
    ASSERT_TRUE(received.has_value());
    const auto* udp = std::get_if<UdpReceived>(&*received);
    ASSERT_NE(nullptr, udp);
    EXPECT_EQ(GOLDEN_PAYLOAD, as_string(udp->datagram.payload));

    const auto ethernet = read_ethernet_header(golden);
    ASSERT_TRUE(ethernet.has_value());

    auto rebuilt = golden;
    std::copy(udp->datagram.payload.begin(), udp->datagram.payload.end(), rebuilt.begin() + IPV4_UDP_HEADER_LENGTH);

    const auto written = write_ipv4_udp_here(
        rebuilt, UdpWrite{Ipv4Write{ethernet->destination, ethernet->source, udp->ipv4.source, udp->ipv4.destination,
                                    udp->ipv4.identification},
                          udp->datagram.source_port, udp->datagram.destination_port, udp->datagram.payload.size()});
    ASSERT_TRUE(written.has_value());
    EXPECT_EQ(golden.size(), *written);
    EXPECT_EQ(golden, rebuilt);
}

TEST(Frame, GoldenCarriesTheFieldsTheRecipeSaysItDoes)
{
    auto golden = load_golden("ipv4-udp.bin");
    ASSERT_FALSE(golden.empty());

    const auto ethernet = read_ethernet_header(golden);
    ASSERT_TRUE(ethernet.has_value());
    EXPECT_EQ((std::array<std::uint8_t, 6>{0x02, 0, 0, 0, 0, 0x02}), ethernet->destination);
    EXPECT_EQ((std::array<std::uint8_t, 6>{0x02, 0, 0, 0, 0, 0x01}), ethernet->source);
    EXPECT_EQ(ETHERNET_TYPE_IPV4, ethernet->ether_type);

    const auto ipv4 = read_ipv4_header(std::span<const std::uint8_t>{golden}.subspan(ETHERNET_HEADER_LENGTH));
    ASSERT_TRUE(ipv4.has_value());
    EXPECT_EQ(58, ipv4->total_length);
    EXPECT_EQ(0x4000, ipv4->flags_and_fragment_offset);
    EXPECT_EQ(DEFAULT_TTL, ipv4->ttl);
    EXPECT_EQ(IP_PROTOCOL_UDP, ipv4->protocol);
    EXPECT_EQ(0x0a4d0001u, ipv4->source);
    EXPECT_EQ(0x0a4d0002u, ipv4->destination);

    const auto received = read(golden, Checksums::software);
    ASSERT_TRUE(received.has_value());
    const auto* udp = std::get_if<UdpReceived>(&*received);
    ASSERT_NE(nullptr, udp);
    EXPECT_EQ(42001, udp->datagram.source_port);
    EXPECT_EQ(40999, udp->datagram.destination_port);
    EXPECT_EQ(38, udp->datagram.length);

    EXPECT_TRUE(ipv4_checksum_is_valid(
        std::span<const std::uint8_t>{golden}.subspan(ETHERNET_HEADER_LENGTH, IPV4_HEADER_LENGTH)));
    EXPECT_TRUE(udp_checksum_is_valid(udp->ipv4.source, udp->ipv4.destination,
                                      std::span<const std::uint8_t>{golden}.subspan(
                                          ETHERNET_HEADER_LENGTH + IPV4_HEADER_LENGTH, udp->ipv4.payload_length())));
}

TEST(Frame, GoldenArpRequestIsReproducedByteForByte)
{
    auto golden = load_golden("arp-request.bin");
    ASSERT_EQ(ETHERNET_HEADER_LENGTH + ARP_PACKET_LENGTH, golden.size());

    const auto received = read(golden, Checksums::software);
    ASSERT_TRUE(received.has_value());
    const auto* packet = std::get_if<ArpPacket>(&*received);
    ASSERT_NE(nullptr, packet);
    EXPECT_EQ(ARP_REQUEST, packet->operation);
    EXPECT_EQ(0x0a4d0001u, packet->sender_protocol);
    EXPECT_EQ(0x0a4d0003u, packet->target_protocol);
    EXPECT_EQ((std::array<std::uint8_t, 6>{}), packet->target_hardware);

    const auto ethernet = read_ethernet_header(golden);
    ASSERT_TRUE(ethernet.has_value());
    EXPECT_EQ(BROADCAST, ethernet->destination);

    auto rebuilt = golden;
    const auto written = write_arp(rebuilt, *ethernet, *packet);
    ASSERT_TRUE(written.has_value());
    EXPECT_EQ(golden.size(), *written);
    EXPECT_EQ(golden, rebuilt);
}

TEST(Frame, AFrameWrittenForTheNicVerifiesOnceTheNicComputesIt)
{
    constexpr std::string_view payload_text{"the nic computes what is in this field"};
    const std::span<const std::uint8_t> payload{reinterpret_cast<const std::uint8_t*>(payload_text.data()),
                                                payload_text.size()};
    const auto write = golden_udp_write(payload.size(), 7);

    std::vector<std::uint8_t> here(IPV4_UDP_HEADER_LENGTH + payload.size());
    std::copy(payload.begin(), payload.end(), here.begin() + IPV4_UDP_HEADER_LENGTH);
    const auto written_here = write_ipv4_udp(here, write, L4Checksum::software);
    ASSERT_TRUE(written_here.has_value());
    EXPECT_EQ(TxOffloads::none().asked(), written_here->second.asked());

    const auto datagram_of = [](std::span<const std::uint8_t> frame, std::size_t written)
    {
        return frame.subspan(ETHERNET_HEADER_LENGTH + IPV4_HEADER_LENGTH,
                             written - ETHERNET_HEADER_LENGTH - IPV4_HEADER_LENGTH);
    };
    EXPECT_TRUE(
        udp_checksum_is_valid(write.ipv4.source, write.ipv4.destination, datagram_of(here, written_here->first)));
    EXPECT_TRUE(ipv4_checksum_is_valid(
        std::span<const std::uint8_t>{here}.subspan(ETHERNET_HEADER_LENGTH, IPV4_HEADER_LENGTH)));

    std::vector<std::uint8_t> by_the_nic(IPV4_UDP_HEADER_LENGTH + payload.size());
    std::copy(payload.begin(), payload.end(), by_the_nic.begin() + IPV4_UDP_HEADER_LENGTH);
    const auto written_nic = write_ipv4_udp(by_the_nic, write, L4Checksum::by_the_nic);
    ASSERT_TRUE(written_nic.has_value());
    EXPECT_TRUE(written_nic->second.compute);
    EXPECT_EQ(ETHERNET_HEADER_LENGTH, written_nic->second.l2_length);
    EXPECT_EQ(IPV4_HEADER_LENGTH, written_nic->second.l3_length);

    EXPECT_FALSE(
        udp_checksum_is_valid(write.ipv4.source, write.ipv4.destination, datagram_of(by_the_nic, written_nic->first)));
    const auto here_datagram = datagram_of(here, here.size());
    const auto nic_datagram = datagram_of(by_the_nic, by_the_nic.size());
    EXPECT_FALSE(std::equal(here_datagram.begin(), here_datagram.end(), nic_datagram.begin(), nic_datagram.end()));
    EXPECT_EQ(0, by_the_nic[ETHERNET_HEADER_LENGTH + 10]);
    EXPECT_EQ(0, by_the_nic[ETHERNET_HEADER_LENGTH + 11]);

    // Play the NIC: it adds the rest of the datagram to the field and
    // complements the sum. The field the builder wrote must make that come out
    // as the checksum the receiver wants.
    const std::uint16_t field = static_cast<std::uint16_t>((nic_datagram[6] << 8) | nic_datagram[7]);
    std::vector<std::uint8_t> without_checksum(nic_datagram.begin(), nic_datagram.end());
    without_checksum[6] = 0;
    without_checksum[7] = 0;
    const std::uint16_t computed = static_cast<std::uint16_t>(
        ~test_folded(static_cast<std::uint64_t>(field) + test_partial_sum(without_checksum)));
    std::vector<std::uint8_t> done(nic_datagram.begin(), nic_datagram.end());
    done[6] = static_cast<std::uint8_t>(computed >> 8);
    done[7] = static_cast<std::uint8_t>(computed & 0xff);
    EXPECT_TRUE(udp_checksum_is_valid(write.ipv4.source, write.ipv4.destination, done));
}

TEST(Frame, WhatTheStackDoesNotServeIsNamed)
{
    auto golden = load_golden("ipv4-udp.bin");
    ASSERT_FALSE(golden.empty());

    auto vlan = golden;
    vlan[12] = 0x81;
    vlan[13] = 0x00;
    EXPECT_EQ(Discard::unsupported_ethernet_type, read(vlan, Checksums::verified).error());

    auto tcp = golden;
    tcp[ETHERNET_HEADER_LENGTH + 9] = 6;
    EXPECT_EQ(Discard::unsupported_protocol, read(tcp, Checksums::verified).error());

    auto fragmented = golden;
    fragmented[ETHERNET_HEADER_LENGTH + 6] = 0x20;
    fragmented[ETHERNET_HEADER_LENGTH + 7] = 0x00;
    EXPECT_EQ(Discard::fragmented, read(fragmented, Checksums::verified).error());

    auto options = golden;
    options[ETHERNET_HEADER_LENGTH] = 0x46;
    EXPECT_EQ(Discard::options, read(options, Checksums::verified).error());

    auto corrupt = golden;
    corrupt[ETHERNET_HEADER_LENGTH + 10] ^= 0xff;
    EXPECT_EQ(Discard::checksum_failure, read(corrupt, Checksums::software).error());
    EXPECT_EQ(Discard::checksum_failure, read(tcp, Checksums::software).error());

    EXPECT_EQ(Discard::short_frame, read(std::span<const std::uint8_t>{golden}.first(8), Checksums::software).error());
}

TEST(Frame, AVerifiedFrameIsNotChecksummedAgain)
{
    auto corrupt = load_golden("ipv4-udp.bin");
    ASSERT_FALSE(corrupt.empty());
    corrupt[ETHERNET_HEADER_LENGTH + 10] ^= 0xff;
    EXPECT_TRUE(read(corrupt, Checksums::verified).has_value());
}

TEST(Frame, AnEchoRequestIsAnsweredWithItsOwnIdentifier)
{
    const std::array<std::uint8_t, 8> payload{'d', 'e', 'e', 'p', 'm', 's', 'g', '!'};
    const IcmpEcho request{ICMP_ECHO_REQUEST, 0x1234, 7, payload};
    const Ipv4Write here{{0x02, 0, 0, 0, 0, 0x02}, {0x02, 0, 0, 0, 0, 0x01}, 0x0a4d0001, 0x0a4d0002, 3};

    std::vector<std::uint8_t> request_frame(ETHERNET_HEADER_LENGTH + IPV4_HEADER_LENGTH + ICMP_ECHO_HEADER_LENGTH +
                                            payload.size());
    const auto written_request = write_icmp_echo(request_frame, here, request, ICMP_ECHO_REQUEST);
    ASSERT_TRUE(written_request.has_value());

    const auto received =
        read(std::span<const std::uint8_t>{request_frame}.first(*written_request), Checksums::software);
    ASSERT_TRUE(received.has_value());
    const auto* echo = std::get_if<EchoReceived>(&*received);
    ASSERT_NE(nullptr, echo);
    EXPECT_EQ(ICMP_ECHO_REQUEST, echo->echo.echo_type);
    EXPECT_EQ(0x1234, echo->echo.identifier);

    const Ipv4Write answer{{0x02, 0, 0, 0, 0, 0x01}, {0x02, 0, 0, 0, 0, 0x02}, 0x0a4d0002, echo->ipv4.source, 4};
    std::vector<std::uint8_t> reply_frame(request_frame.size());
    const auto written_reply = write_icmp_echo_reply(reply_frame, answer, echo->echo);
    ASSERT_TRUE(written_reply.has_value());

    const auto reply = read(std::span<const std::uint8_t>{reply_frame}.first(*written_reply), Checksums::software);
    ASSERT_TRUE(reply.has_value());
    const auto* reply_echo = std::get_if<EchoReceived>(&*reply);
    ASSERT_NE(nullptr, reply_echo);
    EXPECT_EQ(ICMP_ECHO_REPLY, reply_echo->echo.echo_type);
    EXPECT_EQ(0x1234, reply_echo->echo.identifier);
    EXPECT_EQ(7, reply_echo->echo.sequence);
    EXPECT_EQ("deepmsg!", as_string(reply_echo->echo.payload));
    EXPECT_EQ(0x0a4d0002u, reply_echo->ipv4.source);
    EXPECT_EQ(0x0a4d0001u, reply_echo->ipv4.destination);
}

TEST(Frame, ThePayloadBehindTheHeadersIsUntouched)
{
    std::vector<std::uint8_t> payload(64, 0xab);
    std::vector<std::uint8_t> frame(IPV4_UDP_HEADER_LENGTH + payload.size());
    std::copy(payload.begin(), payload.end(), frame.begin() + IPV4_UDP_HEADER_LENGTH);

    const auto written = write_ipv4_udp_here(frame, golden_udp_write(payload.size(), 1));
    ASSERT_TRUE(written.has_value());

    const auto received = read(std::span<const std::uint8_t>{frame}.first(*written), Checksums::software);
    ASSERT_TRUE(received.has_value());
    const auto* udp = std::get_if<UdpReceived>(&*received);
    ASSERT_NE(nullptr, udp);
    EXPECT_EQ(payload, std::vector<std::uint8_t>(udp->datagram.payload.begin(), udp->datagram.payload.end()));
}

TEST(Frame, AFrameForAnotherMacIsNotMine)
{
    auto golden = load_golden("ipv4-udp.bin");
    const auto ethernet = read_ethernet_header(golden);
    ASSERT_TRUE(ethernet.has_value());
    EXPECT_TRUE(ethernet->is_for({0x02, 0, 0, 0, 0, 0x02}));
    EXPECT_FALSE(ethernet->is_for({0x02, 0, 0, 0, 0, 0x09}));

    EthernetHeader broadcast = *ethernet;
    broadcast.destination = BROADCAST;
    EXPECT_TRUE(broadcast.is_for({0x02, 0, 0, 0, 0, 0x09}));
}

TEST(Frame, AUdpChecksumThatFoldsToZeroTravelsAsAllOnes)
{
    constexpr std::uint32_t source = 0x0a4d0001;
    constexpr std::uint32_t destination = 0x0a4d0002;

    std::array<std::uint8_t, UDP_HEADER_LENGTH + 2> found{};
    bool seen = false;
    for (std::uint32_t payload = 0; payload <= 0xffff && !seen; ++payload)
    {
        std::array<std::uint8_t, UDP_HEADER_LENGTH + 2> datagram{};
        datagram[0] = 12001 >> 8;
        datagram[1] = 12001 & 0xff;
        datagram[2] = 10999 >> 8;
        datagram[3] = 10999 & 0xff;
        datagram[4] = 0;
        datagram[5] = 10;
        datagram[8] = static_cast<std::uint8_t>(payload >> 8);
        datagram[9] = static_cast<std::uint8_t>(payload & 0xff);
        if (udp_checksum(source, destination, datagram) == 0xffff)
        {
            found = datagram;
            seen = true;
        }
    }
    ASSERT_TRUE(seen);
    EXPECT_TRUE(udp_checksum_is_valid(source, destination, found));
}
