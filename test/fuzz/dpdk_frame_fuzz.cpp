// DPDK userspace stack: whole Ethernet frames, as a bound port receives them.
//
// The parser is the attack surface that sees bytes from the wire before any
// Aeron code does: a hostile `ihl`, `tot_len`, `udp_len` or a truncated ARP
// packet has to be named and dropped, never read past the frame.

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

#include "core/media/dpdk/stack/frame.h"
#include "fuzz_common.h"

namespace frame = axc::core::media::dpdk::frame;

namespace
{

constexpr std::size_t MAX_FRAME_LENGTH = 9216;  // jumbo frame

void parse_and_reply(const std::span<const std::uint8_t> bytes)
{
    (void)frame::read(bytes, frame::Checksums::software);
    (void)frame::internet_checksum(bytes);

    auto ethernet = frame::read_ethernet_header(bytes);
    if (!ethernet)
    {
        return;
    }
    (void)frame::read_ethernet_header(bytes);

    const std::size_t l3 = std::min(bytes.size(), frame::ETHERNET_HEADER_LENGTH);
    const std::span<const std::uint8_t> payload = bytes.subspan(l3);

    auto ipv4 = frame::read_ipv4_header(payload);
    auto arp = frame::read_arp(payload);

    if (arp)
    {
        std::array<std::uint8_t, 2 * MAX_FRAME_LENGTH> scratch{};
        (void)frame::write_arp(scratch, *ethernet, *arp);
    }

    if (!ipv4)
    {
        return;
    }

    const std::size_t header_length = std::min(payload.size(), ipv4->header_length);
    const std::span<const std::uint8_t> header = payload.first(header_length);
    (void)frame::ipv4_checksum_is_valid(header);

    if (ipv4->total_length < ipv4->header_length || ipv4->total_length > payload.size())
    {
        return;
    }

    const std::span<const std::uint8_t> datagram =
        payload.subspan(ipv4->header_length, ipv4->total_length - ipv4->header_length);
    (void)frame::read_udp(datagram);
    (void)frame::read_icmp_echo(datagram);
    (void)frame::icmp_checksum_is_valid(datagram);
    (void)frame::udp_checksum_is_valid(ipv4->source, ipv4->destination, datagram);
    (void)frame::udp_checksum(ipv4->source, ipv4->destination, datagram);

    // The reply builders take attacker-controlled addresses and ports.
    std::array<std::uint8_t, 2 * MAX_FRAME_LENGTH> scratch{};
    frame::Ipv4Write ipv4_write{};
    ipv4_write.source = ipv4->destination;
    ipv4_write.destination = ipv4->source;

    if (auto udp = frame::read_udp(datagram); udp)
    {
        frame::UdpWrite udp_write{};
        udp_write.ipv4 = ipv4_write;
        udp_write.source_port = udp->destination_port;
        udp_write.destination_port = udp->source_port;
        udp_write.payload_length = std::min(udp->payload.size(), std::size_t{MAX_FRAME_LENGTH});
        (void)frame::write_ipv4_udp(scratch, udp_write, frame::L4Checksum::software);
    }
    if (auto echo = frame::read_icmp_echo(datagram); echo)
    {
        (void)frame::write_icmp_echo_reply(scratch, ipv4_write, *echo);
    }
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size)
{
    if (size > MAX_FRAME_LENGTH)
    {
        return 0;
    }
    parse_and_reply(std::span<const std::uint8_t>{data, size});
    return 0;
}

namespace axc::fuzz
{

void emit_seeds(const std::filesystem::path& dir)
{
    // The two golden frames from the Rust track's captures of Linux-built
    // frames are the corpus; nothing is synthesised here.
    (void)dir;
}

}  // namespace axc::fuzz

extern "C" int LLVMFuzzerInitialize(int* argc, char*** argv)
{
    return axc::fuzz::seed_emitter_initialize(argc, argv);
}
