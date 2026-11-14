#include "core/media/dpdk/stack/frame.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <utility>

namespace axc::core::media::dpdk::frame
{

namespace
{

[[nodiscard]] std::optional<std::uint16_t> read_u16(std::span<const std::uint8_t> bytes, std::size_t offset) noexcept
{
    if (offset + 2 > bytes.size())
    {
        return std::nullopt;
    }
    return static_cast<std::uint16_t>((static_cast<std::uint16_t>(bytes[offset]) << 8) | bytes[offset + 1]);
}

void write_u16(std::span<std::uint8_t> bytes, std::size_t offset, std::uint16_t value) noexcept
{
    bytes[offset] = static_cast<std::uint8_t>(value >> 8);
    bytes[offset + 1] = static_cast<std::uint8_t>(value & 0xff);
}

[[nodiscard]] std::optional<std::array<std::uint8_t, 6>> read_mac(std::span<const std::uint8_t> bytes,
                                                                  std::size_t offset) noexcept
{
    if (offset + 6 > bytes.size())
    {
        return std::nullopt;
    }
    return std::array<std::uint8_t, 6>{bytes[offset],     bytes[offset + 1], bytes[offset + 2],
                                       bytes[offset + 3], bytes[offset + 4], bytes[offset + 5]};
}

[[nodiscard]] std::optional<std::uint32_t> read_ipv4_address(std::span<const std::uint8_t> bytes,
                                                             std::size_t offset) noexcept
{
    if (offset + 4 > bytes.size())
    {
        return std::nullopt;
    }
    return (static_cast<std::uint32_t>(bytes[offset]) << 24) | (static_cast<std::uint32_t>(bytes[offset + 1]) << 16) |
           (static_cast<std::uint32_t>(bytes[offset + 2]) << 8) | static_cast<std::uint32_t>(bytes[offset + 3]);
}

void write_ipv4_address(std::span<std::uint8_t> bytes, std::size_t offset, std::uint32_t address) noexcept
{
    bytes[offset] = static_cast<std::uint8_t>((address >> 24) & 0xff);
    bytes[offset + 1] = static_cast<std::uint8_t>((address >> 16) & 0xff);
    bytes[offset + 2] = static_cast<std::uint8_t>((address >> 8) & 0xff);
    bytes[offset + 3] = static_cast<std::uint8_t>(address & 0xff);
}

/// Sum of every sixteen-bit word in `bytes`, as a sixty-four-bit total. A
/// trailing odd byte is padded on the **right**: it is the high half of its
/// word.
[[nodiscard]] std::uint64_t partial_sum(std::span<const std::uint8_t> bytes) noexcept
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

/// Fold the carries down to sixteen bits, left as they are (what a NIC's
/// checksum engine expects to find in the field).
[[nodiscard]] std::uint16_t folded(std::uint64_t sum) noexcept
{
    while (sum > 0xffff)
    {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    return static_cast<std::uint16_t>(sum);
}

[[nodiscard]] std::uint16_t fold(std::uint64_t sum) noexcept
{
    return static_cast<std::uint16_t>(~folded(sum));
}

[[nodiscard]] std::uint64_t pseudo_header_sum(std::uint32_t source, std::uint32_t destination,
                                              std::span<const std::uint8_t> datagram) noexcept
{
    const std::array<std::uint8_t, 4> source_octets{
        static_cast<std::uint8_t>((source >> 24) & 0xff), static_cast<std::uint8_t>((source >> 16) & 0xff),
        static_cast<std::uint8_t>((source >> 8) & 0xff), static_cast<std::uint8_t>(source & 0xff)};
    const std::array<std::uint8_t, 4> destination_octets{
        static_cast<std::uint8_t>((destination >> 24) & 0xff), static_cast<std::uint8_t>((destination >> 16) & 0xff),
        static_cast<std::uint8_t>((destination >> 8) & 0xff), static_cast<std::uint8_t>(destination & 0xff)};

    return partial_sum(source_octets) + partial_sum(destination_octets) + IP_PROTOCOL_UDP +
           (static_cast<std::uint64_t>(datagram.size()) & 0xffff);
}

[[nodiscard]] std::optional<std::size_t> write_ipv4_header(std::span<std::uint8_t> buffer, const Ipv4Write& frame,
                                                           std::uint8_t protocol, std::size_t total_length,
                                                           L4Checksum checksums) noexcept
{
    const auto ethernet_length = write_ethernet_header(
        buffer, EthernetHeader{frame.destination_hardware, frame.source_hardware, ETHERNET_TYPE_IPV4});
    if (!ethernet_length)
    {
        return std::nullopt;
    }
    if (buffer.size() < ETHERNET_HEADER_LENGTH + IPV4_HEADER_LENGTH || total_length > 0xffff)
    {
        return std::nullopt;
    }

    auto ipv4 = buffer.subspan(ETHERNET_HEADER_LENGTH, IPV4_HEADER_LENGTH);
    ipv4[0] = 0x45;
    ipv4[1] = 0;
    write_u16(ipv4, 2, static_cast<std::uint16_t>(total_length));
    write_u16(ipv4, 4, frame.identification);
    // Don't-fragment: this stack never fragments, and the reference drops what
    // arrives fragmented.
    write_u16(ipv4, 6, 0x4000);
    ipv4[8] = DEFAULT_TTL;
    ipv4[9] = protocol;
    write_u16(ipv4, 10, 0);
    write_ipv4_address(ipv4, 12, frame.source);
    write_ipv4_address(ipv4, 16, frame.destination);

    const std::uint16_t checksum = L4Checksum::software == checksums ? internet_checksum(ipv4) : 0;
    write_u16(ipv4, 10, checksum);
    return ethernet_length;
}

}  // namespace

std::expected<EthernetHeader, Discard> read_ethernet_header(std::span<const std::uint8_t> frame) noexcept
{
    if (frame.size() < ETHERNET_HEADER_LENGTH)
    {
        return std::unexpected(Discard::short_frame);
    }

    const auto destination = read_mac(frame, 0);
    const auto source = read_mac(frame, 6);
    const auto ether_type = read_u16(frame, 12);
    if (!destination || !source || !ether_type)
    {
        return std::unexpected(Discard::short_frame);
    }
    return EthernetHeader{*destination, *source, *ether_type};
}

std::optional<std::size_t> write_ethernet_header(std::span<std::uint8_t> buffer, const EthernetHeader& header) noexcept
{
    if (buffer.size() < ETHERNET_HEADER_LENGTH)
    {
        return std::nullopt;
    }
    std::copy(header.destination.begin(), header.destination.end(), buffer.begin());
    std::copy(header.source.begin(), header.source.end(), buffer.begin() + 6);
    write_u16(buffer, 12, header.ether_type);
    return ETHERNET_HEADER_LENGTH;
}

std::expected<Ipv4Header, Discard> read_ipv4_header(std::span<const std::uint8_t> packet) noexcept
{
    if (packet.size() < IPV4_HEADER_LENGTH)
    {
        return std::unexpected(Discard::short_frame);
    }
    if ((packet[0] >> 4) != 4)
    {
        return std::unexpected(Discard::malformed);
    }

    const std::size_t header_length = static_cast<std::size_t>(packet[0] & 0x0f) * 4;
    if (header_length != IPV4_HEADER_LENGTH)
    {
        return std::unexpected(Discard::options);
    }

    const auto flags_and_fragment_offset = read_u16(packet, 6);
    if (!flags_and_fragment_offset)
    {
        return std::unexpected(Discard::short_frame);
    }
    if ((*flags_and_fragment_offset & 0x3fff) != 0)
    {
        return std::unexpected(Discard::fragmented);
    }

    const auto total_length = read_u16(packet, 2);
    const auto identification = read_u16(packet, 4);
    const auto checksum = read_u16(packet, 10);
    const auto source = read_ipv4_address(packet, 12);
    const auto destination = read_ipv4_address(packet, 16);
    if (!total_length || !identification || !checksum || !source || !destination)
    {
        return std::unexpected(Discard::short_frame);
    }
    if (*total_length < header_length || *total_length > packet.size())
    {
        return std::unexpected(Discard::malformed);
    }

    return Ipv4Header{header_length, *total_length, *identification, *flags_and_fragment_offset, packet[8], packet[9],
                      *checksum,     *source,       *destination};
}

std::expected<UdpDatagram, Discard> read_udp(std::span<const std::uint8_t> datagram) noexcept
{
    if (datagram.size() < UDP_HEADER_LENGTH)
    {
        return std::unexpected(Discard::short_frame);
    }
    const auto length = read_u16(datagram, 4);
    const auto source_port = read_u16(datagram, 0);
    const auto destination_port = read_u16(datagram, 2);
    if (!length || !source_port || !destination_port)
    {
        return std::unexpected(Discard::short_frame);
    }
    if (*length < UDP_HEADER_LENGTH || *length > datagram.size())
    {
        return std::unexpected(Discard::malformed);
    }
    return UdpDatagram{*source_port, *destination_port, *length,
                       datagram.subspan(UDP_HEADER_LENGTH, *length - UDP_HEADER_LENGTH)};
}

std::optional<ArpPacket> read_arp(std::span<const std::uint8_t> packet) noexcept
{
    if (packet.size() < ARP_PACKET_LENGTH)
    {
        return std::nullopt;
    }
    const auto hardware_type = read_u16(packet, 0);
    const auto protocol_type = read_u16(packet, 2);
    const auto operation = read_u16(packet, 6);
    const auto sender_hardware = read_mac(packet, 8);
    const auto sender_protocol = read_ipv4_address(packet, 14);
    const auto target_hardware = read_mac(packet, 18);
    const auto target_protocol = read_ipv4_address(packet, 24);
    if (!hardware_type || *hardware_type != 1 || !protocol_type || *protocol_type != ETHERNET_TYPE_IPV4 ||
        packet[4] != 6 || packet[5] != 4 || !operation || !sender_hardware || !sender_protocol || !target_hardware ||
        !target_protocol)
    {
        return std::nullopt;
    }
    return ArpPacket{*operation, *sender_hardware, *sender_protocol, *target_hardware, *target_protocol};
}

std::expected<IcmpEcho, Discard> read_icmp_echo(std::span<const std::uint8_t> message) noexcept
{
    if (message.size() < ICMP_ECHO_HEADER_LENGTH)
    {
        return std::unexpected(Discard::short_frame);
    }
    const std::uint8_t echo_type = message[0];
    if (echo_type != ICMP_ECHO_REQUEST && echo_type != ICMP_ECHO_REPLY)
    {
        return std::unexpected(Discard::unsupported_protocol);
    }
    const auto identifier = read_u16(message, 4);
    const auto sequence = read_u16(message, 6);
    if (!identifier || !sequence)
    {
        return std::unexpected(Discard::short_frame);
    }
    return IcmpEcho{echo_type, *identifier, *sequence, message.subspan(ICMP_ECHO_HEADER_LENGTH)};
}

std::expected<Received, Discard> read(std::span<const std::uint8_t> frame, Checksums checksums) noexcept
{
    const auto ethernet = read_ethernet_header(frame);
    if (!ethernet)
    {
        return std::unexpected(ethernet.error());
    }
    if (frame.size() < ETHERNET_HEADER_LENGTH)
    {
        return std::unexpected(Discard::short_frame);
    }
    auto body = frame.subspan(ETHERNET_HEADER_LENGTH);

    if (ethernet->ether_type == ETHERNET_TYPE_ARP)
    {
        auto packet = read_arp(body);
        if (!packet)
        {
            return std::unexpected(Discard::malformed);
        }
        return *packet;
    }
    if (ethernet->ether_type != ETHERNET_TYPE_IPV4)
    {
        return std::unexpected(Discard::unsupported_ethernet_type);
    }

    const auto ipv4 = read_ipv4_header(body);
    if (!ipv4)
    {
        return std::unexpected(ipv4.error());
    }
    if (Checksums::software == checksums && !ipv4_checksum_is_valid(body.first(IPV4_HEADER_LENGTH)))
    {
        return std::unexpected(Discard::checksum_failure);
    }

    if (ipv4->total_length < IPV4_HEADER_LENGTH || ipv4->total_length > body.size())
    {
        return std::unexpected(Discard::malformed);
    }
    auto payload = body.subspan(IPV4_HEADER_LENGTH, ipv4->total_length - IPV4_HEADER_LENGTH);

    if (ipv4->protocol == IP_PROTOCOL_UDP)
    {
        const auto datagram = read_udp(payload);
        if (!datagram)
        {
            return std::unexpected(datagram.error());
        }
        if (Checksums::software == checksums && !udp_checksum_is_valid(ipv4->source, ipv4->destination, payload))
        {
            return std::unexpected(Discard::checksum_failure);
        }
        return UdpReceived{*ipv4, *datagram};
    }
    if (ipv4->protocol == IP_PROTOCOL_ICMP)
    {
        const auto echo = read_icmp_echo(payload);
        if (!echo)
        {
            return std::unexpected(echo.error());
        }
        if (Checksums::software == checksums && !icmp_checksum_is_valid(payload))
        {
            return std::unexpected(Discard::checksum_failure);
        }
        return EchoReceived{*ipv4, *echo};
    }
    return std::unexpected(Discard::unsupported_protocol);
}

std::optional<std::size_t> write_ipv4_udp_here(std::span<std::uint8_t> buffer, const UdpWrite& frame) noexcept
{
    auto written = write_ipv4_udp(buffer, frame, L4Checksum::software);
    if (!written)
    {
        return std::nullopt;
    }
    return written->first;
}

std::optional<std::pair<std::size_t, TxOffloads>> write_ipv4_udp(std::span<std::uint8_t> buffer, const UdpWrite& frame,
                                                                 L4Checksum checksums) noexcept
{
    if (buffer.size() < IPV4_UDP_HEADER_LENGTH || frame.payload_length > buffer.size() - IPV4_UDP_HEADER_LENGTH)
    {
        return std::nullopt;
    }
    const std::size_t total = IPV4_UDP_HEADER_LENGTH + frame.payload_length;
    if (total - ETHERNET_HEADER_LENGTH > 0xffff || frame.payload_length + UDP_HEADER_LENGTH > 0xffff)
    {
        return std::nullopt;
    }

    if (!write_ipv4_header(buffer, frame.ipv4, IP_PROTOCOL_UDP, total - ETHERNET_HEADER_LENGTH, checksums))
    {
        return std::nullopt;
    }

    auto datagram = buffer.subspan(ETHERNET_HEADER_LENGTH + IPV4_HEADER_LENGTH,
                                   total - ETHERNET_HEADER_LENGTH - IPV4_HEADER_LENGTH);
    write_u16(datagram, 0, frame.source_port);
    write_u16(datagram, 2, frame.destination_port);
    write_u16(datagram, 4, static_cast<std::uint16_t>(UDP_HEADER_LENGTH + frame.payload_length));
    write_u16(datagram, 6, 0);

    TxOffloads offloads{};
    if (L4Checksum::software == checksums)
    {
        write_u16(datagram, 6, udp_checksum(frame.ipv4.source, frame.ipv4.destination, datagram));
    }
    else
    {
        write_u16(datagram, 6, folded(pseudo_header_sum(frame.ipv4.source, frame.ipv4.destination, datagram)));
        offloads = TxOffloads{true, static_cast<std::uint16_t>(ETHERNET_HEADER_LENGTH),
                              static_cast<std::uint16_t>(IPV4_HEADER_LENGTH)};
    }
    return std::pair{total, offloads};
}

std::optional<std::size_t> write_arp(std::span<std::uint8_t> buffer, const EthernetHeader& ethernet,
                                     const ArpPacket& packet) noexcept
{
    constexpr std::size_t total = ETHERNET_HEADER_LENGTH + ARP_PACKET_LENGTH;
    if (buffer.size() < total)
    {
        return std::nullopt;
    }
    if (!write_ethernet_header(buffer, ethernet))
    {
        return std::nullopt;
    }

    auto arp = buffer.subspan(ETHERNET_HEADER_LENGTH, ARP_PACKET_LENGTH);
    write_u16(arp, 0, 1);
    write_u16(arp, 2, ETHERNET_TYPE_IPV4);
    arp[4] = 6;
    arp[5] = 4;
    write_u16(arp, 6, packet.operation);
    std::copy(packet.sender_hardware.begin(), packet.sender_hardware.end(), arp.begin() + 8);
    write_ipv4_address(arp, 14, packet.sender_protocol);
    std::copy(packet.target_hardware.begin(), packet.target_hardware.end(), arp.begin() + 18);
    write_ipv4_address(arp, 24, packet.target_protocol);
    return total;
}

std::optional<std::size_t> write_icmp_echo_reply(std::span<std::uint8_t> buffer, const Ipv4Write& frame,
                                                 const IcmpEcho& request) noexcept
{
    return write_icmp_echo(buffer, frame, request, ICMP_ECHO_REPLY);
}

std::optional<std::size_t> write_icmp_echo(std::span<std::uint8_t> buffer, const Ipv4Write& frame, const IcmpEcho& echo,
                                           std::uint8_t echo_type) noexcept
{
    if (echo.payload.size() > 0xffff - IPV4_HEADER_LENGTH - ICMP_ECHO_HEADER_LENGTH)
    {
        return std::nullopt;
    }
    const std::size_t total_length = IPV4_HEADER_LENGTH + ICMP_ECHO_HEADER_LENGTH + echo.payload.size();
    const std::size_t total = ETHERNET_HEADER_LENGTH + total_length;
    if (buffer.size() < total)
    {
        return std::nullopt;
    }

    if (!write_ipv4_header(buffer, frame, IP_PROTOCOL_ICMP, total_length, L4Checksum::software))
    {
        return std::nullopt;
    }

    auto message = buffer.subspan(ETHERNET_HEADER_LENGTH + IPV4_HEADER_LENGTH,
                                  total - ETHERNET_HEADER_LENGTH - IPV4_HEADER_LENGTH);
    message[0] = echo_type;
    message[1] = 0;
    write_u16(message, 2, 0);
    write_u16(message, 4, echo.identifier);
    write_u16(message, 6, echo.sequence);
    std::copy(echo.payload.begin(), echo.payload.end(), message.begin() + ICMP_ECHO_HEADER_LENGTH);
    write_u16(message, 2, internet_checksum(message));
    return total;
}

std::uint16_t internet_checksum(std::span<const std::uint8_t> bytes) noexcept
{
    return fold(partial_sum(bytes));
}

std::uint16_t udp_checksum(std::uint32_t source, std::uint32_t destination,
                           std::span<const std::uint8_t> datagram) noexcept
{
    const std::uint16_t checksum = fold(pseudo_header_sum(source, destination, datagram) + partial_sum(datagram));
    return checksum == 0 ? std::numeric_limits<std::uint16_t>::max() : checksum;
}

bool ipv4_checksum_is_valid(std::span<const std::uint8_t> header) noexcept
{
    return internet_checksum(header) == 0;
}

bool udp_checksum_is_valid(std::uint32_t source, std::uint32_t destination,
                           std::span<const std::uint8_t> datagram) noexcept
{
    if (datagram.size() < UDP_HEADER_LENGTH)
    {
        return false;
    }
    return fold(pseudo_header_sum(source, destination, datagram) + partial_sum(datagram)) == 0;
}

bool icmp_checksum_is_valid(std::span<const std::uint8_t> message) noexcept
{
    return internet_checksum(message) == 0;
}

}  // namespace axc::core::media::dpdk::frame
