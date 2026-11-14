// Ethernet II, IPv4, UDP, ICMP echo and ARP, as bytes on the wire.
//
// A kernel-bypass port hands over whole Ethernet frames, so everything the
// kernel used to decide — is this datagram for me, is its header well formed,
// what should its checksums be — is decided here. Transcribed from the Rust
// track's `media/userspace/frame.rs`; the golden frames it is checked against
// are that track's captures of frames Linux built.
//
// What this module builds is what Linux builds (G13), and a frame it cannot
// place is named rather than guessed at (the `Discard` buckets are the
// reference's counters 89/90/93/94).

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <tuple>
#include <variant>

namespace axc::core::media::dpdk::frame
{

inline constexpr std::size_t ETHERNET_HEADER_LENGTH = 14;
inline constexpr std::size_t IPV4_HEADER_LENGTH = 20;
inline constexpr std::size_t UDP_HEADER_LENGTH = 8;
inline constexpr std::size_t ICMP_ECHO_HEADER_LENGTH = 8;
inline constexpr std::size_t ARP_PACKET_LENGTH = 28;
inline constexpr std::size_t IPV4_UDP_HEADER_LENGTH = ETHERNET_HEADER_LENGTH + IPV4_HEADER_LENGTH + UDP_HEADER_LENGTH;

inline constexpr std::uint16_t ETHERNET_TYPE_IPV4 = 0x0800;
inline constexpr std::uint16_t ETHERNET_TYPE_ARP = 0x0806;
inline constexpr std::uint16_t ETHERNET_TYPE_VLAN = 0x8100;

inline constexpr std::uint8_t IP_PROTOCOL_ICMP = 1;
inline constexpr std::uint8_t IP_PROTOCOL_UDP = 17;

inline constexpr std::uint8_t DEFAULT_TTL = 64;

inline constexpr std::array<std::uint8_t, 6> BROADCAST = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

inline constexpr std::uint16_t ARP_REQUEST = 1;
inline constexpr std::uint16_t ARP_REPLY = 2;

inline constexpr std::uint8_t ICMP_ECHO_REQUEST = 8;
inline constexpr std::uint8_t ICMP_ECHO_REPLY = 0;

/// Where a frame this stack cannot place is counted.
enum class Discard
{
    /// Not a whole header. A runt the NIC would have dropped before anyone
    /// counted it.
    short_frame,
    /// An ethertype that is neither IPv4 nor ARP — a VLAN tag included.
    /// Counter 93.
    unsupported_ethernet_type,
    /// IPv4 with options (IHL other than 5). Counted with the fragments (90).
    options,
    /// MF set, or a non-zero fragment offset. Counter 90.
    fragmented,
    /// IPv4 carrying something other than UDP or ICMP. Counter 94.
    unsupported_protocol,
    /// A checksum the port did not vouch for and the arithmetic did not
    /// confirm. Counter 89.
    checksum_failure,
    /// A length field that does not describe a frame that could exist.
    /// Counted as 93.
    malformed,
};

/// Where a received frame's checksum verdict comes from.
enum class Checksums
{
    /// The port already checked them and passed the frame.
    verified,
    /// Nobody has: check with RFC 1071.
    software,
};

/// Who writes a frame's checksums.
enum class L4Checksum
{
    /// This stack computes both, and what it writes is what the receiver finds.
    software,
    /// The NIC computes both: the IPv4 header's field is left zero, and the
    /// datagram's holds the non-complemented pseudo-header sum.
    by_the_nic,
};

/// What a frame asks the NIC to compute. Only a frame builder makes one.
struct TxOffloads
{
    bool compute{false};
    std::uint16_t l2_length{0};
    std::uint16_t l3_length{0};

    static constexpr TxOffloads none() noexcept
    {
        return {};
    }

    [[nodiscard]] constexpr std::tuple<bool, std::uint16_t, std::uint16_t> asked() const noexcept
    {
        return {compute, l2_length, l3_length};
    }
};

struct EthernetHeader
{
    std::array<std::uint8_t, 6> destination{};
    std::array<std::uint8_t, 6> source{};
    std::uint16_t ether_type{0};

    /// Whether a frame carrying this header is for the port whose MAC is
    /// `mac`. Broadcast is for every port; anything else is dropped silently.
    [[nodiscard]] bool is_for(const std::array<std::uint8_t, 6>& mac) const noexcept
    {
        return destination == mac || destination == BROADCAST;
    }
};

struct Ipv4Header
{
    std::size_t header_length{0};
    std::size_t total_length{0};
    std::uint16_t identification{0};
    std::uint16_t flags_and_fragment_offset{0};
    std::uint8_t ttl{0};
    std::uint8_t protocol{0};
    std::uint16_t checksum{0};
    std::uint32_t source{0};       // host byte order
    std::uint32_t destination{0};  // host byte order

    [[nodiscard]] constexpr std::size_t payload_length() const noexcept
    {
        return total_length - header_length;
    }
};

struct UdpDatagram
{
    std::uint16_t source_port{0};
    std::uint16_t destination_port{0};
    std::size_t length{0};
    std::span<const std::uint8_t> payload{};
};

struct ArpPacket
{
    std::uint16_t operation{0};
    std::array<std::uint8_t, 6> sender_hardware{};
    std::uint32_t sender_protocol{0};  // host byte order
    std::array<std::uint8_t, 6> target_hardware{};
    std::uint32_t target_protocol{0};  // host byte order
};

struct IcmpEcho
{
    std::uint8_t echo_type{0};
    std::uint16_t identifier{0};
    std::uint16_t sequence{0};
    std::span<const std::uint8_t> payload{};
};

struct UdpReceived
{
    Ipv4Header ipv4;
    UdpDatagram datagram;
};

struct EchoReceived
{
    Ipv4Header ipv4;
    IcmpEcho echo;
};

/// What a received frame turned out to be.
using Received = std::variant<UdpReceived, EchoReceived, ArpPacket>;

/// Everything an outgoing IPv4 frame needs that is not its payload.
struct Ipv4Write
{
    std::array<std::uint8_t, 6> destination_hardware{};
    std::array<std::uint8_t, 6> source_hardware{};
    std::uint32_t source{0};
    std::uint32_t destination{0};
    std::uint16_t identification{0};
};

/// Everything an outgoing IPv4/UDP frame needs that is not its payload.
struct UdpWrite
{
    Ipv4Write ipv4;
    std::uint16_t source_port{0};
    std::uint16_t destination_port{0};
    /// Payload bytes already written at `IPV4_UDP_HEADER_LENGTH`.
    std::size_t payload_length{0};
};

[[nodiscard]] std::expected<EthernetHeader, Discard> read_ethernet_header(std::span<const std::uint8_t> frame) noexcept;

[[nodiscard]] std::optional<std::size_t> write_ethernet_header(std::span<std::uint8_t> buffer,
                                                               const EthernetHeader& header) noexcept;

[[nodiscard]] std::expected<Ipv4Header, Discard> read_ipv4_header(std::span<const std::uint8_t> packet) noexcept;

[[nodiscard]] std::expected<UdpDatagram, Discard> read_udp(std::span<const std::uint8_t> datagram) noexcept;

[[nodiscard]] std::optional<ArpPacket> read_arp(std::span<const std::uint8_t> packet) noexcept;

[[nodiscard]] std::expected<IcmpEcho, Discard> read_icmp_echo(std::span<const std::uint8_t> message) noexcept;

/// Parse a whole received frame as far as this stack takes it.
[[nodiscard]] std::expected<Received, Discard> read(std::span<const std::uint8_t> frame, Checksums checksums) noexcept;

/// Write a whole IPv4/UDP frame in front of a payload already at
/// `IPV4_UDP_HEADER_LENGTH`; both checksums are this stack's.
[[nodiscard]] std::optional<std::size_t> write_ipv4_udp_here(std::span<std::uint8_t> buffer,
                                                             const UdpWrite& frame) noexcept;

[[nodiscard]] std::optional<std::pair<std::size_t, TxOffloads>> write_ipv4_udp(std::span<std::uint8_t> buffer,
                                                                               const UdpWrite& frame,
                                                                               L4Checksum checksums) noexcept;

[[nodiscard]] std::optional<std::size_t> write_arp(std::span<std::uint8_t> buffer, const EthernetHeader& ethernet,
                                                   const ArpPacket& packet) noexcept;

[[nodiscard]] std::optional<std::size_t> write_icmp_echo_reply(std::span<std::uint8_t> buffer, const Ipv4Write& frame,
                                                               const IcmpEcho& request) noexcept;

/// A request is the same encoder with the other type byte; tests need both.
[[nodiscard]] std::optional<std::size_t> write_icmp_echo(std::span<std::uint8_t> buffer, const Ipv4Write& frame,
                                                         const IcmpEcho& echo, std::uint8_t echo_type) noexcept;

/// RFC 1071's Internet checksum.
[[nodiscard]] std::uint16_t internet_checksum(std::span<const std::uint8_t> bytes) noexcept;

/// UDP's pseudo-header plus `datagram`; a zero result travels as `0xffff`.
[[nodiscard]] std::uint16_t udp_checksum(std::uint32_t source, std::uint32_t destination,
                                         std::span<const std::uint8_t> datagram) noexcept;

[[nodiscard]] bool ipv4_checksum_is_valid(std::span<const std::uint8_t> header) noexcept;

[[nodiscard]] bool udp_checksum_is_valid(std::uint32_t source, std::uint32_t destination,
                                         std::span<const std::uint8_t> datagram) noexcept;

[[nodiscard]] bool icmp_checksum_is_valid(std::span<const std::uint8_t> message) noexcept;

}  // namespace axc::core::media::dpdk::frame
