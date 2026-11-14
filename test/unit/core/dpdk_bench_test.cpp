// E1: the DPDK media on a real port, both directions, with the kernel on the
// other end of the cable.
//
// This case needs hardware and root, so it is skipped unless the environment
// names one:
//
//   AXC_DPDK_BENCH_PCI=0000:03:00.0    the port bound to vfio-pci
//   AXC_DPDK_BENCH_LOCAL=10.10.0.1/24  the address the media owns
//   AXC_DPDK_BENCH_PEER=10.10.0.2      the kernel peer on the other port
//   AXC_DPDK_BENCH_PEER_MAC=aa:bb:..   that peer's MAC (checked, and used by
//                                      the static-neighbour arm)
//
// The peer is a UDP socket in the parent process: it sends a datagram to the
// media and reads the one the media sends to it, so each direction is a real
// frame on the wire. The child owns the EAL (once per process).
//
// Compiled only in a build with `-DAXC_WITH_DPDK=ON`.

#if defined(AXC_WITH_DPDK)

#include <gtest/gtest.h>

#include <fcntl.h>
#include <array>
#include <cerrno>
#include <chrono>
#include <cinttypes>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sched.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include "core/config/property_source.h"
#include "core/media/dpdk/media.h"
#include "core/media/dpdk/port.h"
#include "core/media/dpdk/stack/stack.h"

namespace
{

using axc::core::PropertySource;
using axc::core::media::dpdk::Ipv4Prefix;
using axc::core::media::dpdk::Media;

constexpr std::uint16_t MEDIA_PORT = 41000;
constexpr std::size_t PAYLOAD_LENGTH = 64;
constexpr int LINK_WAIT_SECONDS = 60;

struct BenchSettings
{
    std::string pci;
    std::string local;
    std::string peer;
    std::string peer_mac;
};

[[nodiscard]] std::optional<BenchSettings> bench_settings()
{
    const char* pci = std::getenv("AXC_DPDK_BENCH_PCI");
    const char* local = std::getenv("AXC_DPDK_BENCH_LOCAL");
    const char* peer = std::getenv("AXC_DPDK_BENCH_PEER");
    if (nullptr == pci || nullptr == local || nullptr == peer)
    {
        return std::nullopt;
    }
    const char* mac = std::getenv("AXC_DPDK_BENCH_PEER_MAC");
    return BenchSettings{pci, local, peer, nullptr == mac ? std::string{} : std::string{mac}};
}

/// An IPv4 address, with or without its `/prefix` (`10.10.0.1/24` is the
/// local form and `10.10.0.2` the peer's).
[[nodiscard]] std::uint32_t ipv4_of(const std::string& text)
{
    const std::string address_text = text.substr(0, text.find('/'));
    struct in_addr address{};
    if (1 != ::inet_pton(AF_INET, address_text.c_str(), &address))
    {
        return 0;
    }
    return ntohl(address.s_addr);
}

/// A MAC in `xx:xx:xx:xx:xx:xx` form, when the text is one.
[[nodiscard]] std::optional<std::array<std::uint8_t, 6>> mac_of(const std::string& text)
{
    if (17 != text.size())
    {
        return std::nullopt;
    }
    std::array<std::uint8_t, 6> mac{};
    for (std::size_t index = 0; index < mac.size(); ++index)
    {
        const std::string byte{text.substr(index * 3, 2)};
        char* end = nullptr;
        const long value = std::strtol(byte.c_str(), &end, 16);
        if (nullptr == end || '\0' != *end || value < 0 || value > 0xff)
        {
            return std::nullopt;
        }
        mac[index] = static_cast<std::uint8_t>(value);
    }
    return mac;
}

void fill_payload(std::array<std::uint8_t, PAYLOAD_LENGTH>& payload, std::uint8_t seed)
{
    for (std::size_t index = 0; index < payload.size(); ++index)
    {
        payload[index] = static_cast<std::uint8_t>(seed + index);
    }
}

/// The media's half of the exchange: send once, then pump until the peer's
/// datagram arrives and the port counters show both directions.
int run_media_side(const BenchSettings& settings, std::uint16_t peer_port, bool static_neighbour)
{
    PropertySource properties;
    properties.set("deepmsg.dpdk.enabled", "true");
    properties.set("deepmsg.dpdk.eal.args", std::string{"-a "} + settings.pci);
    properties.set("deepmsg.dpdk.port", settings.pci);
    properties.set("deepmsg.dpdk.ipv4.address", settings.local);
    if (static_neighbour && !settings.peer_mac.empty())
    {
        properties.set("deepmsg.dpdk.static.neighbours", settings.peer + "=" + settings.peer_mac);
    }

    auto media = axc::core::media::dpdk::open_media(properties);
    if (!media)
    {
        std::fprintf(stderr, "open_media: %s\n", std::string{media.error().message.view()}.c_str());
        return 1;
    }
    auto* stack = media->stack();
    auto* port = media->port();
    if (nullptr == stack || nullptr == port)
    {
        std::fprintf(stderr, "the media opened without a stack or a port\n");
        return 1;
    }

    const std::uint32_t local = stack->params().address;
    const std::uint32_t peer = ipv4_of(settings.peer);
    const auto binding = stack->bind(local, MEDIA_PORT, std::nullopt, {local, MEDIA_PORT});
    if (!binding)
    {
        std::fprintf(stderr, "bind failed\n");
        return 1;
    }

    std::array<std::uint8_t, PAYLOAD_LENGTH> out{};
    fill_payload(out, 1);
    std::size_t sent = 0;
    for (int attempt = 0; attempt < 5000 && PAYLOAD_LENGTH != sent; ++attempt)
    {
        sent = stack->send(*binding, {peer, peer_port}, out);
        if (PAYLOAD_LENGTH != sent)
        {
            (void)stack->pump();  // the ARP answer arrives on this path
            ::usleep(2000);
        }
    }
    if (PAYLOAD_LENGTH != sent)
    {
        std::fprintf(stderr, "the media never sent to the peer\n");
        return 1;
    }

    std::array<std::uint8_t, PAYLOAD_LENGTH + 64> in{};
    std::array<std::uint8_t, PAYLOAD_LENGTH> expected{};
    fill_payload(expected, 101);
    std::size_t received = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(LINK_WAIT_SECONDS);
    while (std::chrono::steady_clock::now() < deadline)
    {
        (void)stack->pump();
        received = stack->receive(*binding, in);
        if (PAYLOAD_LENGTH == received)
        {
            break;
        }
        ::usleep(2000);
    }
    if (PAYLOAD_LENGTH != received)
    {
        // A failure has to say where the frames went: the port's own counters
        // tell "never arrived" from "arrived and was dropped here".
        const auto stats = port->stats();
        if (stats)
        {
            std::fprintf(stderr, "port: in=%" PRIu64 " out=%" PRIu64 " missed=%" PRIu64 " errors=%" PRIu64 "\n",
                         stats->ipackets, stats->opackets, stats->imissed, stats->ierrors);
        }
        const auto& counters = stack->counters();
        std::fprintf(stderr,
                     "stack: arp_miss=%" PRIu64 " rx_sender_discard=%" PRIu64 " queue_drop=%" PRIu64
                     " checksum_failure=%" PRIu64 " fragmented=%" PRIu64 " rx_receiver_discard=%" PRIu64
                     " unsupported_ethertype=%" PRIu64 " unsupported_protocol=%" PRIu64 "\n",
                     counters.arp_miss.load(), counters.rx_sender_discard.load(), counters.queue_drop.load(),
                     counters.checksum_failure.load(), counters.fragmented.load(), counters.rx_receiver_discard.load(),
                     counters.unsupported_ethernet_type.load(), counters.unsupported_protocol.load());
        std::fprintf(stderr, "the media never received from the peer\n");
        return 1;
    }
    if (0 != std::memcmp(in.data(), expected.data(), expected.size()))
    {
        std::fprintf(stderr, "the peer's payload came back changed\n");
        return 1;
    }

    // The stack learned the neighbour through ARP (the static arm already
    // knows it, and the first send never asked).
    const auto learned = stack->neighbour(peer);
    if (!learned)
    {
        std::fprintf(stderr, "the peer's MAC was never learned\n");
        return 1;
    }
    if (const auto expected_mac = mac_of(settings.peer_mac); expected_mac && *learned != *expected_mac)
    {
        std::fprintf(stderr, "the learned MAC is not the peer's\n");
        return 1;
    }

    const auto stats = port->stats();
    if (!stats)
    {
        std::fprintf(stderr, "the port's statistics could not be read\n");
        return 1;
    }
    if (stats->ipackets < 2 || stats->opackets < 1)
    {
        std::fprintf(stderr, "counters do not show both directions: in=%" PRIu64 " out=%" PRIu64 "\n", stats->ipackets,
                     stats->opackets);
        return 1;
    }

    if (auto closed = media->close(); !closed)
    {
        std::fprintf(stderr, "close: %s\n", std::string{closed.error().message.view()}.c_str());
        return 1;
    }
    std::printf("E1 media side ok: in=%" PRIu64 " out=%" PRIu64 "\n", stats->ipackets, stats->opackets);
    std::fflush(stdout);
    return 0;
}

/// The parent's half: a kernel UDP socket at the peer's address.
class PeerSocket
{
public:
    PeerSocket()
    {
        fd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
        if (fd_ < 0)
        {
            return;
        }
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(ipv4_of(peer_address()));
        address.sin_port = 0;
        if (0 != ::bind(fd_, reinterpret_cast<const sockaddr*>(&address), sizeof(address)))
        {
            ::close(fd_);
            fd_ = -1;
            return;
        }
        timeval timeout{};
        timeout.tv_sec = LINK_WAIT_SECONDS;
        (void)::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        socklen_t length = sizeof(address);
        (void)::getsockname(fd_, reinterpret_cast<sockaddr*>(&address), &length);
        port_ = ntohs(address.sin_port);
    }

    ~PeerSocket()
    {
        if (fd_ >= 0)
        {
            ::close(fd_);
        }
    }

    PeerSocket(const PeerSocket&) = delete;
    PeerSocket& operator=(const PeerSocket&) = delete;

    [[nodiscard]] bool ok() const noexcept
    {
        return fd_ >= 0;
    }
    [[nodiscard]] std::uint16_t port() const noexcept
    {
        return port_;
    }
    [[nodiscard]] int descriptor() const noexcept
    {
        return fd_;
    }

    [[nodiscard]] bool receive(std::array<std::uint8_t, PAYLOAD_LENGTH>& payload)
    {
        const ssize_t got = ::recv(fd_, payload.data(), payload.size(), 0);
        return got == static_cast<ssize_t>(payload.size());
    }

    [[nodiscard]] bool send(const std::array<std::uint8_t, PAYLOAD_LENGTH>& payload, std::uint32_t media_address,
                            std::uint16_t media_port)
    {
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(media_address);
        address.sin_port = htons(media_port);
        const ssize_t sent = ::sendto(fd_, payload.data(), payload.size(), 0,
                                      reinterpret_cast<const sockaddr*>(&address), sizeof(address));
        return sent == static_cast<ssize_t>(payload.size());
    }

    static std::string peer_address()
    {
        const char* peer = std::getenv("AXC_DPDK_BENCH_PEER");
        return nullptr == peer ? std::string{} : std::string{peer};
    }

private:
    int fd_{-1};
    std::uint16_t port_{0};
};

/// Fork the media side, do the exchange, and answer the child's exit code.
void the_real_port_carries_both_ways(bool static_neighbour)
{
    const auto settings = bench_settings();
    if (!settings)
    {
        GTEST_SKIP() << "set AXC_DPDK_BENCH_PCI, AXC_DPDK_BENCH_LOCAL and AXC_DPDK_BENCH_PEER";
    }

    ASSERT_NE(0u, ipv4_of(settings->local)) << "AXC_DPDK_BENCH_LOCAL is not an IPv4 address";
    ASSERT_NE(0u, ipv4_of(settings->peer)) << "AXC_DPDK_BENCH_PEER is not an IPv4 address";

    PeerSocket peer;
    ASSERT_TRUE(peer.ok()) << "could not bind the peer's UDP socket: " << std::strerror(errno);

    const pid_t child = ::fork();
    ASSERT_GE(child, 0);

    if (0 == child)
    {
        ::_exit(run_media_side(*settings, peer.port(), static_neighbour));
    }

    std::array<std::uint8_t, PAYLOAD_LENGTH> outbound{};
    std::array<std::uint8_t, PAYLOAD_LENGTH> inbound{};
    fill_payload(outbound, 1);
    ASSERT_TRUE(peer.receive(inbound)) << "the kernel peer received nothing from the media";
    EXPECT_EQ(0, std::memcmp(outbound.data(), inbound.data(), outbound.size()));

    std::array<std::uint8_t, PAYLOAD_LENGTH> reply{};
    fill_payload(reply, 101);
    ASSERT_TRUE(peer.send(reply, ipv4_of(settings->local), MEDIA_PORT));

    int status = 0;
    ASSERT_EQ(child, ::waitpid(child, &status, 0));
    EXPECT_TRUE(WIFEXITED(status));
    EXPECT_EQ(0, WEXITSTATUS(status));
}

}  // namespace

TEST(DpdkBench, TheRealPortCarriesBothWays)
{
    the_real_port_carries_both_ways(false);
}

TEST(DpdkBench, TheRealPortCarriesBothWaysWithAStaticNeighbour)
{
    the_real_port_carries_both_ways(true);
}

namespace
{

constexpr std::size_t RTT_PAYLOAD = 64;
constexpr std::uint16_t RTT_LOCAL_PORT = 41000;

/// The arm the child runs: a kernel socket at the media's address, or the
/// media itself. The peer is the same kernel socket in both.
enum class Arm
{
    kernel,
    media
};

[[nodiscard]] std::optional<Arm> arm_from_environment()
{
    const char* arm = std::getenv("AXC_DPDK_BENCH_ARM");
    if (nullptr == arm)
    {
        return std::nullopt;
    }
    if (0 == std::strcmp(arm, "kernel"))
    {
        return Arm::kernel;
    }
    if (0 == std::strcmp(arm, "media"))
    {
        return Arm::media;
    }
    return std::nullopt;
}

[[nodiscard]] std::size_t sample_count()
{
    const char* count = std::getenv("AXC_DPDK_BENCH_N");
    return nullptr == count ? 20'000 : static_cast<std::size_t>(std::strtoul(count, nullptr, 10));
}

[[nodiscard]] std::uint16_t port_of_settings()
{
    return RTT_LOCAL_PORT;
}

int run_arm_side(Arm arm, const BenchSettings& settings, std::uint16_t peer_port, std::size_t count)
{
    const std::uint32_t peer = ipv4_of(settings.peer);
    const std::uint32_t local = ipv4_of(settings.local);
    std::vector<std::int64_t> samples;
    samples.reserve(count);

    if (Arm::kernel == arm)
    {
        const int socket_fd = ::socket(AF_INET, SOCK_DGRAM, 0);
        if (socket_fd < 0)
        {
            std::fprintf(stderr, "kernel arm: socket: %s\n", std::strerror(errno));
            return 1;
        }
        sockaddr_in bind_address{};
        bind_address.sin_family = AF_INET;
        bind_address.sin_addr.s_addr = htonl(local);
        bind_address.sin_port = htons(port_of_settings());
        if (0 != ::bind(socket_fd, reinterpret_cast<const sockaddr*>(&bind_address), sizeof(bind_address)))
        {
            std::fprintf(stderr, "kernel arm: bind: %s\n", std::strerror(errno));
            return 1;
        }
        timeval timeout{};
        timeout.tv_sec = 5;
        (void)::setsockopt(socket_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

        sockaddr_in destination{};
        destination.sin_family = AF_INET;
        destination.sin_addr.s_addr = htonl(peer);
        destination.sin_port = htons(peer_port);

        std::array<std::uint8_t, RTT_PAYLOAD> message{};
        std::array<std::uint8_t, RTT_PAYLOAD> reply{};
        for (std::size_t index = 0; index < count; ++index)
        {
            std::memcpy(message.data(), &index, sizeof(index));
            const auto begin = std::chrono::steady_clock::now();
            if (RTT_PAYLOAD != static_cast<std::size_t>(::sendto(socket_fd, message.data(), message.size(), 0,
                                                                 reinterpret_cast<const sockaddr*>(&destination),
                                                                 sizeof(destination))))
            {
                std::fprintf(stderr, "kernel arm: sendto at %zu\n", index);
                return 1;
            }
            const ssize_t got = ::recv(socket_fd, reply.data(), reply.size(), 0);
            const auto end = std::chrono::steady_clock::now();
            if (RTT_PAYLOAD != static_cast<std::size_t>(got))
            {
                std::fprintf(stderr, "kernel arm: the peer answered %zd bytes at %zu\n", got, index);
                return 1;
            }
            samples.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count());
        }
        ::close(socket_fd);
    }
    else
    {
        PropertySource properties;
        properties.set("deepmsg.dpdk.enabled", "true");
        properties.set("deepmsg.dpdk.eal.args", std::string{"-a "} + settings.pci);
        properties.set("deepmsg.dpdk.port", settings.pci);
        properties.set("deepmsg.dpdk.ipv4.address", settings.local);
        if (!settings.peer_mac.empty())
        {
            properties.set("deepmsg.dpdk.static.neighbours", settings.peer + "=" + settings.peer_mac);
        }
        auto media = axc::core::media::dpdk::open_media(properties);
        if (!media)
        {
            std::fprintf(stderr, "media arm: %s\n", std::string{media.error().message.view()}.c_str());
            return 1;
        }
        auto* stack = media->stack();
        if (nullptr == stack)
        {
            return 1;
        }
        const auto binding = stack->bind(local, port_of_settings(), std::nullopt, {local, port_of_settings()});
        if (!binding)
        {
            std::fprintf(stderr, "media arm: bind failed\n");
            return 1;
        }

        std::array<std::uint8_t, RTT_PAYLOAD> message{};
        std::array<std::uint8_t, RTT_PAYLOAD + 64> reply{};
        const auto per_message = std::chrono::seconds(5);
        for (std::size_t index = 0; index < count; ++index)
        {
            std::memcpy(message.data(), &index, sizeof(index));
            const auto begin = std::chrono::steady_clock::now();
            const auto deadline = begin + per_message;
            std::size_t sent = 0;
            while (RTT_PAYLOAD != sent && std::chrono::steady_clock::now() < deadline)
            {
                sent = stack->send(*binding, {peer, peer_port}, message);
                if (RTT_PAYLOAD != sent)
                {
                    (void)stack->pump();
                }
            }
            if (RTT_PAYLOAD != sent)
            {
                std::fprintf(stderr, "media arm: never sent at %zu\n", index);
                return 1;
            }
            std::size_t received = 0;
            while (RTT_PAYLOAD != received && std::chrono::steady_clock::now() < deadline)
            {
                (void)stack->pump();
                received = stack->receive(*binding, reply);
            }
            const auto end = std::chrono::steady_clock::now();
            if (RTT_PAYLOAD != received)
            {
                std::fprintf(stderr, "media arm: no answer at %zu\n", index);
                return 1;
            }
            samples.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count());
        }
        if (auto closed = media->close(); !closed)
        {
            return 1;
        }
    }

    std::sort(samples.begin(), samples.end());
    if (samples.empty())
    {
        return 1;
    }
    const auto at = [&samples](std::size_t percent)
    { return samples[std::min(samples.size() - 1, samples.size() * percent / 100)]; };
    const auto p50 = at(50);
    const auto p99 = at(99);
    const auto p999 = samples[std::min(samples.size() - 1, samples.size() * 999 / 1000)];
    std::printf("RTT arm=%s n=%zu p50_ns=%lld p99_ns=%lld p999_ns=%lld second_per_message=%.9f\n",
                Arm::kernel == arm ? "kernel" : "media", samples.size(), static_cast<long long>(p50),
                static_cast<long long>(p99), static_cast<long long>(p999), static_cast<double>(p50) / 1e9);
    std::fflush(stdout);
    return 0;
}

/// The peer, in its own network namespace so the kernel cannot short-circuit
/// the datagram locally. It echoes each datagram back to where it came from.
void echo_forever(const PeerSocket& peer)
{
    timeval timeout{};
    timeout.tv_usec = 100'000;
    (void)::setsockopt(peer.descriptor(), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    std::array<std::uint8_t, RTT_PAYLOAD> buffer{};
    for (;;)
    {
        sockaddr_in from{};
        socklen_t from_length = sizeof(from);
        const ssize_t got = ::recvfrom(peer.descriptor(), buffer.data(), buffer.size(), 0,
                                       reinterpret_cast<sockaddr*>(&from), &from_length);
        if (got <= 0)
        {
            continue;
        }
        (void)::sendto(peer.descriptor(), buffer.data(), static_cast<std::size_t>(got), 0,
                       reinterpret_cast<const sockaddr*>(&from), from_length);
    }
}

/// Join the peer's network namespace, when one is named. The runner creates
/// it (`ip netns add ...`, the peer interface moved into it); without a name
/// the peer stays in this one.
[[nodiscard]] bool join_peer_namespace()
{
    const char* name = std::getenv("AXC_DPDK_BENCH_PEER_NETNS");
    if (nullptr == name || '\0' == *name)
    {
        return true;
    }
    const std::string path = std::string{"/var/run/netns/"} + name;
    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0)
    {
        std::fprintf(stderr, "netns %s: %s\n", path.c_str(), std::strerror(errno));
        return false;
    }
    const bool joined = 0 == ::setns(fd, CLONE_NEWNET);
    ::close(fd);
    if (!joined)
    {
        std::fprintf(stderr, "setns(%s): %s\n", path.c_str(), std::strerror(errno));
    }
    return joined;
}

}  // namespace

TEST(DpdkBench, RoundTripLatency)
{
    if (!arm_from_environment())
    {
        GTEST_SKIP() << "set AXC_DPDK_BENCH_ARM=kernel|media (and PCI/LOCAL/PEER)";
    }
    const auto settings = bench_settings();
    if (!settings)
    {
        GTEST_SKIP() << "set AXC_DPDK_BENCH_PCI, AXC_DPDK_BENCH_LOCAL and AXC_DPDK_BENCH_PEER";
    }

    // The peer runs in its own process (and, when named, its own namespace):
    // two addresses on one host would otherwise be delivered by the kernel's
    // local route, which is not the wire this arm is meant to measure.
    int handshake[2] = {-1, -1};
    ASSERT_EQ(0, ::pipe(handshake));
    const pid_t peer = ::fork();
    ASSERT_GE(peer, 0);
    if (0 == peer)
    {
        (void)::close(handshake[0]);
        if (!join_peer_namespace())
        {
            ::_exit(127);
        }
        PeerSocket socket;
        if (!socket.ok())
        {
            ::_exit(127);
        }
        const std::uint16_t port = socket.port();
        if (sizeof(port) != ::write(handshake[1], &port, sizeof(port)))
        {
            ::_exit(127);
        }
        (void)::close(handshake[1]);
        echo_forever(socket);
        ::_exit(0);
    }

    (void)::close(handshake[1]);
    std::uint16_t peer_port = 0;
    ASSERT_EQ(sizeof(peer_port), ::read(handshake[0], &peer_port, sizeof(peer_port)));
    (void)::close(handshake[0]);

    const int result = run_arm_side(*arm_from_environment(), *settings, peer_port, sample_count());
    ::kill(peer, SIGTERM);
    int status = 0;
    ASSERT_EQ(peer, ::waitpid(peer, &status, 0));
    ASSERT_EQ(0, result);
}

#endif  // AXC_WITH_DPDK
