// T4: both ends of the cable are our media driver.
//
// Two `deepmsg-media-driver` processes, each on one port of the cabled pair,
// each with its own EAL (EAL initialises once per process, so the design's T4
// is two processes, never two media in one). A client of driver A publishes to
// B's address and a client of driver B subscribes; then the reverse; then both
// at once. Counters are read from each driver's **own** CnC through the client
// attached to it, so "the media carried this" is a reading and not the absence
// of errors.
//
// Needs root and two cabled ports bound to vfio-pci, so it is skipped unless
// the environment names both ends:
//
//   AXC_DPDK_BENCH_PCI=0000:03:00.0         A's port (vfio-pci)
//   AXC_DPDK_BENCH_LOCAL=10.10.0.1/24       A's address
//   AXC_DPDK_BENCH_PEER_PCI=0000:03:00.1    B's port (vfio-pci, the other end)
//   AXC_DPDK_BENCH_PEER_LOCAL=10.10.0.2/24  B's address
//
// Optional: AXC_DPDK_T4_CPUSET_A / _B (the two drivers' CPUs, default `0-2` and
// `3-5`) and AXC_DPDK_T4_COUNT (messages per direction, default 1000). The rig
// (`bench/dpdk-t4/run-t4.sh`) binds the ports, pins this process, and puts the
// machine back.
//
// Compiled only in a build with `-DAXC_WITH_DPDK=ON`.

#if defined(AXC_WITH_DPDK)

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <aeronc.h>
#include <arpa/inet.h>

#include "core/deepmsg/deepmsg_client.h"
#include "core/media/dpdk/counters.h"
#include "fixture/process.h"

#ifndef AXC_MEDIA_DRIVER_BIN
#define AXC_MEDIA_DRIVER_BIN ""
#endif

namespace
{

using axc::core::ClientConfig;
using axc::core::DeepmsgClient;
using axc::core::PubPtr;
using axc::core::SubPtr;

constexpr std::size_t MESSAGE_LENGTH = 64;
constexpr std::int64_t DEFAULT_MESSAGE_COUNT = 1'000;
constexpr int DRIVER_START_SECONDS = 90;
constexpr int CONNECT_SECONDS = 30;
constexpr int CONNECTED_SECONDS = 30;
constexpr int RECEIVE_SECONDS = 30;
// 75/82/83/84/91 are written by the receiver's poller once a second
// (`bindings.cpp`, `update_port_counters`), so a reading taken sooner is stale.
constexpr int SETTLE_MILLISECONDS = 1'500;

constexpr std::int32_t STREAM_A_TO_B = 1;
constexpr std::int32_t STREAM_B_TO_A = 2;
constexpr std::int32_t STREAM_A_TO_B_AT_ONCE = 3;
constexpr std::int32_t STREAM_B_TO_A_AT_ONCE = 4;
constexpr std::uint16_t PORT_A_TO_B = 24700;
constexpr std::uint16_t PORT_B_TO_A = 24701;
constexpr std::uint16_t PORT_A_TO_B_AT_ONCE = 24702;
constexpr std::uint16_t PORT_B_TO_A_AT_ONCE = 24703;

/// The two ends of the cable, as the environment names them.
struct Pair
{
    std::string a_pci;
    std::string a_address;  // `10.10.0.1/24`
    std::string b_pci;
    std::string b_address;  // `10.10.0.2/24`
};

[[nodiscard]] std::optional<Pair> bench_pair()
{
    const char* a_pci = std::getenv("AXC_DPDK_BENCH_PCI");
    const char* a_address = std::getenv("AXC_DPDK_BENCH_LOCAL");
    const char* b_pci = std::getenv("AXC_DPDK_BENCH_PEER_PCI");
    const char* b_address = std::getenv("AXC_DPDK_BENCH_PEER_LOCAL");
    if (nullptr == a_pci || nullptr == a_address || nullptr == b_pci || nullptr == b_address)
    {
        return std::nullopt;
    }
    return Pair{a_pci, a_address, b_pci, b_address};
}

/// `10.10.0.1` from `10.10.0.1/24`; an endpoint never carries the prefix.
[[nodiscard]] std::string bare_address(const std::string& cidr)
{
    return cidr.substr(0, cidr.find('/'));
}

[[nodiscard]] std::string endpoint(const std::string& cidr, std::uint16_t port)
{
    return "aeron:udp?endpoint=" + bare_address(cidr) + ":" + std::to_string(port);
}

/// The address in host byte order — the media's own order, and what a counter
/// key carries.
[[nodiscard]] std::uint32_t ipv4_of(const std::string& cidr)
{
    struct in_addr address{};
    if (1 != ::inet_pton(AF_INET, bare_address(cidr).c_str(), &address))
    {
        return 0;
    }
    return ntohl(address.s_addr);
}

[[nodiscard]] std::int64_t message_count()
{
    const char* text = std::getenv("AXC_DPDK_T4_COUNT");
    if (nullptr == text)
    {
        return DEFAULT_MESSAGE_COUNT;
    }
    const std::int64_t count = std::atoll(text);
    return count > 0 ? count : DEFAULT_MESSAGE_COUNT;
}

struct Message
{
    std::uint64_t sequence;
    std::array<std::uint8_t, MESSAGE_LENGTH - sizeof(std::uint64_t)> padding;
};

void fill(Message& message, std::uint64_t sequence)
{
    message.sequence = sequence;
    for (std::size_t index = 0; index < message.padding.size(); ++index)
    {
        message.padding[index] = static_cast<std::uint8_t>((sequence * 31U) + index);
    }
}

struct Collected
{
    std::vector<Message> messages;
    std::int64_t wrong_length{0};
};

void on_fragment(void* clientd, const std::uint8_t* buffer, std::size_t length, aeron_header_t* /*header*/)
{
    auto* collected = static_cast<Collected*>(clientd);
    if (MESSAGE_LENGTH != length)
    {
        ++collected->wrong_length;
        return;
    }
    Message message{};
    std::memcpy(&message, buffer, MESSAGE_LENGTH);
    collected->messages.push_back(message);
}

/// The media counters of one driver, from that driver's own CnC. A `-1` means
/// the label was not there at all — which is a failure, not a zero: it would
/// say this driver never opened the media.
struct MediaCounters
{
    std::int64_t port_info{-1};                      // 75
    std::int64_t transports{0};                      // 76, one per transport
    std::int64_t transport_datagrams{0};             // the sum of those 76s
    std::vector<std::uint32_t> transport_addresses;  // the address in each 76's key
    std::int64_t nobufs{-1};                         // 77
    std::int64_t tx_eagain{-1};                      // 78
    std::int64_t error{-1};                          // 79
    std::int64_t packets{-1};                        // 82
    std::int64_t bytes{-1};                          // 83
    std::int64_t missed{-1};                         // 84
    std::int64_t arp_miss{-1};                       // 85
    std::int64_t checksum_failure{-1};               // 89
    std::int64_t mempool_available{-1};              // 91
    std::int64_t rx_receiver_discard{-1};            // 95
};

void collect_counter(std::int64_t value, std::int32_t /*id*/, std::int32_t type_id, const std::uint8_t* key,
                     std::size_t key_length, const char* /*label*/, std::size_t /*label_length*/, void* clientd)
{
    auto* counters = static_cast<MediaCounters*>(clientd);
    namespace ids = axc::core::media::dpdk::counters;
    switch (type_id)
    {
        case ids::PORT_INFO:
            counters->port_info = value;
            break;
        case ids::TRANSPORT:
            ++counters->transports;
            counters->transport_datagrams += value;
            // The key starts with the local address in host byte order
            // (`bindings.cpp`, `TransportKey`), so a reading cannot pass for
            // the other driver's: its keys carry the other address.
            if (key_length >= sizeof(std::uint32_t))
            {
                std::uint32_t address = 0;
                std::memcpy(&address, key, sizeof(address));
                counters->transport_addresses.push_back(address);
            }
            break;
        case ids::NOBUFS:
            counters->nobufs = value;
            break;
        case ids::TX_EAGAIN:
            counters->tx_eagain = value;
            break;
        case ids::ERROR:
            counters->error = value;
            break;
        case ids::PKTS:
            counters->packets = value;
            break;
        case ids::BYTES:
            counters->bytes = value;
            break;
        case ids::MISSED_PACKETS:
            counters->missed = value;
            break;
        case ids::ARP_MISS:
            counters->arp_miss = value;
            break;
        case ids::CHECKSUM_FAILURE:
            counters->checksum_failure = value;
            break;
        case ids::MEMPOOL_AVAILABLE:
            counters->mempool_available = value;
            break;
        case ids::RX_RECEIVER_DISCARD:
            counters->rx_receiver_discard = value;
            break;
        default:
            break;
    }
}

[[nodiscard]] MediaCounters read_counters(DeepmsgClient& client)
{
    MediaCounters counters;
    aeron_counters_reader_t* reader = client.counters_reader();
    if (nullptr != reader)
    {
        aeron_counters_reader_foreach_counter(reader, &collect_counter, &counters);
    }
    return counters;
}

/// The reading goes into the run's log, not only into the assertions: a card's
/// record quotes numbers, and a record cannot re-derive them from a green line.
void print_counters(const char* name, const MediaCounters& established, const MediaCounters& final)
{
    std::printf(
        "T4 counters %s: port75=%lld transports76=%lld datagrams76=%lld nobufs77=%lld tx_eagain78=%lld "
        "error79=%lld packets82=%lld->%lld bytes83=%lld missed84=%lld arp_miss85=%lld->%lld checksum89=%lld "
        "mempool91=%lld rx_discard95=%lld\n",
        name, static_cast<long long>(final.port_info), static_cast<long long>(final.transports),
        static_cast<long long>(final.transport_datagrams), static_cast<long long>(final.nobufs),
        static_cast<long long>(final.tx_eagain), static_cast<long long>(final.error),
        static_cast<long long>(established.packets), static_cast<long long>(final.packets),
        static_cast<long long>(final.bytes), static_cast<long long>(final.missed),
        static_cast<long long>(established.arp_miss), static_cast<long long>(final.arp_miss),
        static_cast<long long>(final.checksum_failure), static_cast<long long>(final.mempool_available),
        static_cast<long long>(final.rx_receiver_discard));
}

/// Both clients are driven together: an unpolled client is reaped by its
/// driver after ~10 s, and both ends take part in every arm.
void drive(DeepmsgClient& first, DeepmsgClient& second)
{
    (void)first.do_work();
    (void)second.do_work();
}

void settle(DeepmsgClient& first, DeepmsgClient& second, int milliseconds)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(milliseconds);
    while (std::chrono::steady_clock::now() < deadline)
    {
        drive(first, second);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

/// Where the two drivers' directories and logs go: the rig names it
/// (`AXC_DPDK_T4_WORK`, so the evidence sits next to the run's output), and a
/// bare invocation gets its own `/tmp/axc-t4-*`.
[[nodiscard]] std::optional<std::string> make_temp_root()
{
    const char* named = std::getenv("AXC_DPDK_T4_WORK");
    if (nullptr != named && '\0' != named[0])
    {
        std::error_code error;
        std::filesystem::create_directories(named, error);
        if (error)
        {
            return std::nullopt;
        }
        return std::string{named};
    }
    std::string pattern = "/tmp/axc-t4-XXXXXX";
    if (nullptr == ::mkdtemp(pattern.data()))
    {
        return std::nullopt;
    }
    return pattern;
}

[[nodiscard]] std::string read_file(const std::string& path)
{
    std::ifstream in(path);
    return std::string{std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

/// One driver process and where its things are. The properties file and the log
/// live **next to** the driver's `aeron.dir`, not inside it: the reference
/// driver deletes an existing aeron directory on start
/// (`aeron_driver_ensure_dir_is_recreated`), and files inside it are unlinked
/// while the driver still holds them open.
struct Driver
{
    axc::interop::ChildProcess process;
    std::string aeron_dir;
    std::string log;
};

[[nodiscard]] std::optional<Driver> start_driver(std::string_view cpuset, const std::string& pci,
                                                 const std::string& cidr, const std::string& directory,
                                                 std::string& failure)
{
    std::error_code error;
    std::filesystem::create_directories(directory, error);
    if (error)
    {
        failure = "cannot create " + directory + ": " + error.message();
        return std::nullopt;
    }

    const std::string aeron_dir = directory + "/aeron";
    const std::string properties = directory + "/driver.properties";
    {
        std::ofstream out(properties);
        out << "aeron.dir=" << aeron_dir << "\n"
            << "aeron.threading.mode=DEDICATED\n"
            << "deepmsg.dpdk.enabled=true\n"
            << "deepmsg.dpdk.eal.args=-a " << pci << "\n"
            << "deepmsg.dpdk.port=" << pci << "\n"
            << "deepmsg.dpdk.ipv4.address=" << cidr << "\n";
        if (!out)
        {
            failure = "cannot write " + properties;
            return std::nullopt;
        }
    }

    // `taskset` gives each driver the CPUs its EAL will claim (`--lcores` is
    // computed from the process's own cpuset). The idle strategies are the
    // P6 rig's frozen values: a DPDK port cannot wake a parked thread.
    const std::string log = directory + "/driver.log";
    auto spawned = axc::interop::ChildProcess::spawn(
        "/usr/bin/taskset", {"-c", std::string{cpuset}, AXC_MEDIA_DRIVER_BIN, properties},
        {"AERON_SENDER_IDLE_STRATEGY=spin", "AERON_RECEIVER_IDLE_STRATEGY=spin"}, log);
    if (!spawned)
    {
        failure = spawned.error().message.view();
        return std::nullopt;
    }
    return Driver{std::move(*spawned), aeron_dir, log};
}

[[nodiscard]] bool wait_for_driver(Driver& driver, std::string& failure)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(DRIVER_START_SECONDS);
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (std::filesystem::exists(driver.aeron_dir + "/cnc.dat"))
        {
            return true;
        }
        if (!driver.process.running())
        {
            failure = "the driver with aeron.dir " + driver.aeron_dir + " exited with code " +
                      std::to_string(driver.process.exit_code()) + " before creating its CnC:\n" +
                      read_file(driver.log);
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    failure = "the driver with aeron.dir " + driver.aeron_dir + " never created its CnC:\n" + read_file(driver.log);
    return false;
}

/// A1's other half: the driver said which port it opened and that the link is
/// up. The line is flushed before the CnC is created (`media.cpp`).
[[nodiscard]] bool log_says_media_is_open(const Driver& driver, const std::string& pci)
{
    const std::string text = read_file(driver.log);
    return std::string::npos != text.find("dpdk media on " + pci) && std::string::npos != text.find("link up (");
}

[[nodiscard]] std::optional<DeepmsgClient> connect_client(const std::string& directory, std::string& failure)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(CONNECT_SECONDS);
    std::string last_error = "the driver was never asked";
    while (std::chrono::steady_clock::now() < deadline)
    {
        ClientConfig config;
        config.dir = directory;
        auto client = DeepmsgClient::connect(config);
        if (client)
        {
            return std::move(*client);
        }
        last_error = std::string{client.error().message.view()};
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    failure = "no client could connect to " + directory + ": " + last_error;
    return std::nullopt;
}

template <class HandlePtr, class AddFn>
[[nodiscard]] std::optional<HandlePtr> add_with_deadline(DeepmsgClient& client, DeepmsgClient& other, AddFn add,
                                                         std::string_view what, std::string& failure)
{
    auto pending = add();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(CONNECT_SECONDS);
    while (std::chrono::steady_clock::now() < deadline)
    {
        drive(client, other);
        auto polled = pending.poll();
        if (!polled)
        {
            failure = std::string{what} + " failed: " + std::string{polled.error().message.view()};
            return std::nullopt;
        }
        if (polled->has_value())
        {
            return std::move(**polled);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    failure = "no " + std::string{what} + " after " + std::to_string(CONNECT_SECONDS) + " s";
    return std::nullopt;
}

[[nodiscard]] bool wait_connected(DeepmsgClient& publisher, DeepmsgClient& other, aeron_publication_t* publication,
                                  std::string& failure)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(CONNECTED_SECONDS);
    while (std::chrono::steady_clock::now() < deadline)
    {
        drive(publisher, other);
        if (aeron_publication_is_connected(publication))
        {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    failure = "the publication never connected: ARP or the status message did not cross";
    return false;
}

[[nodiscard]] bool offer_one(DeepmsgClient& publisher, DeepmsgClient& other, aeron_publication_t* publication,
                             const Message& message)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(RECEIVE_SECONDS);
    while (std::chrono::steady_clock::now() < deadline)
    {
        drive(publisher, other);
        if (aeron_publication_offer(publication, reinterpret_cast<const std::uint8_t*>(&message), MESSAGE_LENGTH,
                                    nullptr, nullptr) >= 0)
        {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
}

[[nodiscard]] bool offer_all(DeepmsgClient& publisher, DeepmsgClient& other, aeron_publication_t* publication,
                             const std::vector<Message>& messages, std::string& failure)
{
    for (const auto& message : messages)
    {
        if (!offer_one(publisher, other, publication, message))
        {
            failure = "the publication refused a message (sequence " + std::to_string(message.sequence) + ")";
            return false;
        }
    }
    return true;
}

[[nodiscard]] bool drain(DeepmsgClient& subscriber, DeepmsgClient& other, aeron_subscription_t* subscription,
                         Collected& collected, std::size_t target, std::string& failure)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(RECEIVE_SECONDS);
    while (collected.messages.size() < target && std::chrono::steady_clock::now() < deadline)
    {
        drive(subscriber, other);
        const int fragments = aeron_subscription_poll(subscription, &on_fragment, &collected, 10);
        if (fragments < 0)
        {
            failure = std::string{"subscription poll failed: "} + aeron_errmsg();
            return false;
        }
        if (0 == fragments)
        {
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
    }
    if (collected.messages.size() != target)
    {
        failure =
            "received " + std::to_string(collected.messages.size()) + " of " + std::to_string(target) + " messages";
        return false;
    }
    return true;
}

void expect_messages(const std::vector<Message>& sent, const Collected& collected)
{
    EXPECT_EQ(0, collected.wrong_length) << "a fragment whose length was not " << MESSAGE_LENGTH;
    ASSERT_EQ(sent.size(), collected.messages.size());
    for (std::size_t index = 0; index < sent.size(); ++index)
    {
        ASSERT_EQ(sent[index].sequence, collected.messages[index].sequence) << index;
        ASSERT_EQ(0, std::memcmp(&sent[index], &collected.messages[index], MESSAGE_LENGTH)) << index;
    }
}

/// A7: the kernel must not own the port. With both ports in vfio there is no
/// address for either end on the host, so a send cannot take the kernel path
/// (`bindings.cpp`'s fallback) and still look successful. A port bound to
/// vfio-pci has no `net` directory at all — that is the good case, not a
/// missing device.
[[nodiscard]] bool port_has_no_kernel_interface(const std::string& pci, std::string& failure)
{
    const std::string device = "/sys/bus/pci/devices/" + pci;
    std::error_code error;
    if (!std::filesystem::exists(device, error))
    {
        failure = "no " + device + " — is " + pci + " a PCI function on this machine?";
        return false;
    }
    const std::string net = device + "/net";
    if (!std::filesystem::exists(net, error))
    {
        return true;
    }
    std::filesystem::directory_iterator entry{net, error};
    if (error)
    {
        failure = "cannot read " + net + ": " + error.message();
        return false;
    }
    if (entry != std::filesystem::directory_iterator{})
    {
        failure = "the kernel still owns " + pci + " as " + entry->path().filename().string() +
                  " — T4 needs both ports in vfio-pci (bound by bench/dpdk-t4/run-t4.sh)";
        return false;
    }
    return true;
}

}  // namespace

TEST(DpdkTwoDrivers, AMessageCrossesBetweenTwoOfOurDrivers)
{
    const auto pair = bench_pair();
    if (!pair)
    {
        GTEST_SKIP() << "set AXC_DPDK_BENCH_PCI/LOCAL and AXC_DPDK_BENCH_PEER_PCI/PEER_LOCAL";
    }
    const std::string driver_binary = AXC_MEDIA_DRIVER_BIN;
    if (driver_binary.empty() || !std::filesystem::exists(driver_binary))
    {
        GTEST_SKIP() << "no media driver binary at AXC_MEDIA_DRIVER_BIN (" << driver_binary << ")";
    }
    if (std::string::npos == pair->a_address.find('/') || std::string::npos == pair->b_address.find('/'))
    {
        GTEST_SKIP() << "both addresses must carry their prefix (10.10.0.1/24)";
    }

    std::string failure;
    ASSERT_TRUE(port_has_no_kernel_interface(pair->a_pci, failure)) << failure;
    ASSERT_TRUE(port_has_no_kernel_interface(pair->b_pci, failure)) << failure;

    const auto root = make_temp_root();
    ASSERT_TRUE(root.has_value()) << "cannot create a temp root";
    const std::string directory_a = *root + "/a";
    const std::string directory_b = *root + "/b";
    const char* cpuset_a = std::getenv("AXC_DPDK_T4_CPUSET_A");
    const char* cpuset_b = std::getenv("AXC_DPDK_T4_CPUSET_B");

    auto driver_a =
        start_driver(nullptr == cpuset_a ? "0-2" : cpuset_a, pair->a_pci, pair->a_address, directory_a, failure);
    ASSERT_TRUE(driver_a.has_value()) << failure;
    auto driver_b =
        start_driver(nullptr == cpuset_b ? "3-5" : cpuset_b, pair->b_pci, pair->b_address, directory_b, failure);
    ASSERT_TRUE(driver_b.has_value()) << failure;

    ASSERT_TRUE(wait_for_driver(*driver_a, failure)) << failure;
    ASSERT_TRUE(wait_for_driver(*driver_b, failure)) << failure;

    // A1: each driver said which port it opened, and its link is up.
    EXPECT_TRUE(log_says_media_is_open(*driver_a, pair->a_pci)) << read_file(driver_a->log);
    EXPECT_TRUE(log_says_media_is_open(*driver_b, pair->b_pci)) << read_file(driver_b->log);

    auto client_a = connect_client(driver_a->aeron_dir, failure);
    ASSERT_TRUE(client_a.has_value()) << failure;
    auto client_b = connect_client(driver_b->aeron_dir, failure);
    ASSERT_TRUE(client_b.has_value()) << failure;

    const auto count = static_cast<std::size_t>(message_count());
    std::vector<Message> a_to_b(count);
    std::vector<Message> b_to_a(count);
    for (std::size_t index = 0; index < count; ++index)
    {
        fill(a_to_b[index], index);
        fill(b_to_a[index], index);
    }

    // A3: A -> B. The subscription is created first in every arm: a publication
    // starts sending status messages immediately, and a status message that
    // reaches a port before its transport exists is a discard (95), which A6
    // would rightly count against the run.
    auto subscription_b = add_with_deadline<SubPtr>(
        *client_b, *client_a,
        [&]() { return client_b->add_subscription(endpoint(pair->b_address, PORT_A_TO_B), STREAM_A_TO_B); },
        "subscription on B", failure);
    ASSERT_TRUE(subscription_b.has_value()) << failure;
    auto publication_a = add_with_deadline<PubPtr>(
        *client_a, *client_b,
        [&]() { return client_a->add_publication(endpoint(pair->b_address, PORT_A_TO_B), STREAM_A_TO_B); },
        "publication on A", failure);
    ASSERT_TRUE(publication_a.has_value()) << failure;
    ASSERT_TRUE(wait_connected(*client_a, *client_b, publication_a->get(), failure)) << failure;
    ASSERT_TRUE(offer_all(*client_a, *client_b, publication_a->get(), a_to_b, failure)) << failure;
    Collected received_b;
    ASSERT_TRUE(drain(*client_b, *client_a, subscription_b->get(), received_b, a_to_b.size(), failure)) << failure;
    expect_messages(a_to_b, received_b);

    // A4: B -> A.
    auto subscription_a = add_with_deadline<SubPtr>(
        *client_a, *client_b,
        [&]() { return client_a->add_subscription(endpoint(pair->a_address, PORT_B_TO_A), STREAM_B_TO_A); },
        "subscription on A", failure);
    ASSERT_TRUE(subscription_a.has_value()) << failure;
    auto publication_b = add_with_deadline<PubPtr>(
        *client_b, *client_a,
        [&]() { return client_b->add_publication(endpoint(pair->a_address, PORT_B_TO_A), STREAM_B_TO_A); },
        "publication on B", failure);
    ASSERT_TRUE(publication_b.has_value()) << failure;
    ASSERT_TRUE(wait_connected(*client_b, *client_a, publication_b->get(), failure)) << failure;
    ASSERT_TRUE(offer_all(*client_b, *client_a, publication_b->get(), b_to_a, failure)) << failure;
    Collected received_a;
    ASSERT_TRUE(drain(*client_a, *client_b, subscription_a->get(), received_a, b_to_a.size(), failure)) << failure;
    expect_messages(b_to_a, received_a);

    // The neighbour is resolved by now; what A6 calls "established" is this
    // reading, taken after a settle so the timer-written counters are current.
    settle(*client_a, *client_b, SETTLE_MILLISECONDS);
    const MediaCounters a_established = read_counters(*client_a);
    const MediaCounters b_established = read_counters(*client_b);

    // A5: both directions at once, on their own ports and streams.
    auto subscription_b_at_once = add_with_deadline<SubPtr>(
        *client_b, *client_a, [&]()
        { return client_b->add_subscription(endpoint(pair->b_address, PORT_A_TO_B_AT_ONCE), STREAM_A_TO_B_AT_ONCE); },
        "subscription on B (at once)", failure);
    ASSERT_TRUE(subscription_b_at_once.has_value()) << failure;
    auto subscription_a_at_once = add_with_deadline<SubPtr>(
        *client_a, *client_b, [&]()
        { return client_a->add_subscription(endpoint(pair->a_address, PORT_B_TO_A_AT_ONCE), STREAM_B_TO_A_AT_ONCE); },
        "subscription on A (at once)", failure);
    ASSERT_TRUE(subscription_a_at_once.has_value()) << failure;
    auto publication_a_at_once = add_with_deadline<PubPtr>(
        *client_a, *client_b, [&]()
        { return client_a->add_publication(endpoint(pair->b_address, PORT_A_TO_B_AT_ONCE), STREAM_A_TO_B_AT_ONCE); },
        "publication on A (at once)", failure);
    ASSERT_TRUE(publication_a_at_once.has_value()) << failure;
    auto publication_b_at_once = add_with_deadline<PubPtr>(
        *client_b, *client_a, [&]()
        { return client_b->add_publication(endpoint(pair->a_address, PORT_B_TO_A_AT_ONCE), STREAM_B_TO_A_AT_ONCE); },
        "publication on B (at once)", failure);
    ASSERT_TRUE(publication_b_at_once.has_value()) << failure;
    ASSERT_TRUE(wait_connected(*client_a, *client_b, publication_a_at_once->get(), failure)) << failure;
    ASSERT_TRUE(wait_connected(*client_b, *client_a, publication_b_at_once->get(), failure)) << failure;
    for (std::size_t index = 0; index < count; ++index)
    {
        ASSERT_TRUE(offer_one(*client_a, *client_b, publication_a_at_once->get(), a_to_b[index])) << index;
        ASSERT_TRUE(offer_one(*client_b, *client_a, publication_b_at_once->get(), b_to_a[index])) << index;
    }
    Collected received_b_at_once;
    Collected received_a_at_once;
    ASSERT_TRUE(drain(*client_b, *client_a, subscription_b_at_once->get(), received_b_at_once, count, failure))
        << failure;
    ASSERT_TRUE(drain(*client_a, *client_b, subscription_a_at_once->get(), received_a_at_once, count, failure))
        << failure;
    expect_messages(a_to_b, received_b_at_once);
    expect_messages(b_to_a, received_a_at_once);

    settle(*client_a, *client_b, SETTLE_MILLISECONDS);
    const MediaCounters a_final = read_counters(*client_a);
    const MediaCounters b_final = read_counters(*client_b);
    print_counters("A", a_established, a_final);
    print_counters("B", b_established, b_final);

    // A2: each driver's own CnC carries its media's counters, and its transports
    // took datagrams off the media. A missing label is reported as -1 and
    // fails here; a zero where traffic crossed fails too.
    EXPECT_GE(a_final.port_info, 0) << "driver A has no `dpdk port` counter (75)";
    EXPECT_GE(b_final.port_info, 0) << "driver B has no `dpdk port` counter (75)";
    EXPECT_GE(a_final.transports, 1) << "driver A has no `dpdk transport` counter (76)";
    EXPECT_GE(b_final.transports, 1) << "driver B has no `dpdk transport` counter (76)";
    // Every 76 this driver carries is labelled with this driver's own address;
    // a reading taken from the other driver's CnC cannot pass here.
    const std::uint32_t address_a = ipv4_of(pair->a_address);
    const std::uint32_t address_b = ipv4_of(pair->b_address);
    ASSERT_NE(0U, address_a);
    ASSERT_NE(0U, address_b);
    EXPECT_FALSE(a_final.transport_addresses.empty()) << "driver A's 76s carry no key";
    EXPECT_FALSE(b_final.transport_addresses.empty()) << "driver B's 76s carry no key";
    for (const std::uint32_t address : a_final.transport_addresses)
    {
        EXPECT_EQ(address_a, address) << "a `dpdk transport` counter on A is labelled with another address";
    }
    for (const std::uint32_t address : b_final.transport_addresses)
    {
        EXPECT_EQ(address_b, address) << "a `dpdk transport` counter on B is labelled with another address";
    }
    EXPECT_GT(a_final.transport_datagrams, 0) << "no datagram reached driver A over the media";
    EXPECT_GT(b_final.transport_datagrams, 0) << "no datagram reached driver B over the media";
    EXPECT_GT(a_final.mempool_available, 0) << "driver A's pool is empty or unreadable (91)";
    EXPECT_GT(b_final.mempool_available, 0) << "driver B's pool is empty or unreadable (91)";
    EXPECT_GT(a_final.packets, 0) << "driver A's port saw no packet (82)";
    EXPECT_GT(b_final.packets, 0) << "driver B's port saw no packet (82)";
    EXPECT_GT(a_final.bytes, 0) << "driver A's port saw no byte (83)";
    EXPECT_GT(b_final.bytes, 0) << "driver B's port saw no byte (83)";
    EXPECT_GT(a_final.packets, a_established.packets) << "driver A's port counter did not move after the first arm";
    EXPECT_GT(b_final.packets, b_established.packets) << "driver B's port counter did not move after the first arm";

    // A6: nothing was lost, refused or invented, and the neighbour was resolved
    // once rather than per frame.
    EXPECT_EQ(0, a_final.error) << "`dpdk error` on A (79)";
    EXPECT_EQ(0, b_final.error) << "`dpdk error` on B (79)";
    EXPECT_EQ(0, a_final.missed) << "`dpdk missed packets` on A (84)";
    EXPECT_EQ(0, b_final.missed) << "`dpdk missed packets` on B (84)";
    EXPECT_EQ(0, a_final.checksum_failure) << "`dpdk checksum failure` on A (89)";
    EXPECT_EQ(0, b_final.checksum_failure) << "`dpdk checksum failure` on B (89)";
    EXPECT_EQ(0, a_final.rx_receiver_discard) << "`dpdk rx receiver discard` on A (95)";
    EXPECT_EQ(0, b_final.rx_receiver_discard) << "`dpdk rx receiver discard` on B (95)";
    EXPECT_EQ(0, a_final.nobufs) << "`dpdk nobufs` on A (77)";
    EXPECT_EQ(0, b_final.nobufs) << "`dpdk nobufs` on B (77)";
    EXPECT_EQ(0, a_final.tx_eagain) << "`dpdk tx eagain` on A (78)";
    EXPECT_EQ(0, b_final.tx_eagain) << "`dpdk tx eagain` on B (78)";
    EXPECT_EQ(a_established.arp_miss, a_final.arp_miss) << "A re-resolved the neighbour during the run (85)";
    EXPECT_EQ(b_established.arp_miss, b_final.arp_miss) << "B re-resolved the neighbour during the run (85)";
}

#endif
