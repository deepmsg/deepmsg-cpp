// The DPDK port seam, over a real PMD.
//
// The EAL may be initialised once per process, so every case that wants one
// runs in a child of this same binary (the Rust track's pattern). `net_pcap`
// needs no privileges, no huge pages and no veth.
//
// Compiled only in a build with `-DAXC_WITH_DPDK=ON`.

#if defined(AXC_WITH_DPDK)

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

#include <fcntl.h>

#include "core/config/property_source.h"
#include "core/driver/embedded_driver.h"
#include "core/media/dpdk/counters.h"
#include "core/media/dpdk/eal.h"
#include "core/media/dpdk/media.h"
#include "core/media/dpdk/mempool.h"
#include "core/media/dpdk/port.h"
#include "core/media/dpdk/stack/frame.h"
#include "core/media/dpdk/stack/stack.h"

extern "C"
{
#include "aeronc.h"
#include "util/aeron_properties_util.h"
}

namespace
{

constexpr const char* PCAP_PATH = "/tmp/axc-dpdk-port-test.pcap";

}  // namespace

TEST(DpdkPortChild, TheEalOpensAVirtualPortAndSendsAFrame)
{
    if (nullptr == std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "runs in a child process";
    }

    (void)std::remove(PCAP_PATH);
    const std::vector<std::string> args{"/proc/self/exe", "--no-huge", "--no-shconf", "--no-telemetry",
                                        std::string{"--vdev=net_pcap0,tx_pcap="} + PCAP_PATH};

    auto eal = axc::core::media::dpdk::Eal::init(args);
    ASSERT_TRUE(eal.has_value()) << eal.error().message.view();

    {
        auto pool = axc::core::media::dpdk::Mempool::create("axc-test-pool", 1023, 128, 2048, -1);
        ASSERT_TRUE(pool.has_value()) << pool.error().message.view();

        // By name, not by index: a host with a probeable NIC puts it in front of
        // the vdev (GitHub's Azure runners probe `net_mana` as port 0), and this
        // case is about the pcap port, wherever it lands.
        auto port = axc::core::media::dpdk::Port::open_by_name("net_pcap0");
        ASSERT_TRUE(port.has_value()) << port.error().message.view();

        const auto info = port->info();
        ASSERT_TRUE(info.has_value()) << info.error().message.view();
        EXPECT_EQ("net_pcap", info->driver);

        ASSERT_TRUE(port->configure(0, 1, 0, 0).has_value());
        ASSERT_TRUE(port->setup_tx_queue(0, 256).has_value());
        ASSERT_TRUE(port->start().has_value());

        shim_tx_slot slot{};
        ASSERT_EQ(1u, port->tx_alloc(*pool, &slot, 1));
        ASSERT_NE(nullptr, slot.data);
        ASSERT_GE(slot.room, 60);

        for (std::size_t index = 0; index < 60; ++index)
        {
            slot.data[index] = static_cast<std::uint8_t>(index);
        }
        slot.len = 60;
        EXPECT_EQ(1u, port->tx_burst(0, &slot, 1));

        port->stop();
        port->close();

        std::error_code error;
        const auto size = std::filesystem::file_size(PCAP_PATH, error);
        EXPECT_FALSE(error);
        EXPECT_GT(size, 24u);
    }

    EXPECT_TRUE(axc::core::media::dpdk::Eal::cleanup().has_value());
    (void)std::remove(PCAP_PATH);
}

TEST(DpdkPort, TheChildProcessRunsTheEal)
{
    if (nullptr != std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "we are the child";
    }

    const pid_t child = ::fork();
    ASSERT_GE(child, 0);

    if (0 == child)
    {
        (void)::setenv("AXC_DPDK_CHILD", "1", 1);
        ::execl("/proc/self/exe", "/proc/self/exe",
                "--gtest_filter=DpdkPortChild.TheEalOpensAVirtualPortAndSendsAFrame", "--gtest_color=no",
                static_cast<char*>(nullptr));
        ::_exit(127);
    }

    int status = 0;
    ASSERT_EQ(child, ::waitpid(child, &status, 0));
    EXPECT_TRUE(WIFEXITED(status));
    EXPECT_EQ(0, WEXITSTATUS(status));
}

namespace
{

constexpr const char* MEDIA_RX_PATH = "/tmp/axc-dpdk-media-rx.pcap";
constexpr const char* MEDIA_TX_PATH = "/tmp/axc-dpdk-media-tx.pcap";

void write_pcap_header(std::ofstream& out)
{
    const std::array<std::uint8_t, 24> header{0xd4, 0xc3, 0xb2, 0xa1, 0x02, 0x00, 0x04, 0x00, 0,    0,    0,    0,
                                              0,    0,    0,    0,    0xff, 0xff, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00};
    out.write(reinterpret_cast<const char*>(header.data()), static_cast<std::streamsize>(header.size()));
}

void append_pcap_record(std::ofstream& out, std::span<const std::uint8_t> frame)
{
    std::array<std::uint8_t, 16> header{};
    const auto length = static_cast<std::uint32_t>(frame.size());
    for (int index = 0; index < 4; ++index)
    {
        header[8 + index] = static_cast<std::uint8_t>((length >> (8 * index)) & 0xff);
        header[12 + index] = static_cast<std::uint8_t>((length >> (8 * index)) & 0xff);
    }
    out.write(reinterpret_cast<const char*>(header.data()), static_cast<std::streamsize>(header.size()));
    out.write(reinterpret_cast<const char*>(frame.data()), static_cast<std::streamsize>(frame.size()));
}

}  // namespace

TEST(DpdkMediaChild, TheStackReceivesAndSendsOnAPcapPort)
{
    if (nullptr == std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "runs in a child process";
    }

    const std::array<std::uint8_t, 5> payload{9, 8, 7, 6, 5};
    std::vector<std::uint8_t> inbound(axc::core::media::dpdk::frame::IPV4_UDP_HEADER_LENGTH + payload.size());
    std::copy(payload.begin(), payload.end(), inbound.begin() + axc::core::media::dpdk::frame::IPV4_UDP_HEADER_LENGTH);
    const axc::core::media::dpdk::frame::UdpWrite write{
        axc::core::media::dpdk::frame::Ipv4Write{
            axc::core::media::dpdk::frame::BROADCAST, {0x02, 0, 0, 0, 0, 0x02}, 0x0a0a0002, 0x0a0a0001, 1},
        6000, 5000, payload.size()};
    const auto written = axc::core::media::dpdk::frame::write_ipv4_udp_here(inbound, write);
    ASSERT_TRUE(written.has_value());
    inbound.resize(*written);

    (void)std::remove(MEDIA_RX_PATH);
    (void)std::remove(MEDIA_TX_PATH);
    {
        std::ofstream pcap{MEDIA_RX_PATH, std::ios::binary};
        ASSERT_TRUE(pcap);
        write_pcap_header(pcap);
        append_pcap_record(pcap, inbound);
    }

    axc::core::media::dpdk::Settings settings;
    settings.enabled = true;
    settings.eal_args = std::string{"--no-huge --no-shconf --no-telemetry --vdev=net_pcap0,rx_pcap="} + MEDIA_RX_PATH +
                        ",tx_pcap=" + MEDIA_TX_PATH;
    settings.port = std::string{"net_pcap0"};
    settings.ipv4_address = axc::core::media::dpdk::Ipv4Prefix{0x0a0a0001, 24};
    settings.static_neighbours = "10.10.0.2=02:00:00:00:00:02";
    settings.rx_descriptors = 256;
    settings.tx_descriptors = 256;
    settings.mempool_size = 4095;
    settings.mempool_cache = 128;
    settings.rx_burst = 32;

    auto media = axc::core::media::dpdk::open_media(settings);
    ASSERT_TRUE(media.has_value()) << media.error().message.view();
    auto* stack = media->stack();
    ASSERT_NE(nullptr, stack);

    const auto binding = stack->bind(0x0a0a0001, 5000, std::nullopt, {0x0a0a0001, 5000});
    ASSERT_TRUE(binding.has_value());

    std::array<std::uint8_t, 64> received{};
    std::size_t got = 0;
    for (int attempt = 0; attempt < 100 && 0 == got; ++attempt)
    {
        (void)stack->pump();
        got = stack->receive(*binding, received);
    }
    ASSERT_EQ(payload.size(), got);
    EXPECT_EQ(payload[0], received[0]);
    EXPECT_EQ(payload[4], received[4]);

    const std::array<std::uint8_t, 3> outbound{1, 2, 3};
    EXPECT_EQ(outbound.size(), *stack->send(*binding, {0x0a0a0002, 6000}, outbound));

    EXPECT_TRUE(media->close().has_value());

    std::error_code error;
    const auto size = std::filesystem::file_size(MEDIA_TX_PATH, error);
    EXPECT_FALSE(error);
    EXPECT_GT(size, 24u);
    (void)std::remove(MEDIA_RX_PATH);
    (void)std::remove(MEDIA_TX_PATH);
}

TEST(DpdkMedia, TheStackRunsInAChildProcess)
{
    if (nullptr != std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "we are the child";
    }

    const pid_t child = ::fork();
    ASSERT_GE(child, 0);

    if (0 == child)
    {
        (void)::setenv("AXC_DPDK_CHILD", "1", 1);
        ::execl("/proc/self/exe", "/proc/self/exe", "--gtest_filter=DpdkMediaChild.TheStackReceivesAndSendsOnAPcapPort",
                "--gtest_color=no", static_cast<char*>(nullptr));
        ::_exit(127);
    }

    int status = 0;
    ASSERT_EQ(child, ::waitpid(child, &status, 0));
    EXPECT_TRUE(WIFEXITED(status));
    EXPECT_EQ(0, WEXITSTATUS(status));
}

namespace
{

struct SeenCounters
{
    int transports{0};
    bool port_info{false};
    bool packets{false};
    bool poller{false};
};

void count_one(std::int64_t /*value*/, std::int32_t /*id*/, std::int32_t type_id, const std::uint8_t* /*key*/,
               std::size_t /*key_length*/, const char* /*label*/, std::size_t /*label_length*/, void* clientd)
{
    auto* seen = static_cast<SeenCounters*>(clientd);
    if (axc::core::media::dpdk::counters::TRANSPORT == type_id)
    {
        ++seen->transports;
    }
    if (axc::core::media::dpdk::counters::PORT_INFO == type_id)
    {
        seen->port_info = true;
    }
    if (axc::core::media::dpdk::counters::PKTS == type_id)
    {
        seen->packets = true;
    }
    if (axc::core::media::dpdk::counters::POLLER == type_id)
    {
        seen->poller = true;
    }
}

}  // namespace

TEST(DpdkCountersChild, TheDpdkCountersAreInTheCnc)
{
    if (nullptr == std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "runs in a child process";
    }

    char directory[] = "/tmp/axc-dpdk-counters-XXXXXX";
    ASSERT_NE(nullptr, ::mkdtemp(directory));
    (void)::aeron_properties_setenv("aeron.dir", directory);
    (void)std::remove("/tmp/axc-dpdk-counters.pcap");

    axc::core::PropertySource properties;
    properties.set("deepmsg.dpdk.enabled", "true");
    properties.set("deepmsg.dpdk.eal.args",
                   "--no-huge --no-shconf --no-telemetry --vdev=net_pcap0,tx_pcap=/tmp/axc-dpdk-counters.pcap");
    properties.set("deepmsg.dpdk.port", "net_pcap0");
    properties.set("deepmsg.dpdk.ipv4.address", "10.10.0.1/24");
    properties.set("deepmsg.dpdk.static.neighbours", "10.10.0.2=02:00:00:00:00:02");

    auto driver = axc::core::driver::EmbeddedDriver::create(properties, nullptr, nullptr);
    ASSERT_TRUE(driver.has_value()) << driver.error().message.view();

    {
        aeron_context_t* context = nullptr;
        aeron_t* client = nullptr;
        ASSERT_EQ(0, aeron_context_init(&context));
        ASSERT_EQ(0, aeron_context_set_dir(context, directory));
        (void)aeron_context_set_use_conductor_agent_invoker(context, true);
        ASSERT_EQ(0, aeron_init(&client, context));

        aeron_async_add_publication_t* async = nullptr;
        ASSERT_EQ(0, aeron_async_add_publication(&async, client, "aeron:udp?endpoint=localhost:20104", 1001));
        aeron_publication_t* publication = nullptr;
        for (int attempt = 0; attempt < 3000 && nullptr == publication; ++attempt)
        {
            (void)aeron_main_do_work(client);
            if (1 == aeron_async_add_publication_poll(&publication, async))
            {
                break;
            }
            ::usleep(1000);
        }
        ASSERT_NE(nullptr, publication);

        for (int pass = 0; pass < 50; ++pass)
        {
            (void)aeron_main_do_work(client);
            ::usleep(1000);
        }

        auto* reader = aeron_counters_reader(client);
        ASSERT_NE(nullptr, reader);
        SeenCounters seen;
        aeron_counters_reader_foreach_counter(reader, &count_one, &seen);

        EXPECT_GE(seen.transports, 1) << "a DPDK transport counter (76) per channel";
        EXPECT_TRUE(seen.port_info) << "port info (75)";
        EXPECT_TRUE(seen.packets) << "port packets (82)";
        EXPECT_TRUE(seen.poller) << "poller (87)";

        (void)aeron_publication_close(publication, nullptr, nullptr);
        (void)aeron_close(client);
        (void)aeron_context_close(context);
    }

    std::ignore = driver->close();
    std::filesystem::remove_all(directory);
    (void)std::remove("/tmp/axc-dpdk-counters.pcap");
}

TEST(DpdkCounters, TheCountersChildProcess)
{
    if (nullptr != std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "we are the child";
    }

    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (0 == child)
    {
        (void)::setenv("AXC_DPDK_CHILD", "1", 1);
        ::execl("/proc/self/exe", "/proc/self/exe", "--gtest_filter=DpdkCountersChild.TheDpdkCountersAreInTheCnc",
                "--gtest_color=no", static_cast<char*>(nullptr));
        ::_exit(127);
    }

    int status = 0;
    ASSERT_EQ(child, ::waitpid(child, &status, 0));
    EXPECT_TRUE(WIFEXITED(status));
    EXPECT_EQ(0, WEXITSTATUS(status));
}

namespace
{

constexpr const char* MEDIA_LINE_PATH = "/tmp/axc-dpdk-media-line.txt";
constexpr const char* MEDIA_LINE_PCAP = "/tmp/axc-dpdk-media-line.pcap";

}  // namespace

TEST(DpdkMediaChild, TheMediaSaysWhichPortItOpened)
{
    if (nullptr == std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "runs in a child process";
    }

    (void)std::remove(MEDIA_LINE_PCAP);

    axc::core::media::dpdk::Settings settings;
    settings.enabled = true;
    settings.eal_args = std::string{"--no-huge --no-shconf --vdev=net_pcap0,tx_pcap="} + MEDIA_LINE_PCAP;
    settings.port = std::string{"net_pcap0"};
    settings.ipv4_address = axc::core::media::dpdk::Ipv4Prefix{0x0a0a0001, 24};
    settings.rx_descriptors = 256;
    settings.tx_descriptors = 256;
    settings.mempool_size = 4095;
    settings.mempool_cache = 128;

    auto media = axc::core::media::dpdk::open_media(settings);
    ASSERT_TRUE(media.has_value()) << media.error().message.view();
    EXPECT_TRUE(media->close().has_value());
    (void)std::remove(MEDIA_LINE_PCAP);
}

// "Configured" is not "on the media": the line that names the port is what a
// reading of stdout can check, and it is on stdout because the reference's
// harnesses read stderr as a failure channel. An empty stderr proves nothing.
TEST(DpdkMedia, TheMediaLineIsOnStdout)
{
    if (nullptr != std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "we are the child";
    }

    (void)std::remove(MEDIA_LINE_PATH);
    const pid_t child = ::fork();
    ASSERT_GE(child, 0);

    if (0 == child)
    {
        const int output = ::open(MEDIA_LINE_PATH, O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (0 > output || 0 > ::dup2(output, STDOUT_FILENO))
        {
            ::_exit(127);
        }
        (void)::close(output);
        (void)::setenv("AXC_DPDK_CHILD", "1", 1);
        ::execl("/proc/self/exe", "/proc/self/exe", "--gtest_filter=DpdkMediaChild.TheMediaSaysWhichPortItOpened",
                "--gtest_color=no", static_cast<char*>(nullptr));
        ::_exit(127);
    }

    int status = 0;
    ASSERT_EQ(child, ::waitpid(child, &status, 0));
    EXPECT_TRUE(WIFEXITED(status));
    ASSERT_EQ(0, WEXITSTATUS(status)) << "the child failed; what it wrote is in " << MEDIA_LINE_PATH;

    std::ifstream input{MEDIA_LINE_PATH};
    ASSERT_TRUE(input);
    const std::string output{std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};

    EXPECT_NE(std::string::npos, output.find("dpdk media on net_pcap0 (net_pcap), address 10.10.0.1/24, mtu "))
        << output;
    // This child opens with the default mode (DEDICATED) on a device that
    // reports one transmit queue: the line says so, and the DEDICATED sender
    // and receiver agents sharing that queue earns the warning rather than
    // silence.
    EXPECT_NE(std::string::npos, output.find("queues 1 rx 1 tx")) << output;
    EXPECT_NE(std::string::npos, output.find("dpdk media warning: this port offers one transmit queue")) << output;
    EXPECT_NE(std::string::npos, output.find("dpdk media closed")) << output;
    (void)std::remove(MEDIA_LINE_PATH);
}

namespace
{

constexpr const char* OVERSIZE_PCAP_PATH = "/tmp/axc-dpdk-oversize.pcap";
constexpr const char* CLUSTER_FREE_PORT = "24610";
constexpr std::size_t OVERSIZE_PAYLOAD = 1500;

/// The driver's error log, read off the running driver's CnC.
struct ErrorLogSeen
{
    bool ours{false};
};

void on_error_log_entry(int32_t /*observation_count*/, int64_t /*first*/, int64_t /*last*/, const char* error,
                        size_t error_length, void* clientd)
{
    auto* seen = static_cast<ErrorLogSeen*>(clientd);
    const std::string message{error, error_length};
    if (std::string::npos != message.find("larger than the port allows"))
    {
        seen->ours = true;
    }
}

}  // namespace

// A datagram the port cannot frame is refused at the seam with `EMSGSIZE`, not
// answered with back pressure: `dpdk_send` returns -1, the driver logs it
// (`aeron_driver_sender.c:433-437`) and counter 79 moves. The channel's MTU
// (`aeron.mtu.length`) is the driver's and is deliberately larger here than
// the port's 1500.
TEST(DpdkOversizeChild, ThePortRefusesADatagramItCannotFrame)
{
    if (nullptr == std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "runs in a child process";
    }

    (void)std::remove(OVERSIZE_PCAP_PATH);
    char directory[] = "/tmp/axc-dpdk-oversize-XXXXXX";
    ASSERT_NE(nullptr, ::mkdtemp(directory));
    (void)::aeron_properties_setenv("aeron.dir", directory);
    (void)::aeron_properties_setenv("aeron.mtu.length", "1600");

    axc::core::PropertySource properties;
    properties.set("deepmsg.dpdk.enabled", "true");
    properties.set("deepmsg.dpdk.eal.args",
                   std::string{"--no-huge --no-shconf --no-telemetry --vdev=net_pcap0,tx_pcap="} + OVERSIZE_PCAP_PATH);
    properties.set("deepmsg.dpdk.port", "net_pcap0");
    properties.set("deepmsg.dpdk.ipv4.address", "10.10.0.1/24");

    auto driver = axc::core::driver::EmbeddedDriver::create(properties, nullptr, nullptr);
    ASSERT_TRUE(driver.has_value()) << driver.error().message.view();

    aeron_context_t* context = nullptr;
    aeron_t* client = nullptr;
    ASSERT_EQ(0, aeron_context_init(&context));
    ASSERT_EQ(0, aeron_context_set_dir(context, directory));
    (void)aeron_context_set_use_conductor_agent_invoker(context, true);
    ASSERT_EQ(0, aeron_init(&client, context));

    const std::string channel = std::string{"aeron:udp?endpoint=10.10.0.1:"} + CLUSTER_FREE_PORT;
    aeron_async_add_subscription_t* sub_add = nullptr;
    aeron_async_add_publication_t* pub_add = nullptr;
    ASSERT_EQ(
        0, aeron_async_add_subscription(&sub_add, client, channel.c_str(), 1001, nullptr, nullptr, nullptr, nullptr));
    ASSERT_EQ(0, aeron_async_add_publication(&pub_add, client, channel.c_str(), 1001));
    aeron_subscription_t* subscription = nullptr;
    aeron_publication_t* publication = nullptr;
    for (int attempt = 0; attempt < 3000 && (nullptr == subscription || nullptr == publication); ++attempt)
    {
        (void)aeron_main_do_work(client);
        if (nullptr == subscription)
        {
            (void)aeron_async_add_subscription_poll(&subscription, sub_add);
        }
        if (nullptr == publication)
        {
            (void)aeron_async_add_publication_poll(&publication, pub_add);
        }
        ::usleep(1000);
    }
    ASSERT_NE(nullptr, subscription);
    ASSERT_NE(nullptr, publication);

    const std::string payload(OVERSIZE_PAYLOAD, 'z');
    bool offered = false;
    for (int attempt = 0; attempt < 3000 && !offered; ++attempt)
    {
        (void)aeron_main_do_work(client);
        offered = 0 < aeron_publication_offer(publication, reinterpret_cast<const std::uint8_t*>(payload.data()),
                                              payload.size(), nullptr, nullptr);
        ::usleep(1000);
    }
    ASSERT_TRUE(offered) << "the client never accepted the payload";

    // Counter 79 is mirrored from the stack on every receive poll.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    std::int64_t errors = 0;
    while (std::chrono::steady_clock::now() < deadline && 0 == errors)
    {
        (void)aeron_main_do_work(client);
        auto* reader = aeron_counters_reader(client);
        ASSERT_NE(nullptr, reader);
        const std::int32_t max_id = aeron_counters_reader_max_counter_id(reader);
        for (std::int32_t id = 0; id < max_id && 0 == errors; ++id)
        {
            char label[256] = {};
            const int length = aeron_counters_reader_counter_label(reader, id, label, sizeof(label));
            if (length > 0 && std::string_view{label, static_cast<std::size_t>(length)} == "dpdk error")
            {
                const std::int64_t* address = aeron_counters_reader_addr(reader, id);
                errors = nullptr == address ? 0 : __atomic_load_n(address, __ATOMIC_ACQUIRE);
            }
        }
        ::usleep(1000);
    }
    EXPECT_GE(errors, 1) << "counter 79 did not move";

    ErrorLogSeen seen;
    aeron_cnc_t* cnc = nullptr;
    ASSERT_EQ(0, aeron_cnc_init(&cnc, directory, 5000));
    (void)aeron_cnc_error_log_read(cnc, &on_error_log_entry, &seen, 0);
    EXPECT_TRUE(seen.ours) << "the driver's error log has no 'larger than the port allows'";
    aeron_cnc_close(cnc);

    (void)aeron_publication_close(publication, nullptr, nullptr);
    (void)aeron_subscription_close(subscription, nullptr, nullptr);
    (void)aeron_close(client);
    (void)aeron_context_close(context);
    EXPECT_TRUE(driver->close().has_value());
    std::filesystem::remove_all(directory);
    (void)std::remove(OVERSIZE_PCAP_PATH);
}

TEST(DpdkOversize, TheChildProcess)
{
    if (nullptr != std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "we are the child";
    }

    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (0 == child)
    {
        (void)::setenv("AXC_DPDK_CHILD", "1", 1);
        ::execl("/proc/self/exe", "/proc/self/exe",
                "--gtest_filter=DpdkOversizeChild.ThePortRefusesADatagramItCannotFrame", "--gtest_color=no",
                static_cast<char*>(nullptr));
        ::_exit(127);
    }
    int status = 0;
    ASSERT_EQ(child, ::waitpid(child, &status, 0));
    EXPECT_TRUE(WIFEXITED(status));
    EXPECT_EQ(0, WEXITSTATUS(status));
}

namespace
{

constexpr const char* SHARED_PCAP_PATH = "/tmp/axc-dpdk-shared.pcap";
constexpr const char* INVOKER_PCAP_PATH = "/tmp/axc-dpdk-invoker.pcap";
constexpr const char* THREADING_CHANNEL = "aeron:udp?endpoint=10.10.0.2:24620";

/// With a manual main loop the calling thread is one of the driver's agents,
/// so it must run, not wait (`aeronmd.c:165-168`).
void drive(aeron_driver_t* driven_driver)
{
    if (nullptr != driven_driver)
    {
        aeron_driver_main_idle_strategy(driven_driver, aeron_driver_main_do_work(driven_driver));
    }
}

/// A publication through the driver in `directory`, one datagram offered at
/// the peer `10.10.0.2` -- a peer and not the media's own address, because the
/// stack loops a self-addressed frame inside itself and only a peer's frame
/// must leave the port. `driven_driver` is set when the driver's agents run on
/// this thread (INVOKER); the other modes run them on their own threads and
/// pass null. Answers whether the publication was created.
[[nodiscard]] bool open_a_publication(const char* directory, aeron_driver_t* driven_driver)
{
    aeron_context_t* context = nullptr;
    aeron_t* client = nullptr;
    if (0 != aeron_context_init(&context))
    {
        return false;
    }
    (void)aeron_context_set_dir(context, directory);
    (void)aeron_context_set_use_conductor_agent_invoker(context, true);
    if (0 != aeron_init(&client, context))
    {
        (void)aeron_context_close(context);
        return false;
    }

    aeron_async_add_publication_t* pub_add = nullptr;
    const bool asked = 0 == aeron_async_add_publication(&pub_add, client, THREADING_CHANNEL, 1001);
    aeron_publication_t* publication = nullptr;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (asked && std::chrono::steady_clock::now() < deadline && nullptr == publication)
    {
        drive(driven_driver);
        (void)aeron_main_do_work(client);
        (void)aeron_async_add_publication_poll(&publication, pub_add);
        if (nullptr != publication)
        {
            const std::array<std::uint8_t, 16> payload{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
            // The offer is attempted, not required: with no receiver the
            // publication's limit stays at its initial position and the sender
            // still announces itself with a setup frame, which is the frame
            // this case reads off the port.
            (void)aeron_publication_offer(publication, payload.data(), payload.size(), nullptr, nullptr);
        }
        ::usleep(1000);
    }

    // Give the sending agent the passes it needs to take the offer.
    for (int pass = 0; pass < 100; ++pass)
    {
        drive(driven_driver);
        (void)aeron_main_do_work(client);
        ::usleep(1000);
    }

    if (nullptr != publication)
    {
        (void)aeron_publication_close(publication, nullptr, nullptr);
    }
    (void)aeron_close(client);
    (void)aeron_context_close(context);
    return nullptr != publication;
}

[[nodiscard]] long long pcap_size(const char* path)
{
    std::error_code error;
    const auto size = std::filesystem::file_size(path, error);
    return error ? -1 : static_cast<long long>(size);
}

}  // namespace

// SHARED runs the sender and the receiver agents on one thread
// (`aeron_driver_shared_do_work`, `aeron_driver.c:753-761`). The media used to
// refuse every mode but DEDICATED; here it must accept the driver and carry a
// frame from that one thread, which is the port's single transmit queue.
TEST(DpdkSharedChild, TheSharedDriverCarriesAFrameThroughTheMedia)
{
    if (nullptr == std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "runs in a child process";
    }

    (void)std::remove(SHARED_PCAP_PATH);
    char directory[] = "/tmp/axc-dpdk-shared-XXXXXX";
    ASSERT_NE(nullptr, ::mkdtemp(directory));
    (void)::aeron_properties_setenv("aeron.dir", directory);
    (void)::aeron_properties_setenv("aeron.threading.mode", "SHARED");

    axc::core::PropertySource properties;
    properties.set("deepmsg.dpdk.enabled", "true");
    properties.set("deepmsg.dpdk.eal.args",
                   std::string{"--no-huge --no-shconf --no-telemetry --vdev=net_pcap0,tx_pcap="} + SHARED_PCAP_PATH);
    properties.set("deepmsg.dpdk.port", "net_pcap0");
    properties.set("deepmsg.dpdk.ipv4.address", "10.10.0.1/24");
    properties.set("deepmsg.dpdk.static.neighbours", "10.10.0.2=02:00:00:00:00:02");

    auto driver = axc::core::driver::EmbeddedDriver::create(properties, nullptr, nullptr);
    ASSERT_TRUE(driver.has_value()) << driver.error().message.view();

    EXPECT_TRUE(open_a_publication(directory, nullptr)) << "the client never got a publication";
    EXPECT_TRUE(driver->close().has_value());

    EXPECT_GT(pcap_size(SHARED_PCAP_PATH), 24) << "no frame left through the shared thread's transmit queue";
    std::filesystem::remove_all(directory);
    (void)std::remove(SHARED_PCAP_PATH);
}

TEST(DpdkShared, TheChildProcess)
{
    if (nullptr != std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "we are the child";
    }

    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (0 == child)
    {
        (void)::setenv("AXC_DPDK_CHILD", "1", 1);
        ::execl("/proc/self/exe", "/proc/self/exe",
                "--gtest_filter=DpdkSharedChild.TheSharedDriverCarriesAFrameThroughTheMedia", "--gtest_color=no",
                static_cast<char*>(nullptr));
        ::_exit(127);
    }
    int status = 0;
    ASSERT_EQ(child, ::waitpid(child, &status, 0));
    EXPECT_TRUE(WIFEXITED(status));
    EXPECT_EQ(0, WEXITSTATUS(status));
}

// INVOKER is the mode that cannot start its agents on a thread of its own: the
// reference's driver start refuses it unless the caller runs the loop
// (`aeron_driver.c:1380-1386`). Both halves are the criterion: the refusal when
// the caller does not run it, and frames carried when it does. They are two
// child processes because the EAL is initialised once per process.
TEST(DpdkInvokerRefusedChild, TheDriverStartRefusesInvokerWithoutAManualLoop)
{
    if (nullptr == std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "runs in a child process";
    }

    char directory[] = "/tmp/axc-dpdk-invoker-refused-XXXXXX";
    ASSERT_NE(nullptr, ::mkdtemp(directory));
    (void)::aeron_properties_setenv("aeron.dir", directory);
    (void)::aeron_properties_setenv("aeron.threading.mode", "INVOKER");

    axc::core::PropertySource properties;
    properties.set("deepmsg.dpdk.enabled", "true");
    properties.set("deepmsg.dpdk.eal.args",
                   std::string{"--no-huge --no-shconf --no-telemetry --vdev=net_pcap0,tx_pcap="} + INVOKER_PCAP_PATH);
    properties.set("deepmsg.dpdk.port", "net_pcap0");
    properties.set("deepmsg.dpdk.ipv4.address", "10.10.0.1/24");

    auto refused = axc::core::driver::EmbeddedDriver::create(properties, nullptr, nullptr);
    ASSERT_FALSE(refused.has_value()) << "INVOKER started a runner of its own";
    EXPECT_NE(std::string::npos, std::string{refused.error().message.view()}.find("manual_main_loop"))
        << refused.error().message.view();
    std::filesystem::remove_all(directory);
    (void)std::remove(INVOKER_PCAP_PATH);
}

TEST(DpdkInvokerRefused, TheChildProcess)
{
    if (nullptr != std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "we are the child";
    }

    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (0 == child)
    {
        (void)::setenv("AXC_DPDK_CHILD", "1", 1);
        ::execl("/proc/self/exe", "/proc/self/exe",
                "--gtest_filter=DpdkInvokerRefusedChild.TheDriverStartRefusesInvokerWithoutAManualLoop",
                "--gtest_color=no", static_cast<char*>(nullptr));
        ::_exit(127);
    }
    int status = 0;
    ASSERT_EQ(child, ::waitpid(child, &status, 0));
    EXPECT_TRUE(WIFEXITED(status));
    EXPECT_EQ(0, WEXITSTATUS(status));
}

TEST(DpdkInvokerChild, TheCallerThreadRunsTheDriver)
{
    if (nullptr == std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "runs in a child process";
    }

    (void)std::remove(INVOKER_PCAP_PATH);
    char directory[] = "/tmp/axc-dpdk-invoker-XXXXXX";
    ASSERT_NE(nullptr, ::mkdtemp(directory));
    (void)::aeron_properties_setenv("aeron.dir", directory);
    (void)::aeron_properties_setenv("aeron.threading.mode", "INVOKER");

    axc::core::PropertySource properties;
    properties.set("deepmsg.dpdk.enabled", "true");
    properties.set("deepmsg.dpdk.eal.args",
                   std::string{"--no-huge --no-shconf --no-telemetry --vdev=net_pcap0,tx_pcap="} + INVOKER_PCAP_PATH);
    properties.set("deepmsg.dpdk.port", "net_pcap0");
    properties.set("deepmsg.dpdk.ipv4.address", "10.10.0.1/24");
    properties.set("deepmsg.dpdk.static.neighbours", "10.10.0.2=02:00:00:00:00:02");

    auto driver = axc::core::driver::EmbeddedDriver::create(properties, nullptr, nullptr,
                                                            axc::core::driver::StartMode::caller_thread);
    ASSERT_TRUE(driver.has_value()) << driver.error().message.view();

    EXPECT_TRUE(open_a_publication(directory, driver->driver())) << "the client never got a publication";
    EXPECT_TRUE(driver->close().has_value());

    EXPECT_GT(pcap_size(INVOKER_PCAP_PATH), 24) << "no frame left while the caller ran the driver";
    std::filesystem::remove_all(directory);
    (void)std::remove(INVOKER_PCAP_PATH);
}

TEST(DpdkInvoker, TheChildProcess)
{
    if (nullptr != std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "we are the child";
    }

    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (0 == child)
    {
        (void)::setenv("AXC_DPDK_CHILD", "1", 1);
        ::execl("/proc/self/exe", "/proc/self/exe", "--gtest_filter=DpdkInvokerChild.TheCallerThreadRunsTheDriver",
                "--gtest_color=no", static_cast<char*>(nullptr));
        ::_exit(127);
    }
    int status = 0;
    ASSERT_EQ(child, ::waitpid(child, &status, 0));
    EXPECT_TRUE(WIFEXITED(status));
    EXPECT_EQ(0, WEXITSTATUS(status));
}

namespace
{

constexpr const char* IDLE_WARNING_PATH = "/tmp/axc-dpdk-idle-warning.txt";
constexpr const char* IDLE_QUIET_PATH = "/tmp/axc-dpdk-idle-quiet.txt";

[[nodiscard]] std::string read_all(const char* path)
{
    std::ifstream input{path};
    if (!input)
    {
        return {};
    }
    return std::string{std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};
}

}  // namespace

// A sleeping idle strategy leaves the frames in the NIC ring (counter 84) with
// no hint of why; the media says so once, on stdout, and still opens (design
// §4.12).
TEST(DpdkIdleWarningChild, ASleepingReceiverIsAWarning)
{
    if (nullptr == std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "runs in a child process";
    }

    (void)::setenv("AERON_RECEIVER_IDLE_STRATEGY", "sleeping", 1);
    char directory[] = "/tmp/axc-dpdk-idle-XXXXXX";
    ASSERT_NE(nullptr, ::mkdtemp(directory));
    (void)::aeron_properties_setenv("aeron.dir", directory);
    (void)::aeron_properties_setenv("aeron.receiver.idle.strategy", "sleeping");

    axc::core::PropertySource properties;
    properties.set("deepmsg.dpdk.enabled", "true");
    properties.set("deepmsg.dpdk.eal.args",
                   "--no-huge --no-shconf --no-telemetry --vdev=net_pcap0,tx_pcap=/tmp/axc-dpdk-idle-warning.pcap");
    properties.set("deepmsg.dpdk.port", "net_pcap0");
    properties.set("deepmsg.dpdk.ipv4.address", "10.10.0.1/24");

    auto driver = axc::core::driver::EmbeddedDriver::create(properties, nullptr, nullptr);
    ASSERT_TRUE(driver.has_value()) << driver.error().message.view();
    EXPECT_TRUE(driver->close().has_value());
    std::filesystem::remove_all(directory);
    (void)std::remove("/tmp/axc-dpdk-idle-warning.pcap");
}

TEST(DpdkIdleWarning, TheChildProcess)
{
    if (nullptr != std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "we are the child";
    }

    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (0 == child)
    {
        const int output = ::open(IDLE_WARNING_PATH, O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (0 > output || 0 > ::dup2(output, STDOUT_FILENO))
        {
            ::_exit(127);
        }
        (void)::close(output);
        (void)::setenv("AXC_DPDK_CHILD", "1", 1);
        ::execl("/proc/self/exe", "/proc/self/exe", "--gtest_filter=DpdkIdleWarningChild.ASleepingReceiverIsAWarning",
                "--gtest_color=no", static_cast<char*>(nullptr));
        ::_exit(127);
    }
    int status = 0;
    ASSERT_EQ(child, ::waitpid(child, &status, 0));
    EXPECT_TRUE(WIFEXITED(status));
    ASSERT_EQ(0, WEXITSTATUS(status)) << "the child failed; what it wrote is in " << IDLE_WARNING_PATH;

    const std::string output = read_all(IDLE_WARNING_PATH);
    EXPECT_NE(std::string::npos, output.find("dpdk media warning: the receiver idle strategy is not a busy spin"))
        << output;
    (void)std::remove(IDLE_WARNING_PATH);
}

// A busy spin keeps the ring drained and earns no idle line. (This child is
// DEDICATED on a vdev that offers one transmit queue, so the queue-sharing
// warning is the other, expected line -- see `DpdkMedia.TheMediaLineIsOnStdout`.)
TEST(DpdkIdleQuietChild, ABusySpinIdleStrategyIsQuiet)
{
    if (nullptr == std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "runs in a child process";
    }

    (void)::setenv("AERON_RECEIVER_IDLE_STRATEGY", "spin", 1);
    (void)::setenv("AERON_SENDER_IDLE_STRATEGY", "spin", 1);
    char directory[] = "/tmp/axc-dpdk-idle-quiet-XXXXXX";
    ASSERT_NE(nullptr, ::mkdtemp(directory));
    (void)::aeron_properties_setenv("aeron.dir", directory);

    axc::core::PropertySource properties;
    properties.set("deepmsg.dpdk.enabled", "true");
    properties.set("deepmsg.dpdk.eal.args",
                   "--no-huge --no-shconf --no-telemetry --vdev=net_pcap0,tx_pcap=/tmp/axc-dpdk-idle-quiet.pcap");
    properties.set("deepmsg.dpdk.port", "net_pcap0");
    properties.set("deepmsg.dpdk.ipv4.address", "10.10.0.1/24");

    auto driver = axc::core::driver::EmbeddedDriver::create(properties, nullptr, nullptr);
    ASSERT_TRUE(driver.has_value()) << driver.error().message.view();
    EXPECT_TRUE(driver->close().has_value());
    std::filesystem::remove_all(directory);
    (void)std::remove("/tmp/axc-dpdk-idle-quiet.pcap");
}

TEST(DpdkIdleQuiet, TheChildProcess)
{
    if (nullptr != std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "we are the child";
    }

    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (0 == child)
    {
        const int output = ::open(IDLE_QUIET_PATH, O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (0 > output || 0 > ::dup2(output, STDOUT_FILENO))
        {
            ::_exit(127);
        }
        (void)::close(output);
        (void)::setenv("AXC_DPDK_CHILD", "1", 1);
        ::execl("/proc/self/exe", "/proc/self/exe", "--gtest_filter=DpdkIdleQuietChild.ABusySpinIdleStrategyIsQuiet",
                "--gtest_color=no", static_cast<char*>(nullptr));
        ::_exit(127);
    }
    int status = 0;
    ASSERT_EQ(child, ::waitpid(child, &status, 0));
    EXPECT_TRUE(WIFEXITED(status));
    ASSERT_EQ(0, WEXITSTATUS(status)) << "the child failed; what it wrote is in " << IDLE_QUIET_PATH;

    const std::string output = read_all(IDLE_QUIET_PATH);
    EXPECT_NE(std::string::npos, output.find("dpdk media on net_pcap0")) << output;
    EXPECT_EQ(std::string::npos, output.find("idle strategy is not a busy spin")) << output;
    (void)std::remove(IDLE_QUIET_PATH);
}

namespace
{

constexpr const char* IDLE_SHARED_PATH = "/tmp/axc-dpdk-idle-shared.txt";

}  // namespace

// In SHARED one runner is both the sender and the receiver
// (`aeron_driver_shared_do_work`), so the strategy that can leave frames in the
// ring is that runner's; a sleeping *receiver* strategy is never consulted
// there. A warning about it would name an agent the mode does not have.
TEST(DpdkIdleQuietSharedChild, ASharedDriverOnlyReadsTheSharedStrategy)
{
    if (nullptr == std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "runs in a child process";
    }

    (void)::setenv("AERON_RECEIVER_IDLE_STRATEGY", "sleeping", 1);
    (void)::setenv("AERON_SHARED_IDLE_STRATEGY", "spin", 1);
    char directory[] = "/tmp/axc-dpdk-idle-shared-XXXXXX";
    ASSERT_NE(nullptr, ::mkdtemp(directory));
    (void)::aeron_properties_setenv("aeron.dir", directory);
    (void)::aeron_properties_setenv("aeron.threading.mode", "SHARED");

    axc::core::PropertySource properties;
    properties.set("deepmsg.dpdk.enabled", "true");
    properties.set("deepmsg.dpdk.eal.args",
                   "--no-huge --no-shconf --no-telemetry --vdev=net_pcap0,tx_pcap=/tmp/axc-dpdk-idle-shared.pcap");
    properties.set("deepmsg.dpdk.port", "net_pcap0");
    properties.set("deepmsg.dpdk.ipv4.address", "10.10.0.1/24");

    auto driver = axc::core::driver::EmbeddedDriver::create(properties, nullptr, nullptr);
    ASSERT_TRUE(driver.has_value()) << driver.error().message.view();
    EXPECT_TRUE(driver->close().has_value());
    std::filesystem::remove_all(directory);
    (void)std::remove("/tmp/axc-dpdk-idle-shared.pcap");
}

TEST(DpdkIdleQuietShared, TheChildProcess)
{
    if (nullptr != std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "we are the child";
    }

    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (0 == child)
    {
        const int output = ::open(IDLE_SHARED_PATH, O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (0 > output || 0 > ::dup2(output, STDOUT_FILENO))
        {
            ::_exit(127);
        }
        (void)::close(output);
        (void)::setenv("AXC_DPDK_CHILD", "1", 1);
        ::execl("/proc/self/exe", "/proc/self/exe",
                "--gtest_filter=DpdkIdleQuietSharedChild.ASharedDriverOnlyReadsTheSharedStrategy", "--gtest_color=no",
                static_cast<char*>(nullptr));
        ::_exit(127);
    }
    int status = 0;
    ASSERT_EQ(child, ::waitpid(child, &status, 0));
    EXPECT_TRUE(WIFEXITED(status));
    ASSERT_EQ(0, WEXITSTATUS(status)) << "the child failed; what it wrote is in " << IDLE_SHARED_PATH;

    const std::string output = read_all(IDLE_SHARED_PATH);
    EXPECT_NE(std::string::npos, output.find("dpdk media on net_pcap0")) << output;
    EXPECT_EQ(std::string::npos, output.find("dpdk media warning:")) << output;
    (void)std::remove(IDLE_SHARED_PATH);
}

namespace
{

constexpr const char* ALIAS_LINE_PATH = "/tmp/axc-dpdk-alias-line.txt";
constexpr const char* ALIAS_PCAP_PATH = "/tmp/axc-dpdk-alias.pcap";

}  // namespace

// `aeron.udp.channel.transport.bindings.media=dpdk` is the reference's name for
// a media the C driver cannot resolve (it knows `default`, and a `dpdk` symbol is
// not exported). The launcher answers `default` and turns the switch on before
// the context exists; without both halves this driver does not start at all.
TEST(DpdkAliasChild, TheReferenceMediaNameIsTheAlias)
{
    if (nullptr == std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "runs in a child process";
    }

    (void)std::remove(ALIAS_PCAP_PATH);
    char directory[] = "/tmp/axc-dpdk-alias-XXXXXX";
    ASSERT_NE(nullptr, ::mkdtemp(directory));
    (void)::aeron_properties_setenv("aeron.dir", directory);
    // What a launcher's `aeron.*` push does; `aeron_driver_context_init` reads it.
    (void)::aeron_properties_setenv("aeron.udp.channel.transport.bindings.media", "dpdk");

    axc::core::PropertySource properties;
    properties.set("aeron.udp.channel.transport.bindings.media", "dpdk");
    properties.set("deepmsg.dpdk.eal.args",
                   std::string{"--no-huge --no-shconf --no-telemetry --vdev=net_pcap0,tx_pcap="} + ALIAS_PCAP_PATH);
    properties.set("deepmsg.dpdk.port", "net_pcap0");
    properties.set("deepmsg.dpdk.ipv4.address", "10.10.0.1/24");

    auto driver = axc::core::driver::EmbeddedDriver::create(properties, nullptr, nullptr);
    ASSERT_TRUE(driver.has_value()) << driver.error().message.view();
    EXPECT_TRUE(driver->close().has_value());
    std::filesystem::remove_all(directory);
    (void)std::remove(ALIAS_PCAP_PATH);
}

TEST(DpdkAlias, TheChildProcess)
{
    if (nullptr != std::getenv("AXC_DPDK_CHILD"))
    {
        GTEST_SKIP() << "we are the child";
    }

    (void)std::remove(ALIAS_LINE_PATH);
    const pid_t child = ::fork();
    ASSERT_GE(child, 0);

    if (0 == child)
    {
        const int output = ::open(ALIAS_LINE_PATH, O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (0 > output || 0 > ::dup2(output, STDOUT_FILENO))
        {
            ::_exit(127);
        }
        (void)::close(output);
        (void)::setenv("AXC_DPDK_CHILD", "1", 1);
        ::execl("/proc/self/exe", "/proc/self/exe", "--gtest_filter=DpdkAliasChild.TheReferenceMediaNameIsTheAlias",
                "--gtest_color=no", static_cast<char*>(nullptr));
        ::_exit(127);
    }

    int status = 0;
    ASSERT_EQ(child, ::waitpid(child, &status, 0));
    EXPECT_TRUE(WIFEXITED(status));
    ASSERT_EQ(0, WEXITSTATUS(status)) << "the child failed; what it wrote is in " << ALIAS_LINE_PATH;

    std::ifstream input{ALIAS_LINE_PATH};
    ASSERT_TRUE(input);
    const std::string output{std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};
    EXPECT_NE(std::string::npos, output.find("dpdk media on net_pcap0 (net_pcap), address 10.10.0.1/24, mtu "))
        << output;
    (void)std::remove(ALIAS_LINE_PATH);
}

#ifndef AXC_MEDIA_DRIVER_BIN
#define AXC_MEDIA_DRIVER_BIN ""
#endif

namespace
{

constexpr const char* LAUNCHER_OUTPUT_PATH = "/tmp/axc-dpdk-launcher.txt";
constexpr const char* LAUNCHER_PCAP_PATH = "/tmp/axc-dpdk-launcher.pcap";

/// Runs the built `deepmsg-media-driver` in `mode` and answers whether it
/// opened the media **and** was still running afterwards, exiting 0 on
/// SIGTERM. A launcher that refuses the mode fails the first half; one that
/// starts INVOKER without a caller-driven loop prints the media line while
/// installing and then dies at `aeron_driver_start` with the reference's
/// `manual_main_loop` error, which only the second half catches.
///
/// `carry_a_frame` adds the half a still-running process cannot prove: a client
/// in this process gets a publication through the child's driver and the
/// child's sender must put a frame on its port. A launcher that selects the
/// caller-thread start but never runs the loop passes "still running" and
/// fails this -- the driver's conductor and sender would never work.
[[nodiscard]] bool the_launcher_runs(const char* mode, bool carry_a_frame)
{
    const std::string binary = AXC_MEDIA_DRIVER_BIN;
    if (binary.empty() || !std::filesystem::exists(binary))
    {
        return false;
    }
    (void)std::remove(LAUNCHER_OUTPUT_PATH);
    (void)std::remove(LAUNCHER_PCAP_PATH);
    char directory[] = "/tmp/axc-dpdk-launcher-XXXXXX";
    if (nullptr == ::mkdtemp(directory))
    {
        return false;
    }

    const std::string eal_args =
        std::string{"--no-huge --no-shconf --no-telemetry --vdev=net_pcap0,tx_pcap="} + LAUNCHER_PCAP_PATH;
    const pid_t child = ::fork();
    if (0 > child)
    {
        return false;
    }
    if (0 == child)
    {
        const int output = ::open(LAUNCHER_OUTPUT_PATH, O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (0 > output || 0 > ::dup2(output, STDOUT_FILENO) || 0 > ::dup2(output, STDERR_FILENO))
        {
            ::_exit(127);
        }
        (void)::close(output);
        (void)::setenv("AERON_DIR", directory, 1);
        (void)::setenv("AERON_THREADING_MODE", mode, 1);
        (void)::setenv("DEEPMSG_DPDK_ENABLED", "true", 1);
        (void)::setenv("DEEPMSG_DPDK_EAL_ARGS", eal_args.c_str(), 1);
        (void)::setenv("DEEPMSG_DPDK_PORT", "net_pcap0", 1);
        (void)::setenv("DEEPMSG_DPDK_IPV4_ADDRESS", "10.10.0.1/24", 1);
        ::execl(binary.c_str(), binary.c_str(), static_cast<char*>(nullptr));
        ::_exit(127);
    }

    bool opened = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (std::string::npos != read_all(LAUNCHER_OUTPUT_PATH).find("dpdk media on net_pcap0"))
        {
            opened = true;
            break;
        }
        if (child == ::waitpid(child, nullptr, WNOHANG))
        {
            break;  // it exited before the media was open
        }
        ::usleep(10000);
    }
    bool running = false;
    bool carried = !carry_a_frame;
    if (opened)
    {
        if (carry_a_frame)
        {
            // The media line is printed while the context is installed; the
            // driver's CnC file appears just after it.
            ::usleep(300000);
            carried = open_a_publication(directory, nullptr);
            ::usleep(200000);
        }
        else
        {
            ::usleep(200000);
        }
        running = 0 == ::waitpid(child, nullptr, WNOHANG);
    }
    (void)::kill(child, SIGTERM);
    int status = 0;
    (void)::waitpid(child, &status, 0);
    const bool frames_on_the_port = !carry_a_frame || pcap_size(LAUNCHER_PCAP_PATH) > 24;
    std::filesystem::remove_all(directory);
    (void)std::remove(LAUNCHER_OUTPUT_PATH);
    (void)std::remove(LAUNCHER_PCAP_PATH);
    return opened && running && carried && frames_on_the_port && WIFEXITED(status) && 0 == WEXITSTATUS(status);
}

void skip_without_the_launcher()
{
    if (!std::filesystem::exists(AXC_MEDIA_DRIVER_BIN))
    {
        GTEST_SKIP() << "the deepmsg-media-driver binary is not in this build";
    }
}

}  // namespace

// The binary the T6 rig substitutes for `aeronmd` (`bench/dpdk-t6/
// aeronmd-wrapper.sh`): every mode the reference accepts must start here too.
TEST(DpdkLauncher, TheSharedModeStartsAndStaysUp)
{
    skip_without_the_launcher();
    EXPECT_TRUE(the_launcher_runs("SHARED", false)) << "the launcher does not run a SHARED driver";
}

TEST(DpdkLauncher, TheSharedNetworkModeStartsAndStaysUp)
{
    skip_without_the_launcher();
    EXPECT_TRUE(the_launcher_runs("SHARED_NETWORK", false)) << "the launcher does not run a SHARED_NETWORK driver";
}

TEST(DpdkLauncher, TheInvokerModeRunsInTheCallerThread)
{
    skip_without_the_launcher();
    EXPECT_TRUE(the_launcher_runs("INVOKER", true)) << "the launcher does not drive an INVOKER driver";
}

#endif  // AXC_WITH_DPDK
