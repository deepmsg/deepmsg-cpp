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
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <span>
#include <string>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

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

        auto port = axc::core::media::dpdk::Port::open(0);
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
    settings.ipv4_address = axc::core::media::dpdk::Ipv4Prefix{0x0a0a0001, 24};
    settings.static_neighbours = "10.10.0.2=02:00:00:00:00:02";
    settings.rx_descriptors = 256;
    settings.tx_descriptors = 256;
    settings.mempool_size = 1023;
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
    EXPECT_EQ(outbound.size(), stack->send(*binding, {0x0a0a0002, 6000}, outbound));

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

#endif  // AXC_WITH_DPDK
