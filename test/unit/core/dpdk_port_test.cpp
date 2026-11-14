// The DPDK port seam, over a real PMD.
//
// The EAL may be initialised once per process, so every case that wants one
// runs in a child of this same binary (the Rust track's pattern). `net_pcap`
// needs no privileges, no huge pages and no veth.
//
// Compiled only in a build with `-DAXC_WITH_DPDK=ON`.

#if defined(AXC_WITH_DPDK)

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

#include <sys/wait.h>
#include <unistd.h>

#include "core/media/dpdk/eal.h"
#include "core/media/dpdk/mempool.h"
#include "core/media/dpdk/port.h"

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

#endif  // AXC_WITH_DPDK
