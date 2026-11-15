#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <string_view>

#include <pthread.h>
#include <sched.h>

#include "core/config/property_source.h"
#include "core/media/dpdk/media.h"

namespace
{

using axc::core::PropertySource;
using axc::core::media::dpdk::install_if_configured;
using axc::core::media::dpdk::parse_settings;

std::string message_of(const axc::core::Error& error)
{
    return std::string{error.message.view()};
}

}  // namespace

TEST(DpdkMediaConfig, DisabledByDefault)
{
    const PropertySource properties;

    const auto settings = parse_settings(properties);
    ASSERT_TRUE(settings.has_value());
    EXPECT_FALSE(settings->enabled);
}

TEST(DpdkMediaConfig, EnabledRequiresAnAddress)
{
    PropertySource properties;
    properties.set("deepmsg.dpdk.enabled", "true");

    const auto settings = parse_settings(properties);
    ASSERT_FALSE(settings.has_value());
    EXPECT_NE(std::string::npos, message_of(settings.error()).find("deepmsg.dpdk.ipv4.address"));
}

TEST(DpdkMediaConfig, ParsesEverySetting)
{
    PropertySource properties;
    properties.set("deepmsg.dpdk.enabled", "true");
    properties.set("deepmsg.dpdk.eal.args", "-a 0000:03:00.0");
    properties.set("deepmsg.dpdk.port", "03:00.0");
    properties.set("deepmsg.dpdk.ipv4.address", "10.10.0.1/24");
    properties.set("deepmsg.dpdk.ipv4.gateway", "10.10.0.254");
    properties.set("deepmsg.dpdk.static.neighbours", "10.10.0.2=02:00:00:00:00:02");
    properties.set("deepmsg.dpdk.rx.descriptors", "512");
    properties.set("deepmsg.dpdk.tx.descriptors", "256");
    properties.set("deepmsg.dpdk.mempool.size", "4095");
    properties.set("deepmsg.dpdk.mempool.cache", "128");
    properties.set("deepmsg.dpdk.rx.burst", "16");
    properties.set("deepmsg.dpdk.sender.ring", "2048");
    properties.set("deepmsg.dpdk.ephemeral.ports", "40000-40100");
    properties.set("deepmsg.dpdk.arp.timeout.ms", "60000");
    properties.set("deepmsg.dpdk.arp.retry.ms", "500");

    const auto settings = parse_settings(properties);
    ASSERT_TRUE(settings.has_value());
    EXPECT_TRUE(settings->enabled);
    EXPECT_EQ("-a 0000:03:00.0", settings->eal_args);
    ASSERT_TRUE(settings->port.has_value());
    EXPECT_EQ("03:00.0", *settings->port);
    ASSERT_TRUE(settings->ipv4_address.has_value());
    EXPECT_EQ(24, settings->ipv4_address->prefix_length);
    ASSERT_TRUE(settings->ipv4_gateway.has_value());
    EXPECT_EQ(512, settings->rx_descriptors);
    EXPECT_EQ(256, settings->tx_descriptors);
    ASSERT_TRUE(settings->mempool_size.has_value());
    EXPECT_EQ(4095u, *settings->mempool_size);
    EXPECT_EQ(128u, settings->mempool_cache);
    EXPECT_EQ(16, settings->rx_burst);
    EXPECT_EQ(2048u, settings->sender_ring);
    EXPECT_EQ(40000, settings->ephemeral_low);
    EXPECT_EQ(40100, settings->ephemeral_high);
    EXPECT_EQ(60000u, settings->arp_timeout_ms);
    EXPECT_EQ(500u, settings->arp_retry_ms);
}

TEST(DpdkMediaConfig, UnknownNameIsRefused)
{
    PropertySource properties;
    properties.set("deepmsg.dpdk.nope", "1");

    const auto settings = parse_settings(properties);
    ASSERT_FALSE(settings.has_value());
    EXPECT_NE(std::string::npos, message_of(settings.error()).find("unknown DPDK property"));
}

TEST(DpdkMediaConfig, BadValuesAreRefused)
{
    {
        PropertySource properties;
        properties.set("deepmsg.dpdk.enabled", "yes");
        EXPECT_FALSE(parse_settings(properties).has_value());
    }
    {
        PropertySource properties;
        properties.set("deepmsg.dpdk.enabled", "true");
        properties.set("deepmsg.dpdk.ipv4.address", "10.0.0.1/33");
        EXPECT_FALSE(parse_settings(properties).has_value());
    }
    {
        PropertySource properties;
        properties.set("deepmsg.dpdk.enabled", "true");
        properties.set("deepmsg.dpdk.ipv4.address", "10.0.0.1/24");
        properties.set("deepmsg.dpdk.ephemeral.ports", "50000-40000");
        EXPECT_FALSE(parse_settings(properties).has_value());
    }
    {
        PropertySource properties;
        properties.set("deepmsg.dpdk.enabled", "true");
        properties.set("deepmsg.dpdk.ipv4.address", "10.0.0.1/24");
        properties.set("deepmsg.dpdk.sender.ring", "1000");
        EXPECT_FALSE(parse_settings(properties).has_value());
    }
    {
        PropertySource properties;
        properties.set("deepmsg.dpdk.enabled", "true");
        properties.set("deepmsg.dpdk.ipv4.address", "10.0.0.1/24");
        properties.set("deepmsg.dpdk.static.neighbours", "10.0.0.2=not-a-mac");
        EXPECT_FALSE(parse_settings(properties).has_value());
    }
}

TEST(DpdkMediaConfig, EnabledIsRefusedInABuildWithoutDpdk)
{
#if defined(AXC_WITH_DPDK)
    GTEST_SKIP() << "a DPDK build opens the media here; this criterion is for a build without it";
#else
    PropertySource properties;
    properties.set("deepmsg.dpdk.enabled", "true");
    properties.set("deepmsg.dpdk.ipv4.address", "10.0.0.1/24");

    const auto media = install_if_configured(nullptr, properties);
    ASSERT_FALSE(media.has_value());
    EXPECT_NE(std::string::npos, message_of(media.error()).find("AXC_WITH_DPDK=ON"));
#endif
}

TEST(DpdkMediaConfig, DisabledInstallIsANoOp)
{
    const PropertySource properties;

    auto media = install_if_configured(nullptr, properties);
    ASSERT_TRUE(media.has_value());
    EXPECT_FALSE(media->enabled());
    EXPECT_TRUE(media->close().has_value());
}

TEST(DpdkMediaConfig, TheEalArgumentsCarryTheProcessCpuset)
{
    axc::core::media::dpdk::Settings settings;
    settings.eal_args = "-a 0000:03:00.0";

    const auto arguments = axc::core::media::dpdk::eal_arguments(settings);
    ASSERT_TRUE(arguments.has_value()) << message_of(arguments.error());
    ASSERT_FALSE(arguments->empty());
    EXPECT_EQ("deepmsg-dpdk", arguments->front());

    // The expected argument is computed here the way the process's own
    // affinity describes it: one lcore allowed on these CPUs.
    cpu_set_t set;
    CPU_ZERO(&set);
    ASSERT_EQ(0, ::pthread_getaffinity_np(::pthread_self(), sizeof(set), &set));
    std::string expected{""};
    bool first = true;
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu)
    {
        if (0 == CPU_ISSET(cpu, &set))
        {
            continue;
        }
        if (!first)
        {
            expected.push_back(',');
        }
        expected += std::to_string(cpu);
        first = false;
    }
    ASSERT_FALSE(expected.empty()) << "the process was given no CPU";
    const std::string lcores = "--lcores=0@(" + expected + ")";

    EXPECT_EQ(1u, static_cast<std::size_t>(std::count(arguments->begin(), arguments->end(), lcores)));
    EXPECT_EQ(1u, static_cast<std::size_t>(std::count(arguments->begin(), arguments->end(), "--no-telemetry")));
    // The operator's own arguments are split into argv tokens and kept.
    EXPECT_EQ(1u, static_cast<std::size_t>(std::count(arguments->begin(), arguments->end(), "-a")));
    EXPECT_EQ(1u, static_cast<std::size_t>(std::count(arguments->begin(), arguments->end(), "0000:03:00.0")));
}

TEST(DpdkMediaConfig, AnOperatorLcoresArgumentIsRefused)
{
    for (const std::string_view given : {"--lcores=0@(0)", "-l 0-3", "--lcore=0"})
    {
        axc::core::media::dpdk::Settings settings;
        settings.eal_args = std::string{"--no-huge "} + std::string{given};

        const auto arguments = axc::core::media::dpdk::eal_arguments(settings);
        ASSERT_FALSE(arguments.has_value()) << given;
        EXPECT_NE(std::string::npos, message_of(arguments.error()).find("chosen by the media")) << given;
    }
}

TEST(DpdkMediaConfig, AnOperatorTelemetryArgumentIsNotDuplicated)
{
    axc::core::media::dpdk::Settings settings;
    settings.eal_args = "--no-telemetry";

    const auto arguments = axc::core::media::dpdk::eal_arguments(settings);
    ASSERT_TRUE(arguments.has_value()) << message_of(arguments.error());

    EXPECT_EQ(1u, static_cast<std::size_t>(std::count(arguments->begin(), arguments->end(), "--no-telemetry")));
}
