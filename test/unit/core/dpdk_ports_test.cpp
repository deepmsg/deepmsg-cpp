#include <gtest/gtest.h>

#include <cerrno>
#include <cstdint>
#include <optional>

#include "core/media/dpdk/stack/ports.h"

namespace
{

using axc::core::media::dpdk::ports::Binding;
using axc::core::media::dpdk::ports::EPHEMERAL_PORTS_FIRST;
using axc::core::media::dpdk::ports::EPHEMERAL_PORTS_LAST;
using axc::core::media::dpdk::ports::Ipv4Endpoint;
using axc::core::media::dpdk::ports::PortTable;
using axc::core::media::dpdk::ports::SLOT_CAPACITY;

constexpr std::uint32_t OWN = 0x0a0a0001;  // 10.10.0.1

constexpr Ipv4Endpoint endpoint(std::uint32_t address, std::uint16_t port) noexcept
{
    return Ipv4Endpoint{address, port};
}

PortTable table()
{
    return PortTable{SLOT_CAPACITY, EPHEMERAL_PORTS_FIRST, EPHEMERAL_PORTS_LAST};
}

TEST(PortTable, ARequestedZeroPortComesFromTheEphemeralRange)
{
    auto ports = table();
    const auto first = ports.bind(OWN, 0, std::nullopt, endpoint(OWN, 0));
    ASSERT_TRUE(first.has_value());
    EXPECT_GE(first->port, EPHEMERAL_PORTS_FIRST);
    EXPECT_LE(first->port, EPHEMERAL_PORTS_LAST);
    EXPECT_EQ(*first, ports.lookup(OWN, OWN, first->port, endpoint(0x0a0a0009, 1)));

    const auto second = ports.bind(OWN, 0, std::nullopt, endpoint(OWN, 0));
    ASSERT_TRUE(second.has_value());
    EXPECT_NE(first->port, second->port);
}

TEST(PortTable, ASecondBindOnOneAddressIsRefused)
{
    auto ports = table();
    const Ipv4Endpoint address = endpoint(OWN, 10456);
    ASSERT_TRUE(ports.bind(OWN, 10456, std::nullopt, address).has_value());

    const auto failure = ports.bind(OWN, 10456, std::nullopt, address);
    ASSERT_FALSE(failure.has_value());
    EXPECT_EQ(EADDRINUSE, failure.error().errcode);
    EXPECT_EQ(address, failure.error().address);
    EXPECT_EQ(-1, failure.error().descriptor);
}

TEST(PortTable, TheWildcardAndTheAddressAreTheSamePort)
{
    auto ports = table();
    ASSERT_TRUE(ports.bind(std::nullopt, 10456, std::nullopt, endpoint(0, 10456)).has_value());
    EXPECT_FALSE(ports.bind(OWN, 10456, std::nullopt, endpoint(OWN, 10456)).has_value());
}

TEST(PortTable, AWildcardBindingHearsThePortsOwnAddress)
{
    auto ports = table();
    const auto wildcard = ports.bind(std::nullopt, 5000, std::nullopt, endpoint(OWN, 5000));
    ASSERT_TRUE(wildcard.has_value());

    EXPECT_EQ(*wildcard, ports.lookup(OWN, OWN, 5000, endpoint(0x0a0a0009, 1)));
    EXPECT_EQ(std::nullopt, ports.lookup(OWN, 0x0a0a0002, 5000, endpoint(0x0a0a0009, 1)));
}

TEST(PortTable, ASpecificBindingHearsOnlyTheAddressItNamed)
{
    auto ports = table();
    constexpr std::uint32_t other = 0x0a0a0007;
    ASSERT_TRUE(ports.bind(other, 5001, std::nullopt, endpoint(other, 5001)).has_value());

    EXPECT_EQ(std::nullopt, ports.lookup(OWN, OWN, 5001, endpoint(0x0a0a0009, 1)));
    EXPECT_TRUE(ports.lookup(OWN, other, 5001, endpoint(0x0a0a0009, 1)).has_value());
}

TEST(PortTable, AConnectedBindingHearsOnlyItsPeer)
{
    auto ports = table();
    const Ipv4Endpoint subscriber = endpoint(0x0a0a0009, 10000);
    const auto binding = ports.bind(OWN, 5002, subscriber, endpoint(OWN, 5002));
    ASSERT_TRUE(binding.has_value());

    EXPECT_EQ(*binding, ports.lookup(OWN, OWN, 5002, subscriber));
    EXPECT_EQ(std::nullopt, ports.lookup(OWN, OWN, 5002, endpoint(0x0a0a0009, 10001)));
    EXPECT_EQ(std::nullopt, ports.lookup(OWN, OWN, 5002, endpoint(0x0a0a0008, 10000)));
}

TEST(PortTable, AReconnectChangesThePeerItHears)
{
    auto ports = table();
    const auto binding = ports.bind(OWN, 5007, endpoint(0x0a0a0009, 10000), endpoint(OWN, 5007));
    ASSERT_TRUE(binding.has_value());

    ports.reconnect(*binding, endpoint(0x0a0a000a, 20000));
    EXPECT_EQ(std::nullopt, ports.lookup(OWN, OWN, 5007, endpoint(0x0a0a0009, 10000)));
    EXPECT_EQ(*binding, ports.lookup(OWN, OWN, 5007, endpoint(0x0a0a000a, 20000)));
}

TEST(PortTable, AReleasedPortCanBeBoundAgain)
{
    auto ports = table();
    const Ipv4Endpoint address = endpoint(OWN, 5003);
    const auto first = ports.bind(OWN, 5003, std::nullopt, address);
    ASSERT_TRUE(first.has_value());

    ports.release(*first);
    EXPECT_EQ(std::nullopt, ports.lookup(OWN, OWN, 5003, endpoint(0x0a0a0009, 1)));

    const auto second = ports.bind(OWN, 5003, std::nullopt, address);
    ASSERT_TRUE(second.has_value());
    EXPECT_GT(second->generation, first->generation);
    EXPECT_EQ(*second, ports.lookup(OWN, OWN, 5003, endpoint(0x0a0a0009, 1)));
}

TEST(PortTable, AReleasedBindingCannotBeLookedUpUnder)
{
    auto ports = table();
    const Ipv4Endpoint address = endpoint(OWN, 5004);
    const auto first = ports.bind(OWN, 5004, std::nullopt, address);
    ASSERT_TRUE(first.has_value());
    ports.release(*first);
    const auto second = ports.bind(OWN, 5004, std::nullopt, address);
    ASSERT_TRUE(second.has_value());

    EXPECT_EQ(first->slot, second->slot);
    EXPECT_NE(first->generation, second->generation);
    const auto found = ports.lookup(OWN, OWN, 5004, endpoint(0x0a0a0009, 1));
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(second->generation, found->generation);
}

TEST(PortTable, ReleasingAStaleBindingLeavesTheNewOneAlone)
{
    auto ports = table();
    const Ipv4Endpoint address = endpoint(OWN, 5005);
    const auto first = ports.bind(OWN, 5005, std::nullopt, address);
    ASSERT_TRUE(first.has_value());
    ports.release(*first);
    const auto second = ports.bind(OWN, 5005, std::nullopt, address);
    ASSERT_TRUE(second.has_value());

    ports.release(*first);
    EXPECT_EQ(*second, ports.lookup(OWN, OWN, 5005, endpoint(0x0a0a0009, 1)));
}

TEST(PortTable, AFullTableRefusesABind)
{
    PortTable ports{2, EPHEMERAL_PORTS_FIRST, EPHEMERAL_PORTS_LAST};
    ASSERT_TRUE(ports.bind(OWN, 6000, std::nullopt, endpoint(OWN, 6000)).has_value());
    ASSERT_TRUE(ports.bind(OWN, 6001, std::nullopt, endpoint(OWN, 6001)).has_value());

    const auto failure = ports.bind(OWN, 6002, std::nullopt, endpoint(OWN, 6002));
    ASSERT_FALSE(failure.has_value());
    EXPECT_EQ(ENOBUFS, failure.error().errcode);
    EXPECT_EQ(2u, ports.in_use());
}

TEST(PortTable, AnExhaustedEphemeralRangeIsAnError)
{
    PortTable ports{4, 10000, 10001};
    ASSERT_TRUE(ports.bind(OWN, 10000, std::nullopt, endpoint(OWN, 10000)).has_value());
    ASSERT_TRUE(ports.bind(OWN, 10001, std::nullopt, endpoint(OWN, 10001)).has_value());

    const auto failure = ports.bind(OWN, 0, std::nullopt, endpoint(OWN, 0));
    ASSERT_FALSE(failure.has_value());
    EXPECT_EQ(EADDRNOTAVAIL, failure.error().errcode);
}

}  // namespace
