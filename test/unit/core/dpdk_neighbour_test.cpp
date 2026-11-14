#include <gtest/gtest.h>

#include <array>
#include <cstdint>

#include "core/media/dpdk/stack/neighbour.h"

namespace
{

using axc::core::media::dpdk::neighbour::Mac;
using axc::core::media::dpdk::neighbour::NeighbourTable;
using axc::core::media::dpdk::neighbour::on_the_same_link;

constexpr std::uint32_t OWN = 0x0a0a0001;   // 10.10.0.1
constexpr std::uint32_t PEER = 0x0a0a0002;  // 10.10.0.2
constexpr Mac MAC{0x02, 0, 0, 0, 0, 0x02};
constexpr std::int64_t TIMEOUT_NS = 300LL * 1000 * 1000 * 1000;
constexpr std::int64_t INTERVAL_NS = 1000LL * 1000 * 1000;

NeighbourTable table()
{
    return NeighbourTable{256, TIMEOUT_NS, INTERVAL_NS};
}

TEST(Neighbour, AnAddressNobodyHasLearnedIsUnknown)
{
    const auto neighbours = table();
    EXPECT_EQ(std::nullopt, neighbours.resolve(PEER, 0));
}

TEST(Neighbour, WhatIsLearnedIsAnsweredWith)
{
    auto neighbours = table();
    neighbours.learn(PEER, MAC, 1000);
    EXPECT_EQ(MAC, neighbours.resolve(PEER, 1000));
    EXPECT_EQ(MAC, neighbours.resolve(PEER, 1000 + TIMEOUT_NS - 1));
}

TEST(Neighbour, AnEntryAgesOut)
{
    auto neighbours = table();
    neighbours.learn(PEER, MAC, 1000);
    EXPECT_EQ(std::nullopt, neighbours.resolve(PEER, 1000 + TIMEOUT_NS));
}

TEST(Neighbour, AFixedEntryNeverAgesAndIsNotOverwritten)
{
    auto neighbours = table();
    constexpr Mac FIXED{0x02, 0, 0, 0, 0, 0x77};
    neighbours.insert_fixed(PEER, FIXED);

    EXPECT_EQ(FIXED, neighbours.resolve(PEER, 0));
    EXPECT_EQ(FIXED, neighbours.resolve(PEER, TIMEOUT_NS * 2));

    neighbours.learn(PEER, MAC, 5);
    EXPECT_EQ(FIXED, neighbours.resolve(PEER, 5));
}

TEST(Neighbour, AskingIsThrottledPerAddress)
{
    auto neighbours = table();
    EXPECT_TRUE(neighbours.ask(PEER, 0));
    EXPECT_FALSE(neighbours.ask(PEER, INTERVAL_NS - 1));
    EXPECT_TRUE(neighbours.ask(PEER, INTERVAL_NS));
    EXPECT_TRUE(neighbours.ask(0x0a0a0009, 0));
}

TEST(Neighbour, AskDueDoesNotRecordAnything)
{
    auto neighbours = table();
    EXPECT_TRUE(neighbours.ask_due(PEER, 0));
    EXPECT_TRUE(neighbours.ask_due(PEER, 0));
    EXPECT_TRUE(neighbours.ask(PEER, 0));
    EXPECT_FALSE(neighbours.ask_due(PEER, INTERVAL_NS - 1));
    EXPECT_TRUE(neighbours.ask_due(PEER, INTERVAL_NS));
}

TEST(Neighbour, ForgetRemovesTheAddress)
{
    auto neighbours = table();
    neighbours.learn(PEER, MAC, 0);
    neighbours.forget(PEER);
    EXPECT_EQ(std::nullopt, neighbours.resolve(PEER, 0));
    EXPECT_TRUE(neighbours.empty());
}

TEST(Neighbour, TheOldestLearnedEntryMakesWayForANewOne)
{
    NeighbourTable neighbours{2, TIMEOUT_NS, INTERVAL_NS};
    neighbours.learn(0x0a0a0002, MAC, 100);
    neighbours.learn(0x0a0a0003, MAC, 200);
    EXPECT_EQ(2u, neighbours.size());

    neighbours.learn(0x0a0a0004, MAC, 300);
    EXPECT_EQ(std::nullopt, neighbours.resolve(0x0a0a0002, 300));
    EXPECT_TRUE(neighbours.resolve(0x0a0a0003, 300).has_value());
    EXPECT_TRUE(neighbours.resolve(0x0a0a0004, 300).has_value());
}

TEST(Neighbour, AConfiguredEntryIsNeverDisplaced)
{
    NeighbourTable neighbours{1, TIMEOUT_NS, INTERVAL_NS};
    neighbours.insert_fixed(0x0a0a0002, MAC);
    neighbours.learn(0x0a0a0003, MAC, 100);

    EXPECT_EQ(MAC, neighbours.resolve(0x0a0a0002, 100));
    EXPECT_EQ(std::nullopt, neighbours.resolve(0x0a0a0003, 100));
}

TEST(Neighbour, SameLinkUsesTheOursPrefix)
{
    EXPECT_TRUE(on_the_same_link(OWN, PEER, 24));
    EXPECT_FALSE(on_the_same_link(OWN, 0x0a0a0909, 24));
    EXPECT_TRUE(on_the_same_link(OWN, 0x0a0a0909, 0));
    EXPECT_FALSE(on_the_same_link(OWN, 0x0a0a0909, 40));
}

}  // namespace
