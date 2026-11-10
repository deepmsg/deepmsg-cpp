#include <gtest/gtest.h>

#include <string_view>

#include "core/agent/cpu_affinity.h"

using axc::core::CpuAffinity;

TEST(CpuAffinity, EmptyAndMinusOneMeanNone)
{
    for (const std::string_view spec : {"", "-1"})
    {
        const auto affinity = CpuAffinity::parse(spec);
        ASSERT_TRUE(affinity.has_value()) << spec;
        EXPECT_FALSE(affinity->any());
    }
}

TEST(CpuAffinity, SingleIntegerIsAnIndexIntoTheAllowedCpuset)
{
    const auto affinity = CpuAffinity::parse("0");
    ASSERT_TRUE(affinity.has_value());
    EXPECT_TRUE(affinity->any());
    EXPECT_TRUE(affinity->is_index());
    EXPECT_EQ(0, affinity->index());
}

TEST(CpuAffinity, ListAndRangePinAbsoluteCpus)
{
    const auto affinity = CpuAffinity::parse("0,2,4-5");
    ASSERT_TRUE(affinity.has_value());
    EXPECT_FALSE(affinity->is_index());
    EXPECT_TRUE(affinity->contains(0));
    EXPECT_FALSE(affinity->contains(1));
    EXPECT_TRUE(affinity->contains(2));
    EXPECT_TRUE(affinity->contains(4));
    EXPECT_TRUE(affinity->contains(5));
    EXPECT_EQ(4U, affinity->count());
}

// A6f: one configured recorder affinity fans out to N recorder agents.
TEST(CpuAffinity, NthWalksTheSetOrShiftsTheIndex)
{
    const auto set = CpuAffinity::parse("0,2,4-5");
    ASSERT_TRUE(set.has_value());

    const auto first = set->nth(0);
    ASSERT_TRUE(first.any());
    EXPECT_TRUE(first.contains(0));
    EXPECT_EQ(1U, first.count());

    const auto third = set->nth(2);
    ASSERT_TRUE(third.any());
    EXPECT_TRUE(third.contains(4));
    EXPECT_EQ(1U, third.count());

    // Out of range means "no affinity", never a crash or a wrong core.
    EXPECT_FALSE(set->nth(4).any());

    const auto index = CpuAffinity::parse("3");
    ASSERT_TRUE(index.has_value());
    const auto shifted = index->nth(2);
    ASSERT_TRUE(shifted.any());
    EXPECT_TRUE(shifted.is_index());
    EXPECT_EQ(5, shifted.index());

    EXPECT_FALSE(CpuAffinity::none().nth(0).any());
}

TEST(CpuAffinity, RejectsBadSpecs)
{
    for (const std::string_view spec : {"x", "5-3", "0,,1", "1-2000", "-2"})
    {
        EXPECT_FALSE(CpuAffinity::parse(spec).has_value()) << spec;
    }
}
