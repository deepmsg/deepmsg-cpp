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

TEST(CpuAffinity, RejectsBadSpecs)
{
    for (const std::string_view spec : {"x", "5-3", "0,,1", "1-2000", "-2"})
    {
        EXPECT_FALSE(CpuAffinity::parse(spec).has_value()) << spec;
    }
}
