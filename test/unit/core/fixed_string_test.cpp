#include <gtest/gtest.h>

#include "core/collections/fixed_string.h"

using axc::core::FixedString;

TEST(FixedString, AssignsAndCompares)
{
    FixedString<16> value("hello");
    EXPECT_EQ(5U, value.size());
    EXPECT_STREQ("hello", value.c_str());
    EXPECT_EQ(std::string_view("hello"), value.view());
    EXPECT_TRUE(value == std::string_view("hello"));
}

TEST(FixedString, TruncatesAtCapacityMinusOne)
{
    FixedString<8> value("0123456789");
    EXPECT_EQ(7U, value.size());
    EXPECT_STREQ("0123456", value.c_str());
}

TEST(FixedString, ClearAndReassign)
{
    FixedString<8> value("abc");
    value.clear();
    EXPECT_TRUE(value.empty());
    EXPECT_STREQ("", value.c_str());

    value = "xyz";
    EXPECT_EQ(std::string_view("xyz"), value.view());
}

TEST(FixedString, NullAssignsEmpty)
{
    FixedString<8> value("abc");
    value.assign(nullptr);
    EXPECT_TRUE(value.empty());
}
