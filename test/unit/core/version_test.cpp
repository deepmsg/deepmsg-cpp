#include <gtest/gtest.h>

#include "core/version.h"

TEST(CoreVersion, ReturnsNonEmptyVersion)
{
    const char* version = axc::core::version_string();
    ASSERT_NE(nullptr, version);
    EXPECT_STRNE("", version);
}
