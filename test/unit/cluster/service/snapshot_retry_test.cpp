// The retry loop the service snapshot taker uses: sustained back-pressure must
// not grow the stack, and a permanent error must come straight back.

#include <gtest/gtest.h>

#include <cstdint>
#include <expected>

#include "cluster/service/snapshot.h"

namespace
{

using deepmsg::cluster::service::retry_while_back_pressured;

TEST(SnapshotRetry, aSustainedBackPressureIsRetriedWithoutGrowingTheStack)
{
    // A recursive retry (-O0, no tail call) overflows the stack well before
    // this count; the loop completes it.
    constexpr int RETRIES = 1'000'000;
    int attempts = 0;
    auto claimed = retry_while_back_pressured(
        [&attempts]() -> std::expected<int, std::int64_t>
        {
            if (++attempts < RETRIES)
            {
                return std::unexpected(AERON_PUBLICATION_BACK_PRESSURED);
            }
            return 7;
        },
        []() {});
    ASSERT_TRUE(claimed.has_value());
    EXPECT_EQ(7, *claimed);
    EXPECT_EQ(RETRIES, attempts);
}

TEST(SnapshotRetry, anAdminActionIsRetried)
{
    int attempts = 0;
    auto claimed = retry_while_back_pressured(
        [&attempts]() -> std::expected<int, std::int64_t>
        {
            if (++attempts < 3)
            {
                return std::unexpected(AERON_PUBLICATION_ADMIN_ACTION);
            }
            return 11;
        },
        []() {});
    ASSERT_TRUE(claimed.has_value());
    EXPECT_EQ(11, *claimed);
    EXPECT_EQ(3, attempts);
}

TEST(SnapshotRetry, aNonRetryableErrorIsReturnedImmediately)
{
    int attempts = 0;
    auto claimed = retry_while_back_pressured(
        [&attempts]() -> std::expected<int, std::int64_t>
        {
            ++attempts;
            return std::unexpected(AERON_PUBLICATION_NOT_CONNECTED);
        },
        []() {});
    ASSERT_FALSE(claimed.has_value());
    EXPECT_EQ(AERON_PUBLICATION_NOT_CONNECTED, claimed.error());
    EXPECT_EQ(1, attempts);
}

}  // namespace
