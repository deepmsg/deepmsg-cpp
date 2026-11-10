// Criterion for card A6b's ring: raw-UAPI io_uring setup, submission and
// completions (`io/uring_ring.h`).

#include <gtest/gtest.h>

#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

#include "archive/io/uring_ring.h"

namespace
{

using deepmsg::archive::server::io::UringCompletion;
using deepmsg::archive::server::io::UringRing;

class UringRingTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::array<char, 32> pattern{};
        std::snprintf(pattern.data(), pattern.size(), "/tmp/axc-uring-XXXXXX");
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        directory_ = created;
        path_ = directory_ + "/uring.dat";
        fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        ASSERT_GE(fd_, 0);
    }

    void TearDown() override
    {
        if (fd_ >= 0)
        {
            ::close(fd_);
        }
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
    }

    [[nodiscard]] std::vector<std::uint8_t> read_back(std::size_t length) const
    {
        std::vector<std::uint8_t> bytes(length);
        const ssize_t read = ::pread(fd_, bytes.data(), length, 0);
        EXPECT_EQ(static_cast<ssize_t>(length), read);
        return bytes;
    }

    std::string directory_;
    std::string path_;
    int fd_{-1};
};

TEST_F(UringRingTest, theProbeMatchesWhatTheKernelAndSandboxAllow)
{
    if (!UringRing::is_available())
    {
        GTEST_SKIP() << "io_uring is not available on this kernel";
    }

    const auto ring = UringRing::create(4);
    ASSERT_TRUE(ring) << ring.error().message.view();
}

TEST_F(UringRingTest, aLinkedWriteAndFsyncCompleteInSubmissionOrder)
{
    if (!UringRing::is_available())
    {
        GTEST_SKIP() << "io_uring is not available on this kernel";
    }

    auto ring = UringRing::create(8);
    ASSERT_TRUE(ring) << ring.error().message.view();

    const std::array<std::uint8_t, 11> payload{'h', 'e', 'l', 'l', 'o', ' ', 'u', 'r', 'i', 'n', 'g'};
    ASSERT_TRUE(ring->queue_write(fd_, 0, payload.data(), static_cast<std::uint32_t>(payload.size()), 101, true));
    ASSERT_TRUE(ring->queue_fsync(fd_, true, 102));
    EXPECT_EQ(2u, ring->pending());
    EXPECT_EQ(6u, ring->sq_space());

    const auto submitted = ring->submit();
    ASSERT_TRUE(submitted) << submitted.error().message.view();
    EXPECT_EQ(2u, *submitted);
    EXPECT_EQ(0u, ring->pending());

    ASSERT_TRUE(ring->wait(2));

    UringCompletion first{};
    ASSERT_TRUE(ring->peek(first));
    EXPECT_EQ(101u, first.user_data);
    EXPECT_EQ(static_cast<std::int32_t>(payload.size()), first.result);

    UringCompletion second{};
    ASSERT_TRUE(ring->peek(second));
    EXPECT_EQ(102u, second.user_data);
    EXPECT_EQ(0, second.result);

    UringCompletion extra{};
    EXPECT_FALSE(ring->peek(extra));

    EXPECT_EQ(std::vector<std::uint8_t>(payload.begin(), payload.end()), read_back(payload.size()));
}

TEST_F(UringRingTest, everyCompletionCarriesItsOwnRequestAndResult)
{
    if (!UringRing::is_available())
    {
        GTEST_SKIP() << "io_uring is not available on this kernel";
    }

    auto ring = UringRing::create(8);
    ASSERT_TRUE(ring) << ring.error().message.view();

    std::array<std::uint8_t, 12> payload{};
    for (std::size_t index = 0; index < payload.size(); ++index)
    {
        payload[index] = static_cast<std::uint8_t>(index);
    }

    ASSERT_TRUE(ring->queue_write(fd_, 0, payload.data(), 4, 1, false));
    ASSERT_TRUE(ring->queue_write(fd_, 8, payload.data() + 4, 4, 2, false));
    ASSERT_TRUE(ring->queue_write(fd_, 16, payload.data() + 8, 4, 3, false));

    const auto submitted = ring->submit();
    ASSERT_TRUE(submitted) << submitted.error().message.view();
    EXPECT_EQ(3u, *submitted);
    ASSERT_TRUE(ring->wait(3));

    std::array<bool, 3> seen{false, false, false};
    for (int index = 0; index < 3; ++index)
    {
        UringCompletion completion{};
        ASSERT_TRUE(ring->peek(completion));
        EXPECT_EQ(4, completion.result);
        ASSERT_GE(completion.user_data, 1u);
        ASSERT_LE(completion.user_data, 3u);
        seen[completion.user_data - 1] = true;
    }
    EXPECT_TRUE(seen[0] && seen[1] && seen[2]);

    UringCompletion extra{};
    EXPECT_FALSE(ring->peek(extra));

    const std::vector<std::uint8_t> file = read_back(20);
    EXPECT_EQ(std::vector<std::uint8_t>(payload.begin(), payload.begin() + 4),
              std::vector<std::uint8_t>(file.begin(), file.begin() + 4));
    EXPECT_EQ(std::vector<std::uint8_t>(payload.begin() + 4, payload.begin() + 8),
              std::vector<std::uint8_t>(file.begin() + 8, file.begin() + 12));
    EXPECT_EQ(std::vector<std::uint8_t>(payload.begin() + 8, payload.end()),
              std::vector<std::uint8_t>(file.begin() + 16, file.begin() + 20));
}

TEST_F(UringRingTest, aFullSubmissionQueueReportsBackPressure)
{
    if (!UringRing::is_available())
    {
        GTEST_SKIP() << "io_uring is not available on this kernel";
    }

    auto ring = UringRing::create(2);
    ASSERT_TRUE(ring) << ring.error().message.view();

    const std::uint8_t byte = 0x5a;
    ASSERT_TRUE(ring->queue_write(fd_, 0, &byte, 1, 1, false));
    ASSERT_TRUE(ring->queue_write(fd_, 1, &byte, 1, 2, false));
    EXPECT_EQ(0u, ring->sq_space());

    const auto third = ring->queue_write(fd_, 2, &byte, 1, 3, false);
    ASSERT_FALSE(third);
    EXPECT_EQ(axc::core::ErrorCode::no_capacity, third.error().code);
}

TEST_F(UringRingTest, aFailedOperationIsReportedInItsCompletion)
{
    if (!UringRing::is_available())
    {
        GTEST_SKIP() << "io_uring is not available on this kernel";
    }

    auto ring = UringRing::create(4);
    ASSERT_TRUE(ring) << ring.error().message.view();

    const int readonly_fd = ::open("/dev/null", O_RDONLY);
    ASSERT_GE(readonly_fd, 0);

    const std::uint8_t byte = 0x11;
    ASSERT_TRUE(ring->queue_write(readonly_fd, 0, &byte, 1, 7, false));
    ASSERT_TRUE(ring->submit().has_value());
    ASSERT_TRUE(ring->wait(1));

    UringCompletion completion{};
    ASSERT_TRUE(ring->peek(completion));
    EXPECT_EQ(7u, completion.user_data);
    EXPECT_EQ(-EBADF, completion.result);

    ::close(readonly_fd);
}

TEST_F(UringRingTest, zeroEntriesIsRejected)
{
    const auto ring = UringRing::create(0);
    ASSERT_FALSE(ring);
    EXPECT_EQ(axc::core::ErrorCode::invalid_argument, ring.error().code);
}

TEST_F(UringRingTest, submittingNothingIsANoOp)
{
    if (!UringRing::is_available())
    {
        GTEST_SKIP() << "io_uring is not available on this kernel";
    }

    auto ring = UringRing::create(4);
    ASSERT_TRUE(ring) << ring.error().message.view();

    const auto submitted = ring->submit();
    ASSERT_TRUE(submitted) << submitted.error().message.view();
    EXPECT_EQ(0u, *submitted);
}

}  // namespace
