// Criterion for card A6b's async writer: submission order, back pressure,
// copy-before-submit, drain on roll/close (`io/uring_writer.h`).

#include <gtest/gtest.h>

#include <fcntl.h>
#include <sched.h>
#include <unistd.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "archive/io/segment_file.h"
#include "archive/io/uring_writer.h"

namespace
{

using deepmsg::archive::server::io::BlockWrite;
using deepmsg::archive::server::io::CommitOrder;
using deepmsg::archive::server::io::make_uring_writer;
using deepmsg::archive::server::io::SegmentWriter;
using deepmsg::archive::server::io::UringRing;
using deepmsg::archive::server::io::UringWriter;
using deepmsg::archive::server::io::WriterConfig;

constexpr std::int32_t TERM_LENGTH = 64 * 1024;
constexpr std::int32_t SEGMENT_LENGTH = 4 * TERM_LENGTH;
constexpr std::int64_t RECORDING_ID = 11;

class UringWriterTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        if (!UringRing::is_available())
        {
            GTEST_SKIP() << "io_uring is not available on this kernel";
        }

        std::array<char, 32> pattern{};
        std::snprintf(pattern.data(), pattern.size(), "/tmp/axc-uring-w-XXXXXX");
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        directory_ = created;

        directory_fd_ = ::open(directory_.c_str(), O_RDONLY | O_DIRECTORY);
        ASSERT_GE(directory_fd_, 0);
    }

    void TearDown() override
    {
        if (directory_fd_ >= 0)
        {
            ::close(directory_fd_);
        }
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
    }

    [[nodiscard]] WriterConfig config(int sync_level, int inflight, bool copy = true) const
    {
        WriterConfig config;
        config.archive_dir = directory_;
        config.recording_id = RECORDING_ID;
        config.segment_base_position = 0;
        config.segment_length = SEGMENT_LENGTH;
        config.file_sync_level = sync_level;
        config.archive_dir_fd = directory_fd_;
        config.inflight_max = inflight;
        config.copy_before_submit = copy;
        config.max_block_length = TERM_LENGTH;
        config.start_position = 0;
        return config;
    }

    [[nodiscard]] std::unique_ptr<SegmentWriter> writer(int sync_level, int inflight, bool copy = true) const
    {
        auto made = make_uring_writer(config(sync_level, inflight, copy));
        if (!made)
        {
            ADD_FAILURE() << made.error().message.view();
            return nullptr;
        }
        return std::move(*made);
    }

    [[nodiscard]] std::vector<std::uint8_t> read_segment(std::int64_t base_position, std::size_t length) const
    {
        const std::string path =
            directory_ + "/" + std::to_string(RECORDING_ID) + "-" + std::to_string(base_position) + ".rec";
        const int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0)
        {
            ADD_FAILURE() << "cannot open " << path;
            return {};
        }

        std::vector<std::uint8_t> bytes(length);
        const ssize_t read = ::pread(fd, bytes.data(), length, 0);
        ::close(fd);
        EXPECT_EQ(static_cast<ssize_t>(length), read);
        return bytes;
    }

    static bool wait_for(SegmentWriter& writer, std::int64_t position)
    {
        for (int attempt = 0; attempt < 2000000; ++attempt)
        {
            if (writer.committed_position() >= position)
            {
                return true;
            }
            (void)writer.poll_completions();
            ::sched_yield();
        }
        return writer.committed_position() >= position;
    }

    std::string directory_;
    int directory_fd_{-1};
};

TEST_F(UringWriterTest, aBlockIsCommittedOnlyWhenItsCompletionArrives)
{
    auto written = writer(0, 4);
    ASSERT_NE(nullptr, written);
    EXPECT_EQ(0, written->committed_position());

    const std::array<std::uint8_t, 8> payload{'c', 'o', 'm', 'm', 'i', 't', 'e', 'd'};
    ASSERT_TRUE(written->submit(BlockWrite{payload.data(), 8, 0, 8}));
    EXPECT_EQ(0, written->committed_position());

    ASSERT_TRUE(wait_for(*written, 8));
    EXPECT_EQ(8, written->committed_position());
    EXPECT_EQ(std::vector<std::uint8_t>(payload.begin(), payload.end()), read_segment(0, 8));
}

TEST_F(UringWriterTest, thePositionStartsAtTheJoinPosition)
{
    WriterConfig value = config(0, 4);
    value.start_position = 4096;
    auto made = make_uring_writer(value);
    ASSERT_TRUE(made) << made.error().message.view();
    EXPECT_EQ(4096, (*made)->committed_position());

    const std::array<std::uint8_t, 4> payload{'j', 'o', 'i', 'n'};
    ASSERT_TRUE((*made)->submit(BlockWrite{payload.data(), 4, 4096, 4100}));
    ASSERT_TRUE(wait_for(**made, 4100));
    EXPECT_EQ(4100, (*made)->committed_position());
}

TEST_F(UringWriterTest, backPressureStopsSubmittingUntilACompletionFreesASlot)
{
    auto written = writer(0, 1);
    ASSERT_NE(nullptr, written);

    const std::array<std::uint8_t, 4> payload{'p', 'r', 'e', 's'};
    ASSERT_TRUE(written->submit(BlockWrite{payload.data(), 4, 0, 4}));

    const auto second = written->submit(BlockWrite{payload.data(), 4, 4, 8});
    ASSERT_TRUE(second);
    EXPECT_FALSE(*second);

    ASSERT_TRUE(wait_for(*written, 4));
    const auto third = written->submit(BlockWrite{payload.data(), 4, 4, 8});
    ASSERT_TRUE(third);
    EXPECT_TRUE(*third);
    ASSERT_TRUE(wait_for(*written, 8));
}

TEST_F(UringWriterTest, copyBeforeSubmitKeepsTheBlockOutOfTheTermBuffer)
{
    auto written = writer(0, 4);
    ASSERT_NE(nullptr, written);

    std::array<std::uint8_t, 8> source{'o', 'r', 'i', 'g', 'i', 'n', 'a', 'l'};
    ASSERT_TRUE(written->submit(BlockWrite{source.data(), 8, 0, 8}));

    // The recorder is free to reuse its term buffer as soon as submit returns.
    source.fill(0xff);

    ASSERT_TRUE(wait_for(*written, 8));
    const std::array<std::uint8_t, 8> expected{'o', 'r', 'i', 'g', 'i', 'n', 'a', 'l'};
    EXPECT_EQ(std::vector<std::uint8_t>(expected.begin(), expected.end()), read_segment(0, 8));
}

TEST_F(UringWriterTest, aForceLevelSubmitsOneForcePerBlock)
{
    auto written = writer(2, 4);
    ASSERT_NE(nullptr, written);

    const std::array<std::uint8_t, 4> payload{'s', 'y', 'n', 'c'};
    ASSERT_TRUE(written->submit(BlockWrite{payload.data(), 4, 0, 4}));
    ASSERT_TRUE(wait_for(*written, 4));

    const auto* uring = dynamic_cast<const UringWriter*>(written.get());
    ASSERT_NE(nullptr, uring);
    EXPECT_EQ(1u, uring->force_count());
    EXPECT_EQ(1u, uring->completed_blocks());
}

TEST_F(UringWriterTest, rollSegmentDrainsTheOldFileAndRefusesAnExistingOne)
{
    auto written = writer(2, 4);
    ASSERT_NE(nullptr, written);

    const std::array<std::uint8_t, 6> payload{'r', 'o', 'l', 'l', 'e', 'd'};
    ASSERT_TRUE(written->submit(BlockWrite{payload.data(), 6, SEGMENT_LENGTH - 6, SEGMENT_LENGTH}));
    ASSERT_TRUE(written->roll_segment(SEGMENT_LENGTH));

    // The completion of the old file arrived before roll returned.
    EXPECT_EQ(SEGMENT_LENGTH, written->committed_position());
    const std::vector<std::uint8_t> segment = read_segment(0, static_cast<std::size_t>(SEGMENT_LENGTH));
    ASSERT_EQ(static_cast<std::size_t>(SEGMENT_LENGTH), segment.size());
    EXPECT_EQ(std::vector<std::uint8_t>(payload.begin(), payload.end()),
              std::vector<std::uint8_t>(segment.end() - 6, segment.end()));

    // A second roll onto the same base position must fail, like the reference.
    const auto again = written->roll_segment(SEGMENT_LENGTH);
    EXPECT_FALSE(again);
}

TEST_F(UringWriterTest, closeDrainsWithoutForcingAtLevelZero)
{
    auto written = writer(0, 4);
    ASSERT_NE(nullptr, written);

    const std::array<std::uint8_t, 4> payload{'c', 'l', 'o', 's'};
    ASSERT_TRUE(written->submit(BlockWrite{payload.data(), 4, 0, 4}));
    EXPECT_TRUE(written->close());

    const auto* uring = dynamic_cast<const UringWriter*>(written.get());
    ASSERT_NE(nullptr, uring);
    EXPECT_EQ(0u, uring->force_count());
    EXPECT_EQ(4, written->committed_position());
    EXPECT_EQ(std::vector<std::uint8_t>(payload.begin(), payload.end()), read_segment(0, 4));
}

TEST_F(UringWriterTest, outOfOrderCompletionsAdvanceOnlyOverTheReadyPrefix)
{
    CommitOrder order(4, 0);
    order.start_at(0);

    const auto first = order.reserve(10, 10);
    const auto second = order.reserve(10, 20);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(2u, order.in_flight());

    // The second block finishes first, but nothing may commit past the first.
    EXPECT_EQ(CommitOrder::Completion::accepted, order.write_completed(second->seq, 10));
    EXPECT_EQ(0, order.committed_position());
    // The finished block still holds its slot: it cannot be reused before the
    // position has actually moved past it.
    EXPECT_EQ(2u, order.in_flight());

    // Completing the first block releases the whole contiguous prefix.
    EXPECT_EQ(CommitOrder::Completion::accepted, order.write_completed(first->seq, 10));
    EXPECT_EQ(20, order.committed_position());
    EXPECT_EQ(0u, order.in_flight());
    EXPECT_EQ(2u, order.completed_blocks());

    // A short write is not accepted as that block's completion.
    const auto third = order.reserve(10, 30);
    ASSERT_TRUE(third.has_value());
    EXPECT_EQ(CommitOrder::Completion::wrong_length, order.write_completed(third->seq, 9));
    EXPECT_EQ(CommitOrder::Completion::unknown, order.write_completed(999, 10));
}

TEST_F(UringWriterTest, aForceLevelBlockWaitsForItsForceEvenWhenTheForceArrivesFirst)
{
    CommitOrder order(4, 2);
    order.start_at(0);

    const auto block = order.reserve(10, 10);
    ASSERT_TRUE(block.has_value());

    EXPECT_TRUE(order.force_completed(block->seq));
    EXPECT_EQ(0, order.committed_position());

    EXPECT_EQ(CommitOrder::Completion::accepted, order.write_completed(block->seq, 10));
    EXPECT_EQ(10, order.committed_position());
    EXPECT_EQ(0u, order.in_flight());
}

TEST_F(UringWriterTest, aClosedWriterReportsItsErrorInsteadOfSubmitting)
{
    auto written = writer(0, 4);
    ASSERT_NE(nullptr, written);
    ASSERT_TRUE(written->close());

    const std::array<std::uint8_t, 4> payload{'c', 'l', 'o', 's'};
    const auto submitted = written->submit(BlockWrite{payload.data(), 4, 0, 4});
    ASSERT_FALSE(submitted);
    EXPECT_EQ(axc::core::ErrorCode::internal_error, submitted.error().code);
}

TEST_F(UringWriterTest, theMakerRejectsAConfigWithoutABudgetOrABlockLength)
{
    auto no_budget = config(0, 0);
    EXPECT_FALSE(make_uring_writer(no_budget));

    auto no_block = config(0, 4);
    no_block.max_block_length = 0;
    EXPECT_FALSE(make_uring_writer(no_block));
}

}  // namespace
