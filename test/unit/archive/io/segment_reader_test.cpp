// A6d: the two read backends behind `SegmentReader` (pread and mmap), their
// roll-over behaviour and the optional read-ahead advice.

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

#include "archive/io/mmap_reader.h"
#include "archive/io/pread_reader.h"
#include "archive/server/segment_files.h"

namespace
{

using deepmsg::archive::server::segment_file_name;
using deepmsg::archive::server::io::Fadvise;
using deepmsg::archive::server::io::make_mmap_reader;
using deepmsg::archive::server::io::make_pread_reader;
using deepmsg::archive::server::io::ReaderConfig;
using deepmsg::archive::server::io::SegmentReader;

constexpr std::int64_t RECORDING_ID = 7;
constexpr std::int32_t SEGMENT_LENGTH = 64 * 1024;

int g_advice_calls{0};
int g_last_advice{-1};

int counting_fadvise(int, std::int64_t, int advice) noexcept
{
    ++g_advice_calls;
    g_last_advice = advice;
    return 0;
}

int refused_fadvise(int, std::int64_t, int) noexcept
{
    return EINVAL;
}

class SegmentReaderTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::array<char, 32> pattern{};
        std::snprintf(pattern.data(), pattern.size(), "/tmp/axc-reader-XXXXXX");
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        directory_ = created;
    }

    void TearDown() override
    {
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
    }

    void write_segment(std::int64_t base_position, std::uint8_t fill) const
    {
        const std::string path = directory_ + "/" + segment_file_name(RECORDING_ID, base_position);
        const int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(0, ::ftruncate(fd, SEGMENT_LENGTH));
        std::array<std::uint8_t, 32> block{};
        block.fill(fill);
        ASSERT_EQ(static_cast<ssize_t>(block.size()), ::pwrite(fd, block.data(), block.size(), 0));
        ::close(fd);
    }

    [[nodiscard]] std::array<std::uint8_t, 32> read_file_prefix(std::int64_t base_position) const
    {
        const std::string path = directory_ + "/" + segment_file_name(RECORDING_ID, base_position);
        const int fd = ::open(path.c_str(), O_RDONLY);
        EXPECT_GE(fd, 0);
        std::array<std::uint8_t, 32> bytes{};
        EXPECT_EQ(static_cast<ssize_t>(bytes.size()), ::pread(fd, bytes.data(), bytes.size(), 0));
        ::close(fd);
        return bytes;
    }

    [[nodiscard]] ReaderConfig config(Fadvise fadvise = Fadvise::none) const
    {
        ReaderConfig value;
        value.archive_dir = directory_;
        value.recording_id = RECORDING_ID;
        value.segment_base_position = 0;
        value.segment_length = SEGMENT_LENGTH;
        value.fadvise = fadvise;
        return value;
    }

    std::string directory_;
};

TEST_F(SegmentReaderTest, bothBackendsReturnTheSameBytes)
{
    write_segment(0, 0x5a);

    for (auto make : {&make_pread_reader, &make_mmap_reader})
    {
        auto reader = make(config());
        ASSERT_TRUE(reader) << reader.error().message.view();

        auto head = (*reader)->read(0, 32);
        ASSERT_TRUE(head);
        ASSERT_EQ(32u, head->size());
        for (const std::uint8_t byte : *head)
        {
            EXPECT_EQ(0x5a, byte);
        }

        // Clipped at the segment end.
        auto tail = (*reader)->read(SEGMENT_LENGTH - 8, 32);
        ASSERT_TRUE(tail);
        EXPECT_EQ(8u, tail->size());

        // Past the end: empty.
        auto past = (*reader)->read(SEGMENT_LENGTH, 32);
        ASSERT_TRUE(past);
        EXPECT_TRUE(past->empty());

        ASSERT_TRUE((*reader)->close());
    }
}

TEST_F(SegmentReaderTest, rewritingTheReturnedSpanDoesNotTouchTheFile)
{
    write_segment(0, 0x11);

    for (auto make : {&make_pread_reader, &make_mmap_reader})
    {
        auto reader = make(config());
        ASSERT_TRUE(reader) << reader.error().message.view();

        auto span = (*reader)->read(0, 32);
        ASSERT_TRUE(span);
        ASSERT_EQ(32u, span->size());
        // The replay session stamps the recording's own ids into the frame
        // headers; that must never reach the recorded file.
        for (std::uint8_t& byte : *span)
        {
            byte = 0xee;
        }

        const auto file = read_file_prefix(0);
        for (const std::uint8_t byte : file)
        {
            EXPECT_EQ(0x11, byte);
        }

        ASSERT_TRUE((*reader)->close());
    }
}

TEST_F(SegmentReaderTest, rollSegmentSwitchesFiles)
{
    write_segment(0, 0x01);
    write_segment(SEGMENT_LENGTH, 0x02);

    for (auto make : {&make_pread_reader, &make_mmap_reader})
    {
        auto reader = make(config());
        ASSERT_TRUE(reader) << reader.error().message.view();

        auto first = (*reader)->read(0, 4);
        ASSERT_TRUE(first);
        EXPECT_EQ(0x01, (*first)[0]);

        ASSERT_TRUE((*reader)->roll_segment(SEGMENT_LENGTH));
        auto second = (*reader)->read(0, 4);
        ASSERT_TRUE(second);
        EXPECT_EQ(0x02, (*second)[0]);

        const auto missing = (*reader)->roll_segment(2 * SEGMENT_LENGTH);
        ASSERT_FALSE(missing);
        EXPECT_EQ(axc::core::ErrorCode::not_found, missing.error().code);

        ASSERT_TRUE((*reader)->close());
    }
}

TEST_F(SegmentReaderTest, willneedIsAdvisedOnOpenAndDontneedOnClose)
{
    write_segment(0, 0x2a);

    for (auto make : {&make_pread_reader, &make_mmap_reader})
    {
        ReaderConfig value = config(Fadvise::willneed);
        value.fadvise_fn = &counting_fadvise;
        g_advice_calls = 0;
        g_last_advice = -1;

        auto reader = make(value);
        ASSERT_TRUE(reader) << reader.error().message.view();
        EXPECT_EQ(1, g_advice_calls);
        EXPECT_EQ(POSIX_FADV_WILLNEED, g_last_advice);

        ASSERT_TRUE((*reader)->close());
        EXPECT_EQ(2, g_advice_calls);
        EXPECT_EQ(POSIX_FADV_DONTNEED, g_last_advice);
    }
}

TEST_F(SegmentReaderTest, fadviseNoneIssuesNothing)
{
    write_segment(0, 0x2a);

    for (auto make : {&make_pread_reader, &make_mmap_reader})
    {
        ReaderConfig value = config(Fadvise::none);
        value.fadvise_fn = &counting_fadvise;
        g_advice_calls = 0;

        auto reader = make(value);
        ASSERT_TRUE(reader) << reader.error().message.view();
        ASSERT_TRUE((*reader)->close());
        EXPECT_EQ(0, g_advice_calls);
    }
}

TEST_F(SegmentReaderTest, aRefusedAdviceIsNotAnError)
{
    write_segment(0, 0x2a);

    for (auto make : {&make_pread_reader, &make_mmap_reader})
    {
        ReaderConfig value = config(Fadvise::willneed);
        value.fadvise_fn = &refused_fadvise;

        auto reader = make(value);
        ASSERT_TRUE(reader) << reader.error().message.view();

        auto head = (*reader)->read(0, 4);
        ASSERT_TRUE(head);
        EXPECT_EQ(0x2a, (*head)[0]);
        ASSERT_TRUE((*reader)->close());
    }
}

TEST_F(SegmentReaderTest, theRealAdvicePathKeepsTheReadsWorking)
{
    write_segment(0, 0x2a);

    for (auto make : {&make_pread_reader, &make_mmap_reader})
    {
        auto reader = make(config(Fadvise::willneed));
        ASSERT_TRUE(reader) << reader.error().message.view();

        auto head = (*reader)->read(0, 4);
        ASSERT_TRUE(head);
        EXPECT_EQ(0x2a, (*head)[0]);
        ASSERT_TRUE((*reader)->close());
    }
}

}  // namespace
