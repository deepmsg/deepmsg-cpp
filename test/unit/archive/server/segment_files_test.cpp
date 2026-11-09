// A4a-1 criteria: the segment-file helpers the delete / detach / attach paths
// use. Reference: `ArchiveConductor.java:1722-1745`, `:1789-1871`, `:1981-2005`,
// `:2570-2623` (1.54.0 = 454da1e4e6).

#include <gtest/gtest.h>

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include "archive/server/frame_layout.h"
#include "archive/server/segment_files.h"

namespace
{

using deepmsg::archive::server::check_first_frame;
using deepmsg::archive::server::DATA_HEADER_LENGTH;
using deepmsg::archive::server::erase_remaining_segment;
using deepmsg::archive::server::find_detached_segments;
using deepmsg::archive::server::find_term_offset_for_start;
using deepmsg::archive::server::FRAME_LENGTH_OFFSET;
using deepmsg::archive::server::FrameHeaderCheck;
using deepmsg::archive::server::list_segment_files;
using deepmsg::archive::server::list_segment_files_with_delete_suffix;
using deepmsg::archive::server::segment_file_name;
using deepmsg::archive::server::SESSION_ID_OFFSET;
using deepmsg::archive::server::STREAM_ID_OFFSET;
using deepmsg::archive::server::TERM_ID_OFFSET;
using deepmsg::archive::server::TERM_OFFSET_FIELD_OFFSET;

constexpr std::int32_t TERM_LENGTH = 64 * 1024;
constexpr std::int32_t SEGMENT_LENGTH = 128 * 1024;

class SegmentFilesSegmentOpsTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::error_code error;
        archive_dir_ = std::filesystem::temp_directory_path(error) / "axc-segment-files-test";
        std::filesystem::remove_all(archive_dir_, error);
        std::filesystem::create_directories(archive_dir_, error);
        ASSERT_FALSE(error);
    }

    void TearDown() override
    {
        std::error_code error;
        std::filesystem::remove_all(archive_dir_, error);
    }

    [[nodiscard]] std::string dir() const
    {
        return archive_dir_.string();
    }

    void create_file(const std::string& name, std::int32_t length, std::uint8_t fill)
    {
        const int fd = ::open((archive_dir_ / name).c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        ASSERT_GE(fd, 0) << name;
        const std::vector<std::uint8_t> bytes(static_cast<std::size_t>(length), fill);
        ASSERT_EQ(static_cast<ssize_t>(bytes.size()), ::pwrite(fd, bytes.data(), bytes.size(), 0));
        ASSERT_EQ(0, ::close(fd));
    }

    void write_frame(const std::string& name, std::int64_t offset, std::int32_t frame_length, std::int32_t term_offset,
                     std::int32_t session_id, std::int32_t stream_id, std::int32_t term_id)
    {
        const int fd = ::open((archive_dir_ / name).c_str(), O_RDWR | O_CREAT, 0644);
        ASSERT_GE(fd, 0) << name;

        std::array<std::uint8_t, DATA_HEADER_LENGTH> header{};
        std::memcpy(header.data() + FRAME_LENGTH_OFFSET, &frame_length, sizeof(frame_length));
        std::memcpy(header.data() + TERM_OFFSET_FIELD_OFFSET, &term_offset, sizeof(term_offset));
        std::memcpy(header.data() + SESSION_ID_OFFSET, &session_id, sizeof(session_id));
        std::memcpy(header.data() + STREAM_ID_OFFSET, &stream_id, sizeof(stream_id));
        std::memcpy(header.data() + TERM_ID_OFFSET, &term_id, sizeof(term_id));

        ASSERT_EQ(static_cast<ssize_t>(header.size()),
                  ::pwrite(fd, header.data(), header.size(), static_cast<off_t>(offset)));
        ASSERT_EQ(0, ::close(fd));
    }

    [[nodiscard]] std::vector<std::uint8_t> read_file(const std::string& name) const
    {
        std::vector<std::uint8_t> bytes;
        std::error_code error;
        const auto size = std::filesystem::file_size(archive_dir_ / name, error);
        if (error)
        {
            return bytes;
        }

        bytes.resize(static_cast<std::size_t>(size));
        const int fd = ::open((archive_dir_ / name).c_str(), O_RDONLY);
        if (fd < 0)
        {
            return {};
        }
        if (!bytes.empty())
        {
            (void)::pread(fd, bytes.data(), bytes.size(), 0);
        }
        (void)::close(fd);
        return bytes;
    }

    std::filesystem::path archive_dir_;
};

TEST_F(SegmentFilesSegmentOpsTest, theConductorListIncludesDeleteSuffixButTheCatalogOneDoesNot)
{
    create_file("7-0.rec", 1, 0);
    create_file("7-131072.rec", 1, 0);
    create_file("7-262144.del", 1, 0);
    create_file("8-0.rec", 1, 0);
    create_file("7-0.txt", 1, 0);

    auto conductor = list_segment_files_with_delete_suffix(dir(), 7);
    std::sort(conductor.begin(), conductor.end());
    EXPECT_EQ((std::vector<std::string>{"7-0.rec", "7-131072.rec", "7-262144.del"}), conductor);

    auto catalog = list_segment_files(dir(), 7);
    std::sort(catalog.begin(), catalog.end());
    EXPECT_EQ((std::vector<std::string>{"7-0.rec", "7-131072.rec"}), catalog);
}

TEST_F(SegmentFilesSegmentOpsTest, detachedSegmentsWalkDownToThePreviousStart)
{
    // `shouldPurgeSegments`: start moves 0 -> 2 segments, two files detached.
    EXPECT_EQ((std::vector<std::string>{segment_file_name(7, 0), segment_file_name(7, SEGMENT_LENGTH)}),
              find_detached_segments(dir(), 7, 2 * SEGMENT_LENGTH, 0, TERM_LENGTH, SEGMENT_LENGTH));

    // `shouldDetachAndReattachSegments`: start is 4 segments in, four files.
    EXPECT_EQ(
        (std::vector<std::string>{segment_file_name(7, 0), segment_file_name(7, SEGMENT_LENGTH),
                                  segment_file_name(7, 2 * SEGMENT_LENGTH), segment_file_name(7, 3 * SEGMENT_LENGTH)}),
        find_detached_segments(dir(), 7, 4 * SEGMENT_LENGTH, 0, TERM_LENGTH, SEGMENT_LENGTH));
}

TEST_F(SegmentFilesSegmentOpsTest, checkFirstFrameComparesTheThreeFields)
{
    const std::string name = segment_file_name(7, 0);
    create_file(name, DATA_HEADER_LENGTH, 0);
    write_frame(name, 0, 64, 64, 5, 11, 3);

    const int fd = ::open((archive_dir_ / name).c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);

    EXPECT_EQ(FrameHeaderCheck::ok, check_first_frame(fd, 0, 64, 3, 11));
    EXPECT_EQ(FrameHeaderCheck::unaligned, check_first_frame(fd, 0, 65, 3, 11));
    EXPECT_EQ(FrameHeaderCheck::unaligned, check_first_frame(fd, 0, 64, 4, 11));
    EXPECT_EQ(FrameHeaderCheck::unaligned, check_first_frame(fd, 0, 64, 3, 12));
    EXPECT_EQ(FrameHeaderCheck::read_error, check_first_frame(fd, 1000, 64, 3, 11));

    EXPECT_EQ(0, ::close(fd));
}

TEST_F(SegmentFilesSegmentOpsTest, eraseRemainingSegmentTruncatesAndKeepsTheSegmentLength)
{
    const std::string name = segment_file_name(7, 0);
    create_file(name, SEGMENT_LENGTH, 0xAA);
    write_frame(name, 128, 64, 128, 5, 11, 3);

    const auto erased = erase_remaining_segment(dir(), name, 128, SEGMENT_LENGTH, 128, TERM_LENGTH, 3, 11);
    ASSERT_TRUE(erased.has_value()) << erased.error();

    const auto bytes = read_file(name);
    ASSERT_EQ(static_cast<std::size_t>(SEGMENT_LENGTH), bytes.size());

    // Before the cut the file is untouched.
    for (std::size_t index = 0; index < 128; index++)
    {
        ASSERT_EQ(0xAA, bytes[index]) << index;
    }
    // From the cut to the last byte everything is zero.
    for (std::size_t index = 128; index < bytes.size(); index++)
    {
        ASSERT_EQ(0, bytes[index]) << index;
    }
}

TEST_F(SegmentFilesSegmentOpsTest, eraseRemainingSegmentRefusesAnUnalignedPosition)
{
    const std::string name = segment_file_name(7, 0);
    create_file(name, SEGMENT_LENGTH, 0xAA);
    // The header at 64 claims term offset 0, which is not what position 64 needs.
    write_frame(name, 64, 64, 0, 5, 11, 3);

    const auto erased = erase_remaining_segment(dir(), name, 64, SEGMENT_LENGTH, 64, TERM_LENGTH, 3, 11);
    ASSERT_FALSE(erased.has_value());
    EXPECT_EQ("64 position not aligned to a data header", erased.error());
    EXPECT_EQ(static_cast<std::size_t>(SEGMENT_LENGTH), read_file(name).size());
}

TEST_F(SegmentFilesSegmentOpsTest, findTermOffsetReturnsZeroWhenTheFrameIsAtTheStart)
{
    const std::string name = segment_file_name(7, 0);
    create_file(name, SEGMENT_LENGTH, 0);
    write_frame(name, 0, 64, 0, 5, 11, 3);

    const auto offset = find_term_offset_for_start(dir(), name, 11, 3, TERM_LENGTH);
    ASSERT_TRUE(offset.has_value()) << offset.error();
    EXPECT_EQ(0, *offset);
}

TEST_F(SegmentFilesSegmentOpsTest, findTermOffsetScansTheFirstTermForTheFirstFrame)
{
    const std::string name = segment_file_name(7, 0);
    create_file(name, SEGMENT_LENGTH, 0);
    write_frame(name, 96, 64, 96, 5, 11, 3);

    const auto offset = find_term_offset_for_start(dir(), name, 11, 3, TERM_LENGTH);
    ASSERT_TRUE(offset.has_value()) << offset.error();
    EXPECT_EQ(96, *offset);
}

TEST_F(SegmentFilesSegmentOpsTest, findTermOffsetReportsOneFlavourOfMismatchAtATime)
{
    const std::string name = segment_file_name(7, 0);
    create_file(name, SEGMENT_LENGTH, 0);
    write_frame(name, 0, 64, 0, 5, 11, 4);

    const auto term = find_term_offset_for_start(dir(), name, 11, 3, TERM_LENGTH);
    ASSERT_FALSE(term.has_value());
    EXPECT_EQ("term id does not match: actual=4 expected=3", term.error());

    write_frame(name, 0, 64, 0, 5, 12, 3);
    const auto stream = find_term_offset_for_start(dir(), name, 11, 3, TERM_LENGTH);
    ASSERT_FALSE(stream.has_value());
    EXPECT_EQ("stream id does not match: actual=12 expected=11", stream.error());
}

TEST_F(SegmentFilesSegmentOpsTest, findTermOffsetReportsAnEmptyFirstTerm)
{
    const std::string name = segment_file_name(7, 0);
    create_file(name, SEGMENT_LENGTH, 0);

    const auto offset = find_term_offset_for_start(dir(), name, 11, 3, TERM_LENGTH);
    ASSERT_FALSE(offset.has_value());
    EXPECT_EQ("fragment not found in first term of segment " + (archive_dir_ / name).string(), offset.error());
}

}  // namespace
