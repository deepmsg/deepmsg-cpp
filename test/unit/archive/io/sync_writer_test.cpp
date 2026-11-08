// Criterion for card A1b's write backend and segment-file naming.
//
// (`segment_files.h` + `io/sync_writer.h`.)

#include <gtest/gtest.h>

#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include "archive/io/sync_writer.h"
#include "archive/server/segment_files.h"

namespace
{

using deepmsg::archive::server::find_segment_file_with_highest_position;
using deepmsg::archive::server::index_segment_files;
using deepmsg::archive::server::list_segment_files;
using deepmsg::archive::server::parse_segment_file_position;
using deepmsg::archive::server::parse_segment_file_recording_id;
using deepmsg::archive::server::segment_file_base_position;
using deepmsg::archive::server::segment_file_length;
using deepmsg::archive::server::segment_file_name;
using deepmsg::archive::server::io::BlockWrite;
using deepmsg::archive::server::io::make_sync_writer;
using deepmsg::archive::server::io::SyncWriter;
using deepmsg::archive::server::io::WriterConfig;

constexpr std::int32_t TERM_LENGTH = 64 * 1024;
constexpr std::int32_t SEGMENT_LENGTH = 4 * TERM_LENGTH;
constexpr std::int64_t RECORDING_ID = 11;

TEST(SegmentFilesTest, namesFollowTheReference)
{
    // `Archive.segmentFileName` (`Archive.java:4091`).
    EXPECT_EQ("0-0.rec", segment_file_name(0, 0));
    EXPECT_EQ("42-262144.rec", segment_file_name(42, 262144));
}

TEST(SegmentFilesTest, lengthIsTheLargerOfSegmentAndTerm)
{
    // `ArchiveConductor.java:2028`: `max(ctx.segmentFileLength(), termBufferLength)`.
    EXPECT_EQ(TERM_LENGTH, segment_file_length(TERM_LENGTH, TERM_LENGTH));
    EXPECT_EQ(TERM_LENGTH, segment_file_length(TERM_LENGTH, TERM_LENGTH / 2));
    EXPECT_EQ(4 * TERM_LENGTH, segment_file_length(TERM_LENGTH, 4 * TERM_LENGTH));
}

TEST(SegmentFilesTest, basePositionFollowsTheReference)
{
    // `AeronArchive.segmentFileBasePosition` (`AeronArchive.java:195`).
    EXPECT_EQ(0, segment_file_base_position(0, 0, TERM_LENGTH, SEGMENT_LENGTH));
    EXPECT_EQ(0, segment_file_base_position(0, SEGMENT_LENGTH - 1, TERM_LENGTH, SEGMENT_LENGTH));
    EXPECT_EQ(SEGMENT_LENGTH, segment_file_base_position(0, SEGMENT_LENGTH, TERM_LENGTH, SEGMENT_LENGTH));
    EXPECT_EQ(2 * SEGMENT_LENGTH, segment_file_base_position(0, 2 * SEGMENT_LENGTH + 17, TERM_LENGTH, SEGMENT_LENGTH));
    // A start position that is not at a term base shifts the whole window.
    const std::int64_t start = TERM_LENGTH + 64;
    EXPECT_EQ(TERM_LENGTH, segment_file_base_position(start, start + 128, TERM_LENGTH, SEGMENT_LENGTH));
    EXPECT_EQ(TERM_LENGTH,
              segment_file_base_position(start, TERM_LENGTH + SEGMENT_LENGTH - 1, TERM_LENGTH, SEGMENT_LENGTH));
    EXPECT_EQ(TERM_LENGTH + SEGMENT_LENGTH,
              segment_file_base_position(start, TERM_LENGTH + SEGMENT_LENGTH, TERM_LENGTH, SEGMENT_LENGTH));
}

TEST(SegmentFilesTest, parsesNamesLikeTheReference)
{
    // `Catalog.parseSegmentFilePosition` / `parseSegmentFileRecordingId`.
    ASSERT_TRUE(parse_segment_file_position("7-262144.rec"));
    EXPECT_EQ(262144, *parse_segment_file_position("7-262144.rec"));
    ASSERT_TRUE(parse_segment_file_recording_id("7-262144.rec"));
    EXPECT_EQ(7, *parse_segment_file_recording_id("7-262144.rec"));

    auto no_dash = parse_segment_file_position("262144.rec");
    ASSERT_FALSE(no_dash);
    EXPECT_EQ("invalid filename format: 262144.rec", no_dash.error().message.view());

    auto no_position = parse_segment_file_position("7-.rec");
    ASSERT_FALSE(no_position);
    EXPECT_EQ("no position encoded in the segment file: 7-.rec", no_position.error().message.view());

    auto bad_id = parse_segment_file_recording_id("-262144.rec");
    ASSERT_FALSE(bad_id);
    EXPECT_EQ("invalid filename format: -262144.rec", bad_id.error().message.view());
}

class SyncWriterTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::array<char, 64> pattern{};
        std::snprintf(pattern.data(), pattern.size(), "/tmp/axc-writer-XXXXXX");
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        archive_dir_ = created;

        directory_fd_ = ::open(archive_dir_.c_str(), O_RDONLY | O_DIRECTORY);
        ASSERT_GE(directory_fd_, 0);
    }

    void TearDown() override
    {
        if (directory_fd_ >= 0)
        {
            ::close(directory_fd_);
        }
        std::error_code error;
        std::filesystem::remove_all(archive_dir_, error);
    }

    [[nodiscard]] WriterConfig config(int sync_level) const
    {
        WriterConfig config;
        config.archive_dir = archive_dir_;
        config.recording_id = RECORDING_ID;
        config.segment_base_position = 0;
        config.segment_length = SEGMENT_LENGTH;
        config.file_sync_level = sync_level;
        config.archive_dir_fd = directory_fd_;
        return config;
    }

    std::string archive_dir_;
    int directory_fd_{-1};
};

TEST_F(SyncWriterTest, levelZeroNeverForces)
{
    auto writer = make_sync_writer(config(0));
    ASSERT_TRUE(writer) << writer.error().message.view();

    std::vector<std::uint8_t> bytes(1024, 7);
    auto submitted = (*writer)->submit(BlockWrite{bytes.data(), 1024, 0, 1024});
    ASSERT_TRUE(submitted) << submitted.error().message.view();
    EXPECT_TRUE(*submitted);

    auto* sync_writer = dynamic_cast<SyncWriter*>((*writer).get());
    EXPECT_EQ(0u, sync_writer->force_count());
    EXPECT_EQ(0u, sync_writer->directory_force_count());
    EXPECT_EQ(1024, (*writer)->committed_position());
}

TEST_F(SyncWriterTest, levelsAboveZeroForceEveryBlock)
{
    for (const int sync_level : {1, 2})
    {
        auto writer = make_sync_writer(config(sync_level));
        ASSERT_TRUE(writer) << writer.error().message.view();
        auto* sync_writer = dynamic_cast<SyncWriter*>((*writer).get());
        EXPECT_EQ(1u, sync_writer->directory_force_count()) << "the segment creation forces the directory";
        EXPECT_EQ(0u, sync_writer->force_count());

        std::vector<std::uint8_t> bytes(1024, 7);
        for (int index = 0; index < 3; index++)
        {
            auto submitted = (*writer)->submit(
                BlockWrite{bytes.data(), 1024, static_cast<std::int64_t>(index) * 1024, (index + 1) * 1024});
            ASSERT_TRUE(submitted) << submitted.error().message.view();
        }

        EXPECT_EQ(3u, sync_writer->force_count()) << "sync level " << sync_level;
        EXPECT_EQ(1u, sync_writer->directory_force_count()) << "sync level " << sync_level;
    }
}

TEST_F(SyncWriterTest, committedPositionNeverRunsAheadOfTheWrite)
{
    auto writer = make_sync_writer(config(0));
    ASSERT_TRUE(writer) << writer.error().message.view();
    EXPECT_EQ(0, (*writer)->committed_position());

    std::vector<std::uint8_t> bytes(512, 3);
    auto submitted = (*writer)->submit(BlockWrite{bytes.data(), 512, 0, 512});
    ASSERT_TRUE(submitted);
    EXPECT_EQ(512, (*writer)->committed_position());

    auto second = (*writer)->submit(BlockWrite{bytes.data(), 512, 512, 1024});
    ASSERT_TRUE(second);
    EXPECT_EQ(1024, (*writer)->committed_position());
}

TEST_F(SyncWriterTest, rollsToTheNextSegmentFileAndRefusesToOverwriteIt)
{
    WriterConfig writer_config = config(0);
    auto writer = make_sync_writer(writer_config);
    ASSERT_TRUE(writer) << writer.error().message.view();

    std::vector<std::uint8_t> bytes(1024, 9);
    auto submitted = (*writer)->submit(BlockWrite{bytes.data(), 1024, 0, 1024});
    ASSERT_TRUE(submitted);

    auto rolled = (*writer)->roll_segment(SEGMENT_LENGTH);
    ASSERT_TRUE(rolled) << rolled.error().message.view();
    EXPECT_TRUE(std::filesystem::exists(archive_dir_ + "/" + segment_file_name(RECORDING_ID, SEGMENT_LENGTH)));
    // The rolled-to file is empty apart from its length: the first block of the
    // new segment starts at file offset 0.
    auto after = (*writer)->submit(BlockWrite{bytes.data(), 1024, 0, SEGMENT_LENGTH + 1024});
    ASSERT_TRUE(after);

    // Rolling onto a file that already exists is an error (`RecordingWriter.java:242`).
    auto conflict = (*writer)->roll_segment(SEGMENT_LENGTH);
    ASSERT_FALSE(conflict);
    EXPECT_EQ("segment file already exists: " + archive_dir_ + "/" + segment_file_name(RECORDING_ID, SEGMENT_LENGTH),
              conflict.error().message.view());
}

TEST_F(SyncWriterTest, listsAndIndexesSegmentFiles)
{
    auto writer = make_sync_writer(config(0));
    ASSERT_TRUE(writer) << writer.error().message.view();
    ASSERT_TRUE((*writer)->roll_segment(SEGMENT_LENGTH));

    const auto files = list_segment_files(archive_dir_, RECORDING_ID);
    EXPECT_EQ(2u, files.size());

    auto index = index_segment_files(archive_dir_);
    ASSERT_TRUE(index) << index.error().message.view();
    ASSERT_EQ(1u, index->count(RECORDING_ID));
    EXPECT_EQ(2u, (*index)[RECORDING_ID].size());

    auto highest = find_segment_file_with_highest_position((*index)[RECORDING_ID]);
    ASSERT_TRUE(highest) << highest.error().message.view();
    EXPECT_EQ(segment_file_name(RECORDING_ID, SEGMENT_LENGTH), *highest);
}

}  // namespace
