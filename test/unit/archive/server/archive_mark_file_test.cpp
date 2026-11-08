// Reference: `aeron-archive/src/test/java/io/aeron/archive/ArchiveMarkFileTest.java`
// (Aeron 1.54.0, 454da1e4e6). Test names are the reference method names.
//
// Not ported, and why (recorded in `records/a1b.md`): the cases that build an
// `Archive.Context`, launch a media driver, or assert on the `archive-mark.lnk`
// link file need A2a's `Archive` context and lifecycle; the two
// `shouldBeAbleToReadOldMarkFileV0/V1` cases write byte-exact legacy files that
// are covered by the layout test below instead.

#include <gtest/gtest.h>

#include <fcntl.h>
#include <unistd.h>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "archive/server/archive_mark_file.h"
#include "archive/server/codecs.h"

namespace
{

using deepmsg::archive::server::ArchiveMarkFile;

constexpr std::int32_t ERROR_BUFFER_LENGTH = 4096;
constexpr std::int32_t TOTAL_FILE_LENGTH = ArchiveMarkFile::HEADER_LENGTH + ERROR_BUFFER_LENGTH;
constexpr std::int32_t NULL_VALUE = -1;

class ArchiveMarkFileTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::array<char, 64> pattern{};
        std::snprintf(pattern.data(), pattern.size(), "/tmp/axc-markfile-XXXXXX");
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        directory_ = created;
        clock_.now = 1234;
    }

    void TearDown() override
    {
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
    }

    [[nodiscard]] ArchiveMarkFile::Options options() const
    {
        ArchiveMarkFile::Options options;
        options.directory = directory_;
        options.total_file_length = TOTAL_FILE_LENGTH;
        options.error_buffer_length = ERROR_BUFFER_LENGTH;
        options.epoch_clock = &clock_;
        return options;
    }

    [[nodiscard]] static ArchiveMarkFile::HeaderValues values()
    {
        ArchiveMarkFile::HeaderValues values;
        values.start_timestamp = 77;
        values.control_stream_id = 10;
        values.local_control_stream_id = 11;
        values.events_stream_id = 12;
        values.archive_id = -462348234343LL;
        values.control_channel = "aeron:udp?endpoint=localhost:8010";
        values.local_control_channel = "aeron:ipc";
        values.events_channel = "aeron:udp?endpoint=localhost:8011";
        values.aeron_directory = "/dev/shm/aeron-test";
        return values;
    }

    class TestClock final : public deepmsg::archive::server::EpochClockSource
    {
    public:
        std::int64_t now{0};

        [[nodiscard]] std::int64_t time_ms() const noexcept override
        {
            return now;
        }
    };

    std::string directory_;
    TestClock clock_;
};

TEST_F(ArchiveMarkFileTest, headersRoundTrip)
{
    auto mark_file = ArchiveMarkFile::create_or_open(options(), values());
    ASSERT_TRUE(mark_file) << mark_file.error().message.view();
    EXPECT_FALSE(mark_file->is_closed());

    const ArchiveMarkFile::HeaderValues expected = values();
    EXPECT_EQ(ArchiveMarkFile::HEADER_LENGTH, mark_file->header_length());
    EXPECT_EQ(ERROR_BUFFER_LENGTH, mark_file->error_buffer_length());
    EXPECT_EQ(expected.start_timestamp, mark_file->start_timestamp());
    EXPECT_EQ(expected.control_stream_id, mark_file->control_stream_id());
    EXPECT_EQ(expected.local_control_stream_id, mark_file->local_control_stream_id());
    EXPECT_EQ(expected.events_stream_id, mark_file->events_stream_id());
    EXPECT_EQ(expected.archive_id, mark_file->archive_id());
    EXPECT_EQ(expected.control_channel, mark_file->control_channel());
    EXPECT_EQ(expected.local_control_channel, mark_file->local_control_channel());
    EXPECT_EQ(expected.events_channel, mark_file->events_channel());
    EXPECT_EQ(expected.aeron_directory, mark_file->aeron_directory());
    EXPECT_EQ(ArchiveMarkFile::HEADER_OFFSET, mark_file->header_offset());
    EXPECT_EQ(static_cast<std::int32_t>(::getpid()), mark_file->pid());
    EXPECT_EQ(static_cast<std::size_t>(ERROR_BUFFER_LENGTH), mark_file->error_buffer().size());
}

TEST_F(ArchiveMarkFileTest, activityTimestamp)
{
    auto mark_file = ArchiveMarkFile::create_or_open(options(), values());
    ASSERT_TRUE(mark_file) << mark_file.error().message.view();

    constexpr std::int64_t activity_timestamp = 7383439454305LL;
    mark_file->update_activity_timestamp(activity_timestamp);
    EXPECT_EQ(activity_timestamp, mark_file->activity_timestamp_volatile());

    mark_file->close();
    EXPECT_TRUE(mark_file->is_closed());

    mark_file->update_activity_timestamp(1111);
    EXPECT_EQ(NULL_VALUE, mark_file->activity_timestamp_volatile());
}

TEST_F(ArchiveMarkFileTest, signalReady)
{
    ArchiveMarkFile::HeaderValues header_values = values();
    header_values.archive_id = 0;

    auto mark_file = ArchiveMarkFile::create_or_open(options(), header_values);
    ASSERT_TRUE(mark_file) << mark_file.error().message.view();
    EXPECT_EQ(0, mark_file->version());

    mark_file->signal_ready(5555);
    EXPECT_EQ(ArchiveMarkFile::SEMANTIC_VERSION, mark_file->version());
    EXPECT_EQ(5555, mark_file->activity_timestamp_volatile());

    mark_file->close();
    mark_file->signal_ready(7777);
    EXPECT_TRUE(mark_file->is_closed());
}

TEST_F(ArchiveMarkFileTest, signalTerminatedWritesTheNullActivityTimestamp)
{
    auto mark_file = ArchiveMarkFile::create_or_open(options(), values());
    ASSERT_TRUE(mark_file) << mark_file.error().message.view();

    mark_file->signal_terminated();
    EXPECT_EQ(NULL_VALUE, mark_file->activity_timestamp_volatile());
    EXPECT_EQ(ArchiveMarkFile::SEMANTIC_VERSION, mark_file->version());
}

TEST_F(ArchiveMarkFileTest, shouldUnmapMarkFileBufferUponClose)
{
    auto mark_file = ArchiveMarkFile::create_or_open(options(), values());
    ASSERT_TRUE(mark_file) << mark_file.error().message.view();
    EXPECT_FALSE(mark_file->buffer().empty());

    mark_file->close();
    EXPECT_TRUE(mark_file->is_closed());
    EXPECT_TRUE(mark_file->buffer().empty());
    EXPECT_TRUE(mark_file->error_buffer().empty());

    mark_file->close();
    EXPECT_TRUE(mark_file->is_closed());
}

TEST_F(ArchiveMarkFileTest, createOrOpenKeepsAnExistingMarkFile)
{
    {
        ArchiveMarkFile::HeaderValues header_values = values();
        header_values.archive_id = 42;
        auto created = ArchiveMarkFile::create_or_open(options(), header_values);
        ASSERT_TRUE(created) << created.error().message.view();
        created->signal_ready(999);
    }

    auto reopened = ArchiveMarkFile::create_or_open(options(), values());
    ASSERT_TRUE(reopened) << reopened.error().message.view();
    // `create_or_open` re-encodes the header (`ArchiveMarkFile.java:107`), so the
    // passed values win; the activity timestamp is not part of the encoding and
    // survives.
    EXPECT_EQ(values().archive_id, reopened->archive_id());
    EXPECT_EQ(999, reopened->activity_timestamp_volatile());
}

TEST_F(ArchiveMarkFileTest, rejectsAnIncompatibleMajorVersion)
{
    {
        auto created = ArchiveMarkFile::create_or_open(options(), values());
        ASSERT_TRUE(created) << created.error().message.view();
    }

    const std::string path = directory_ + "/" + ArchiveMarkFile::FILENAME;
    const int fd = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    const std::int32_t bad_version = (4 << 16) | (1 << 8);
    ASSERT_EQ(static_cast<ssize_t>(sizeof(bad_version)),
              ::pwrite(fd, &bad_version, sizeof(bad_version),
                       static_cast<off_t>(
                           ArchiveMarkFile::HEADER_OFFSET +
                           deepmsg::archive::server::archive_mark_codecs::MarkFileHeader::versionEncodingOffset())));
    ::close(fd);

    auto opened = ArchiveMarkFile::open_existing(options());
    ASSERT_FALSE(opened);
    EXPECT_EQ("mark file (" + directory_ + "/" + ArchiveMarkFile::FILENAME +
                  ") major version 4 does not match software: " + std::to_string(ArchiveMarkFile::MAJOR_VERSION),
              opened.error().message.view());
}

TEST_F(ArchiveMarkFileTest, readsALegacyMarkFileWithTheBlockAtOffsetZero)
{
    // A pre-message-header layout: the `MarkFileHeader` block starts at 0.
    std::vector<std::uint8_t> bytes(static_cast<std::size_t>(TOTAL_FILE_LENGTH), 0);
    deepmsg::archive::server::archive_mark_codecs::MarkFileHeader encoder;
    encoder.wrapForEncode(reinterpret_cast<char*>(bytes.data()), 0, bytes.size());
    encoder.pid(4242);
    encoder.startTimestamp(77);
    encoder.headerLength(ArchiveMarkFile::HEADER_LENGTH);
    encoder.errorBufferLength(ERROR_BUFFER_LENGTH);
    encoder.archiveId(7);

    const std::string path = directory_ + "/" + ArchiveMarkFile::FILENAME;
    std::ofstream output{path, std::ios::binary};
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    output.close();

    // A legacy file has no version yet; the reference's read path accepts it
    // with a permissive check (`ArchiveMarkFile.java:249-258`).
    auto opened = ArchiveMarkFile::open_existing(options(), [](std::int32_t) {});
    ASSERT_TRUE(opened) << opened.error().message.view();
    EXPECT_EQ(0, opened->header_offset());
    EXPECT_EQ(4242, opened->pid());
    EXPECT_EQ(77, opened->start_timestamp());
    EXPECT_EQ(ArchiveMarkFile::HEADER_LENGTH, opened->header_length());
    // `ArchiveMarkFile.java:276-279` decodes a legacy file with acting version 1,
    // so fields added later read as their null value.
    EXPECT_EQ(NULL_VALUE, opened->archive_id());
}

}  // namespace
