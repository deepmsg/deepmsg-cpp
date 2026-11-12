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
#include <iterator>
#include <string>
#include <vector>

#include "archive/server/archive_mark_file.h"
#include "archive/server/codecs.h"
#include "core/io/mark_file.h"

namespace
{

using axc::core::MarkFile;
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
    const std::int64_t stale = clock_.now - ArchiveMarkFile::LIVENESS_TIMEOUT_MS - 1000;
    {
        ArchiveMarkFile::HeaderValues header_values = values();
        header_values.archive_id = 42;
        auto created = ArchiveMarkFile::create_or_open(options(), header_values);
        ASSERT_TRUE(created) << created.error().message.view();
        created->signal_ready(stale);
    }

    auto reopened = ArchiveMarkFile::create_or_open(options(), values());
    ASSERT_TRUE(reopened) << reopened.error().message.view();
    // `create_or_open` re-encodes the header (`ArchiveMarkFile.java:107`), so the
    // passed values win; the activity timestamp is not part of the encoding and
    // is claimed for activation (`ACTIVATION_IN_PROGRESS_TIMESTAMP`).
    EXPECT_EQ(values().archive_id, reopened->archive_id());
    EXPECT_EQ(MarkFile::ACTIVATION_IN_PROGRESS_TIMESTAMP, reopened->activity_timestamp_volatile());
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

/// Reference: `ArchiveMarkFileTest.shouldThrowIllegalStateExceptionIfThereIsAnActiveMarkFile`
/// (`ArchiveContextTest` variant) — the `MarkFile` liveness check, without the
/// polling of `mapExistingMarkFile`.
TEST_F(ArchiveMarkFileTest, anActiveMarkFileIsRefused)
{
    ArchiveMarkFile::HeaderValues old_values = values();
    old_values.archive_id = 42;
    {
        auto created = ArchiveMarkFile::create_or_open(options(), old_values);
        ASSERT_TRUE(created) << created.error().message.view();
        created->signal_ready(clock_.now);
    }

    auto refused = ArchiveMarkFile::create_or_open(options(), values());
    ASSERT_FALSE(refused);
    const std::string message{refused.error().message.view()};
    EXPECT_NE(std::string::npos, message.find("active mark file detected: ")) << message;
    EXPECT_NE(std::string::npos, message.find(directory_ + "/" + ArchiveMarkFile::FILENAME)) << message;

    // The refused open must not have overwritten the old header.
    auto reopened = ArchiveMarkFile::open_existing(options());
    ASSERT_TRUE(reopened) << reopened.error().message.view();
    EXPECT_EQ(42, reopened->archive_id());
    EXPECT_EQ(clock_.now, reopened->activity_timestamp_volatile());
}

TEST_F(ArchiveMarkFileTest, aStaleTimestampIsClaimedForActivation)
{
    const std::int64_t stale = clock_.now - ArchiveMarkFile::LIVENESS_TIMEOUT_MS - 1000;
    {
        auto created = ArchiveMarkFile::create_or_open(options(), values());
        ASSERT_TRUE(created) << created.error().message.view();
        created->signal_ready(stale);
    }

    auto claimed = ArchiveMarkFile::create_or_open(options(), values());
    ASSERT_TRUE(claimed) << claimed.error().message.view();
    // The CAS target, i.e. "activation in progress" (`MarkFile.ACTIVATION_IN_PROGRESS_TIMESTAMP`).
    EXPECT_EQ(MarkFile::ACTIVATION_IN_PROGRESS_TIMESTAMP, claimed->activity_timestamp_volatile());

    claimed->signal_ready(clock_.now);
    EXPECT_EQ(clock_.now, claimed->activity_timestamp_volatile());
}

TEST_F(ArchiveMarkFileTest, aConcurrentActivationLosesTheCas)
{
    std::vector<std::uint8_t> bytes(16, 0);
    const std::int64_t stale = 42;
    std::memcpy(bytes.data() + 8, &stale, sizeof(stale));

    EXPECT_TRUE(MarkFile::try_activate(bytes, 8, stale));
    std::int64_t claimed = 0;
    std::memcpy(&claimed, bytes.data() + 8, sizeof(claimed));
    EXPECT_EQ(MarkFile::ACTIVATION_IN_PROGRESS_TIMESTAMP, claimed);

    // The same observed value no longer matches: the other process won.
    EXPECT_FALSE(MarkFile::try_activate(bytes, 8, stale));

    // The upper layer turns a lost CAS into the reference's message.
    const std::vector<std::uint8_t> zeros(static_cast<std::size_t>(TOTAL_FILE_LENGTH), 0);
    const std::string path = directory_ + "/" + ArchiveMarkFile::FILENAME;
    std::ofstream output{path, std::ios::binary};
    output.write(reinterpret_cast<const char*>(zeros.data()), static_cast<std::streamsize>(zeros.size()));
    output.close();

    auto mark_file = MarkFile::open_existing(path, ArchiveMarkFile::HEADER_OFFSET, ArchiveMarkFile::HEADER_OFFSET + 8);
    ASSERT_TRUE(mark_file) << mark_file.error().message.view();
    // Pretend we observed 0 while the field now holds 7.
    mark_file->update_activity(7);
    auto lost = mark_file->claim_activity(0, path);
    ASSERT_FALSE(lost);
    EXPECT_NE(std::string::npos, std::string{lost.error().message.view()}.find("concurrent mark file activation: "))
        << lost.error().message.view();
}

TEST_F(ArchiveMarkFileTest, anExistingErrorLogIsSavedAndCleared)
{
    constexpr std::int64_t RECORDED_AT_MS = 5'000'000;
    constexpr std::int64_t RECORDED_FROM_MS = 4'999'000;
    constexpr std::string_view description = "existing failure";

    {
        auto created = ArchiveMarkFile::create_or_open(options(), values());
        ASSERT_TRUE(created) << created.error().message.view();

        // The distinct-error-log layout (`aeron_distinct_error_log.h:28-43`):
        // length, observations, last/first timestamps, then the description.
        const std::span<std::uint8_t> error_buffer = created->error_buffer();
        ASSERT_EQ(ERROR_BUFFER_LENGTH, static_cast<std::int32_t>(error_buffer.size()));
        const auto length = static_cast<std::int32_t>(24 + description.size());
        const std::int32_t observations = 1;
        std::memcpy(error_buffer.data(), &length, sizeof(length));
        std::memcpy(error_buffer.data() + 4, &observations, sizeof(observations));
        std::memcpy(error_buffer.data() + 8, &RECORDED_AT_MS, sizeof(RECORDED_AT_MS));
        std::memcpy(error_buffer.data() + 16, &RECORDED_FROM_MS, sizeof(RECORDED_FROM_MS));
        std::memcpy(error_buffer.data() + 24, description.data(), description.size());

        created->signal_ready(clock_.now - ArchiveMarkFile::LIVENESS_TIMEOUT_MS - 1000);
    }

    auto reopened = ArchiveMarkFile::create_or_open(options(), values());
    ASSERT_TRUE(reopened) << reopened.error().message.view();

    std::string error_log;
    for (const auto& entry : std::filesystem::directory_iterator{directory_})
    {
        const std::string name = entry.path().filename().string();
        if (name.rfind("archive-", 0) == 0 && name.ends_with("-error.log"))
        {
            std::ifstream input{entry.path(), std::ios::binary};
            error_log.assign(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>());
        }
    }
    ASSERT_FALSE(error_log.empty());
    EXPECT_NE(std::string::npos, error_log.find("1 observations from ")) << error_log;
    EXPECT_NE(std::string::npos, error_log.find("existing failure")) << error_log;

    const std::span<std::uint8_t> cleared = reopened->error_buffer();
    ASSERT_EQ(ERROR_BUFFER_LENGTH, static_cast<std::int32_t>(cleared.size()));
    for (const std::uint8_t byte : cleared)
    {
        ASSERT_EQ(0, byte);
    }
}

/// Reference: `ArchiveMarkFileTest.shouldCreateLinkFileToMarkFileDirAndRemoveLinkFileIfArchiveDirMatchesMarkFileDir`.
TEST_F(ArchiveMarkFileTest, theLinkFileNamesTheMarkDirectory)
{
    const std::string archive_dir = directory_ + "/archive-dir";
    const std::string mark_dir = directory_ + "/mark/file/dir";
    ASSERT_TRUE(std::filesystem::create_directories(archive_dir));
    ASSERT_TRUE(std::filesystem::create_directories(mark_dir));

    const std::string link_path = archive_dir + "/" + ArchiveMarkFile::LINK_FILENAME;
    auto linked = axc::core::ensure_mark_file_link(archive_dir, mark_dir + "/" + ArchiveMarkFile::FILENAME,
                                                   ArchiveMarkFile::LINK_FILENAME);
    ASSERT_TRUE(linked) << linked.error().message.view();

    std::ifstream input{link_path};
    ASSERT_TRUE(input.good());
    const std::string content{std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
    EXPECT_EQ(std::filesystem::canonical(mark_dir).string(), content);

    // The tool resolves the link content as a directory and appends the filename.
    EXPECT_TRUE(std::filesystem::is_directory(content));
    EXPECT_FALSE(std::filesystem::is_regular_file(content));

    // Same directory on both sides: the link is removed, not written.
    auto removed = axc::core::ensure_mark_file_link(archive_dir, archive_dir + "/" + ArchiveMarkFile::FILENAME,
                                                    ArchiveMarkFile::LINK_FILENAME);
    ASSERT_TRUE(removed) << removed.error().message.view();
    EXPECT_FALSE(std::filesystem::exists(link_path));
}

/// Reference: `ArchiveMarkFileTest.shouldResetActivityTimestampIfMarkFileCreationFailsPartway`.
/// A modern file too short for its header passes the liveness claim and then
/// fails to decode, which must put the activity timestamp back to `NULL_VALUE`.
TEST_F(ArchiveMarkFileTest, aFailedStartResetsTheActivity)
{
    // A real epoch clock: 0 means "created but not populated", which the
    // reference treats as stale (`now - 0` is far beyond the timeout).
    clock_.now = 100'000;
    std::vector<std::uint8_t> bytes(24, 0);
    deepmsg::archive::server::archive_mark_codecs::MessageHeader message_header;
    message_header
        .wrap(reinterpret_cast<char*>(bytes.data()), 0,
              deepmsg::archive::server::archive_mark_codecs::MessageHeader::sbeSchemaVersion(), bytes.size())
        .blockLength(deepmsg::archive::server::archive_mark_codecs::MarkFileHeader::SBE_BLOCK_LENGTH)
        .templateId(deepmsg::archive::server::archive_mark_codecs::MarkFileHeader::SBE_TEMPLATE_ID)
        .schemaId(deepmsg::archive::server::archive_mark_codecs::MarkFileHeader::SBE_SCHEMA_ID);

    const std::int32_t version = ArchiveMarkFile::SEMANTIC_VERSION;
    std::memcpy(bytes.data() + ArchiveMarkFile::HEADER_OFFSET, &version, sizeof(version));
    // The activity field at HEADER_OFFSET + 8 stays 0, i.e. stale.

    const std::string path = directory_ + "/" + ArchiveMarkFile::FILENAME;
    std::ofstream output{path, std::ios::binary};
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    output.close();

    auto opened = ArchiveMarkFile::create_or_open(options(), values());
    ASSERT_FALSE(opened) << "expected a failed start";

    std::int64_t activity = 0;
    std::ifstream input{path, std::ios::binary};
    input.seekg(ArchiveMarkFile::HEADER_OFFSET + 8);
    input.read(reinterpret_cast<char*>(&activity), sizeof(activity));
    EXPECT_EQ(NULL_VALUE, activity);
}

/// A legacy file (block at offset 0) goes through the same liveness decision.
TEST_F(ArchiveMarkFileTest, aLegacyMarkFileWithALiveTimestampIsRefused)
{
    std::vector<std::uint8_t> bytes(1024, 0);
    const std::int32_t version = ArchiveMarkFile::SEMANTIC_VERSION;
    std::memcpy(bytes.data(), &version, sizeof(version));
    std::memcpy(bytes.data() + 8, &clock_.now, sizeof(clock_.now));

    const std::string path = directory_ + "/" + ArchiveMarkFile::FILENAME;
    std::ofstream output{path, std::ios::binary};
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    output.close();

    auto refused = ArchiveMarkFile::create_or_open(options(), values());
    ASSERT_FALSE(refused);
    EXPECT_NE(std::string::npos, std::string{refused.error().message.view()}.find("active mark file detected: "))
        << refused.error().message.view();
}

TEST_F(ArchiveMarkFileTest, aStaleLegacyMarkFileIsRebuiltInTheCurrentLayout)
{
    std::vector<std::uint8_t> bytes(1024, 0);
    const std::int32_t version = ArchiveMarkFile::SEMANTIC_VERSION;
    const std::int64_t stale = clock_.now - ArchiveMarkFile::LIVENESS_TIMEOUT_MS - 1000;
    std::memcpy(bytes.data(), &version, sizeof(version));
    std::memcpy(bytes.data() + 8, &stale, sizeof(stale));

    const std::string path = directory_ + "/" + ArchiveMarkFile::FILENAME;
    std::ofstream output{path, std::ios::binary};
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    output.close();

    auto rebuilt = ArchiveMarkFile::create_or_open(options(), values());
    ASSERT_TRUE(rebuilt) << rebuilt.error().message.view();
    EXPECT_EQ(ArchiveMarkFile::HEADER_OFFSET, rebuilt->header_offset());
    // `ArchiveMarkFile.java:182-199` zeroes the new mapping after the claim, so
    // the timestamp reads 0 (unpopulated) until `signal_ready`.
    EXPECT_EQ(0, rebuilt->activity_timestamp_volatile());
    EXPECT_EQ(ArchiveMarkFile::HEADER_LENGTH, rebuilt->header_length());
    EXPECT_EQ(static_cast<std::size_t>(TOTAL_FILE_LENGTH), std::filesystem::file_size(path));
}

}  // namespace
