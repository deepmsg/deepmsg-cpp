// A2b-1 criterion: the nine recording names and the reference's startup checks
// (`Archive.java:1274`, `:1548-1556`, `:1306-1311`, 1.54.0 = 454da1e4e6).

#include <gtest/gtest.h>

#include <string>

#include "archive/server/archive_config.h"

namespace
{

using axc::core::PropertySource;
using deepmsg::archive::server::parse_recording_settings;
using deepmsg::archive::server::parse_replay_settings;
using deepmsg::archive::server::parse_size;
using deepmsg::archive::server::io::Backend;
using deepmsg::archive::server::io::Fadvise;

TEST(ArchiveConfigTest, defaultsMatchTheReference)
{
    PropertySource source;
    const auto settings = parse_recording_settings(source);
    ASSERT_TRUE(settings.has_value()) << settings.error().message.view();

    EXPECT_EQ(20, settings->max_concurrent_recordings);
    EXPECT_EQ(128 * 1024 * 1024, settings->segment_file_length);
    EXPECT_EQ(1024 * 1024, settings->file_io_max_length);
    EXPECT_EQ(128 * 1024 * 1024, settings->low_storage_space_threshold);
    EXPECT_EQ(0, settings->file_sync_level);
    EXPECT_FALSE(settings->recording_events_enabled);
    EXPECT_TRUE(settings->recording_events_channel.empty());
    EXPECT_EQ(30, settings->recording_events_stream_id);
    EXPECT_FALSE(settings->file_io_fallocate);
}

TEST(ArchiveConfigTest, sizeSuffixesFollowParseSize)
{
    ASSERT_EQ(static_cast<std::int64_t>(65536), *parse_size("64k"));
    ASSERT_EQ(static_cast<std::int64_t>(65536), *parse_size("64K"));
    ASSERT_EQ(static_cast<std::int64_t>(128 * 1024 * 1024), *parse_size("128m"));
    ASSERT_EQ(2LL * 1024 * 1024 * 1024, *parse_size("2g"));
    ASSERT_EQ(static_cast<std::int64_t>(1024), *parse_size("1024"));
    ASSERT_EQ(static_cast<std::int64_t>(1024), *parse_size(" 1024 "));

    EXPECT_FALSE(parse_size("").has_value());
    EXPECT_FALSE(parse_size("12x").has_value());
    EXPECT_FALSE(parse_size("k").has_value());
}

TEST(ArchiveConfigTest, configuredValuesAreRead)
{
    PropertySource source;
    source.set("aeron.archive.max.concurrent.recordings", "7");
    source.set("aeron.archive.segment.file.length", "65536");
    source.set("aeron.archive.file.io.max.length", "64k");
    source.set("aeron.archive.low.storage.space.threshold", "1024");
    source.set("aeron.archive.file.sync.level", "2");
    source.set("aeron.archive.recording.events.enabled", "true");
    source.set("aeron.archive.recording.events.channel", "aeron:ipc");
    source.set("aeron.archive.recording.events.stream.id", "31");

    const auto settings = parse_recording_settings(source);
    ASSERT_TRUE(settings.has_value()) << settings.error().message.view();

    EXPECT_EQ(7, settings->max_concurrent_recordings);
    EXPECT_EQ(65536, settings->segment_file_length);
    EXPECT_EQ(65536, settings->file_io_max_length);
    EXPECT_EQ(1024, settings->low_storage_space_threshold);
    EXPECT_EQ(2, settings->file_sync_level);
    EXPECT_TRUE(settings->recording_events_enabled);
    EXPECT_EQ("aeron:ipc", settings->recording_events_channel);
    EXPECT_EQ(31, settings->recording_events_stream_id);
}

TEST(ArchiveConfigTest, segmentFileLengthMustBeAPowerOfTwo)
{
    PropertySource source;
    source.set("aeron.archive.segment.file.length", "98304");  // 96k

    const auto settings = parse_recording_settings(source);
    ASSERT_FALSE(settings.has_value());
    EXPECT_EQ("segment file length not a power of 2: 98304", settings.error().message.view());
}

TEST(ArchiveConfigTest, segmentFileLengthMustBeInTheTermRange)
{
    PropertySource source;
    source.set("aeron.archive.segment.file.length", "32768");  // below 64 KiB

    const auto settings = parse_recording_settings(source);
    ASSERT_FALSE(settings.has_value());
    EXPECT_EQ("segment file length not in valid range: 32768", settings.error().message.view());
}

TEST(ArchiveConfigTest, fileIoMaxLengthMustBeAPowerOfTwoAtLeastATerm)
{
    PropertySource source;
    source.set("aeron.archive.file.io.max.length", "1000");

    const auto settings = parse_recording_settings(source);
    ASSERT_FALSE(settings.has_value());
    EXPECT_EQ("invalid fileIoMaxLength=1000", settings.error().message.view());
}

TEST(ArchiveConfigTest, enabledRecordingEventsRequireAChannel)
{
    PropertySource source;
    source.set("aeron.archive.recording.events.enabled", "true");

    const auto settings = parse_recording_settings(source);
    ASSERT_FALSE(settings.has_value());
    EXPECT_EQ(
        "Archive.Context.recordingEventsChannel must be set if "
        "Archive.Context.recordingEventsEnabled is true",
        settings.error().message.view());
}

// A3b-2: the three replay names (`Archive.java:474-481`, `:555-564`, `:676`).

TEST(ArchiveConfigTest, replayDefaultsMatchTheReference)
{
    PropertySource source;
    const auto settings = parse_replay_settings(source);
    ASSERT_TRUE(settings.has_value()) << settings.error().message.view();

    EXPECT_EQ(20, settings->max_concurrent_replays);
    EXPECT_EQ(5'000'000'000, settings->replay_linger_timeout_ns);
    EXPECT_TRUE(settings->replay_checksum_name.empty());
}

TEST(ArchiveConfigTest, replayValuesAreRead)
{
    PropertySource source;
    source.set("aeron.archive.max.concurrent.replays", "100");
    source.set("aeron.archive.replay.linger.timeout", "3s");
    source.set("aeron.archive.replay.checksum", "crc32");

    const auto settings = parse_replay_settings(source);
    ASSERT_TRUE(settings.has_value()) << settings.error().message.view();

    EXPECT_EQ(100, settings->max_concurrent_replays);
    EXPECT_EQ(3'000'000'000, settings->replay_linger_timeout_ns);
    EXPECT_EQ("crc32", settings->replay_checksum_name);
}

TEST(ArchiveConfigTest, replayLingerRejectsAnInvalidDuration)
{
    PropertySource source;
    source.set("aeron.archive.replay.linger.timeout", "5x");

    const auto settings = parse_replay_settings(source);
    EXPECT_FALSE(settings.has_value());
}

TEST(ArchiveConfigTest, theCppOnlyFallocateSwitchIsRead)
{
    {
        PropertySource source;
        source.set("deepmsg.archive.file.io.fallocate", "true");
        const auto settings = parse_recording_settings(source);
        ASSERT_TRUE(settings.has_value()) << settings.error().message.view();
        EXPECT_TRUE(settings->file_io_fallocate);
    }
    {
        PropertySource source;
        source.set("deepmsg.archive.file.io.fallocate", "false");
        const auto settings = parse_recording_settings(source);
        ASSERT_TRUE(settings.has_value()) << settings.error().message.view();
        EXPECT_FALSE(settings->file_io_fallocate);
    }
}

// A6d-3: the two C++-only read-backend switches (`docs/compat.md`).

TEST(ArchiveConfigTest, replayIoDefaultsKeepTheReferenceBehaviour)
{
    PropertySource source;
    const auto settings = parse_replay_settings(source);
    ASSERT_TRUE(settings.has_value()) << settings.error().message.view();

    EXPECT_EQ(Backend::pread, settings->io_backend);
    EXPECT_EQ(Fadvise::none, settings->io_fadvise);
}

TEST(ArchiveConfigTest, replayIoSwitchesAreRead)
{
    PropertySource source;
    source.set("deepmsg.archive.replay.io.backend", "mmap");
    source.set("deepmsg.archive.replay.io.fadvise", "willneed");

    const auto settings = parse_replay_settings(source);
    ASSERT_TRUE(settings.has_value()) << settings.error().message.view();

    EXPECT_EQ(Backend::mmap, settings->io_backend);
    EXPECT_EQ(Fadvise::willneed, settings->io_fadvise);
}

TEST(ArchiveConfigTest, replayIoRejectsAnUnknownBackend)
{
    PropertySource source;
    source.set("deepmsg.archive.replay.io.backend", "io_uring");

    const auto settings = parse_replay_settings(source);
    ASSERT_FALSE(settings.has_value());
    EXPECT_EQ("invalid deepmsg.archive.replay.io.backend=io_uring (pread, mmap)", settings.error().message.view());
}

TEST(ArchiveConfigTest, replayIoRejectsAnUnknownFadvise)
{
    PropertySource source;
    source.set("deepmsg.archive.replay.io.fadvise", "dontneed");

    const auto settings = parse_replay_settings(source);
    ASSERT_FALSE(settings.has_value());
    EXPECT_EQ("invalid deepmsg.archive.replay.io.fadvise=dontneed (none, willneed)", settings.error().message.view());
}

}  // namespace
