// A4a-3 criteria: the three validators the segment requests gate on
// (`ArchiveConductor.java:2244-2381`, 1.54.0 = 454da1e4e6). The C suite only
// reaches the happy paths, so the messages and codes are pinned here.

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "archive/server/segment_validation.h"

namespace
{

using deepmsg::archive::server::RecordingSummary;
using deepmsg::archive::server::ReplayInProgressInfo;
using deepmsg::archive::server::validate_detach;
using deepmsg::archive::server::validate_migrate_seam;
using deepmsg::archive::server::validate_migrate_source;
using deepmsg::archive::server::validate_migrate_stream_parameters;
using deepmsg::archive::server::validate_purge;
using deepmsg::archive::server::validate_truncate;

constexpr std::int32_t TERM_LENGTH = 64 * 1024;
constexpr std::int32_t SEGMENT_LENGTH = 128 * 1024;

[[nodiscard]] RecordingSummary summary(std::int64_t start_position, std::int64_t stop_position)
{
    RecordingSummary summary;
    summary.recording_id = 7;
    summary.start_position = start_position;
    summary.stop_position = stop_position;
    summary.initial_term_id = 1;
    summary.segment_file_length = SEGMENT_LENGTH;
    summary.term_buffer_length = TERM_LENGTH;
    summary.mtu_length = 1024;
    summary.stream_id = 11;
    summary.session_id = 5;
    return summary;
}

TEST(SegmentValidationTest, truncateRefusesAnActiveRecording)
{
    const auto invalid = validate_truncate(summary(0, -1), 0);
    ASSERT_TRUE(invalid.has_value());
    EXPECT_EQ(2, invalid->code);
    EXPECT_EQ("cannot truncate active recording", invalid->message);
}

TEST(SegmentValidationTest, truncateChecksTheRangeAndTheAlignment)
{
    const auto ok = validate_truncate(summary(0, 262144), 131072);
    EXPECT_FALSE(ok.has_value());

    const auto unaligned = validate_truncate(summary(0, 262144), 100);
    ASSERT_TRUE(unaligned.has_value());
    EXPECT_EQ(16, unaligned->code);
    EXPECT_EQ("invalid position 100: start=0 stop=262144 alignment=32", unaligned->message);

    const auto after_stop = validate_truncate(summary(0, 262144), 262176);
    ASSERT_TRUE(after_stop.has_value());
    EXPECT_EQ(16, after_stop->code);

    const auto before_start = validate_truncate(summary(65536, 262144), 0);
    ASSERT_TRUE(before_start.has_value());
    EXPECT_EQ("invalid position 0: start=65536 stop=262144 alignment=32", before_start->message);
}

TEST(SegmentValidationTest, purgeNamesTheActiveReplayBeforeTheActiveRecording)
{
    const auto replay = validate_purge(summary(0, 262144), true);
    ASSERT_TRUE(replay.has_value());
    EXPECT_EQ(2, replay->code);
    EXPECT_EQ("cannot purge recording with active replay 7", replay->message);

    const auto active = validate_purge(summary(0, -1), false);
    ASSERT_TRUE(active.has_value());
    EXPECT_EQ(2, active->code);
    EXPECT_EQ("cannot purge active recording 7", active->message);

    EXPECT_FALSE(validate_purge(summary(0, 262144), false).has_value());
}

TEST(SegmentValidationTest, detachRequiresASegmentBasePosition)
{
    const auto invalid = validate_detach(summary(0, 262144), 1000, -1, {});
    ASSERT_TRUE(invalid.has_value());
    EXPECT_EQ(0, invalid->code);
    EXPECT_EQ("invalid segment start: newStartPosition=1000", invalid->message);
}

TEST(SegmentValidationTest, detachRefusesTheFirstSegmentAndAnythingAboveTheEnd)
{
    const auto first = validate_detach(summary(0, 262144), 0, -1, {});
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ("invalid detach: newStartPosition=0 lowerBound=131072", first->message);

    const auto above = validate_detach(summary(0, 262144), 393216, -1, {});
    ASSERT_TRUE(above.has_value());
    EXPECT_EQ("invalid detach: in use, newStartPosition=393216 upperBound=262144", above->message);

    EXPECT_FALSE(validate_detach(summary(0, 262144), 262144, -1, {}).has_value());
}

TEST(SegmentValidationTest, detachUsesTheRecordedPositionForAnActiveRecording)
{
    const auto invalid = validate_detach(summary(0, -1), 131072, 1000, {});
    ASSERT_TRUE(invalid.has_value());
    EXPECT_EQ("invalid detach: in use, newStartPosition=131072 upperBound=0", invalid->message);

    EXPECT_FALSE(validate_detach(summary(0, -1), 131072, 300000, {}).has_value());
}

TEST(SegmentValidationTest, detachReportsTheNearestReplayInProgress)
{
    ReplayInProgressInfo far;
    far.recording_id = 7;
    far.segment_file_base_position = 262144;
    far.session_id = 0x100000001LL;
    far.stream_id = 33;
    far.channel = "aeron:udp?endpoint=localhost:44444";
    far.state = "REPLAY";

    ReplayInProgressInfo near = far;
    near.segment_file_base_position = 131072;
    near.session_id = 9;

    ReplayInProgressInfo other_recording = far;
    other_recording.recording_id = 8;
    other_recording.segment_file_base_position = 65536;

    const std::vector<ReplayInProgressInfo> replays{far, near, other_recording};
    const auto invalid = validate_detach(summary(0, 262144), 262144, -1, replays);
    ASSERT_TRUE(invalid.has_value());
    EXPECT_EQ(0, invalid->code);
    EXPECT_EQ(
        "invalid detach: replay in progress -  state=REPLAY newStartPosition=262144 upperBound=131072 sessionId=9 "
        "streamId=33 channel=aeron:udp?endpoint=localhost:44444",
        invalid->message);

    // A replay at the requested position does not block it (strict `>`).
    EXPECT_FALSE(validate_detach(summary(0, 262144), 131072, -1, replays).has_value());
}

TEST(SegmentValidationTest, migrateRefusesAnActiveSource)
{
    const auto invalid = validate_migrate_source(summary(0, -1));
    ASSERT_TRUE(invalid.has_value());
    EXPECT_EQ(0, invalid->code);
    EXPECT_EQ("recording 7 is still active", invalid->message);

    EXPECT_FALSE(validate_migrate_source(summary(0, 262144)).has_value());
}

TEST(SegmentValidationTest, migrateComparesTheFiveStreamParametersInOrder)
{
    const RecordingSummary src = summary(0, 262144);

    auto dst = src;
    dst.segment_file_length = SEGMENT_LENGTH / 2;
    EXPECT_EQ("invalid migrate: srcSegmentFileLength=131072 dstSegmentFileLength=65536",
              validate_migrate_stream_parameters(src, dst)->message);

    dst = src;
    dst.term_buffer_length = TERM_LENGTH / 2;
    EXPECT_EQ("invalid migrate: srcTermBufferLength=65536 dstTermBufferLength=32768",
              validate_migrate_stream_parameters(src, dst)->message);

    dst = src;
    dst.initial_term_id = 2;
    EXPECT_EQ("invalid migrate: srcInitialTermId=1 dstInitialTermId=2",
              validate_migrate_stream_parameters(src, dst)->message);

    dst = src;
    dst.stream_id = 12;
    EXPECT_EQ("invalid migrate: srcStreamId=11 dstStreamId=12", validate_migrate_stream_parameters(src, dst)->message);

    dst = src;
    dst.mtu_length = 2048;
    EXPECT_EQ("invalid migrate: srcMtuLength=1024 dstMtuLength=2048",
              validate_migrate_stream_parameters(src, dst)->message);

    EXPECT_FALSE(validate_migrate_stream_parameters(src, src).has_value());
}

TEST(SegmentValidationTest, migrateRequiresTheJoinPositionOnASegmentBoundary)
{
    const RecordingSummary recording = summary(0, 262144);

    const auto invalid = validate_migrate_seam("src", recording, 1000);
    ASSERT_TRUE(invalid.has_value());
    EXPECT_EQ(
        "invalid migrate: join position is not on segment boundary of src recording seamPosition=1000 "
        "startPosition=0 stopPosition=262144 termBufferLength=65536 segmentFileLength=131072",
        invalid->message);

    EXPECT_FALSE(validate_migrate_seam("src", recording, 131072).has_value());
}

}  // namespace
