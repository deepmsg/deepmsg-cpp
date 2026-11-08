// A3b-2 criterion: the replay counter ids, keys and labels the reference
// allocates (`AeronCounters.java:792-818`, `Archive.java:1674-1730`,
// `ArchiveCounters.java:52-68`, 1.54.0 = 454da1e4e6).
//
// The key is the 8-byte archive id alone; the label is
// `<name> - archiveId=<id>`.

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <string>

#include "archive/server/archive_counters.h"

namespace
{

using deepmsg::archive::server::ArchiveCounters;

TEST(ArchiveCountersTest, replayTypeIdsMatchAeronCounters)
{
    EXPECT_EQ(108, ArchiveCounters::REPLAYER_MAX_READ_TIME_TYPE_ID);
    EXPECT_EQ(109, ArchiveCounters::REPLAYER_TOTAL_READ_BYTES_TYPE_ID);
    EXPECT_EQ(110, ArchiveCounters::REPLAYER_TOTAL_READ_TIME_TYPE_ID);
    EXPECT_EQ(111, ArchiveCounters::RECORDING_SESSION_COUNT_TYPE_ID);
    EXPECT_EQ(112, ArchiveCounters::REPLAY_SESSION_COUNT_TYPE_ID);
}

TEST(ArchiveCountersTest, archiveIdKeyIsTheRawEightBytes)
{
    const auto key = ArchiveCounters::archive_id_key(42);

    std::int64_t decoded = 0;
    std::memcpy(&decoded, key.data(), sizeof(decoded));
    EXPECT_EQ(42, decoded);
}

TEST(ArchiveCountersTest, replayLabelsMatchTheReference)
{
    EXPECT_EQ("archive-replayer max read time in ns - archiveId=42",
              ArchiveCounters::make_label(ArchiveCounters::REPLAYER_MAX_READ_TIME_NAME, 42));
    EXPECT_EQ("archive-replayer total read bytes - archiveId=-7777",
              ArchiveCounters::make_label(ArchiveCounters::REPLAYER_TOTAL_READ_BYTES_NAME, -7777));
    EXPECT_EQ("archive-replayer total read time in ns - archiveId=0",
              ArchiveCounters::make_label(ArchiveCounters::REPLAYER_TOTAL_READ_TIME_NAME, 0));
    EXPECT_EQ("Archive Replay Sessions - archiveId=42",
              ArchiveCounters::make_label(ArchiveCounters::REPLAY_SESSION_COUNT_NAME, 42));
}

}  // namespace
