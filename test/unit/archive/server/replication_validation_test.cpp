// A4b-3 criteria: the refusals `ArchiveConductor.replicate` sends before a
// session exists (`ArchiveConductor.java:1394-1428`, 1.54.0 = 454da1e4e6).
// The C suite only sends valid requests, so the messages and codes are pinned
// here.

#include <gtest/gtest.h>

#include <string>

#include "archive/server/replication_validation.h"

namespace
{

using deepmsg::archive::server::validate_replication_channel;
using deepmsg::archive::server::validate_replication_channel_configured;
using deepmsg::archive::server::validate_replication_destination;

TEST(ReplicationValidationTest, aResponseChannelCannotCarryALiveDestinationOrTags)
{
    const auto live = validate_replication_channel(true, true, false);
    ASSERT_TRUE(live.has_value());
    EXPECT_EQ(0, live->code);
    EXPECT_EQ("response channels can't be used with live destinations", live->message);

    const auto tagged = validate_replication_channel(true, false, true);
    ASSERT_TRUE(tagged.has_value());
    EXPECT_EQ("response channels can't be used with tagged replication", tagged->message);

    EXPECT_FALSE(validate_replication_channel(true, false, false).has_value());
    EXPECT_FALSE(validate_replication_channel(false, true, true).has_value());
}

TEST(ReplicationValidationTest, anUnknownDestinationIsRefused)
{
    const auto unknown = validate_replication_destination(17, false, false);
    ASSERT_TRUE(unknown.has_value());
    EXPECT_EQ(5, unknown->code);
    EXPECT_EQ("unknown destination recording id 17", unknown->message);
}

TEST(ReplicationValidationTest, anActiveDestinationIsRefused)
{
    const auto active = validate_replication_destination(17, true, true);
    ASSERT_TRUE(active.has_value());
    EXPECT_EQ(2, active->code);
    EXPECT_EQ("cannot replicate to active recording 17", active->message);

    EXPECT_FALSE(validate_replication_destination(17, true, false).has_value());
    // `Aeron.NULL_VALUE` means "create the row" and is not looked up.
    EXPECT_FALSE(validate_replication_destination(-1, false, false).has_value());
}

TEST(ReplicationValidationTest, aMissingReplicationChannelIsRefused)
{
    const auto missing = validate_replication_channel_configured("");
    ASSERT_TRUE(missing.has_value());
    EXPECT_EQ(0, missing->code);
    EXPECT_EQ("Archive.Context.replicationChannel must be set", missing->message);

    EXPECT_FALSE(validate_replication_channel_configured("aeron:udp?endpoint=localhost:8012").has_value());
}

}  // namespace
