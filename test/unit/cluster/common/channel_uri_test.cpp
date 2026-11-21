// The response channel both sides publish to: the clients' `rejoin=false` is a
// subscription option and must not reach the publication's URI.

#include <gtest/gtest.h>

#include <string>

#include "cluster/common/channel_uri.h"

namespace
{

using deepmsg::cluster::common::publication_channel_from_response_channel;

TEST(ChannelUri, aTrailingRejoinIsRemovedWithItsSeparator)
{
    EXPECT_EQ("aeron:udp?endpoint=localhost:1234",
              publication_channel_from_response_channel("aeron:udp?endpoint=localhost:1234|rejoin=false"));
    EXPECT_EQ("aeron:udp?endpoint=localhost:1234",
              publication_channel_from_response_channel("aeron:udp?endpoint=localhost:1234|rejoin=true"));
}

TEST(ChannelUri, aLeadingRejoinLeavesTheFollowingOptions)
{
    EXPECT_EQ("aeron:udp?endpoint=localhost:1234",
              publication_channel_from_response_channel("aeron:udp?rejoin=false|endpoint=localhost:1234"));
}

TEST(ChannelUri, aMiddleRejoinLeavesTheOtherOptionsJoined)
{
    EXPECT_EQ(
        "aeron:udp?endpoint=localhost:1234|term-length=64k",
        publication_channel_from_response_channel("aeron:udp?endpoint=localhost:1234|rejoin=false|term-length=64k"));
}

TEST(ChannelUri, aChannelWithoutRejoinIsUnchanged)
{
    EXPECT_EQ("aeron:ipc", publication_channel_from_response_channel("aeron:ipc"));
    EXPECT_EQ("aeron:udp?endpoint=localhost:1234|term-length=64k",
              publication_channel_from_response_channel("aeron:udp?endpoint=localhost:1234|term-length=64k"));
}

TEST(ChannelUri, aRejoinInsideAnotherValueIsNotAParameter)
{
    EXPECT_EQ("aeron:udp?alias=xrejoin=false",
              publication_channel_from_response_channel("aeron:udp?alias=xrejoin=false"));
}

}  // namespace
