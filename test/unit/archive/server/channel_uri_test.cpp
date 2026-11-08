// A2b-1 criterion: the channel URI rewriting the conductor needs
// (`ArchiveConductor.strippedChannelBuilder` / `makeKey`, 1.54.0 = 454da1e4e6).
//
// The expected strings are the reference `ChannelUriStringBuilder.build()`
// output for the same input: the 23-parameter whitelist, canonical parameter
// order, then `newControlSession`'s `termLength` / `sparse` / `mtu`.

#include <gtest/gtest.h>

#include <string>

#include "archive/server/channel_uri.h"

namespace
{

using deepmsg::archive::server::ChannelUriOptions;
using deepmsg::archive::server::control_response_channel;
using deepmsg::archive::server::is_udp_channel;
using deepmsg::archive::server::make_key;
using deepmsg::archive::server::stripped_channel;

TEST(ChannelUriTest, ipcChannelStaysBare)
{
    const auto stripped = stripped_channel("aeron:ipc");
    ASSERT_TRUE(stripped.has_value()) << stripped.error().message.view();
    EXPECT_EQ("aeron:ipc", *stripped);
}

TEST(ChannelUriTest, udpEndpointSurvivesAndNonWhitelistedParamsAreDropped)
{
    const auto stripped =
        stripped_channel("aeron:udp?endpoint=localhost:3333|term-length=65536|mtu=1408|sparse=true|ssc=true");
    ASSERT_TRUE(stripped.has_value()) << stripped.error().message.view();
    EXPECT_EQ("aeron:udp?endpoint=localhost:3333", *stripped);
}

TEST(ChannelUriTest, whitelistedParamsUseTheReferenceOrder)
{
    const auto stripped = stripped_channel("aeron:udp?endpoint=localhost:3333|interface=eth0|ttl=32|tags=a,b");
    ASSERT_TRUE(stripped.has_value()) << stripped.error().message.view();
    EXPECT_EQ("aeron:udp?tags=a,b|endpoint=localhost:3333|interface=eth0|ttl=32", *stripped);
}

TEST(ChannelUriTest, controlResponseChannelAddsMtuTermLengthAndSparse)
{
    ChannelUriOptions options;
    options.term_length = 65536;
    options.mtu_length = 1408;
    options.sparse = false;

    const auto channel =
        control_response_channel("aeron:udp?endpoint=localhost:0|session-id=7|term-length=32768|mtu=1024", options);
    ASSERT_TRUE(channel.has_value()) << channel.error().message.view();
    EXPECT_EQ("aeron:udp?endpoint=localhost:0|mtu=1408|term-length=65536|session-id=7|sparse=false", *channel);
}

TEST(ChannelUriTest, mdsResponseChannelGetsTheCorrelationId)
{
    ChannelUriOptions options;
    options.term_length = 65536;
    options.mtu_length = 1408;
    options.sparse = false;
    options.has_response_correlation_id = true;
    options.response_correlation_id = 42;

    const auto channel = control_response_channel("aeron:udp?control-mode=response|endpoint=localhost:0", options);
    ASSERT_TRUE(channel.has_value()) << channel.error().message.view();
    EXPECT_EQ(
        "aeron:udp?endpoint=localhost:0|control-mode=response|mtu=1408|term-length=65536|sparse=false|"
        "response-correlation-id=42",
        *channel);
}

TEST(ChannelUriTest, makeKeyMatchesTheReferenceShape)
{
    auto key = make_key(33, "aeron:udp?endpoint=localhost:3333");
    ASSERT_TRUE(key.has_value()) << key.error().message.view();
    EXPECT_EQ("33:udp?endpoint=localhost:3333", *key);

    key = make_key(10, "aeron:ipc");
    ASSERT_TRUE(key.has_value()) << key.error().message.view();
    EXPECT_EQ("10:ipc", *key);

    key = make_key(7, "aeron:udp?endpoint=localhost:3333|interface=eth0|tags=a,b");
    ASSERT_TRUE(key.has_value()) << key.error().message.view();
    EXPECT_EQ("7:udp?endpoint=localhost:3333|interface=eth0|tags=a,b", *key);

    key = make_key(5, "aeron:udp?control=localhost:9000|session-id=17");
    ASSERT_TRUE(key.has_value()) << key.error().message.view();
    EXPECT_EQ("5:udp?control=localhost:9000|session-id=17", *key);
}

TEST(ChannelUriTest, udpDetectionIgnoresTheSpyPrefix)
{
    EXPECT_TRUE(is_udp_channel("aeron:udp?endpoint=localhost:3333"));
    EXPECT_TRUE(is_udp_channel("aeron-spy:aeron:udp?endpoint=localhost:3333"));
    EXPECT_FALSE(is_udp_channel("aeron:ipc"));
    EXPECT_FALSE(is_udp_channel("aeron-spy:aeron:ipc"));
}

TEST(ChannelUriTest, malformedChannelIsRefused)
{
    const auto stripped = stripped_channel("nonsense");
    EXPECT_FALSE(stripped.has_value());
}

}  // namespace
