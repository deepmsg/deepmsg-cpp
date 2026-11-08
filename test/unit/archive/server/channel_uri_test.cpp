// A2b-1 criterion: the channel URI rewriting the conductor needs
// (`ArchiveConductor.strippedChannelBuilder` / `makeKey`, 1.54.0 = 454da1e4e6).
//
// The expected strings are the reference `ChannelUriStringBuilder.build()`
// output for the same input: the 23-parameter whitelist, canonical parameter
// order, then `newControlSession`'s `termLength` / `sparse` / `mtu`.

#include <gtest/gtest.h>

#include <string>

#include "archive/server/archive_config.h"
#include "archive/server/channel_uri.h"

namespace
{

using deepmsg::archive::server::ChannelUriOptions;
using deepmsg::archive::server::control_response_channel;
using deepmsg::archive::server::format_duration_ns;
using deepmsg::archive::server::is_udp_channel;
using deepmsg::archive::server::make_key;
using deepmsg::archive::server::parse_duration_ns;
using deepmsg::archive::server::replay_channel;
using deepmsg::archive::server::ReplayChannelOptions;
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

TEST(ChannelUriTest, controlResponseChannelUsesTheContextDefaultsWhenTheChannelHasNone)
{
    ChannelUriOptions options;
    options.term_length = 65536;
    options.mtu_length = 1408;
    options.sparse = false;

    const auto channel = control_response_channel("aeron:udp?endpoint=localhost:0|session-id=7", options);
    ASSERT_TRUE(channel.has_value()) << channel.error().message.view();
    EXPECT_EQ("aeron:udp?endpoint=localhost:0|mtu=1408|term-length=65536|session-id=7|sparse=false", *channel);
}

TEST(ChannelUriTest, controlResponseChannelLetsTheChannelOverrideTheContextDefaults)
{
    ChannelUriOptions options;
    options.term_length = 65536;
    options.mtu_length = 1408;
    options.sparse = false;

    // `ArchiveConductor.java:453-468`: the channel's own values win.
    const auto channel =
        control_response_channel("aeron:udp?endpoint=localhost:0|term-length=32768|mtu=1024|sparse=true", options);
    ASSERT_TRUE(channel.has_value()) << channel.error().message.view();
    EXPECT_EQ("aeron:udp?endpoint=localhost:0|mtu=1024|term-length=32768|sparse=true", *channel);
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

// A3b-1 criterion: the replay publication channel
// (`ArchiveConductor.java:892-901`, `ChannelUriStringBuilder.build()` order).

TEST(ChannelUriTest, replayChannelCarriesTheInitialPositionAndLinger)
{
    ReplayChannelOptions options;
    options.position = 0;
    options.initial_term_id = 1;
    options.term_length = 65536;
    options.mtu_length = 1408;
    options.linger_timeout_ns = 5'000'000'000;

    const auto channel = replay_channel("aeron:udp?endpoint=localhost:6666", options);
    ASSERT_TRUE(channel.has_value()) << channel.error().message.view();
    EXPECT_EQ(
        "aeron:udp?endpoint=localhost:6666|mtu=1408|term-length=65536|"
        "init-term-id=1|term-id=1|term-offset=0|linger=5s",
        *channel);
}

TEST(ChannelUriTest, replayChannelComputesTheTermFromAMidTermPosition)
{
    ReplayChannelOptions options;
    options.position = 100000;
    options.initial_term_id = 7;
    options.term_length = 65536;
    options.mtu_length = 1408;

    const auto channel = replay_channel("aeron:udp?endpoint=localhost:6666", options);
    ASSERT_TRUE(channel.has_value()) << channel.error().message.view();
    EXPECT_EQ(
        "aeron:udp?endpoint=localhost:6666|mtu=1408|term-length=65536|"
        "init-term-id=7|term-id=8|term-offset=34464|linger=5s",
        *channel);
}

TEST(ChannelUriTest, replayChannelForcesMtuAndTermLengthAndCopiesSparseAndEos)
{
    ReplayChannelOptions options;
    options.position = 0;
    options.initial_term_id = 1;
    options.term_length = 65536;
    options.mtu_length = 1408;

    const auto channel = replay_channel(
        "aeron:udp?endpoint=localhost:6666|mtu=1024|term-length=32768|sparse=true|eos=true|linger=3s", options);
    ASSERT_TRUE(channel.has_value()) << channel.error().message.view();
    EXPECT_EQ(
        "aeron:udp?endpoint=localhost:6666|mtu=1408|term-length=65536|"
        "init-term-id=1|term-id=1|term-offset=0|linger=3s|sparse=true|eos=true",
        *channel);
}

TEST(ChannelUriTest, replayChannelOverridesTheResponseCorrelationId)
{
    ReplayChannelOptions options;
    options.position = 0;
    options.initial_term_id = 1;
    options.term_length = 65536;
    options.mtu_length = 1408;
    options.has_response_correlation_id = true;
    options.response_correlation_id = 42;

    const auto channel = replay_channel("aeron:udp?control-mode=response|control=localhost:10002", options);
    ASSERT_TRUE(channel.has_value()) << channel.error().message.view();
    EXPECT_EQ(
        "aeron:udp?control=localhost:10002|control-mode=response|mtu=1408|term-length=65536|"
        "init-term-id=1|term-id=1|term-offset=0|linger=5s|response-correlation-id=42",
        *channel);
}

TEST(ChannelUriTest, replayChannelRefusesAnUnalignedPosition)
{
    ReplayChannelOptions options;
    options.position = 17;
    options.term_length = 65536;

    const auto channel = replay_channel("aeron:udp?endpoint=localhost:6666", options);
    EXPECT_FALSE(channel.has_value());
}

TEST(ChannelUriTest, durationParsingMatchesAgronaSystemUtil)
{
    const auto seconds = parse_duration_ns("5s");
    ASSERT_TRUE(seconds.has_value());
    EXPECT_EQ(5'000'000'000, *seconds);

    const auto millis = parse_duration_ns("5ms");
    ASSERT_TRUE(millis.has_value());
    EXPECT_EQ(5'000'000, *millis);

    // Agrona maps a lone `m` to milliseconds, not minutes.
    const auto lone_millis = parse_duration_ns("5m");
    ASSERT_TRUE(lone_millis.has_value());
    EXPECT_EQ(5'000'000, *lone_millis);

    const auto micros = parse_duration_ns("5us");
    ASSERT_TRUE(micros.has_value());
    EXPECT_EQ(5'000, *micros);

    const auto nanos = parse_duration_ns("5ns");
    ASSERT_TRUE(nanos.has_value());
    EXPECT_EQ(5, *nanos);

    const auto bare = parse_duration_ns("5");
    ASSERT_TRUE(bare.has_value());
    EXPECT_EQ(5, *bare);

    EXPECT_FALSE(parse_duration_ns("-5s").has_value());
    EXPECT_FALSE(parse_duration_ns("5x").has_value());
    EXPECT_FALSE(parse_duration_ns("").has_value());
}

TEST(ChannelUriTest, durationFormattingMatchesAgronaSystemUtil)
{
    EXPECT_EQ("0ns", format_duration_ns(0));
    EXPECT_EQ("5s", format_duration_ns(5'000'000'000));
    EXPECT_EQ("2ms", format_duration_ns(2'000'000));
    EXPECT_EQ("3us", format_duration_ns(3'000));
    EXPECT_EQ("1500ns", format_duration_ns(1500));
    EXPECT_EQ("123456789ns", format_duration_ns(123456789));
}

}  // namespace
