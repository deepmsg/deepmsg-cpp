// C3a: `AeronCluster.Context.conclude()` (`AeronCluster.java:1471-1517`) and
// the seven property names, without a driver.

#include <gtest/gtest.h>

#include <string>

#include "cluster/client/channel_util.h"
#include "cluster/client/client_config.h"
#include "core/config/property_source.h"

namespace
{

using deepmsg::cluster::client::client_config_from_properties;
using deepmsg::cluster::client::client_info;
using deepmsg::cluster::client::ClientConfig;
using deepmsg::cluster::client::format_version_info;
using deepmsg::cluster::client::MAX_CLIENT_NAME_LENGTH;
using deepmsg::cluster::client::PROTOCOL_SEMANTIC_VERSION;

[[nodiscard]] ClientConfig minimal_config()
{
    ClientConfig config;
    config.ingress_channel = "aeron:ipc";
    config.egress_channel = "aeron:ipc";
    return config;
}

TEST(ClientConfigTest, ingressChannelMustBeSpecified)
{
    ClientConfig config;
    config.egress_channel = "aeron:ipc";

    const auto concluded = config.conclude();
    ASSERT_FALSE(concluded.has_value());
    EXPECT_EQ(std::string{"ingressChannel must be specified"}, concluded.error().message.view());
}

TEST(ClientConfigTest, anEmptyIngressChannelIsNotSpecified)
{
    ClientConfig config;
    config.ingress_channel = "";
    config.egress_channel = "aeron:ipc";

    const auto concluded = config.conclude();
    ASSERT_FALSE(concluded.has_value());
    EXPECT_EQ(std::string{"ingressChannel must be specified"}, concluded.error().message.view());
}

TEST(ClientConfigTest, egressChannelMustBeSpecified)
{
    ClientConfig config;
    config.ingress_channel = "aeron:ipc";

    const auto concluded = config.conclude();
    ASSERT_FALSE(concluded.has_value());
    EXPECT_EQ(std::string{"egressChannel must be specified"}, concluded.error().message.view());
}

TEST(ClientConfigTest, ingressEndpointsMustBeNullWhenUsingIpcIngress)
{
    ClientConfig config = minimal_config();
    config.ingress_endpoints = "0=localhost:20111";

    const auto concluded = config.conclude();
    ASSERT_FALSE(concluded.has_value());
    EXPECT_EQ(std::string{"AeronCluster.Context ingressEndpoints must be null when using IPC ingress"},
              concluded.error().message.view());
}

TEST(ClientConfigTest, aUdpEgressChannelGetsRejoinFalse)
{
    ClientConfig config;
    config.ingress_channel = "aeron:ipc";
    config.egress_channel = "aeron:udp?endpoint=localhost:0";

    ASSERT_TRUE(config.conclude().has_value());
    EXPECT_EQ(std::string{"aeron:udp?endpoint=localhost:0|rejoin=false"}, *config.egress_channel);
}

TEST(ClientConfigTest, anExistingRejoinParameterIsReplacedNotAppended)
{
    ClientConfig config;
    config.ingress_channel = "aeron:ipc";
    config.egress_channel = "aeron:udp?endpoint=localhost:0|rejoin=true";

    ASSERT_TRUE(config.conclude().has_value());
    EXPECT_EQ(std::string{"aeron:udp?endpoint=localhost:0|rejoin=false"}, *config.egress_channel);
}

TEST(ClientConfigTest, anIpcEgressChannelDoesNotGetRejoin)
{
    ClientConfig config = minimal_config();

    ASSERT_TRUE(config.conclude().has_value());
    EXPECT_EQ(std::string{"aeron:ipc"}, *config.egress_channel);
}

TEST(ClientConfigTest, theClientNameIsLimitedToOneHundredCharacters)
{
    ClientConfig config = minimal_config();
    config.client_name = std::string(MAX_CLIENT_NAME_LENGTH, 'x');
    EXPECT_TRUE(config.conclude().has_value());

    ClientConfig too_long = minimal_config();
    too_long.client_name = std::string(MAX_CLIENT_NAME_LENGTH + 1, 'x');
    const auto concluded = too_long.conclude();
    ASSERT_FALSE(concluded.has_value());
    EXPECT_EQ(std::string{"AeronCluster.Context.clientName length must be <= 100"}, concluded.error().message.view());
}

TEST(ClientConfigTest, concludeRunsOnce)
{
    ClientConfig config = minimal_config();
    ASSERT_TRUE(config.conclude().has_value());
    EXPECT_TRUE(config.is_concluded());

    const auto again = config.conclude();
    ASSERT_FALSE(again.has_value());
    EXPECT_EQ(std::string{"AeronCluster.Context has already been concluded"}, again.error().message.view());
}

TEST(ClientConfigTest, fromPropertiesReadsTheSevenNames)
{
    axc::core::PropertySource properties;
    properties.set("aeron.cluster.message.timeout", "250ms");
    properties.set("aeron.cluster.ingress.endpoints", "0=localhost:20111,1=localhost:20112");
    properties.set("aeron.cluster.ingress.channel", "aeron:udp?endpoint=localhost:0");
    properties.set("aeron.cluster.ingress.stream.id", "111");
    properties.set("aeron.cluster.egress.channel", "aeron:udp?endpoint=localhost:0");
    properties.set("aeron.cluster.egress.stream.id", "112");
    properties.set("aeron.cluster.client.name", "the-client");

    auto config = client_config_from_properties(properties);
    ASSERT_TRUE(config.has_value());
    EXPECT_EQ(250'000'000, config->message_timeout_ns);
    EXPECT_EQ(std::string{"0=localhost:20111,1=localhost:20112"}, *config->ingress_endpoints);
    EXPECT_EQ(std::string{"aeron:udp?endpoint=localhost:0"}, *config->ingress_channel);
    EXPECT_EQ(111, config->ingress_stream_id);
    EXPECT_EQ(std::string{"aeron:udp?endpoint=localhost:0"}, *config->egress_channel);
    EXPECT_EQ(112, config->egress_stream_id);
    EXPECT_EQ(std::string{"the-client"}, config->client_name);
}

TEST(ClientConfigTest, durationsAreCompoundLikeTheReference)
{
    axc::core::PropertySource properties;
    properties.set("aeron.cluster.message.timeout", "1s 500ms");

    auto config = client_config_from_properties(properties);
    ASSERT_TRUE(config.has_value());
    EXPECT_EQ(1'500'000'000, config->message_timeout_ns);
}

TEST(ClientConfigTest, aBadDurationIsARefusedConfiguration)
{
    axc::core::PropertySource properties;
    properties.set("aeron.cluster.message.timeout", "5 fortnights");

    const auto config = client_config_from_properties(properties);
    ASSERT_FALSE(config.has_value());
    EXPECT_EQ(axc::core::ErrorCode::config_error, config.error().code);
}

TEST(ClientConfigTest, theClientInfoIsTheReferencesBytes)
{
    ClientConfig config = minimal_config();
    config.client_name = "probe";

    EXPECT_EQ("version=1.54.0 commit=454da1e4e6", format_version_info());
    EXPECT_EQ("name=probe version=1.54.0 commit=454da1e4e6", client_info(config));
    EXPECT_EQ(768, PROTOCOL_SEMANTIC_VERSION);
}

TEST(ChannelUtilTest, mediaAndParameters)
{
    using namespace deepmsg::cluster::client::detail;

    EXPECT_EQ("udp", channel_media("aeron:udp?endpoint=localhost:0"));
    EXPECT_EQ("ipc", channel_media("aeron:ipc"));
    EXPECT_EQ("", channel_media("nonsense"));

    EXPECT_EQ(std::optional<std::string>{"localhost:0"},
              channel_parameter("aeron:udp?endpoint=localhost:0", "endpoint"));
    EXPECT_EQ(std::nullopt, channel_parameter("aeron:udp?endpoint=localhost:0", "mtu"));
}

TEST(ChannelUtilTest, withParameterReplacesOrAppends)
{
    using namespace deepmsg::cluster::client::detail;

    EXPECT_EQ("aeron:udp?endpoint=localhost:1",
              with_parameter("aeron:udp?endpoint=localhost:0", "endpoint", "localhost:1"));
    EXPECT_EQ("aeron:udp?endpoint=localhost:0|mtu=1408",
              with_parameter("aeron:udp?endpoint=localhost:0", "mtu", "1408"));
    EXPECT_EQ("aeron:udp?mtu=1408", with_parameter("aeron:udp", "mtu", "1408"));
}

TEST(ChannelUtilTest, onlyAWildcardUdpPortIsReplaced)
{
    using namespace deepmsg::cluster::client::detail;

    EXPECT_TRUE(should_replace_wildcard_port("aeron:udp?endpoint=localhost:0"));
    EXPECT_FALSE(should_replace_wildcard_port("aeron:udp?endpoint=localhost:9020"));
    EXPECT_FALSE(should_replace_wildcard_port("aeron:ipc"));
    EXPECT_FALSE(should_replace_wildcard_port("aeron:udp?endpoint=localhost:0|control-mode=manual"));
}

TEST(ChannelUtilTest, aSessionIdIsAddedAsAParameter)
{
    using namespace deepmsg::cluster::client::detail;

    EXPECT_EQ("aeron:ipc?session-id=3", with_session_id("aeron:ipc", 3));
    EXPECT_EQ("aeron:ipc?session-id=4", with_session_id("aeron:ipc?session-id=3", 4));
}

}  // namespace
