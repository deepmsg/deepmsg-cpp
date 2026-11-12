#include "cluster/client/client_config.h"

#include <cctype>
#include <charconv>
#include <string>

#include "cluster/client/channel_util.h"

namespace deepmsg::cluster::client
{
namespace
{

constexpr std::string_view MESSAGE_TIMEOUT_PROP_NAME = "aeron.cluster.message.timeout";
constexpr std::string_view INGRESS_ENDPOINTS_PROP_NAME = "aeron.cluster.ingress.endpoints";
constexpr std::string_view INGRESS_CHANNEL_PROP_NAME = "aeron.cluster.ingress.channel";
constexpr std::string_view INGRESS_STREAM_ID_PROP_NAME = "aeron.cluster.ingress.stream.id";
constexpr std::string_view EGRESS_CHANNEL_PROP_NAME = "aeron.cluster.egress.channel";
constexpr std::string_view EGRESS_STREAM_ID_PROP_NAME = "aeron.cluster.egress.stream.id";
constexpr std::string_view CLIENT_NAME_PROP_NAME = "aeron.cluster.client.name";

using axc::core::ErrorCode;
using axc::core::make_error;

/// `SystemUtil.parseDuration`: a sequence of `<number><unit>` tokens, spaces
/// between them; a bare number is nanoseconds.
[[nodiscard]] bool parse_duration_ns(std::string_view value, std::int64_t& result)
{
    std::int64_t total = 0;
    std::size_t position = 0;

    while (position < value.size())
    {
        while (position < value.size() && ' ' == value[position])
        {
            ++position;
        }
        if (position == value.size())
        {
            break;
        }

        std::int64_t number = 0;
        const char* begin = value.data() + position;
        const char* end = value.data() + value.size();
        const auto parsed = std::from_chars(begin, end, number);
        if (std::errc{} != parsed.ec || parsed.ptr == begin)
        {
            return false;
        }
        position = static_cast<std::size_t>(parsed.ptr - value.data());

        std::string_view unit;
        const std::size_t unit_begin = position;
        while (position < value.size() && std::isalpha(static_cast<unsigned char>(value[position])))
        {
            ++position;
        }
        unit = value.substr(unit_begin, position - unit_begin);

        std::int64_t scale = 1;
        if (unit.empty() || "ns" == unit)
        {
            scale = 1;
        }
        else if ("us" == unit)
        {
            scale = 1'000;
        }
        else if ("ms" == unit)
        {
            scale = 1'000'000;
        }
        else if ("s" == unit)
        {
            scale = 1'000'000'000;
        }
        else if ("m" == unit)
        {
            scale = 60LL * 1'000'000'000;
        }
        else if ("h" == unit)
        {
            scale = 3'600LL * 1'000'000'000;
        }
        else if ("d" == unit)
        {
            scale = 86'400LL * 1'000'000'000;
        }
        else
        {
            return false;
        }

        total += number * scale;
    }

    result = total;
    return true;
}

[[nodiscard]] std::expected<std::int32_t, Error> parse_int(const axc::core::PropertySource& properties,
                                                           std::string_view name, std::int32_t default_value)
{
    const auto text = properties.get(name);
    if (!text.has_value())
    {
        return default_value;
    }

    std::int32_t value = 0;
    const auto result = std::from_chars(text->data(), text->data() + text->size(), value);
    if (std::errc{} != result.ec || result.ptr != text->data() + text->size())
    {
        return std::unexpected(
            make_error(ErrorCode::config_error, std::string{name} + "=" + std::string{*text} + " is not a value"));
    }
    return value;
}

}  // namespace

std::expected<void, Error> ClientConfig::conclude()
{
    // `AeronCluster.java:1473-1476` (`ConcurrentConcludeException`).
    if (concluded_)
    {
        return std::unexpected(make_error(ErrorCode::config_error, "AeronCluster.Context has already been concluded"));
    }
    concluded_ = true;

    // `AeronCluster.java:1478-1481`.
    if (!ingress_channel.has_value() || ingress_channel->empty())
    {
        return std::unexpected(make_error(ErrorCode::config_error, "ingressChannel must be specified"));
    }

    // `AeronCluster.java:1483-1490`: `startsWith(IPC_CHANNEL)`.
    if (0 == ingress_channel->rfind(IPC_CHANNEL, 0) && ingress_endpoints.has_value())
    {
        return std::unexpected(make_error(ErrorCode::config_error,
                                          "AeronCluster.Context ingressEndpoints must be null when using IPC ingress"));
    }

    // `AeronCluster.java:1492-1495`.
    if (!egress_channel.has_value() || egress_channel->empty())
    {
        return std::unexpected(make_error(ErrorCode::config_error, "egressChannel must be specified"));
    }

    // `AeronCluster.java:1497-1502`: UDP egress gets `rejoin=false`, IPC does not.
    if ("udp" == detail::channel_media(*egress_channel))
    {
        egress_channel = detail::with_parameter(*egress_channel, REJOIN_PARAM_NAME, "false");
    }

    // `AeronCluster.java:1504-1508`.
    if (client_name.size() > MAX_CLIENT_NAME_LENGTH)
    {
        return std::unexpected(
            make_error(ErrorCode::config_error,
                       "AeronCluster.Context.clientName length must be <= " + std::to_string(MAX_CLIENT_NAME_LENGTH)));
    }

    return {};
}

std::string format_version_info()
{
    return "version=" + std::string{CLUSTER_VERSION} + " commit=" + std::string{CLUSTER_GIT_SHA};
}

std::string client_info(const ClientConfig& config)
{
    return "name=" + config.client_name + " " + format_version_info();
}

std::expected<ClientConfig, Error> client_config_from_properties(const axc::core::PropertySource& properties)
{
    ClientConfig config;

    if (const auto timeout = properties.get(MESSAGE_TIMEOUT_PROP_NAME); timeout.has_value())
    {
        std::int64_t parsed = 0;
        if (!parse_duration_ns(*timeout, parsed))
        {
            return std::unexpected(make_error(
                ErrorCode::config_error,
                std::string{MESSAGE_TIMEOUT_PROP_NAME} + "=" + std::string{*timeout} + " is not a duration"));
        }
        config.message_timeout_ns = parsed;
    }

    // A present-but-empty value stays empty (the reference reads the system
    // property, so `""` is a value, not `null`); only an absent one is null.
    if (const auto endpoints = properties.get(INGRESS_ENDPOINTS_PROP_NAME); endpoints.has_value())
    {
        config.ingress_endpoints = std::string{*endpoints};
    }
    if (const auto channel = properties.get(INGRESS_CHANNEL_PROP_NAME); channel.has_value())
    {
        config.ingress_channel = std::string{*channel};
    }
    if (const auto channel = properties.get(EGRESS_CHANNEL_PROP_NAME); channel.has_value())
    {
        config.egress_channel = std::string{*channel};
    }
    config.client_name = std::string{properties.get_or(CLIENT_NAME_PROP_NAME, "")};

    auto ingress_stream_id = parse_int(properties, INGRESS_STREAM_ID_PROP_NAME, config.ingress_stream_id);
    if (!ingress_stream_id)
    {
        return std::unexpected(ingress_stream_id.error());
    }
    config.ingress_stream_id = *ingress_stream_id;

    auto egress_stream_id = parse_int(properties, EGRESS_STREAM_ID_PROP_NAME, config.egress_stream_id);
    if (!egress_stream_id)
    {
        return std::unexpected(egress_stream_id.error());
    }
    config.egress_stream_id = *egress_stream_id;

    return config;
}

}  // namespace deepmsg::cluster::client
