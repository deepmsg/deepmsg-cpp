#include "cluster/client/channel_util.h"

#include <vector>

namespace deepmsg::cluster::client::detail
{
namespace
{

struct Parameter
{
    std::string key;
    std::string value;
};

[[nodiscard]] std::string_view trim(std::string_view text) noexcept
{
    while (!text.empty() && ' ' == text.front())
    {
        text.remove_prefix(1);
    }
    while (!text.empty() && ' ' == text.back())
    {
        text.remove_suffix(1);
    }
    return text;
}

/// `aeron:<media>?<params>` — the params keep their order.
[[nodiscard]] std::vector<Parameter> parse_parameters(std::string_view channel)
{
    std::vector<Parameter> parameters;
    const std::size_t question = channel.find('?');
    if (std::string::npos == question)
    {
        return parameters;
    }

    std::string_view rest = channel.substr(question + 1);
    while (!rest.empty())
    {
        std::size_t separator = rest.find_first_of("|&");
        const std::string_view part = trim(rest.substr(0, separator));
        if (!part.empty())
        {
            const std::size_t equals = part.find('=');
            if (std::string::npos == equals)
            {
                parameters.push_back(Parameter{std::string{part}, ""});
            }
            else
            {
                parameters.push_back(
                    Parameter{std::string{part.substr(0, equals)}, std::string{part.substr(equals + 1)}});
            }
        }
        if (std::string::npos == separator)
        {
            break;
        }
        rest.remove_prefix(separator + 1);
    }
    return parameters;
}

[[nodiscard]] std::string build(std::string_view base, const std::vector<Parameter>& parameters)
{
    std::string channel{base};
    if (parameters.empty())
    {
        return channel;
    }

    channel.push_back('?');
    for (std::size_t i = 0; i < parameters.size(); ++i)
    {
        if (0 != i)
        {
            channel.push_back('|');
        }
        channel.append(parameters[i].key);
        if (!parameters[i].value.empty())
        {
            channel.push_back('=');
            channel.append(parameters[i].value);
        }
    }
    return channel;
}

[[nodiscard]] std::string_view base_of(std::string_view channel) noexcept
{
    const std::size_t question = channel.find('?');
    return std::string::npos == question ? channel : channel.substr(0, question);
}

}  // namespace

std::string_view channel_media(std::string_view channel) noexcept
{
    const std::string_view base = base_of(channel);
    const std::size_t colon = base.find(':');
    return std::string::npos == colon ? std::string_view{} : base.substr(colon + 1);
}

std::optional<std::string> channel_parameter(std::string_view channel, std::string_view key)
{
    for (const Parameter& parameter : parse_parameters(channel))
    {
        if (parameter.key == key)
        {
            return parameter.value;
        }
    }
    return std::nullopt;
}

std::string with_parameter(std::string_view channel, std::string_view key, std::string_view value)
{
    std::vector<Parameter> parameters = parse_parameters(channel);

    for (Parameter& parameter : parameters)
    {
        if (parameter.key == key)
        {
            parameter.value = std::string{value};
            return build(base_of(channel), parameters);
        }
    }

    parameters.push_back(Parameter{std::string{key}, std::string{value}});
    return build(base_of(channel), parameters);
}

bool should_replace_wildcard_port(std::string_view channel) noexcept
{
    if ("udp" != channel_media(channel))
    {
        return false;
    }

    const auto control_mode = channel_parameter(channel, "control-mode");
    if (control_mode.has_value() && "manual" == *control_mode)
    {
        return false;
    }

    const auto endpoint = channel_parameter(channel, "endpoint");
    return endpoint.has_value() && endpoint->size() >= 2 && ":0" == endpoint->substr(endpoint->size() - 2);
}

std::string with_session_id(std::string_view channel, std::int32_t session_id)
{
    return with_parameter(channel, "session-id", std::to_string(session_id));
}

}  // namespace deepmsg::cluster::client::detail
