#include "cluster/backup/channel_uri.h"

#include <string>
#include <string_view>

namespace deepmsg::cluster::backup
{
namespace
{

struct Parts
{
    std::string_view base;
    std::string_view params;
};

[[nodiscard]] Parts split(std::string_view channel) noexcept
{
    const std::size_t query = channel.find('?');
    if (std::string_view::npos == query)
    {
        return Parts{channel, {}};
    }
    return Parts{channel.substr(0, query), channel.substr(query + 1)};
}

}  // namespace

std::string channel_with_param(std::string_view channel, std::string_view name, std::string_view value)
{
    const Parts parts = split(channel);
    std::string result{parts.base};
    result.push_back('?');

    bool replaced = false;
    bool first = true;
    std::string_view rest = parts.params;
    while (!rest.empty())
    {
        const std::size_t bar = rest.find('|');
        const std::string_view parameter = rest.substr(0, bar);
        if (!first)
        {
            result.push_back('|');
        }
        first = false;

        const std::size_t equals = parameter.find('=');
        if (std::string_view::npos != equals && parameter.substr(0, equals) == name)
        {
            result.append(name);
            result.push_back('=');
            result.append(value);
            replaced = true;
        }
        else
        {
            result.append(parameter);
        }

        if (std::string_view::npos == bar)
        {
            break;
        }
        rest.remove_prefix(bar + 1);
    }

    if (!replaced)
    {
        if (!first)
        {
            result.push_back('|');
        }
        result.append(name);
        result.push_back('=');
        result.append(value);
    }
    return result;
}

std::string channel_with_endpoint(std::string_view channel, std::string_view endpoint)
{
    return channel_with_param(channel, "endpoint", endpoint);
}

std::string channel_without_param(std::string_view channel, std::string_view name)
{
    const Parts parts = split(channel);
    std::string result{parts.base};
    result.push_back('?');

    bool first = true;
    std::string_view rest = parts.params;
    while (!rest.empty())
    {
        const std::size_t bar = rest.find('|');
        const std::string_view parameter = rest.substr(0, bar);
        const std::size_t equals = parameter.find('=');
        const bool matches = std::string_view::npos != equals && parameter.substr(0, equals) == name;
        if (!matches)
        {
            if (!first)
            {
                result.push_back('|');
            }
            first = false;
            result.append(parameter);
        }
        if (std::string_view::npos == bar)
        {
            break;
        }
        rest.remove_prefix(bar + 1);
    }
    return result;
}

}  // namespace deepmsg::cluster::backup
