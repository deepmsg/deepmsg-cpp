#pragma once

#include <string>
#include <string_view>

namespace deepmsg::cluster::backup
{

/// `ChannelUri.parse(channel).put(name, value)` for the parameter helpers the
/// backup needs. The media part and all other parameters are kept in order.
[[nodiscard]] std::string channel_with_param(std::string_view channel, std::string_view name, std::string_view value);

[[nodiscard]] std::string channel_with_endpoint(std::string_view channel, std::string_view endpoint);

/// `ChannelUri.parse(channel).remove(name)`.
[[nodiscard]] std::string channel_without_param(std::string_view channel, std::string_view name);

}  // namespace deepmsg::cluster::backup
