#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace deepmsg::cluster::client::detail
{

/// The media part of an Aeron channel (`aeron:udp?…` -> `udp`).
[[nodiscard]] std::string_view channel_media(std::string_view channel) noexcept;

/// A `key=value` parameter, or `std::nullopt` when absent.
[[nodiscard]] std::optional<std::string> channel_parameter(std::string_view channel, std::string_view key);

/// Replace an existing parameter, or append one (`ChannelUri.put`).
[[nodiscard]] std::string with_parameter(std::string_view channel, std::string_view key, std::string_view value);

/// `aeron_subscription_should_replace_wildcard_port`: udp, not `control-mode=manual`,
/// and an endpoint whose port is zero.
[[nodiscard]] bool should_replace_wildcard_port(std::string_view channel) noexcept;

/// `ChannelUri.addSessionId`.
[[nodiscard]] std::string with_session_id(std::string_view channel, std::int32_t session_id);

}  // namespace deepmsg::cluster::client::detail
