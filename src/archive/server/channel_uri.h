#pragma once

#include <cstdint>
#include <expected>
#include <string>
#include <string_view>

#include "core/error/error.h"

namespace deepmsg::archive::server
{

/// The extra parameters `ArchiveConductor.newControlSession` puts on top of the
/// stripped channel (`ArchiveConductor.java:469-482`).
struct ChannelUriOptions
{
    std::int32_t term_length{0};
    std::int32_t mtu_length{0};
    bool sparse{false};
    /// `response-correlation-id` is written only for an MDS response channel
    /// (`CONTROL_MODE_RESPONSE`, `ArchiveConductor.java:476-479`).
    bool has_response_correlation_id{false};
    std::int64_t response_correlation_id{0};
};

/// True for a `aeron:udp` channel; an optional `aeron-spy:` prefix is ignored.
[[nodiscard]] bool is_udp_channel(std::string_view channel) noexcept;

/// `ArchiveConductor.strippedChannelBuilder(uri).build()` (`ArchiveConductor.java:1889-1911`):
/// the 23-parameter whitelist, in `ChannelUriStringBuilder.build()` order.
[[nodiscard]] std::expected<std::string, axc::core::Error> stripped_channel(std::string_view channel);

/// `newControlSession`'s stripped channel plus `term-length` / `sparse` / `mtu`
/// (`ArchiveConductor.java:469-482`).
[[nodiscard]] std::expected<std::string, axc::core::Error> control_response_channel(std::string_view channel,
                                                                                    const ChannelUriOptions& options);

/// `ArchiveConductor.makeKey` (`ArchiveConductor.java:1914-1953`): the in-process
/// `streamId:media?endpoint=|interface=|control=|session-id=|tags=` key with the
/// trailing character chopped off (unconditionally, `:1945`).
[[nodiscard]] std::expected<std::string, axc::core::Error> make_key(std::int32_t stream_id, std::string_view channel);

}  // namespace deepmsg::archive::server
