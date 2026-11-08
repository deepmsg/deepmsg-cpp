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

/// True when the channel carries `control-mode=response` (MDS), which is what
/// makes `newControlSession` add `response-correlation-id`
/// (`ArchiveConductor.java:467-479`).
[[nodiscard]] bool is_mds_response_channel(std::string_view channel) noexcept;

/// `ArchiveConductor.strippedChannelBuilder(uri).build()` (`ArchiveConductor.java:1889-1911`):
/// the 23-parameter whitelist, in `ChannelUriStringBuilder.build()` order.
[[nodiscard]] std::expected<std::string, axc::core::Error> stripped_channel(std::string_view channel);

/// `newControlSession`'s stripped channel plus `term-length` / `sparse` / `mtu`
/// (`ArchiveConductor.java:469-482`).
[[nodiscard]] std::expected<std::string, axc::core::Error> control_response_channel(std::string_view channel,
                                                                                    const ChannelUriOptions& options);

/// The extra parameters `ArchiveConductor.startReplay` puts on a replay
/// publication (`ArchiveConductor.java:892-901`).
struct ReplayChannelOptions
{
    /// `replayPosition`; `initialPosition(position, initialTermId, termLength)`.
    std::int64_t position{0};
    std::int32_t initial_term_id{0};
    std::int32_t term_length{0};
    /// `recordingSummary.mtuLength`.
    std::int32_t mtu_length{0};
    /// `ctx.replayLingerTimeoutNs()` when the channel itself has no `linger`.
    std::int64_t linger_timeout_ns{5'000'000'000};
    /// Set by the adapter for a response-mode replay (the request image's
    /// correlation id); otherwise the channel's own value is copied.
    bool has_response_correlation_id{false};
    std::int64_t response_correlation_id{0};
};

/// `strippedChannelBuilder(uri).initialPosition(...).eos(uri).sparse(uri).mtu(...).linger(...)`
/// (`ArchiveConductor.java:893-901`): the stripped channel plus the four
/// initial-position parameters, `eos`/`sparse` copied from the channel,
/// `mtu`/`term-length` forced, and a formatted `linger`.
[[nodiscard]] std::expected<std::string, axc::core::Error> replay_channel(std::string_view channel,
                                                                          const ReplayChannelOptions& options);

/// `ArchiveConductor.makeKey` (`ArchiveConductor.java:1914-1953`): the in-process
/// `streamId:media?endpoint=|interface=|control=|session-id=|tags=` key with the
/// trailing character chopped off (unconditionally, `:1945`).
[[nodiscard]] std::expected<std::string, axc::core::Error> make_key(std::int32_t stream_id, std::string_view channel);

}  // namespace deepmsg::archive::server
