#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace deepmsg::archive::server
{

/// A refusal `ArchiveConductor.replicate` sends before any session exists.
struct ReplicationRefusal
{
    std::int64_t code{0};
    std::string message;
};

/// `ArchiveConductor.replicate` (`ArchiveConductor.java:1394-1412`): the
/// channel the copy really runs over, and the two refusals a response channel
/// earns.
[[nodiscard]] std::optional<ReplicationRefusal> validate_replication_channel(bool is_response_channel,
                                                                             bool has_live_destination, bool has_tags);

/// `:1413-1428`: the destination recording checks. `has_recording` is
/// `catalog.hasRecording(dst)` and `is_active_recording` is "stop position is
/// NULL or a live recording session exists".
[[nodiscard]] std::optional<ReplicationRefusal> validate_replication_destination(std::int64_t dst_recording_id,
                                                                                 bool has_recording,
                                                                                 bool is_active_recording);

/// The request names no channel and the archive has none configured
/// (`Archive.java:1301-1303`). The reference refuses to **start**; this build
/// answers the request instead (a recorded deviation).
[[nodiscard]] std::optional<ReplicationRefusal> validate_replication_channel_configured(
    std::string_view replication_channel);

}  // namespace deepmsg::archive::server
