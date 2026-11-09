#include "archive/server/replication_validation.h"

#include <string>

namespace deepmsg::archive::server
{
namespace
{

/// `ArchiveException` codes carried as the relevant id of an ERROR response.
constexpr std::int64_t GENERIC = 0;
constexpr std::int64_t ACTIVE_RECORDING = 2;
constexpr std::int64_t UNKNOWN_RECORDING = 5;
/// `Aeron.NULL_VALUE`.
constexpr std::int64_t NULL_VALUE = -1;

}  // namespace

std::optional<ReplicationRefusal> validate_replication_channel(bool is_response_channel, bool has_live_destination,
                                                               bool has_tags)
{
    // `ArchiveConductor.replicate` (`:1399-1410`).
    if (is_response_channel && has_live_destination)
    {
        return ReplicationRefusal{GENERIC, "response channels can't be used with live destinations"};
    }
    if (is_response_channel && has_tags)
    {
        return ReplicationRefusal{GENERIC, "response channels can't be used with tagged replication"};
    }
    return std::nullopt;
}

std::optional<ReplicationRefusal> validate_replication_destination(std::int64_t dst_recording_id, bool has_recording,
                                                                   bool is_active_recording)
{
    // `:1413-1428`.
    if (NULL_VALUE != dst_recording_id && !has_recording)
    {
        return ReplicationRefusal{UNKNOWN_RECORDING,
                                  "unknown destination recording id " + std::to_string(dst_recording_id)};
    }
    if (has_recording && is_active_recording)
    {
        return ReplicationRefusal{ACTIVE_RECORDING,
                                  "cannot replicate to active recording " + std::to_string(dst_recording_id)};
    }
    return std::nullopt;
}

std::optional<ReplicationRefusal> validate_replication_channel_configured(std::string_view replication_channel)
{
    if (replication_channel.empty())
    {
        return ReplicationRefusal{GENERIC, "Archive.Context.replicationChannel must be set"};
    }
    return std::nullopt;
}

}  // namespace deepmsg::archive::server
