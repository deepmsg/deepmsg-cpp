#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "archive/server/catalog.h"

namespace deepmsg::archive::server
{

/// The reference's `sendErrorResponse(correlationId, code, message)` pair.
struct ValidationError
{
    std::int64_t code{0};
    std::string message;
};

/// The replay fields `isValidDetach` looks at (`ArchiveConductor.java:2357-2381`).
struct ReplayInProgressInfo
{
    std::int64_t recording_id{0};
    std::int64_t segment_file_base_position{0};
    std::int64_t session_id{0};
    std::int32_t stream_id{0};
    std::string channel;
    std::string state;
};

/// `ArchiveConductor.isValidTruncate` (`ArchiveConductor.java:2244-2265`).
[[nodiscard]] std::optional<ValidationError> validate_truncate(const RecordingSummary& summary, std::int64_t position);

/// `ArchiveConductor.isValidPurge` (`:2269-2290`). The replay test comes first.
[[nodiscard]] std::optional<ValidationError> validate_purge(const RecordingSummary& summary,
                                                            bool has_in_progress_replays);

/// `ArchiveConductor.isValidDetach` (`:2325-2381`). `recorded_position` is only
/// read for an active recording (NULL stop position), like the reference's
/// `recordingSessionByIdMap.get(recordingId).recordedPosition()`.
[[nodiscard]] std::optional<ValidationError> validate_detach(const RecordingSummary& summary, std::int64_t position,
                                                             std::int64_t recorded_position,
                                                             const std::vector<ReplayInProgressInfo>& replays);

}  // namespace deepmsg::archive::server
