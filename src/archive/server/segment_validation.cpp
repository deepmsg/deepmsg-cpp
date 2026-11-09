#include "archive/server/segment_validation.h"

#include <string>

#include "archive/server/frame_layout.h"
#include "archive/server/segment_files.h"

namespace deepmsg::archive::server
{
namespace
{

/// `Aeron.NULL_POSITION` / `ArchiveException` codes.
constexpr std::int64_t NULL_POSITION = -1;
constexpr std::int64_t ACTIVE_RECORDING = 2;
constexpr std::int64_t INVALID_POSITION = 16;

}  // namespace

std::optional<ValidationError> validate_truncate(const RecordingSummary& summary, std::int64_t position)
{
    // `ArchiveConductor.isValidTruncate` (`:2244-2265`).
    const std::int64_t stop_position = summary.stop_position;
    const std::int64_t start_position = summary.start_position;

    if (NULL_POSITION == stop_position)
    {
        return ValidationError{ACTIVE_RECORDING, "cannot truncate active recording"};
    }

    if (position < start_position || position > stop_position || 0 != (position & (FRAME_ALIGNMENT - 1)))
    {
        return ValidationError{INVALID_POSITION, "invalid position " + std::to_string(position) +
                                                     ": start=" + std::to_string(start_position) +
                                                     " stop=" + std::to_string(stop_position) +
                                                     " alignment=" + std::to_string(FRAME_ALIGNMENT)};
    }

    return std::nullopt;
}

std::optional<ValidationError> validate_purge(const RecordingSummary& summary, bool has_in_progress_replays)
{
    // `ArchiveConductor.isValidPurge` (`:2269-2290`).
    if (has_in_progress_replays)
    {
        return ValidationError{ACTIVE_RECORDING,
                               "cannot purge recording with active replay " + std::to_string(summary.recording_id)};
    }

    if (NULL_POSITION == summary.stop_position)
    {
        return ValidationError{ACTIVE_RECORDING,
                               "cannot purge active recording " + std::to_string(summary.recording_id)};
    }

    return std::nullopt;
}

std::optional<ValidationError> validate_detach(const RecordingSummary& summary, std::int64_t position,
                                               std::int64_t recorded_position,
                                               const std::vector<ReplayInProgressInfo>& replays)
{
    // `ArchiveConductor.isValidDetach` (`:2325-2381`).
    const std::int32_t segment_length = summary.segment_file_length;
    const std::int64_t start_position = summary.start_position;
    const std::int32_t term_length = summary.term_buffer_length;
    const std::int64_t lower_bound =
        segment_file_base_position(start_position, start_position, term_length, segment_length) + segment_length;

    if (position != segment_file_base_position(start_position, position, term_length, segment_length))
    {
        return ValidationError{0, "invalid segment start: newStartPosition=" + std::to_string(position)};
    }

    if (position < lower_bound)
    {
        return ValidationError{0, "invalid detach: newStartPosition=" + std::to_string(position) +
                                      " lowerBound=" + std::to_string(lower_bound)};
    }

    std::int64_t end_position = NULL_POSITION == summary.stop_position ? recorded_position : summary.stop_position;
    end_position = segment_file_base_position(start_position, end_position, term_length, segment_length);

    if (position > end_position)
    {
        return ValidationError{0, "invalid detach: in use, newStartPosition=" + std::to_string(position) +
                                      " upperBound=" + std::to_string(end_position)};
    }

    const ReplayInProgressInfo* min_replay = nullptr;
    for (const ReplayInProgressInfo& replay : replays)
    {
        if (summary.recording_id == replay.recording_id && position > replay.segment_file_base_position &&
            (nullptr == min_replay || replay.segment_file_base_position < min_replay->segment_file_base_position))
        {
            min_replay = &replay;
        }
    }

    if (nullptr != min_replay)
    {
        return ValidationError{
            0, "invalid detach: replay in progress -  state=" + min_replay->state + " newStartPosition=" +
                   std::to_string(position) + " upperBound=" + std::to_string(min_replay->segment_file_base_position) +
                   " sessionId=" + std::to_string(static_cast<std::int32_t>(min_replay->session_id)) +
                   " streamId=" + std::to_string(min_replay->stream_id) + " channel=" + min_replay->channel};
    }

    return std::nullopt;
}

}  // namespace deepmsg::archive::server
