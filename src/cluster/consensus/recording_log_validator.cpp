#include "cluster/consensus/recording_log_validator.h"

#include <algorithm>

namespace deepmsg::cluster::consensus
{

RecordingLogValidator::RecordingLogValidator(std::int32_t member_id, common::RecordingLog& recording_log,
                                             RecordingLookup lookup, ErrorReporter report)
    : member_id_(member_id), recording_log_(recording_log), lookup_(std::move(lookup)), report_(std::move(report))
{
}

int RecordingLogValidator::poll()
{
    if (!ids_collected_)
    {
        for (const common::RecordingLog::Entry& entry : recording_log_.entries())
        {
            if (entry.is_snapshot() && entry.is_valid)
            {
                recording_ids_.push_back(entry.recording_id);
            }
        }
        ids_collected_ = true;
    }

    if (!recording_ids_.empty())
    {
        // `recordingIds.removeAt(recordingIds.size() - 1)` (`:73`): one query
        // per slow tick keeps a large recording log from blocking the agent.
        const std::int64_t recording_id = recording_ids_.back();
        recording_ids_.pop_back();

        Error error{};
        switch (lookup_(recording_id, error))
        {
            case Lookup::unknown:
                unknown_recording_ids_.push_back(recording_id);
                break;
            case Lookup::error:
                if (report_)
                {
                    report_(error);
                }
                break;
            case Lookup::known:
                break;
        }
        return 1;
    }

    // The reference iterates `recordingLog.entries()` and invalidates in place
    // (`:83-94`); a copy is used here so the vector cannot move underneath the
    // loop if `invalidate_entry` ever reloads the file.
    const std::vector<common::RecordingLog::Entry> entries = recording_log_.entries();
    // The reference passes `entry.entryIndex` because its list order and the
    // serialized index are the same; this port sorts `entries_`, so the vector
    // position is what `invalidate_entry` indexes.
    for (std::size_t index = 0; index < entries.size(); index++)
    {
        const common::RecordingLog::Entry& entry = entries[index];
        if (entry.is_snapshot() && entry.is_valid &&
            std::find(unknown_recording_ids_.begin(), unknown_recording_ids_.end(), entry.recording_id) !=
                unknown_recording_ids_.end())
        {
            auto invalidated = recording_log_.invalidate_entry(static_cast<std::int32_t>(index));
            if (!invalidated && report_)
            {
                report_(axc::core::make_error(
                    invalidated.error().code,
                    "memberId=" + std::to_string(member_id_) + " failed to invalidate recordingId=" +
                        std::to_string(entry.recording_id) + ": " + std::string{invalidated.error().message.view()}));
            }
        }
    }

    is_complete_ = true;
    return 1;
}

}  // namespace deepmsg::cluster::consensus
