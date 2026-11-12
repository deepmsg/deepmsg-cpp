#include "cluster/consensus/standby_startup_check.h"

#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace deepmsg::cluster::consensus
{
namespace
{

[[nodiscard]] std::string describe(
    const std::map<std::string, std::vector<const common::RecordingLog::Entry*>, std::less<>>& newer)
{
    std::ostringstream out;
    bool first_endpoint = true;
    for (const auto& [endpoint, entries] : newer)
    {
        if (!first_endpoint)
        {
            out << "; ";
        }
        first_endpoint = false;
        out << "archiveEndpoint=" << (endpoint.empty() ? "<empty>" : endpoint);
        for (const common::RecordingLog::Entry* entry : entries)
        {
            out << " [recordingId=" << entry->recording_id << " leadershipTermId=" << entry->leadership_term_id
                << " logPosition=" << entry->log_position << " serviceId=" << entry->service_id << "]";
        }
    }
    return out.str();
}

}  // namespace

std::expected<void, Error> check_standby_snapshots_on_startup(const common::RecordingLog& recording_log,
                                                              std::int32_t service_count,
                                                              StandbySnapshotStartupMode mode,
                                                              const std::function<void(const Error&)>& report)
{
    const auto newer = recording_log.newer_standby_snapshots(service_count);
    if (newer.empty())
    {
        return {};
    }

    const std::string detail = describe(newer);

    if (StandbySnapshotStartupMode::refuse == mode)
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::config_error,
                                                     "standby snapshot is newer than the local snapshot and "
                                                     "aeron.cluster.cpp.standby.snapshot.on.startup=refuse - " +
                                                         detail));
    }

    report(axc::core::make_error(
        axc::core::ErrorCode::internal_error,
        "standby snapshot is newer than the local snapshot; this build does not replicate it and will start from "
        "the local snapshot - " +
            detail));
    return {};
}

}  // namespace deepmsg::cluster::consensus
