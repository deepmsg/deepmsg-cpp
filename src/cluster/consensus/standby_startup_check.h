#pragma once

#include <cstdint>
#include <expected>
#include <functional>
#include <string>

#include "cluster/common/recording_log.h"
#include "cluster/consensus/consensus_module_context.h"
#include "core/error/error.h"

namespace deepmsg::cluster::consensus
{

using axc::core::Error;

/// Q6: `StandbySnapshotReplicator` is kept as a shell -- the C++ node does not
/// replicate standby snapshots -- but starting with a snapshot that is newer
/// than every local snapshot must not be silent.
///
/// The data comes from `RecordingLog.newer_standby_snapshots`
/// (`StandbySnapshotReplicator.java:309-332`), which the C1a transcription
/// already provides.
///
/// `warn` reports through `report` (the CM's counted error handler: counter
/// 212 + the distinct error log) and the node still starts from its local
/// snapshot. `refuse` returns the error from `on_start`, so the node does not
/// start. The reference has no such switch; the name
/// `aeron.cluster.cpp.standby.snapshot.on.startup` is this build's own and is
/// registered in `docs/compat.md`.
[[nodiscard]] std::expected<void, Error> check_standby_snapshots_on_startup(
    const common::RecordingLog& recording_log, std::int32_t service_count, StandbySnapshotStartupMode mode,
    const std::function<void(const Error&)>& report);

}  // namespace deepmsg::cluster::consensus
