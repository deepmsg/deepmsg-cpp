#pragma once

// T2: the text formats `clustertool` shares between the operator and the
// snapshot printer. Every format here is the reference's own
// (`ClusterToolOperator.printTypeAndActivityTimestamp`, `RecordingLog.toString`,
// `ConsensusModuleSnapshotPrinter.formatHexDump`); the interop tests compare
// the bytes.

#include <cstdint>
#include <span>
#include <string>

#include "cluster/common/cluster_mark_file.h"
#include "cluster/common/recording_log.h"

namespace deepmsg::tools
{

/// Epoch milliseconds from the system clock.
[[nodiscard]] std::int64_t now_ms() noexcept;

/// `String.format("%1$tH:%1$tM:%1$tS", new Date(ms))` in the local zone.
[[nodiscard]] std::string time_of_day(std::int64_t epoch_ms);

/// `String.format("%2$tF %2$tH:%2$tM:%2$tS", …)` in the local zone.
[[nodiscard]] std::string date_time(std::int64_t epoch_ms);

/// `String.format("%2$tF", …)`: `yyyy-MM-dd` in the local zone.
[[nodiscard]] std::string date_only(std::int64_t epoch_ms);

/// `[MarkFileHeader](...):field=value|...` — the SBE generated `toString`
/// (`ClusterToolOperator.java:1147`, `:431`).
[[nodiscard]] std::string mark_header_line(const cluster::common::ClusterMarkFile& mark_file);

/// `RecordingLog.toString()` (`RecordingLog.java:1277-1283`).
[[nodiscard]] std::string recording_log_text(const cluster::common::RecordingLog& log);

/// `RecordingLog.RecoveryPlan.toString()` (`RecordingLog.java:419-427`).
[[nodiscard]] std::string recovery_plan_text(const cluster::common::RecordingLog::RecoveryPlan& plan);

/// `ConsensusModuleSnapshotPrinter.formatHexDump` (`:…`): rows of 16 bytes,
/// `%06d:  ` + hex + `  |  ` + ASCII. The string ends with a newline per row;
/// the caller's `println` adds the blank line, as the reference does.
[[nodiscard]] std::string hex_dump(std::span<const std::uint8_t> bytes);

}  // namespace deepmsg::tools
