#pragma once

#include <cstdint>
#include <functional>
#include <vector>

#include "cluster/common/recording_log.h"
#include "core/error/error.h"

namespace deepmsg::cluster::consensus
{

using axc::core::Error;

/// `RecordingLogValidator` (`RecordingLogValidator.java:25-120`): asks the
/// archive about every valid SNAPSHOT recording id and invalidates the log
/// entries whose recording the archive does not know.
///
/// The archive lookup is injected so the unit test can use the reference's
/// mock instead of a live archive; the agent supplies one backed by its own
/// `AeronArchive` (the reference's `newInstance`).
class RecordingLogValidator
{
public:
    enum class Lookup
    {
        known,
        unknown,
        error
    };

    /// `unknown` is `ArchiveException.UNKNOWN_RECORDING`; `error` is anything
    /// else and is reported without invalidating the entry.
    using RecordingLookup = std::function<Lookup(std::int64_t recording_id, Error& error)>;
    using ErrorReporter = std::function<void(const Error&)>;

    RecordingLogValidator(std::int32_t member_id, common::RecordingLog& recording_log, RecordingLookup lookup,
                          ErrorReporter report);

    /// `poll()` (`:58-101`): one archive query per call, then the invalidation
    /// pass; `is_complete()` turns true once the entries have been visited.
    int poll();

    [[nodiscard]] bool is_complete() const noexcept
    {
        return is_complete_;
    }

private:
    std::int32_t member_id_;
    common::RecordingLog& recording_log_;
    RecordingLookup lookup_;
    ErrorReporter report_;
    std::vector<std::int64_t> recording_ids_{};
    std::vector<std::int64_t> unknown_recording_ids_{};
    bool ids_collected_{false};
    bool is_complete_{false};
};

}  // namespace deepmsg::cluster::consensus
