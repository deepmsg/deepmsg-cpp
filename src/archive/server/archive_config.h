#pragma once

#include <cstdint>
#include <expected>
#include <string>

#include "core/config/property_source.h"
#include "core/error/error.h"

namespace deepmsg::archive::server
{

/// `Archive.Configuration` values the recording path reads
/// (`Archive.java:328-369`, `:429-438`, `:1109-1245`).
struct RecordingSettings
{
    std::int32_t max_concurrent_recordings{20};
    std::int32_t segment_file_length{128 * 1024 * 1024};
    std::int32_t file_io_max_length{1024 * 1024};
    /// `LOW_STORAGE_SPACE_THRESHOLD_DEFAULT = SEGMENT_FILE_LENGTH_DEFAULT`
    /// (`Archive.java:356`), not the configured segment length.
    std::int64_t low_storage_space_threshold{128 * 1024 * 1024};
    int file_sync_level{0};
    bool recording_events_enabled{false};
    std::string recording_events_channel;
    std::int32_t recording_events_stream_id{30};
};

/// Reads the nine names and applies the reference's startup checks
/// (`Archive.java:1274`, `:1548-1556`, `:1306-1311`).
[[nodiscard]] std::expected<RecordingSettings, axc::core::Error> parse_recording_settings(
    const axc::core::PropertySource& source) noexcept;

/// `Archive.Configuration` values the replay path reads
/// (`Archive.java:474-481`, `:555-564`, `:676`, `:2461-2486`).
struct ReplaySettings
{
    std::int32_t max_concurrent_replays{20};
    /// `REPLAY_LINGER_TIMEOUT_DEFAULT_NS` = `publicationLingerTimeoutNs()` = 5 s.
    std::int64_t replay_linger_timeout_ns{5'000'000'000};
    /// `aeron.archive.replay.checksum`; empty ⇒ checksums are not verified.
    std::string replay_checksum_name;
};

/// Reads the three replay names.
[[nodiscard]] std::expected<ReplaySettings, axc::core::Error> parse_replay_settings(
    const axc::core::PropertySource& source) noexcept;

/// `SystemUtil.parseSize`: a decimal integer with an optional `k`/`m`/`g`
/// suffix (`64k`, `128m`).
[[nodiscard]] std::expected<std::int64_t, axc::core::Error> parse_size(std::string_view value) noexcept;

/// `SystemUtil.parseDuration`: a non-negative decimal integer with an optional
/// `ns` / `us` / `ms` / `s` suffix; a bare number is nanoseconds, and a lone
/// `m`/`u`/`n` is milliseconds/microseconds/nanoseconds (Agrona 2.7.0, verified
/// against its bytecode).
[[nodiscard]] std::expected<std::int64_t, axc::core::Error> parse_duration_ns(std::string_view value) noexcept;

/// `SystemUtil.formatDuration` (`SystemUtil.java`): whole seconds, milliseconds
/// or microseconds when exactly divisible, otherwise nanoseconds; `0` is `0ns`.
[[nodiscard]] std::string format_duration_ns(std::int64_t value);

}  // namespace deepmsg::archive::server
