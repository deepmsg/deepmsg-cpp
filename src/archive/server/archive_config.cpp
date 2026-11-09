#include "archive/server/archive_config.h"

#include <charconv>
#include <cstdint>
#include <string>
#include <string_view>

namespace deepmsg::archive::server
{
namespace
{

/// `AERON_LOGBUFFER_TERM_MIN_LENGTH` / `..._TERM_MAX_LENGTH`.
constexpr std::int64_t TERM_MIN_LENGTH = 64 * 1024;
constexpr std::int64_t TERM_MAX_LENGTH = 1024 * 1024 * 1024;

[[nodiscard]] bool is_power_of_two(std::int64_t value) noexcept
{
    return value > 0 && 0 == (value & (value - 1));
}

[[nodiscard]] std::string_view trim(std::string_view value) noexcept
{
    while (!value.empty() && (' ' == value.front() || '\t' == value.front()))
    {
        value.remove_prefix(1);
    }
    while (!value.empty() && (' ' == value.back() || '\t' == value.back()))
    {
        value.remove_suffix(1);
    }
    return value;
}

[[nodiscard]] std::expected<std::int64_t, axc::core::Error> get_size(const axc::core::PropertySource& source,
                                                                     std::string_view name, std::int64_t fallback)
{
    const auto value = source.get(name);
    if (!value.has_value())
    {
        return fallback;
    }

    auto parsed = parse_size(*value);
    if (!parsed)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::config_error,
                                  std::string{"invalid "} + std::string{name} + ": " + std::string{*value}));
    }
    return *parsed;
}

[[nodiscard]] std::expected<std::int32_t, axc::core::Error> get_int32(const axc::core::PropertySource& source,
                                                                      std::string_view name, std::int32_t fallback)
{
    const auto value = source.get(name);
    if (!value.has_value())
    {
        return fallback;
    }

    std::int32_t parsed = 0;
    const char* begin = value->data();
    const char* end = begin + value->size();
    const auto result = std::from_chars(begin, end, parsed);
    if (result.ec != std::errc{} || result.ptr != end)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::config_error,
                                  std::string{"invalid "} + std::string{name} + ": " + std::string{*value}));
    }
    return parsed;
}

}  // namespace

std::expected<std::int64_t, axc::core::Error> parse_size(std::string_view value) noexcept
{
    value = trim(value);
    if (value.empty())
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::config_error, "empty size"));
    }

    std::int64_t multiplier = 1;
    const char suffix = value.back();
    if ('k' == suffix || 'K' == suffix)
    {
        multiplier = 1024;
        value.remove_suffix(1);
    }
    else if ('m' == suffix || 'M' == suffix)
    {
        multiplier = 1024 * 1024;
        value.remove_suffix(1);
    }
    else if ('g' == suffix || 'G' == suffix)
    {
        multiplier = 1024 * 1024 * 1024;
        value.remove_suffix(1);
    }

    std::int64_t magnitude = 0;
    const char* begin = value.data();
    const char* end = begin + value.size();
    const auto result = std::from_chars(begin, end, magnitude);
    if (result.ec != std::errc{} || result.ptr != end || value.empty())
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::config_error, "invalid size"));
    }

    return magnitude * multiplier;
}

std::expected<std::int64_t, axc::core::Error> parse_duration_ns(std::string_view value) noexcept
{
    value = trim(value);
    if (value.empty())
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::config_error, "empty duration"));
    }

    // `SystemUtil.parseDuration`: the last character is a digit ⇒ nanoseconds.
    const char last = value.back();
    std::int64_t multiplier = 1;
    if (last >= '0' && last <= '9')
    {
        // plain number in nanoseconds
    }
    else if ('s' == last || 'S' == last)
    {
        value.remove_suffix(1);
        // `5s` is seconds; `5ms`/`5us`/`5ns` carry the unit in the character
        // before the `s` (Agrona switches on it when it is not a digit).
        if (!value.empty() && !(value.back() >= '0' && value.back() <= '9'))
        {
            const char unit = value.back();
            value.remove_suffix(1);
            switch (unit)
            {
                case 'm':
                case 'M':
                    multiplier = 1000000;
                    break;
                case 'u':
                case 'U':
                    multiplier = 1000;
                    break;
                case 'n':
                case 'N':
                    multiplier = 1;
                    break;
                default:
                    return std::unexpected(axc::core::make_error(axc::core::ErrorCode::config_error,
                                                                 "invalid duration unit: " + std::string{value}));
            }
        }
        else
        {
            multiplier = 1000000000;
        }
    }
    else
    {
        value.remove_suffix(1);
        switch (last)
        {
            case 'n':
            case 'N':
                multiplier = 1;
                break;
            case 'u':
            case 'U':
                multiplier = 1000;
                break;
            case 'm':
            case 'M':
                multiplier = 1000000;
                break;
            default:
                return std::unexpected(axc::core::make_error(axc::core::ErrorCode::config_error,
                                                             "invalid duration unit: " + std::string{value}));
        }
    }

    std::int64_t magnitude = 0;
    const char* begin = value.data();
    const char* end = begin + value.size();
    const auto result = std::from_chars(begin, end, magnitude);
    if (result.ec != std::errc{} || result.ptr != end || value.empty() || magnitude < 0)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::config_error, "invalid duration: " + std::string{value}));
    }

    return magnitude * multiplier;
}

std::string format_duration_ns(std::int64_t value)
{
    // `SystemUtil.formatDuration` (Agrona 2.7.0): only an exact division changes
    // the unit; otherwise the value stays in nanoseconds.
    if (value >= 1000000000 && 0 == value % 1000000000)
    {
        return std::to_string(value / 1000000000) + "s";
    }
    if (value >= 1000000 && 0 == value % 1000000)
    {
        return std::to_string(value / 1000000) + "ms";
    }
    if (value >= 1000 && 0 == value % 1000)
    {
        return std::to_string(value / 1000) + "us";
    }
    return std::to_string(value) + "ns";
}

std::expected<RecordingSettings, axc::core::Error> parse_recording_settings(
    const axc::core::PropertySource& source) noexcept
{
    RecordingSettings settings;

    auto max_concurrent =
        get_int32(source, "aeron.archive.max.concurrent.recordings", settings.max_concurrent_recordings);
    if (!max_concurrent)
    {
        return std::unexpected(max_concurrent.error());
    }
    settings.max_concurrent_recordings = *max_concurrent;

    auto segment_length = get_size(source, "aeron.archive.segment.file.length", settings.segment_file_length);
    if (!segment_length)
    {
        return std::unexpected(segment_length.error());
    }
    // `Archive.java:1548-1556`: power of two first, then the term range.
    if (!is_power_of_two(*segment_length))
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::config_error,
                                  "segment file length not a power of 2: " + std::to_string(*segment_length)));
    }
    if (*segment_length < TERM_MIN_LENGTH || *segment_length > TERM_MAX_LENGTH)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::config_error,
                                  "segment file length not in valid range: " + std::to_string(*segment_length)));
    }
    settings.segment_file_length = static_cast<std::int32_t>(*segment_length);

    auto file_io_max_length = get_size(source, "aeron.archive.file.io.max.length", settings.file_io_max_length);
    if (!file_io_max_length)
    {
        return std::unexpected(file_io_max_length.error());
    }
    // `Archive.java:1274-1277`.
    if (*file_io_max_length < TERM_MIN_LENGTH || !is_power_of_two(*file_io_max_length))
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::config_error,
                                                     "invalid fileIoMaxLength=" + std::to_string(*file_io_max_length)));
    }
    settings.file_io_max_length = static_cast<std::int32_t>(*file_io_max_length);

    auto low_storage =
        get_size(source, "aeron.archive.low.storage.space.threshold", settings.low_storage_space_threshold);
    if (!low_storage)
    {
        return std::unexpected(low_storage.error());
    }
    settings.low_storage_space_threshold = *low_storage;

    auto file_sync_level = get_int32(source, "aeron.archive.file.sync.level", settings.file_sync_level);
    if (!file_sync_level)
    {
        return std::unexpected(file_sync_level.error());
    }
    settings.file_sync_level = *file_sync_level;

    const auto events_enabled = source.get("aeron.archive.recording.events.enabled");
    if (events_enabled.has_value())
    {
        settings.recording_events_enabled = "true" == *events_enabled;
    }
    const auto events_channel = source.get("aeron.archive.recording.events.channel");
    if (events_channel.has_value())
    {
        settings.recording_events_channel = std::string{*events_channel};
    }
    auto events_stream_id =
        get_int32(source, "aeron.archive.recording.events.stream.id", settings.recording_events_stream_id);
    if (!events_stream_id)
    {
        return std::unexpected(events_stream_id.error());
    }
    settings.recording_events_stream_id = *events_stream_id;

    // `Archive.java:1306-1311`.
    if (settings.recording_events_enabled && settings.recording_events_channel.empty())
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::config_error,
                                                     "Archive.Context.recordingEventsChannel must be set if "
                                                     "Archive.Context.recordingEventsEnabled is true"));
    }

    return settings;
}

std::expected<ReplaySettings, axc::core::Error> parse_replay_settings(const axc::core::PropertySource& source) noexcept
{
    ReplaySettings settings;

    // `Archive.Configuration.maxConcurrentReplays` (`Archive.java:474-481`).
    auto max_concurrent = get_int32(source, "aeron.archive.max.concurrent.replays", settings.max_concurrent_replays);
    if (!max_concurrent)
    {
        return std::unexpected(max_concurrent.error());
    }
    settings.max_concurrent_replays = *max_concurrent;

    // `Archive.Configuration.replayLingerTimeoutNs` (`Archive.java:555-564`).
    if (const auto linger = source.get("aeron.archive.replay.linger.timeout"); linger.has_value())
    {
        auto parsed = parse_duration_ns(*linger);
        if (!parsed)
        {
            return std::unexpected(parsed.error());
        }
        settings.replay_linger_timeout_ns = *parsed;
    }

    // `Archive.Configuration.replayChecksum` (`Archive.java:676`).
    if (const auto checksum = source.get("aeron.archive.replay.checksum"); checksum.has_value())
    {
        settings.replay_checksum_name = std::string{*checksum};
    }

    return settings;
}

}  // namespace deepmsg::archive::server
