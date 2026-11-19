#pragma once

#include <cstdint>
#include <expected>
#include <span>
#include <string>

#include "core/error/error.h"

namespace axc::core
{

/// `CommonContext.DATE_TIME_FORMATTER` (`CommonContext.java:572`):
/// `uuuu-MM-dd HH:mm:ss.SSSSSSZ` in the system default time zone.
[[nodiscard]] std::string format_error_log_time(std::int64_t epoch_millis);

/// `CommonContext.FILE_NAME_FORMATTER` (`CommonContext.java:574`):
/// `uuuu-MM-dd-HH-mm-ss-SSSSSSZ` in the system default time zone.
[[nodiscard]] std::string format_file_name_time(std::int64_t epoch_millis);

struct RenderedErrorLog
{
    std::string text;
    std::int32_t distinct_error_count{0};
};

/// `CommonContext.printErrorLog` (`CommonContext.java:1274-1297`) over an
/// Aeron distinct-error-log region (`aeron_distinct_error_log.h:27-43`): a
/// 24-byte header per entry (length, observation count, last/first
/// timestamps) followed by the description, entries 8-byte aligned, the
/// length stored last.
///
/// `text` is byte-for-byte what the reference prints (leading newline before
/// each entry, the `0 distinct errors observed` summary without the trailing
/// period when there are no entries).
[[nodiscard]] RenderedErrorLog render_distinct_error_log(std::span<const std::uint8_t> buffer);

/// The media driver's error-log region inside a mapped `cnc.dat`
/// (`CncFileDescriptor.createErrorLogBuffer`): the 128-byte metadata header
/// holds `toDriver`, `toClients`, `countersMetadata`, `countersValues` and
/// `errorLogLength` as little-endian int32 from offset 4.
[[nodiscard]] std::expected<std::span<const std::uint8_t>, Error> cnc_error_log_region(
    std::span<const std::uint8_t> cnc);

}  // namespace axc::core
