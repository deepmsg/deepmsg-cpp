#include "core/io/distinct_error_log.h"

#include <cstdio>
#include <cstring>
#include <ctime>

namespace axc::core
{
namespace
{

constexpr std::size_t ENTRY_HEADER_LENGTH = 24;
constexpr std::size_t ENTRY_ALIGNMENT = 8;

[[nodiscard]] std::int32_t read_int32(const std::uint8_t* bytes) noexcept
{
    std::int32_t value = 0;
    std::memcpy(&value, bytes, sizeof(value));
    return value;
}

[[nodiscard]] std::int64_t read_int64(const std::uint8_t* bytes) noexcept
{
    std::int64_t value = 0;
    std::memcpy(&value, bytes, sizeof(value));
    return value;
}

/// The shared tail of both `DateTimeFormatter` patterns: `.SSSSSS` then the
/// numeric zone offset (`Z` is `+HHMM`).
[[nodiscard]] std::string with_micros_and_offset(const std::tm& local, std::int64_t epoch_millis)
{
    const std::int64_t micros = (((epoch_millis % 1000) + 1000) % 1000) * 1000;
    const long offset_seconds = local.tm_gmtoff;
    const char sign = offset_seconds < 0 ? '-' : '+';
    const long offset = offset_seconds < 0 ? -offset_seconds : offset_seconds;

    char tail[32];
    std::snprintf(tail, sizeof(tail), ".%06lld%c%02ld%02ld", static_cast<long long>(micros), sign, offset / 3600,
                  (offset % 3600) / 60);
    return tail;
}

}  // namespace

std::string format_error_log_time(std::int64_t epoch_millis)
{
    const std::time_t seconds = static_cast<std::time_t>(epoch_millis / 1000);
    std::tm local{};
    localtime_r(&seconds, &local);

    char date[64];
    std::strftime(date, sizeof(date), "%Y-%m-%d %H:%M:%S", &local);
    return std::string{date} + with_micros_and_offset(local, epoch_millis);
}

std::string format_file_name_time(std::int64_t epoch_millis)
{
    const std::time_t seconds = static_cast<std::time_t>(epoch_millis / 1000);
    std::tm local{};
    localtime_r(&seconds, &local);

    char date[64];
    std::strftime(date, sizeof(date), "%Y-%m-%d-%H-%M-%S", &local);
    return std::string{date} + with_micros_and_offset(local, epoch_millis);
}

RenderedErrorLog render_distinct_error_log(std::span<const std::uint8_t> buffer)
{
    RenderedErrorLog rendered;

    if (buffer.size() < ENTRY_HEADER_LENGTH || 0 == read_int32(buffer.data()))
    {
        // `CommonContext.printErrorLog`'s else branch (`:1297-1301`): no
        // errors still prints the summary, without the trailing period.
        rendered.text = "\n0 distinct errors observed\n";
        return rendered;
    }

    std::size_t offset = 0;
    while (offset + ENTRY_HEADER_LENGTH <= buffer.size())
    {
        const std::int32_t length = read_int32(buffer.data() + offset);
        if (0 == length)
        {
            break;
        }
        if (length < static_cast<std::int32_t>(ENTRY_HEADER_LENGTH) ||
            offset + static_cast<std::size_t>(length) > buffer.size())
        {
            break;
        }

        const std::int32_t observations = read_int32(buffer.data() + offset + 4);
        const std::int64_t last_timestamp = read_int64(buffer.data() + offset + 8);
        const std::int64_t first_timestamp = read_int64(buffer.data() + offset + 16);
        const auto payload = static_cast<std::size_t>(length) - ENTRY_HEADER_LENGTH;

        rendered.text += "\n";
        rendered.text += std::to_string(observations);
        rendered.text += " observations from ";
        rendered.text += format_error_log_time(first_timestamp);
        rendered.text += " to ";
        rendered.text += format_error_log_time(last_timestamp);
        rendered.text += " for:\n";
        rendered.text.append(reinterpret_cast<const char*>(buffer.data() + offset + ENTRY_HEADER_LENGTH), payload);
        rendered.text += "\n";

        ++rendered.distinct_error_count;
        offset += (static_cast<std::size_t>(length) + ENTRY_ALIGNMENT - 1) & ~(ENTRY_ALIGNMENT - 1);
    }

    rendered.text += "\n";
    rendered.text += std::to_string(rendered.distinct_error_count);
    rendered.text += " distinct errors observed.\n";
    return rendered;
}

}  // namespace axc::core
