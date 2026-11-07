#include "archive/server/segment_files.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <limits>
#include <system_error>

namespace deepmsg::archive::server
{
namespace
{

[[nodiscard]] bool has_suffix(const std::string& text, std::string_view suffix) noexcept
{
    return text.size() >= suffix.size() && 0 == text.compare(text.size() - suffix.size(), suffix.size(), suffix);
}

[[nodiscard]] axc::core::Error message_error(axc::core::ErrorCode code, std::string_view message)
{
    return axc::core::make_error(code, message);
}

/// `org.agrona.AsciiEncoding.parseLongAscii` over `[offset, offset + length)`.
[[nodiscard]] std::expected<std::int64_t, axc::core::Error> parse_long_ascii(const std::string& text,
                                                                             std::size_t offset, std::size_t length)
{
    if (0 == length)
    {
        return std::unexpected(message_error(axc::core::ErrorCode::invalid_argument, "no digits to parse"));
    }

    std::int64_t value = 0;
    for (std::size_t index = 0; index < length; index++)
    {
        const char character = text[offset + index];
        if (std::isdigit(static_cast<unsigned char>(character)) == 0)
        {
            return std::unexpected(message_error(axc::core::ErrorCode::invalid_argument,
                                                 "invalid long value in: " + text.substr(offset, length)));
        }
        if (value > (std::numeric_limits<std::int64_t>::max() - 9) / 10)
        {
            return std::unexpected(message_error(axc::core::ErrorCode::invalid_argument,
                                                 "long value out of range: " + text.substr(offset, length)));
        }
        value = (value * 10) + (character - '0');
    }
    return value;
}

}  // namespace

std::string segment_file_name(std::int64_t recording_id, std::int64_t segment_base_position)
{
    return std::to_string(recording_id) + "-" + std::to_string(segment_base_position) +
           std::string(RECORDING_SEGMENT_SUFFIX);
}

std::int32_t segment_file_length(std::int32_t term_length, std::int32_t segment_length) noexcept
{
    return std::max(segment_length, term_length);
}

std::int64_t segment_file_base_position(std::int64_t start_position, std::int64_t position, std::int32_t term_length,
                                        std::int32_t segment_length) noexcept
{
    // `AeronArchive.segmentFileBasePosition` (`AeronArchive.java:195-203`).
    const std::int64_t start_term_base_position = start_position - (start_position & (term_length - 1));
    const std::int64_t length_from_base_position = position - start_term_base_position;
    const std::int64_t segments = length_from_base_position - (length_from_base_position & (segment_length - 1));

    return start_term_base_position + segments;
}

std::vector<std::string> list_segment_files(std::string_view archive_dir, std::int64_t recording_id)
{
    std::vector<std::string> segment_files;
    std::error_code error;
    std::filesystem::directory_iterator iterator{std::filesystem::path(archive_dir), error};
    if (error)
    {
        return segment_files;
    }

    const std::string prefix = std::to_string(recording_id) + "-";
    for (const auto& entry : iterator)
    {
        const std::string name = entry.path().filename().string();
        if (0 == name.rfind(prefix, 0) && has_suffix(name, RECORDING_SEGMENT_SUFFIX))
        {
            segment_files.push_back(name);
        }
    }
    return segment_files;
}

std::expected<std::unordered_map<std::int64_t, std::vector<std::string>>, axc::core::Error> index_segment_files(
    std::string_view archive_dir)
{
    std::unordered_map<std::int64_t, std::vector<std::string>> index;
    std::error_code error;
    std::filesystem::directory_iterator iterator{std::filesystem::path(archive_dir), error};
    if (error)
    {
        return index;
    }

    for (const auto& entry : iterator)
    {
        const std::string name = entry.path().filename().string();
        if (!has_suffix(name, RECORDING_SEGMENT_SUFFIX))
        {
            continue;
        }

        // `parseSegmentFileRecordingId` throws `InvalidRecordingNameException`,
        // which `indexSegmentFiles` skips (`Catalog.java:1157-1163`); a bad
        // number is not caught there and aborts the open.
        const std::size_t dash = name.find('-');
        if (std::string::npos == dash || 0 == dash)
        {
            continue;
        }
        auto recording_id = parse_long_ascii(name, 0, dash);
        if (!recording_id)
        {
            return std::unexpected(recording_id.error());
        }
        index[*recording_id].push_back(name);
    }
    return index;
}

std::expected<std::string, axc::core::Error> find_segment_file_with_highest_position(
    const std::vector<std::string>& segment_files)
{
    std::int64_t max_segment_position = -1;  // `AeronArchive.NULL_POSITION`
    std::string max_file_name;

    for (const auto& filename : segment_files)
    {
        auto file_position = parse_segment_file_position(filename);
        if (!file_position)
        {
            return std::unexpected(file_position.error());
        }
        if (*file_position < 0)
        {
            return std::unexpected(message_error(axc::core::ErrorCode::protocol_error,
                                                 "negative position encoded in the file name: " + filename));
        }
        if (*file_position > max_segment_position)
        {
            max_segment_position = *file_position;
            max_file_name = filename;
        }
    }

    return max_file_name;
}

std::expected<std::int64_t, axc::core::Error> parse_segment_file_position(const std::string& filename)
{
    const std::size_t dash_offset = filename.find('-');
    if (std::string::npos == dash_offset)
    {
        return std::unexpected(
            message_error(axc::core::ErrorCode::invalid_argument, "invalid filename format: " + filename));
    }

    const auto position_offset = dash_offset + 1;
    const auto position_length = static_cast<std::int64_t>(filename.size()) -
                                 static_cast<std::int64_t>(position_offset) -
                                 static_cast<std::int64_t>(RECORDING_SEGMENT_SUFFIX.size());
    if (0 >= position_length)
    {
        return std::unexpected(message_error(axc::core::ErrorCode::invalid_argument,
                                             "no position encoded in the segment file: " + filename));
    }

    return parse_long_ascii(filename, position_offset, static_cast<std::size_t>(position_length));
}

std::expected<std::int64_t, axc::core::Error> parse_segment_file_recording_id(const std::string& filename)
{
    const std::size_t dash_offset = filename.find('-');
    if (std::string::npos == dash_offset || 0 == dash_offset)
    {
        return std::unexpected(
            message_error(axc::core::ErrorCode::invalid_argument, "invalid filename format: " + filename));
    }
    return parse_long_ascii(filename, 0, dash_offset);
}

}  // namespace deepmsg::archive::server
