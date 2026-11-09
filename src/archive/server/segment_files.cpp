#include "archive/server/segment_files.h"

#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <string>
#include <system_error>
#include <vector>

#include "archive/server/frame_layout.h"

namespace deepmsg::archive::server
{
namespace
{

[[nodiscard]] bool has_suffix(const std::string& text, std::string_view suffix) noexcept
{
    return text.size() >= suffix.size() && 0 == text.compare(text.size() - suffix.size(), suffix.size(), suffix);
}

[[nodiscard]] std::int32_t read_int32(const std::uint8_t* address) noexcept
{
    std::int32_t value = 0;
    std::memcpy(&value, address, sizeof(value));
    return value;
}

/// Owns a file descriptor; control-plane paths only, so no hot-path concerns.
struct FdGuard
{
    int fd{-1};

    FdGuard() = default;
    FdGuard(const FdGuard&) = delete;
    FdGuard& operator=(const FdGuard&) = delete;
    ~FdGuard()
    {
        if (fd >= 0)
        {
            (void)::close(fd);
        }
    }
};

[[nodiscard]] std::string file_path(std::string_view archive_dir, std::string_view file_name)
{
    return std::string(archive_dir) + "/" + std::string(file_name);
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

std::vector<std::string> list_segment_files_with_delete_suffix(std::string_view archive_dir, std::int64_t recording_id)
{
    // `ArchiveConductor.listSegmentFiles` (`ArchiveConductor.java:1981-2005`).
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
        if (0 == name.rfind(prefix, 0) &&
            (has_suffix(name, RECORDING_SEGMENT_SUFFIX) || has_suffix(name, DELETE_SUFFIX)))
        {
            segment_files.push_back(name);
        }
    }
    return segment_files;
}

std::vector<std::string> find_detached_segments(std::string_view archive_dir, std::int64_t recording_id,
                                                std::int64_t start_position, std::int64_t prev_start_position,
                                                std::int32_t term_length, std::int32_t segment_length)
{
    // `ArchiveConductor.findDetachedSegments` (`ArchiveConductor.java:1722-1745`).
    std::vector<std::string> files;

    const std::int64_t prev_segment_file_position =
        segment_file_base_position(prev_start_position, prev_start_position, term_length, segment_length);
    const std::int64_t start_segment_file_position =
        segment_file_base_position(start_position, start_position, term_length, segment_length);

    for (std::int64_t position = start_segment_file_position - segment_length; position >= prev_segment_file_position;
         position -= segment_length)
    {
        files.push_back(segment_file_name(recording_id, position));
    }

    // The reference collects with `addFirst`, i.e. ascending file order.
    std::reverse(files.begin(), files.end());
    return files;
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

bool parse_segment_file_position_for_recording(std::string_view file_name, std::int64_t recording_id,
                                               std::int64_t& position) noexcept
{
    const std::string prefix = std::to_string(recording_id) + "-";
    if (file_name.size() <= prefix.size() || 0 != file_name.compare(0, prefix.size(), prefix))
    {
        return false;
    }

    const std::size_t dot_index = file_name.find('.', prefix.size());
    if (std::string::npos == dot_index || dot_index == prefix.size())
    {
        return false;
    }

    std::int64_t value = 0;
    const char* begin = file_name.data() + prefix.size();
    const char* end = file_name.data() + dot_index;
    const auto parsed = std::from_chars(begin, end, value);
    if (parsed.ec != std::errc{} || parsed.ptr != end)
    {
        return false;
    }

    position = value;
    return true;
}

FrameHeaderCheck check_first_frame(int fd, std::int64_t offset, std::int32_t term_offset, std::int32_t term_id,
                                   std::int32_t stream_id) noexcept
{
    // `ReplaySession.notHeaderAligned` (`ReplaySession.java:618-661`).
    std::uint8_t header[DATA_HEADER_LENGTH];
    const ssize_t bytes_read = ::pread(fd, header, sizeof(header), static_cast<off_t>(offset));
    if (static_cast<ssize_t>(sizeof(header)) != bytes_read)
    {
        return FrameHeaderCheck::read_error;
    }

    if (read_int32(header + TERM_OFFSET_FIELD_OFFSET) != term_offset ||
        read_int32(header + TERM_ID_OFFSET) != term_id || read_int32(header + STREAM_ID_OFFSET) != stream_id)
    {
        return FrameHeaderCheck::unaligned;
    }

    return FrameHeaderCheck::ok;
}

std::expected<void, std::string> erase_remaining_segment(std::string_view archive_dir, std::string_view file_name,
                                                         std::int64_t position, std::int32_t segment_length,
                                                         std::int32_t segment_offset, std::int32_t term_length,
                                                         std::int32_t term_id, std::int32_t stream_id) noexcept
{
    // `ArchiveConductor.eraseRemainingSegment` (`ArchiveConductor.java:2570-2623`).
    const std::string path = file_path(archive_dir, file_name);
    FdGuard file;
    file.fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC);
    if (file.fd < 0)
    {
        return std::unexpected(path + ": " + std::strerror(errno));
    }

    const auto term_offset = static_cast<std::int32_t>(position & (term_length - 1));
    switch (check_first_frame(file.fd, segment_offset, term_offset, term_id, stream_id))
    {
        case FrameHeaderCheck::read_error:
            return std::unexpected(std::string{"failed to read fragment header"});
        case FrameHeaderCheck::unaligned:
            return std::unexpected(std::to_string(position) + " position not aligned to a data header");
        case FrameHeaderCheck::ok:
            break;
    }

    if (0 != ::ftruncate(file.fd, static_cast<off_t>(segment_offset)))
    {
        return std::unexpected(path + ": " + std::strerror(errno));
    }

    // The reference writes one zero byte at the end of the segment so the file
    // keeps its full length; the tail after the truncate point stays sparse.
    const std::uint8_t zero = 0;
    for (;;)
    {
        const ssize_t written = ::pwrite(file.fd, &zero, 1, static_cast<off_t>(segment_length - 1));
        if (1 == written)
        {
            break;
        }
        if (written < 0 && EINTR != errno)
        {
            return std::unexpected(path + ": " + std::strerror(errno));
        }
    }

    return {};
}

std::expected<std::int32_t, std::string> find_term_offset_for_start(std::string_view archive_dir,
                                                                    std::string_view file_name, std::int32_t stream_id,
                                                                    std::int32_t term_id, std::int32_t term_length)
{
    // `ArchiveConductor.findTermOffsetForStart` (`ArchiveConductor.java:1789-1871`).
    const std::string path = file_path(archive_dir, file_name);
    FdGuard file;
    file.fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (file.fd < 0)
    {
        return std::unexpected(path + ": " + std::strerror(errno));
    }

    std::uint8_t header[DATA_HEADER_LENGTH];
    {
        const ssize_t bytes_read = ::pread(file.fd, header, sizeof(header), 0);
        if (static_cast<ssize_t>(sizeof(header)) != bytes_read)
        {
            return std::unexpected(std::string{"failed to read segment file"});
        }
    }

    std::int32_t term_offset = 0;
    if (read_int32(header) <= 0)
    {
        std::vector<std::uint8_t> buffer(static_cast<std::size_t>(std::min(term_length, 64 * 1024)));
        bool found = false;
        do
        {
            const auto length = std::min(term_length - term_offset, static_cast<std::int32_t>(buffer.size()));
            const ssize_t bytes_read =
                ::pread(file.fd, buffer.data(), static_cast<std::size_t>(length), static_cast<off_t>(term_offset));
            if (bytes_read <= 0)
            {
                return std::unexpected("read failed on " + path);
            }

            const auto limit =
                static_cast<std::int32_t>(bytes_read) - (static_cast<std::int32_t>(bytes_read) & (FRAME_ALIGNMENT - 1));
            std::int32_t offset = 0;
            while (offset < limit)
            {
                if (read_int32(buffer.data() + offset) > 0)
                {
                    found = true;
                    break;
                }
                offset += FRAME_ALIGNMENT;
            }
            term_offset += offset;
        } while (term_offset < term_length && !found);
    }

    if (term_offset >= term_length)
    {
        return std::unexpected("fragment not found in first term of segment " + path);
    }

    // The frame is at `term_offset` in the file, so its header is read there.
    {
        const ssize_t bytes_read = ::pread(file.fd, header, sizeof(header), static_cast<off_t>(term_offset));
        if (static_cast<ssize_t>(sizeof(header)) != bytes_read)
        {
            return std::unexpected("read failed on " + path);
        }
    }

    const std::int32_t file_term_id = read_int32(header + TERM_ID_OFFSET);
    if (file_term_id != term_id)
    {
        return std::unexpected("term id does not match: actual=" + std::to_string(file_term_id) +
                               " expected=" + std::to_string(term_id));
    }

    const std::int32_t file_stream_id = read_int32(header + STREAM_ID_OFFSET);
    if (file_stream_id != stream_id)
    {
        return std::unexpected("stream id does not match: actual=" + std::to_string(file_stream_id) +
                               " expected=" + std::to_string(stream_id));
    }

    return term_offset;
}

}  // namespace deepmsg::archive::server
