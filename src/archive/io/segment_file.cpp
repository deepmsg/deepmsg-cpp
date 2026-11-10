#include "archive/io/segment_file.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <string>

#include "archive/server/segment_files.h"

namespace deepmsg::archive::server::io
{
namespace
{

[[nodiscard]] axc::core::Error file_error(std::string_view message) noexcept
{
    return axc::core::make_error(axc::core::ErrorCode::io_error, message, errno);
}

}  // namespace

std::expected<OpenedSegment, axc::core::Error> open_segment_file(const WriterConfig& config, std::int64_t base_position,
                                                                 bool fail_if_exists) noexcept
{
    const std::string path = config.archive_dir + "/" + segment_file_name(config.recording_id, base_position);

    if (fail_if_exists && 0 == ::access(path.c_str(), F_OK))
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::invalid_argument, "segment file already exists: " + path));
    }

    // `RandomAccessFile(segmentFile, "rw")` + `setLength(segmentLength)`
    // (`RecordingWriter.java:218-219`).
    const int fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0)
    {
        return std::unexpected(file_error("open segment file failed"));
    }

    if (0 != ::ftruncate(fd, static_cast<off_t>(config.segment_length)))
    {
        const axc::core::Error error = file_error("resize segment file failed");
        ::close(fd);
        return std::unexpected(error);
    }

    bool directory_forced = false;
    if (config.file_sync_level > 0 && config.archive_dir_fd >= 0)
    {
        directory_forced = true;
        const int result =
            config.file_sync_level > 1 ? ::fsync(config.archive_dir_fd) : ::fdatasync(config.archive_dir_fd);
        if (0 != result)
        {
            const axc::core::Error error = file_error("archive directory force failed");
            ::close(fd);
            return std::unexpected(error);
        }
    }

    return OpenedSegment{fd, directory_forced};
}

}  // namespace deepmsg::archive::server::io
