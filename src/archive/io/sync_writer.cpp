#include "archive/io/sync_writer.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <string>
#include <utility>

#include "archive/server/segment_files.h"

namespace deepmsg::archive::server::io
{
namespace
{

[[nodiscard]] axc::core::Error io_error(axc::core::ErrorCode code, std::string_view message) noexcept
{
    return axc::core::make_error(code, message, errno);
}

}  // namespace

SyncWriter::SyncWriter(WriterConfig config) noexcept : config_(std::move(config)) {}

SyncWriter::~SyncWriter()
{
    close_fd();
}

std::expected<void, axc::core::Error> SyncWriter::open() noexcept
{
    return open_segment(config_.segment_base_position, false);
}

std::expected<void, axc::core::Error> SyncWriter::open_segment(std::int64_t segment_base_position,
                                                               bool fail_if_exists) noexcept
{
    closed_ = false;
    segment_base_position_ = segment_base_position;

    const std::string path = config_.archive_dir + "/" + segment_file_name(config_.recording_id, segment_base_position);

    if (fail_if_exists && 0 == ::access(path.c_str(), F_OK))
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::invalid_argument, "segment file already exists: " + path));
    }

    // `RandomAccessFile(segmentFile, "rw")` + `setLength(segmentLength)`
    // (`RecordingWriter.java:218-219`).
    fd_ = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0)
    {
        return std::unexpected(io_error(axc::core::ErrorCode::io_error, "open segment file failed"));
    }

    if (0 != ::ftruncate(fd_, static_cast<off_t>(config_.segment_length)))
    {
        const axc::core::Error error = io_error(axc::core::ErrorCode::io_error, "resize segment file failed");
        close_fd();
        return std::unexpected(error);
    }

    // `RecordingWriter.java:221`: the directory entry is forced only when the
    // archive also forces file contents.
    if (config_.file_sync_level > 0 && config_.archive_dir_fd >= 0)
    {
        ++directory_force_count_;
        const int result =
            config_.file_sync_level > 1 ? ::fsync(config_.archive_dir_fd) : ::fdatasync(config_.archive_dir_fd);
        if (0 != result)
        {
            const axc::core::Error error = io_error(axc::core::ErrorCode::io_error, "archive directory force failed");
            close_fd();
            return std::unexpected(error);
        }
    }

    return {};
}

std::expected<bool, axc::core::Error> SyncWriter::submit(const BlockWrite& block) noexcept
{
    if (fd_ < 0)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::internal_error, "segment writer is not open"));
    }

    std::int32_t written = 0;
    while (written < block.len)
    {
        const ssize_t result = ::pwrite(fd_, block.src + written, static_cast<std::size_t>(block.len - written),
                                        static_cast<off_t>(block.file_offset + written));
        if (result < 0)
        {
            if (EINTR == errno)
            {
                continue;
            }
            return std::unexpected(io_error(axc::core::ErrorCode::io_error, "segment write failed"));
        }
        written += static_cast<std::int32_t>(result);
    }

    // `recordingFileChannel.force(forceMetadata)` (`RecordingWriter.java:138-141`):
    // level 1 = data only (`fdatasync`), level 2 = data and metadata (`fsync`).
    if (config_.file_sync_level > 0)
    {
        ++force_count_;
        const int result = config_.file_sync_level > 1 ? ::fsync(fd_) : ::fdatasync(fd_);
        if (0 != result)
        {
            return std::unexpected(io_error(axc::core::ErrorCode::io_error, "segment force failed"));
        }
    }

    // The invariant of `SegmentWriter`: the published position never runs ahead
    // of what is on (and, at a sync level above zero, in) the file.
    committed_position_ = block.end_position;
    return true;
}

std::expected<void, axc::core::Error> SyncWriter::roll_segment(std::int64_t next_base_position) noexcept
{
    close_fd();
    return open_segment(next_base_position, true);
}

std::expected<void, axc::core::Error> SyncWriter::close() noexcept
{
    closed_ = true;
    close_fd();
    return {};
}

void SyncWriter::close_fd() noexcept
{
    if (fd_ >= 0)
    {
        ::close(fd_);
        fd_ = -1;
    }
}

std::expected<std::unique_ptr<SegmentWriter>, axc::core::Error> make_sync_writer(const WriterConfig& config)
{
    auto writer = std::make_unique<SyncWriter>(config);
    auto opened = writer->open();
    if (!opened)
    {
        return std::unexpected(opened.error());
    }
    return std::unique_ptr<SegmentWriter>{std::move(writer)};
}

std::expected<std::unique_ptr<SegmentWriter>, axc::core::Error> make_uring_writer(const WriterConfig& config)
{
    (void)config;
    return std::unexpected(
        axc::core::make_error(axc::core::ErrorCode::not_found, "the io_uring writer lands with A6-3"));
}

}  // namespace deepmsg::archive::server::io
