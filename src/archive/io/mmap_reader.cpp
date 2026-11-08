#include "archive/io/mmap_reader.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
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

[[nodiscard]] std::string segment_path(const ReaderConfig& config, std::int64_t segment_base_position)
{
    return config.archive_dir + "/" + segment_file_name(config.recording_id, segment_base_position);
}

}  // namespace

MmapReader::MmapReader(ReaderConfig config) noexcept : config_(std::move(config)) {}

MmapReader::~MmapReader()
{
    unmap();
}

std::expected<void, axc::core::Error> MmapReader::open() noexcept
{
    return roll_segment(config_.segment_base_position);
}

std::expected<void, axc::core::Error> MmapReader::roll_segment(std::int64_t next_base_position) noexcept
{
    unmap();

    const std::string path = segment_path(config_, next_base_position);
    if (0 != ::access(path.c_str(), F_OK))
    {
        // `RecordingReader.java:207-210`.
        return std::unexpected(axc::core::make_error(
            axc::core::ErrorCode::not_found,
            "failed to open recording segment file " + segment_file_name(config_.recording_id, next_base_position)));
    }

    const int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0)
    {
        return std::unexpected(io_error(axc::core::ErrorCode::io_error, "open segment file failed"));
    }

    struct stat info{};
    const std::size_t length = 0 == ::fstat(fd, &info) ? static_cast<std::size_t>(info.st_size)
                                                       : static_cast<std::size_t>(config_.segment_length);

    // `MAP_PRIVATE`: the replay session rewrites frame headers in place and must
    // not touch the recorded file (impl-archive §4.2).
    void* mapping = ::mmap(nullptr, length, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
    const int error = errno;
    ::close(fd);

    if (MAP_FAILED == mapping)
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::io_error, "mmap segment failed", error));
    }

    mapping_ = static_cast<std::uint8_t*>(mapping);
    length_ = length;
    return {};
}

std::expected<std::span<std::uint8_t>, axc::core::Error> MmapReader::read(std::int64_t file_offset,
                                                                          std::int32_t max_len) noexcept
{
    if (nullptr == mapping_ || file_offset < 0 || file_offset >= static_cast<std::int64_t>(length_))
    {
        return std::span<std::uint8_t>{};
    }

    const auto available = static_cast<std::int32_t>(length_ - static_cast<std::size_t>(file_offset));
    const std::int32_t len = max_len < available ? max_len : available;
    return std::span<std::uint8_t>{mapping_ + file_offset, static_cast<std::size_t>(len)};
}

std::expected<void, axc::core::Error> MmapReader::close() noexcept
{
    unmap();
    return {};
}

void MmapReader::unmap() noexcept
{
    if (nullptr != mapping_)
    {
        ::munmap(mapping_, length_);
        mapping_ = nullptr;
        length_ = 0;
    }
}

std::expected<std::unique_ptr<SegmentReader>, axc::core::Error> make_mmap_reader(const ReaderConfig& config)
{
    auto reader = std::make_unique<MmapReader>(config);
    auto opened = reader->open();
    if (!opened)
    {
        return std::unexpected(opened.error());
    }
    return std::unique_ptr<SegmentReader>{std::move(reader)};
}

}  // namespace deepmsg::archive::server::io
