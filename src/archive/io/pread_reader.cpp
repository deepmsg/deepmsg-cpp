#include "archive/io/pread_reader.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
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

}  // namespace

PreadReader::PreadReader(ReaderConfig config, std::size_t buffer_length) noexcept
    : config_(std::move(config)), buffer_(buffer_length, 0)
{
}

PreadReader::~PreadReader()
{
    close_fd();
}

std::expected<void, axc::core::Error> PreadReader::open() noexcept
{
    return roll_segment(config_.segment_base_position);
}

std::expected<void, axc::core::Error> PreadReader::roll_segment(std::int64_t next_base_position) noexcept
{
    close_fd();

    const std::string path = config_.archive_dir + "/" + segment_file_name(config_.recording_id, next_base_position);
    if (0 != ::access(path.c_str(), F_OK))
    {
        return std::unexpected(axc::core::make_error(
            axc::core::ErrorCode::not_found,
            "failed to open recording segment file " + segment_file_name(config_.recording_id, next_base_position)));
    }

    fd_ = ::open(path.c_str(), O_RDONLY);
    if (fd_ < 0)
    {
        return std::unexpected(io_error(axc::core::ErrorCode::io_error, "open segment file failed"));
    }
    return {};
}

std::expected<std::span<std::uint8_t>, axc::core::Error> PreadReader::read(std::int64_t file_offset,
                                                                           std::int32_t max_len) noexcept
{
    if (fd_ < 0 || file_offset < 0)
    {
        return std::span<std::uint8_t>{};
    }

    const auto capacity = static_cast<std::int32_t>(buffer_.size());
    const std::int32_t len = std::min(max_len, capacity);
    std::int32_t total = 0;

    while (total < len)
    {
        const ssize_t result = ::pread(fd_, buffer_.data() + total, static_cast<std::size_t>(len - total),
                                       static_cast<off_t>(file_offset + total));
        if (result <= 0)
        {
            break;  // end of segment
        }
        total += static_cast<std::int32_t>(result);
    }

    return std::span<std::uint8_t>{buffer_.data(), static_cast<std::size_t>(total)};
}

std::expected<void, axc::core::Error> PreadReader::close() noexcept
{
    close_fd();
    return {};
}

void PreadReader::close_fd() noexcept
{
    if (fd_ >= 0)
    {
        ::close(fd_);
        fd_ = -1;
    }
}

std::expected<std::unique_ptr<SegmentReader>, axc::core::Error> make_pread_reader(const ReaderConfig& config)
{
    auto reader = std::make_unique<PreadReader>(config);
    auto opened = reader->open();
    if (!opened)
    {
        return std::unexpected(opened.error());
    }
    return std::unique_ptr<SegmentReader>{std::move(reader)};
}

}  // namespace deepmsg::archive::server::io
