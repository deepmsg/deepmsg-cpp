#include "core/io/mark_file.h"

#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <string>

namespace axc::core
{

std::expected<MarkFile, Error> MarkFile::create(std::string_view path, std::size_t length,
                                                std::size_t version_field_offset,
                                                std::size_t activity_timestamp_offset) noexcept
{
    auto file = MappedFile::create(path, length);
    if (!file)
    {
        return std::unexpected(file.error());
    }

    MarkFile mark_file;
    mark_file.file_ = std::move(*file);
    mark_file.version_field_offset_ = version_field_offset;
    mark_file.activity_timestamp_offset_ = activity_timestamp_offset;

    const bool version_fits = mark_file.field_fits(version_field_offset, sizeof(std::int32_t));
    const bool activity_fits = mark_file.field_fits(activity_timestamp_offset, sizeof(std::int64_t));

    if (!version_fits || !activity_fits)
    {
        // Reference 1.54 `b8b7015a24`: a failed creation must not leave a stale
        // activity timestamp behind. Only touch the field when it fits.
        if (activity_fits)
        {
            mark_file.update_activity(0);
        }
        return std::unexpected(make_error(ErrorCode::invalid_argument, "mark file fields do not fit the mapping"));
    }

    mark_file.set_version(0);
    mark_file.update_activity(EpochClock::now_ms());
    return mark_file;
}

std::expected<MarkFile, Error> MarkFile::open_existing(std::string_view path, std::size_t version_field_offset,
                                                       std::size_t activity_timestamp_offset) noexcept
{
    auto file = MappedFile::open(path);
    if (!file)
    {
        return std::unexpected(file.error());
    }

    MarkFile mark_file;
    mark_file.file_ = std::move(*file);
    mark_file.version_field_offset_ = version_field_offset;
    mark_file.activity_timestamp_offset_ = activity_timestamp_offset;

    if (!mark_file.field_fits(version_field_offset, sizeof(std::int32_t)) ||
        !mark_file.field_fits(activity_timestamp_offset, sizeof(std::int64_t)))
    {
        return std::unexpected(make_error(ErrorCode::invalid_argument, "mark file fields do not fit the mapping"));
    }

    return mark_file;
}

bool MarkFile::field_fits(std::size_t offset, std::size_t width) const noexcept
{
    const auto length = file_.length();
    return offset <= length && width <= length - offset;
}

std::int32_t MarkFile::version() const noexcept
{
    std::int32_t value = 0;
    const auto bytes = file_.mapping();
    std::memcpy(&value, bytes.data() + version_field_offset_, sizeof(value));
    return value;
}

void MarkFile::set_version(std::int32_t version) noexcept
{
    // No fence here: the version is written before update_activity(), whose
    // release store publishes both.
    const auto bytes = file_.mapping();
    std::memcpy(bytes.data() + version_field_offset_, &version, sizeof(version));
}

std::int64_t MarkFile::activity_timestamp() const noexcept
{
    const auto bytes = file_.mapping();
    return __atomic_load_n(reinterpret_cast<const std::int64_t*>(bytes.data() + activity_timestamp_offset_),
                           __ATOMIC_ACQUIRE);
}

void MarkFile::update_activity(std::int64_t timestamp) noexcept
{
    // GCC/Clang atomic builtins are used instead of std::atomic_thread_fence:
    // TSan rejects the latter outright (-Werror=tsan). The offset is not
    // guaranteed to be 8-aligned (SBE encoding offsets), which is fine on
    // x86-64, the only target we build for.
    const auto bytes = file_.mapping();
    __atomic_store_n(reinterpret_cast<std::int64_t*>(bytes.data() + activity_timestamp_offset_), timestamp,
                     __ATOMIC_RELEASE);
}

std::expected<void, Error> MarkFile::activate(std::int64_t now_ms, std::int64_t timeout_ms,
                                              std::string_view file_name) noexcept
{
    // `MarkFile.mapNewOrExistingMarkFile`, the `true` (existing) branch:
    // capacity check, then one liveness decision, then the CAS claim.
    auto bytes = file_.mapping();
    if (bytes.size() < activity_timestamp_offset_ + sizeof(std::int64_t))
    {
        return std::unexpected(make_error(ErrorCode::invalid_argument,
                                          "mark file mapping is to small: capacity=" + std::to_string(bytes.size()) +
                                              ", file=" + std::string{file_name}));
    }

    const std::int64_t timestamp = activity_timestamp();
    const std::int64_t time_diff = now_ms - timestamp;
    if (time_diff < timeout_ms)
    {
        return std::unexpected(
            make_error(ErrorCode::invalid_argument, "active mark file detected: " + std::string{file_name}));
    }

    return claim_activity(timestamp, file_name);
}

std::expected<void, Error> MarkFile::claim_activity(std::int64_t expected, std::string_view file_name) noexcept
{
    if (!try_activate(file_.mapping(), activity_timestamp_offset_, expected))
    {
        return std::unexpected(
            make_error(ErrorCode::invalid_argument, "concurrent mark file activation: " + std::string{file_name}));
    }
    return {};
}

bool MarkFile::try_activate(std::span<std::uint8_t> mapping, std::size_t activity_timestamp_offset,
                            std::int64_t expected) noexcept
{
    if (activity_timestamp_offset > mapping.size() || sizeof(std::int64_t) > mapping.size() - activity_timestamp_offset)
    {
        return false;
    }
    auto* field = reinterpret_cast<std::int64_t*>(mapping.data() + activity_timestamp_offset);
    return __atomic_compare_exchange_n(field, &expected, ACTIVATION_IN_PROGRESS_TIMESTAMP, false, __ATOMIC_ACQ_REL,
                                       __ATOMIC_ACQUIRE);
}

std::expected<void, Error> ensure_mark_file_link(std::string_view archive_dir, std::string_view mark_file_path,
                                                 std::string_view link_filename) noexcept
{
    // `MarkFile.ensureMarkFileLink`: both sides are canonical paths; the link
    // content is the mark file's *parent directory*, not the file.
    std::error_code error;
    const std::filesystem::path canonical_archive =
        std::filesystem::canonical(std::filesystem::path{std::string{archive_dir}}, error);
    if (error)
    {
        return std::unexpected(make_error(
            ErrorCode::io_error, "failed to resolve canonical path for archiveDir=" + std::string{archive_dir}));
    }

    const std::filesystem::path mark_parent = std::filesystem::path{std::string{mark_file_path}}.parent_path();
    const std::filesystem::path canonical_mark_parent = std::filesystem::canonical(mark_parent, error);
    if (error)
    {
        return std::unexpected(
            make_error(ErrorCode::io_error,
                       "failed to resolve canonical path for markFile parent dir of " + std::string{mark_file_path}));
    }

    const std::filesystem::path link_file = canonical_archive / std::filesystem::path{std::string{link_filename}};

    if (canonical_archive == canonical_mark_parent)
    {
        // `Files.deleteIfExists`: absence is fine.
        std::filesystem::remove(link_file, error);
        if (error)
        {
            return std::unexpected(make_error(ErrorCode::io_error, "failed to remove old link file"));
        }
        return {};
    }

    const std::string content = canonical_mark_parent.string();
    const int fd = ::open(link_file.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0)
    {
        return std::unexpected(make_error(ErrorCode::io_error, "failed to create link for mark file directory", errno));
    }

    const ssize_t written = ::write(fd, content.data(), content.size());
    const int write_error = errno;
    ::close(fd);
    if (written != static_cast<ssize_t>(content.size()))
    {
        return std::unexpected(
            make_error(ErrorCode::io_error, "failed to create link for mark file directory", write_error));
    }
    return {};
}

}  // namespace axc::core
