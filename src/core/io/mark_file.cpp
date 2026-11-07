#include "core/io/mark_file.h"

#include <cstring>

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

}  // namespace axc::core
