#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <string_view>

#include "core/error/error.h"
#include "core/io/mapped_file.h"
#include "core/time/clock.h"

namespace axc::core
{

/// Memory-mapped mark file: a version field and an activity timestamp at
/// caller-supplied offsets (the reference encodes its header with SBE, so the
/// offsets are schema-derived; this class does not hardcode a layout).
class MarkFile
{
public:
    static std::expected<MarkFile, Error> create(std::string_view path, std::size_t length,
                                                 std::size_t version_field_offset,
                                                 std::size_t activity_timestamp_offset) noexcept;

    static std::expected<MarkFile, Error> open_existing(std::string_view path, std::size_t version_field_offset,
                                                        std::size_t activity_timestamp_offset) noexcept;

    MarkFile() noexcept = default;
    MarkFile(MarkFile&&) noexcept = default;
    MarkFile& operator=(MarkFile&&) noexcept = default;
    ~MarkFile() = default;

    MarkFile(const MarkFile&) = delete;
    MarkFile& operator=(const MarkFile&) = delete;

    [[nodiscard]] bool valid() const noexcept
    {
        return file_.valid();
    }

    [[nodiscard]] std::span<std::uint8_t> mapped() noexcept
    {
        return file_.mapping();
    }

    [[nodiscard]] std::span<const std::uint8_t> mapped() const noexcept
    {
        return file_.mapping();
    }

    [[nodiscard]] std::int32_t version() const noexcept;
    void set_version(std::int32_t version) noexcept;

    [[nodiscard]] std::int64_t activity_timestamp() const noexcept;

    /// Reference behaviour: a release store of the timestamp. Also used on
    /// creation, and reset when creation fails half-way (1.54 change
    /// `b8b7015a24`).
    void update_activity(std::int64_t timestamp) noexcept;

    [[nodiscard]] std::expected<void, Error> flush() noexcept
    {
        return file_.flush();
    }

private:
    [[nodiscard]] bool field_fits(std::size_t offset, std::size_t width) const noexcept;

    MappedFile file_;
    std::size_t version_field_offset_{0};
    std::size_t activity_timestamp_offset_{0};
};

}  // namespace axc::core
