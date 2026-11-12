#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
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
    /// `MarkFile.ACTIVATION_IN_PROGRESS_TIMESTAMP` (Agrona 2.6.1): the CAS
    /// target that claims an existing file between its liveness check and
    /// `signal_ready`.
    static constexpr std::int64_t ACTIVATION_IN_PROGRESS_TIMESTAMP = std::numeric_limits<std::int64_t>::max();

    static std::expected<MarkFile, Error> create(std::string_view path, std::size_t length,
                                                 std::size_t version_field_offset,
                                                 std::size_t activity_timestamp_offset) noexcept;

    static std::expected<MarkFile, Error> open_existing(std::string_view path, std::size_t version_field_offset,
                                                        std::size_t activity_timestamp_offset) noexcept;

    /// `MarkFile.mapNewOrExistingMarkFile`'s activation half (Agrona 2.6.1): a
    /// timestamp younger than `timeout_ms` rejects the open, otherwise the
    /// field is compare-and-set from the observed value to
    /// `ACTIVATION_IN_PROGRESS_TIMESTAMP`. Single decision, no polling.
    [[nodiscard]] std::expected<void, Error> activate(std::int64_t now_ms, std::int64_t timeout_ms,
                                                      std::string_view file_name) noexcept;

    /// The claim half of `activate`, given the timestamp the caller observed:
    /// CAS it to `ACTIVATION_IN_PROGRESS_TIMESTAMP` or report the loss.
    [[nodiscard]] std::expected<void, Error> claim_activity(std::int64_t expected, std::string_view file_name) noexcept;

    /// The CAS alone, exposed so the losing side can be tested: the mapping is
    /// shared, so two processes can both pass the age check and only one wins.
    [[nodiscard]] static bool try_activate(std::span<std::uint8_t> mapping, std::size_t activity_timestamp_offset,
                                           std::int64_t expected) noexcept;

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

/// `MarkFile.ensureMarkFileLink` (Agrona 2.6.1): `archive_dir/link_filename`
/// holds the canonical path of the directory containing the mark file (the
/// directory, not the file). When both directories canonicalise to the same
/// path the link is removed instead of written.
[[nodiscard]] std::expected<void, Error> ensure_mark_file_link(std::string_view archive_dir,
                                                               std::string_view mark_file_path,
                                                               std::string_view link_filename) noexcept;

}  // namespace axc::core
