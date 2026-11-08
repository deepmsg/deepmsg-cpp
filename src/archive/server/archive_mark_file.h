#pragma once

#include <cstdint>
#include <expected>
#include <functional>
#include <span>
#include <string>

#include "archive/server/codecs.h"
#include "archive/server/epoch_clock_source.h"
#include "core/error/error.h"
#include "core/io/mark_file.h"
#include "io_aeron_archive_codecs_mark/MarkFileHeader.h"

namespace deepmsg::archive::server
{

/// `ArchiveMarkFile` (`ArchiveMarkFile.java:50-581`): the `archive-mark.dat`
/// guard file in the archive directory. Its header is an SBE
/// `MarkFileHeader` behind an 8-byte message header, followed by the error
/// buffer.
///
/// Not implemented here, with the card they land on:
///   * the Agrona liveness wait (another archive process holding the directory) —
///     A2a, with `Archive.launch`;
///   * rendering existing errors to a logger and the `archive-mark.lnk` link file —
///     A2a/A5a.
class ArchiveMarkFile
{
public:
    static constexpr std::int32_t MAJOR_VERSION = 3;
    static constexpr std::int32_t MINOR_VERSION = 1;
    static constexpr std::int32_t PATCH_VERSION = 0;
    static constexpr std::int32_t SEMANTIC_VERSION = (MAJOR_VERSION << 16) | (MINOR_VERSION << 8) | PATCH_VERSION;

    static constexpr std::int32_t HEADER_LENGTH = 8 * 1024;
    static constexpr const char* FILENAME = "archive-mark.dat";

    /// `MessageHeaderDecoder.ENCODED_LENGTH` (`ArchiveMarkFile.java:90`).
    static constexpr std::int32_t HEADER_OFFSET = 8;

    /// `Archive.Configuration.ARCHIVE_DIR_*` and `errorBufferLength` inputs.
    struct Options
    {
        std::string directory;
        /// `alignedTotalFileLength(ctx)` (`ArchiveMarkFile.java:462`).
        std::int32_t total_file_length{0};
        std::int32_t error_buffer_length{0};
        /// nullptr ⇒ system clock.
        const EpochClockSource* epoch_clock{nullptr};
    };

    /// What `ArchiveMarkFile.encode(ctx)` writes (`ArchiveMarkFile.java:502-516`).
    struct HeaderValues
    {
        std::int64_t start_timestamp{0};
        std::int32_t control_stream_id{0};
        std::int32_t local_control_stream_id{0};
        std::int32_t events_stream_id{0};
        std::int64_t archive_id{-1};
        std::string control_channel;
        std::string local_control_channel;
        std::string events_channel;
        std::string aeron_directory;
    };

    /// The main constructor (`ArchiveMarkFile.java:110`) plus `encode(ctx)`.
    static std::expected<ArchiveMarkFile, axc::core::Error> create_or_open(const Options& options,
                                                                           const HeaderValues& values);

    /// `ArchiveMarkFile(File directory, String filename, EpochClock, timeoutMs, logger)`
    /// (`ArchiveMarkFile.java:223`): open an existing mark file and validate its
    /// major version.
    static std::expected<ArchiveMarkFile, axc::core::Error> open_existing(const Options& options);

    /// Same, with a caller-supplied version check (`ArchiveMarkFile.java:249`).
    static std::expected<ArchiveMarkFile, axc::core::Error> open_existing(
        const Options& options, const std::function<void(std::int32_t)>& version_check);

    ArchiveMarkFile() = default;
    ArchiveMarkFile(ArchiveMarkFile&& other) noexcept;
    ArchiveMarkFile& operator=(ArchiveMarkFile&& other) noexcept;
    ~ArchiveMarkFile() = default;

    ArchiveMarkFile(const ArchiveMarkFile&) = delete;
    ArchiveMarkFile& operator=(const ArchiveMarkFile&) = delete;

    [[nodiscard]] bool is_closed() const noexcept
    {
        return !mark_file_.valid();
    }

    void close();

    /// `ArchiveMarkFile.signalReady(long)` (`ArchiveMarkFile.java:342`).
    void signal_ready(std::int64_t activity_timestamp);

    /// `ArchiveMarkFile.signalTerminated` (`ArchiveMarkFile.java:355`).
    void signal_terminated();

    /// `ArchiveMarkFile.updateActivityTimestamp` (`ArchiveMarkFile.java:365`).
    void update_activity_timestamp(std::int64_t now_ms);

    /// `ArchiveMarkFile.activityTimestampVolatile` (`ArchiveMarkFile.java:378`).
    [[nodiscard]] std::int64_t activity_timestamp_volatile() const;

    [[nodiscard]] std::int32_t version() const;

    /// `ArchiveMarkFile.force` (`ArchiveMarkFile.java:454`).
    [[nodiscard]] std::expected<void, axc::core::Error> force();

    [[nodiscard]] std::span<std::uint8_t> buffer() noexcept;
    [[nodiscard]] std::span<std::uint8_t> error_buffer() noexcept;
    [[nodiscard]] std::int32_t header_offset() const noexcept
    {
        return header_offset_;
    }

    /// `ArchiveMarkFile.archiveId` returns `long` (`ArchiveMarkFile.java:318`).
    [[nodiscard]] std::int64_t archive_id() const;
    [[nodiscard]] std::int32_t pid() const;
    [[nodiscard]] std::int64_t start_timestamp() const;
    [[nodiscard]] std::int32_t control_stream_id() const;
    [[nodiscard]] std::int32_t local_control_stream_id() const;
    [[nodiscard]] std::int32_t events_stream_id() const;
    [[nodiscard]] std::int32_t header_length() const;
    [[nodiscard]] std::int32_t error_buffer_length() const;
    [[nodiscard]] std::string control_channel() const;
    [[nodiscard]] std::string local_control_channel() const;
    [[nodiscard]] std::string events_channel() const;
    [[nodiscard]] std::string aeron_directory() const;

private:
    void refresh_header() noexcept;
    void apply(const HeaderValues& values);

    axc::core::MarkFile mark_file_;
    std::int32_t header_offset_{HEADER_OFFSET};
    std::int32_t error_buffer_length_{0};
    archive_mark_codecs::MarkFileHeader header_{};
};

}  // namespace deepmsg::archive::server
