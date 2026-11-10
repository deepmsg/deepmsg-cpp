#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <span>
#include <string>
#include <string_view>

#include "archive/io/segment_writer.h"
#include "archive/server/checksum.h"
#include "core/error/error.h"

namespace deepmsg::archive::server
{

/// Nanosecond clock the writer samples around each block; injectable so the
/// write-duration metric can be pinned in a test. The interface lives in the io
/// layer because an asynchronous backend times its completions there.
using NanoTimeSource = io::NanoClock;

class SystemNanoTime final : public NanoTimeSource
{
public:
    [[nodiscard]] std::int64_t nano_time() const noexcept override;
};

/// The reference's `ArchiveConductor.Recorder` metrics that the writer feeds
/// (`RecordingWriter.java:144-145`). Shared with the asynchronous backends,
/// which report from their completions instead of `on_block`.
using RecordingMetrics = io::WriteMetrics;

/// The values `RecordingWriter` takes from `Archive.Context`
/// (`RecordingWriter.java:84-96`).
struct RecordingWriterContext
{
    std::string archive_dir;
    int file_sync_level{0};
    int archive_dir_fd{-1};
    /// `ctx.recordChecksum()`; nullptr means "do not checksum" (the default).
    const Checksum* record_checksum{nullptr};
    /// `ctx.recordChecksumBuffer()`; must be at least one term long when a
    /// checksum is configured.
    std::span<std::uint8_t> record_checksum_buffer{};
    const NanoTimeSource* nano_clock{nullptr};
    /// `deepmsg.archive.file.io.backend`: `sync` (default) or `uring`.
    /// C++-only; see `docs/compat.md`.
    std::string io_backend{"sync"};
    /// `deepmsg.archive.file.io.inflight.max`.
    int io_inflight_max{4};
    /// `deepmsg.archive.file.io.uring.copy_before_submit`.
    bool io_copy_before_submit{true};
    /// The recorder's `block_length_limit`; sizes the async copy buffers.
    std::int32_t max_block_length{0};
};

/// `RecordingWriter` (`RecordingWriter.java:53-274`): writes a term buffer's
/// blocks into `<recordingId>-<segmentBasePosition>.rec` files, rolling to the
/// next segment when the current one is full.
///
/// Deviations, recorded in `records/a1b.md`: the file handling lives behind the
/// `SegmentWriter` seam (`impl-archive` §4.1), so `on_block` hands the backend a
/// `BlockWrite` instead of writing the channel itself; the reference's
/// low-storage-space classification needs `ArchiveFileStore`, which lands with
/// A2a.
class RecordingWriter
{
public:
    RecordingWriter(std::int64_t recording_id, std::int64_t start_position, std::int32_t segment_length,
                    std::int32_t term_length, std::int64_t join_position, RecordingWriterContext context,
                    RecordingMetrics* metrics);

    RecordingWriter(const RecordingWriter&) = delete;
    RecordingWriter& operator=(const RecordingWriter&) = delete;
    RecordingWriter(RecordingWriter&&) noexcept = default;
    RecordingWriter& operator=(RecordingWriter&&) noexcept = default;

    /// `RecordingWriter.init` (`RecordingWriter.java:188`): create the first
    /// segment file and position it at the join offset.
    [[nodiscard]] std::expected<void, axc::core::Error> init();

    /// `RecordingWriter.onBlock` (`RecordingWriter.java:108`).
    [[nodiscard]] std::expected<void, axc::core::Error> on_block(const std::uint8_t* term_buffer,
                                                                 std::int32_t term_offset, std::int32_t length,
                                                                 std::int32_t session_id, std::int32_t term_id);

    /// `RecordingWriter.close` (`RecordingWriter.java:174`): closes the file
    /// without forcing, like the reference.
    [[nodiscard]] std::expected<void, axc::core::Error> close();

    /// `RecordingWriter.position` (`RecordingWriter.java:183`): the published
    /// recording position. With an asynchronous backend this is the committed
    /// position, not the handed one.
    [[nodiscard]] std::int64_t position() const noexcept
    {
        return committed_position();
    }

    [[nodiscard]] std::int64_t committed_position() const noexcept;

    /// False when the backend has no free in-flight slot.
    [[nodiscard]] bool has_capacity() const noexcept;

    /// Drains the backend's completions; `-1` after a failure.
    int poll_completions() noexcept;

    [[nodiscard]] std::expected<void, axc::core::Error> take_error() const noexcept;

    /// `sync`, `uring` or `sync (fallback)`.
    [[nodiscard]] std::string_view backend_name() const noexcept
    {
        return backend_name_;
    }

private:
    /// `RecordingWriter.computeChecksum` (`RecordingWriter.java:198`).
    void compute_checksum(std::uint8_t* buffer, std::int32_t length) noexcept;

    /// `RecordingWriter.onFileRollOver` (`RecordingWriter.java:234`).
    [[nodiscard]] std::expected<void, axc::core::Error> on_file_roll_over();

    std::int64_t recording_id_;
    std::int32_t segment_length_;
    RecordingWriterContext context_;
    RecordingMetrics* metrics_{nullptr};

    std::int64_t segment_base_position_{0};
    std::int64_t join_position_{0};
    std::int32_t segment_offset_{0};
    std::string backend_name_{"sync"};
    std::unique_ptr<io::SegmentWriter> writer_;
    bool closed_{false};
};

}  // namespace deepmsg::archive::server
