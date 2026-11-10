#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <string>

#include "core/error/error.h"

namespace deepmsg::archive::server::io
{

/// One block handed to a `SegmentWriter`. `end_position` is the recording
/// position just past the block, i.e. what `committed_position()` reaches once
/// the block (and, at a sync level above zero, its force) has completed.
struct BlockWrite
{
    const std::uint8_t* src{nullptr};
    std::int32_t len{0};
    std::int64_t file_offset{0};
    std::int64_t end_position{0};
};

struct WriterConfig
{
    std::string archive_dir;
    std::int64_t recording_id{0};
    /// Base position of the first segment file (`segment_file_base_position`).
    std::int64_t segment_base_position{0};
    std::int32_t segment_length{0};
    /// `file.sync.level`: 0 = never force, 1 = `fdatasync`, 2 = `fsync`.
    int file_sync_level{0};
    /// Directory channel forced when a segment is created (`RecordingWriter.java:221`);
    /// -1 = none.
    int archive_dir_fd{-1};
    /// Preallocate the segment with `fallocate` instead of `ftruncate` (A6-4).
    bool fallocate{false};
    /// In-flight block budget for asynchronous backends (A6-3); ignored by the
    /// synchronous one.
    int inflight_max{0};
    /// Copy each block into a private buffer before submitting it (A6-3 option 1).
    bool copy_before_submit{false};
    /// Largest block `submit` will be handed (the recorder's
    /// `block_length_limit`); the io_uring backend sizes its copy buffers from
    /// it.
    std::int32_t max_block_length{0};
    /// Recording position the writer starts from (the join position); nothing
    /// is committed before it.
    std::int64_t start_position{0};
};

/// `/home/gavin/trader/analysis/cpp/impl-2026-11-07-cpp-archive.md` §4.1.
///
/// Invariant: `committed_position() <=` the position actually written, and, when
/// `file.sync.level > 0`, actually persisted. `RecordingPosition` only ever
/// publishes `committed_position()`.
class SegmentWriter
{
public:
    SegmentWriter() = default;
    virtual ~SegmentWriter() = default;

    SegmentWriter(const SegmentWriter&) = delete;
    SegmentWriter& operator=(const SegmentWriter&) = delete;

    /// Returns false when the in-flight budget is full (back pressure); the
    /// caller then stops polling this pass. The synchronous backend never
    /// returns false.
    [[nodiscard]] virtual std::expected<bool, axc::core::Error> submit(const BlockWrite& block) noexcept = 0;

    /// Completions collected since the last call; always 0 for the synchronous
    /// backend.
    virtual int poll_completions() noexcept = 0;

    [[nodiscard]] virtual std::int64_t committed_position() const noexcept = 0;

    /// Close the current segment file and open `next_base_position`. Fails if
    /// that file already exists (`RecordingWriter.java:242`).
    [[nodiscard]] virtual std::expected<void, axc::core::Error> roll_segment(
        std::int64_t next_base_position) noexcept = 0;

    /// Close the current file. Like the reference, this does **not** force.
    [[nodiscard]] virtual std::expected<void, axc::core::Error> close() noexcept = 0;
};

/// Default backend (`RecordingWriter.java`'s `FileChannel`): `pwrite` plus
/// `fdatasync`/`fsync` per `file.sync.level`.
[[nodiscard]] std::expected<std::unique_ptr<SegmentWriter>, axc::core::Error> make_sync_writer(
    const WriterConfig& config);

/// io_uring backend (A6-3). Returns an error when the runtime probe fails; the
/// caller falls back to `make_sync_writer` and records one event
/// (`impl-archive` §4.1).
[[nodiscard]] std::expected<std::unique_ptr<SegmentWriter>, axc::core::Error> make_uring_writer(
    const WriterConfig& config);

}  // namespace deepmsg::archive::server::io
