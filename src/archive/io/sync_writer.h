#pragma once

#include "archive/io/segment_writer.h"

namespace deepmsg::archive::server::io
{

/// The synchronous write backend: every `submit` writes the whole block with
/// `pwrite`, forces when `file.sync.level` asks for it, and only then moves
/// `committed_position()`. It is the reference's `RecordingWriter.onBlock`
/// (`RecordingWriter.java:108-151`) with the file handling moved behind the
/// `SegmentWriter` seam.
class SyncWriter final : public SegmentWriter
{
public:
    explicit SyncWriter(WriterConfig config) noexcept;

    ~SyncWriter() override;

    SyncWriter(const SyncWriter&) = delete;
    SyncWriter& operator=(const SyncWriter&) = delete;

    /// Opens (creating and sizing) the segment file named by the config.
    [[nodiscard]] std::expected<void, axc::core::Error> open() noexcept;

    [[nodiscard]] std::expected<bool, axc::core::Error> submit(const BlockWrite& block) noexcept override;

    int poll_completions() noexcept override
    {
        return 0;
    }

    [[nodiscard]] std::int64_t committed_position() const noexcept override
    {
        return committed_position_;
    }

    [[nodiscard]] std::expected<void, axc::core::Error> roll_segment(std::int64_t next_base_position) noexcept override;

    [[nodiscard]] std::expected<void, axc::core::Error> close() noexcept override;

    /// Number of file forces issued (test observable, like `Catalog::force_count`).
    [[nodiscard]] std::uint64_t force_count() const noexcept
    {
        return force_count_;
    }

    /// Number of archive-directory forces issued when a segment was created.
    [[nodiscard]] std::uint64_t directory_force_count() const noexcept
    {
        return directory_force_count_;
    }

    /// Whether a requested preallocation was refused and the segment stayed
    /// sparse (test observable; the warning goes to stderr once).
    [[nodiscard]] bool fallocate_fell_back() const noexcept
    {
        return fallocate_warning_emitted_;
    }

private:
    [[nodiscard]] std::expected<void, axc::core::Error> open_segment(std::int64_t segment_base_position,
                                                                     bool fail_if_exists) noexcept;
    void close_fd() noexcept;

    WriterConfig config_;
    std::int64_t segment_base_position_{0};
    std::int64_t committed_position_{0};
    int fd_{-1};
    bool closed_{false};
    std::uint64_t force_count_{0};
    std::uint64_t directory_force_count_{0};
    bool fallocate_warning_emitted_{false};
};

}  // namespace deepmsg::archive::server::io
