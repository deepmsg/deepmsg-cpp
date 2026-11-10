#pragma once

#include <cstdint>
#include <expected>
#include <optional>
#include <string_view>
#include <vector>

#include "archive/io/segment_writer.h"
#include "archive/io/uring_ring.h"
#include "core/error/error.h"

namespace deepmsg::archive::server::io
{

/// The position-ordering rule of an asynchronous write backend.
///
/// A block may complete out of order (the kernel is free to finish later
/// requests first), but `committed_position()` only moves over the contiguous
/// prefix of blocks whose write — and, when `file_sync_level > 0`, whose force —
/// has completed. A completion that arrives early waits for its predecessors.
///
/// This is the A6b-2 criterion in one place: the writer feeds it from CQEs and
/// the unit test feeds it completions in any order it likes.
class CommitOrder
{
public:
    enum class Completion
    {
        accepted,
        unknown,
        wrong_length
    };

    struct SlotHandle
    {
        unsigned slot{0};
        std::uint64_t seq{0};
    };

    CommitOrder(unsigned capacity, int file_sync_level) noexcept;

    /// The position published before any block completes (the join position).
    void start_at(std::int64_t position) noexcept;

    /// Takes a slot for a block; `std::nullopt` when every slot is in flight.
    [[nodiscard]] std::optional<SlotHandle> reserve(std::int32_t length, std::int64_t end_position) noexcept;

    [[nodiscard]] Completion write_completed(std::uint64_t seq, std::int32_t bytes_written) noexcept;
    [[nodiscard]] bool force_completed(std::uint64_t seq) noexcept;

    [[nodiscard]] std::int64_t committed_position() const noexcept
    {
        return committed_position_;
    }

    [[nodiscard]] unsigned in_flight() const noexcept
    {
        return in_flight_;
    }

    [[nodiscard]] std::uint64_t completed_blocks() const noexcept
    {
        return completed_blocks_;
    }

private:
    struct Slot
    {
        std::uint64_t seq{0};
        std::int64_t end_position{0};
        std::int32_t length{0};
        bool write_done{false};
        bool force_done{false};
        bool in_use{false};
    };

    [[nodiscard]] Slot* find(std::uint64_t seq) noexcept;
    void commit_ready() noexcept;

    std::vector<Slot> slots_;
    int file_sync_level_{0};
    std::uint64_t next_seq_{0};
    std::uint64_t next_commit_seq_{0};
    std::int64_t committed_position_{0};
    unsigned in_flight_{0};
    std::uint64_t completed_blocks_{0};
};

/// The io_uring write backend (`impl-archive` §4.1 option ①).
///
/// Every block is copied into a private slot buffer before it is submitted
/// (`copy_before_submit`), so the recorder may hand the writer a term buffer
/// and move the subscriber position as soon as `submit` returns: the bytes are
/// already ours. At `file_sync_level > 0` the write is linked to an
/// `IORING_OP_FSYNC` (`IOSQE_IO_LINK`), and a failed write cancels the force
/// (`-ECANCELED`), which is the reference's "write failure ends the session".
///
/// A failed completion poisons the writer: every later `submit`/`poll` reports
/// the same error through `take_error()`.
class UringWriter final : public SegmentWriter
{
public:
    UringWriter(WriterConfig config, UringRing ring) noexcept;

    ~UringWriter() override;

    UringWriter(const UringWriter&) = delete;
    UringWriter& operator=(const UringWriter&) = delete;

    /// Opens (creating and sizing) the first segment file.
    [[nodiscard]] std::expected<void, axc::core::Error> open() noexcept;

    [[nodiscard]] std::expected<bool, axc::core::Error> submit(const BlockWrite& block) noexcept override;

    /// Non-blocking drain of the completion queue; `-1` once the writer has
    /// failed (the error is in `take_error()`).
    int poll_completions() noexcept override;

    [[nodiscard]] std::int64_t committed_position() const noexcept override
    {
        return order_.committed_position();
    }

    [[nodiscard]] bool has_capacity() const noexcept override
    {
        return order_.in_flight() < static_cast<unsigned>(config_.inflight_max);
    }

    [[nodiscard]] bool is_asynchronous() const noexcept override
    {
        return true;
    }

    [[nodiscard]] std::expected<void, axc::core::Error> roll_segment(std::int64_t next_base_position) noexcept override;

    [[nodiscard]] std::expected<void, axc::core::Error> close() noexcept override;

    /// The sticky failure, if any.
    [[nodiscard]] std::expected<void, axc::core::Error> take_error() const noexcept override;

    [[nodiscard]] std::uint64_t completed_blocks() const noexcept
    {
        return order_.completed_blocks();
    }

    /// Forces the backend issued (`file.sync.level > 0`).
    [[nodiscard]] std::uint64_t force_count() const noexcept
    {
        return force_count_;
    }

    [[nodiscard]] std::uint64_t directory_force_count() const noexcept
    {
        return directory_force_count_;
    }

private:
    /// `user_data` carries the sequence (ordering), the slot (metrics and the
    /// copy buffer) and the operation kind.
    static std::uint64_t entry_id(unsigned slot, std::uint64_t seq, bool force) noexcept
    {
        return (seq << 8) | (static_cast<std::uint64_t>(slot) << 1) | (force ? 1u : 0u);
    }

    void process(const UringCompletion& completion) noexcept;

    /// Waits until every submitted block has completed (or the writer failed).
    [[nodiscard]] std::expected<void, axc::core::Error> drain() noexcept;

    /// Records the first failure; later calls keep it.
    void fail(axc::core::ErrorCode code, std::string_view message, std::int32_t errcode = 0) noexcept;

    void close_fd() noexcept;

    struct Slot
    {
        std::vector<std::uint8_t> buffer;
        std::uint32_t length{0};
        std::uint64_t seq{0};
        std::int64_t submitted_ns{0};
    };

    /// Feeds the completion metrics (`bytes_written` / `write_time_ns`).
    void report_written(const Slot& slot) noexcept;

    WriterConfig config_;
    UringRing ring_;
    CommitOrder order_;
    std::vector<Slot> slots_;

    int fd_{-1};
    axc::core::Error failure_;
    std::uint64_t force_count_{0};
    std::uint64_t directory_force_count_{0};
    bool closed_{false};
};

}  // namespace deepmsg::archive::server::io
