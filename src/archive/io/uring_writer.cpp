#include "archive/io/uring_writer.h"

#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <utility>

#include "archive/io/segment_file.h"

namespace deepmsg::archive::server::io
{
namespace
{

[[nodiscard]] axc::core::Error unavailable(std::string_view message) noexcept
{
    return axc::core::make_error(axc::core::ErrorCode::not_found, message);
}

}  // namespace

// ---------------------------------------------------------------------------
// CommitOrder
// ---------------------------------------------------------------------------

CommitOrder::CommitOrder(unsigned capacity, int file_sync_level) noexcept
    : slots_(capacity), file_sync_level_(file_sync_level)
{
}

void CommitOrder::start_at(std::int64_t position) noexcept
{
    committed_position_ = position;
}

std::optional<CommitOrder::SlotHandle> CommitOrder::reserve(std::int32_t length, std::int64_t end_position) noexcept
{
    for (unsigned index = 0; index < slots_.size(); ++index)
    {
        Slot& slot = slots_[index];
        if (slot.in_use)
        {
            continue;
        }

        slot.seq = next_seq_++;
        slot.length = length;
        slot.end_position = end_position;
        slot.write_done = false;
        slot.force_done = false;
        slot.in_use = true;
        ++in_flight_;
        return SlotHandle{index, slot.seq};
    }

    return std::nullopt;
}

CommitOrder::Completion CommitOrder::write_completed(std::uint64_t seq, std::int32_t bytes_written) noexcept
{
    Slot* slot = find(seq);
    if (nullptr == slot)
    {
        return Completion::unknown;
    }
    if (slot->length != bytes_written)
    {
        return Completion::wrong_length;
    }

    slot->write_done = true;
    commit_ready();
    return Completion::accepted;
}

bool CommitOrder::force_completed(std::uint64_t seq) noexcept
{
    Slot* slot = find(seq);
    if (nullptr == slot)
    {
        return false;
    }

    slot->force_done = true;
    commit_ready();
    return true;
}

CommitOrder::Slot* CommitOrder::find(std::uint64_t seq) noexcept
{
    for (Slot& slot : slots_)
    {
        if (slot.in_use && slot.seq == seq)
        {
            return &slot;
        }
    }
    return nullptr;
}

void CommitOrder::commit_ready() noexcept
{
    for (;;)
    {
        Slot* slot = find(next_commit_seq_);
        if (nullptr == slot || !slot->write_done)
        {
            return;
        }
        if (file_sync_level_ > 0 && !slot->force_done)
        {
            return;
        }

        committed_position_ = slot->end_position;
        slot->in_use = false;
        --in_flight_;
        ++next_commit_seq_;
        ++completed_blocks_;
    }
}

// ---------------------------------------------------------------------------
// UringWriter
// ---------------------------------------------------------------------------

UringWriter::UringWriter(WriterConfig config, UringRing ring) noexcept
    : config_(std::move(config)),
      ring_(std::move(ring)),
      order_(static_cast<unsigned>(config_.inflight_max), config_.file_sync_level)
{
    buffers_.resize(static_cast<std::size_t>(config_.inflight_max));
    for (auto& buffer : buffers_)
    {
        buffer.resize(static_cast<std::size_t>(config_.max_block_length));
    }
}

UringWriter::~UringWriter()
{
    (void)close();
}

std::expected<void, axc::core::Error> UringWriter::open() noexcept
{
    auto opened = open_segment_file(config_, config_.segment_base_position, false);
    if (!opened)
    {
        return std::unexpected(opened.error());
    }

    fd_ = opened->fd;
    if (opened->directory_forced)
    {
        ++directory_force_count_;
    }
    order_.start_at(config_.start_position);
    return {};
}

std::expected<bool, axc::core::Error> UringWriter::submit(const BlockWrite& block) noexcept
{
    if (!failure_.ok())
    {
        return std::unexpected(failure_);
    }
    if (fd_ < 0)
    {
        fail(axc::core::ErrorCode::internal_error, "segment writer is not open");
        return std::unexpected(failure_);
    }
    if (block.len <= 0)
    {
        return true;
    }
    if (block.len > config_.max_block_length)
    {
        fail(axc::core::ErrorCode::invalid_argument, "block exceeds the writer's maximum block length");
        return std::unexpected(failure_);
    }

    auto handle = order_.reserve(block.len, block.end_position);
    if (!handle)
    {
        return false;
    }

    const std::uint8_t* source = block.src;
    if (config_.copy_before_submit)
    {
        // Option ①: the bytes leave the term buffer before `submit` returns,
        // so the recorder may advance its subscriber position immediately.
        std::memcpy(buffers_[handle->slot].data(), block.src, static_cast<std::size_t>(block.len));
        source = buffers_[handle->slot].data();
    }

    const bool link = config_.file_sync_level > 0;
    auto queued = ring_.queue_write(fd_, static_cast<std::uint64_t>(block.file_offset), source,
                                    static_cast<std::uint32_t>(block.len), entry_id(handle->seq, false), link);
    if (!queued)
    {
        fail(queued.error().code, queued.error().message.view(), queued.error().errcode);
        return std::unexpected(failure_);
    }

    if (link)
    {
        ++force_count_;
        auto forced = ring_.queue_fsync(fd_, 1 == config_.file_sync_level, entry_id(handle->seq, true));
        if (!forced)
        {
            fail(forced.error().code, forced.error().message.view(), forced.error().errcode);
            return std::unexpected(failure_);
        }
    }

    auto submitted = ring_.submit();
    if (!submitted)
    {
        fail(submitted.error().code, submitted.error().message.view(), submitted.error().errcode);
        return std::unexpected(failure_);
    }
    return true;
}

int UringWriter::poll_completions() noexcept
{
    int completed = 0;

    UringCompletion completion{};
    while (ring_.peek(completion))
    {
        process(completion);
        ++completed;
    }

    return failure_.ok() ? completed : -1;
}

void UringWriter::process(const UringCompletion& completion) noexcept
{
    if (!failure_.ok())
    {
        // The session is already stopping; keep draining so no slot leaks into
        // a later roll/close decision.
        return;
    }

    const std::uint64_t seq = completion.user_data >> 1;
    const bool is_force = 0 != (completion.user_data & 1u);

    if (is_force)
    {
        // A failed write cancels its linked force (`-ECANCELED`); the write
        // itself already reported the failure, and the reference treats the
        // cancel as part of it. Leave the slot alone: the writer is poisoned
        // and no position may move past it.
        if (completion.result < 0)
        {
            if (-ECANCELED != completion.result)
            {
                fail(axc::core::ErrorCode::io_error, "segment force failed", -completion.result);
            }
            return;
        }
        if (!order_.force_completed(seq))
        {
            fail(axc::core::ErrorCode::internal_error, "completion for an unknown request");
        }
        return;
    }

    if (completion.result < 0)
    {
        fail(axc::core::ErrorCode::io_error, "segment write failed", -completion.result);
        return;
    }

    switch (order_.write_completed(seq, completion.result))
    {
        case CommitOrder::Completion::accepted:
            break;
        case CommitOrder::Completion::unknown:
            fail(axc::core::ErrorCode::internal_error, "completion for an unknown request");
            break;
        case CommitOrder::Completion::wrong_length:
            fail(axc::core::ErrorCode::io_error, "short segment write");
            break;
    }
}

std::expected<void, axc::core::Error> UringWriter::roll_segment(std::int64_t next_base_position) noexcept
{
    if (!failure_.ok())
    {
        return std::unexpected(failure_);
    }

    auto drained = drain();
    if (!drained)
    {
        return std::unexpected(drained.error());
    }

    close_fd();

    // `RecordingWriter.java:242`: the next segment must not already exist.
    auto opened = open_segment_file(config_, next_base_position, true);
    if (!opened)
    {
        fail(opened.error().code, opened.error().message.view(), opened.error().errcode);
        return std::unexpected(failure_);
    }

    fd_ = opened->fd;
    if (opened->directory_forced)
    {
        ++directory_force_count_;
    }
    return {};
}

std::expected<void, axc::core::Error> UringWriter::close() noexcept
{
    closed_ = true;

    auto drained = drain();
    close_fd();
    return drained;
}

std::expected<void, axc::core::Error> UringWriter::take_error() const noexcept
{
    if (!failure_.ok())
    {
        return std::unexpected(failure_);
    }
    return {};
}

std::expected<void, axc::core::Error> UringWriter::drain() noexcept
{
    while (order_.in_flight() > 0)
    {
        if (!failure_.ok())
        {
            return std::unexpected(failure_);
        }

        auto waited = ring_.wait(1);
        if (!waited)
        {
            fail(waited.error().code, waited.error().message.view(), waited.error().errcode);
            return std::unexpected(failure_);
        }
        (void)poll_completions();
    }

    if (!failure_.ok())
    {
        return std::unexpected(failure_);
    }
    return {};
}

void UringWriter::fail(axc::core::ErrorCode code, std::string_view message, std::int32_t errcode) noexcept
{
    if (failure_.ok())
    {
        failure_ = axc::core::make_error(code, message, errcode);
    }
}

void UringWriter::close_fd() noexcept
{
    if (fd_ >= 0)
    {
        ::close(fd_);
        fd_ = -1;
    }
}

std::expected<std::unique_ptr<SegmentWriter>, axc::core::Error> make_uring_writer(const WriterConfig& config)
{
    if (config.inflight_max <= 0)
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::invalid_argument,
                                                     "the io_uring writer needs an in-flight budget"));
    }
    if (config.max_block_length <= 0)
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::invalid_argument,
                                                     "the io_uring writer needs a maximum block length"));
    }
    if (!UringRing::is_available())
    {
        return std::unexpected(unavailable("io_uring is not available on this kernel"));
    }

    // Each block uses at most two SQEs (write + linked force).
    const auto entries = static_cast<unsigned>(config.inflight_max) * 2u + 2u;
    auto ring = UringRing::create(entries);
    if (!ring)
    {
        return std::unexpected(ring.error());
    }

    auto writer = std::make_unique<UringWriter>(config, std::move(*ring));
    auto opened = writer->open();
    if (!opened)
    {
        return std::unexpected(opened.error());
    }

    return std::unique_ptr<SegmentWriter>{std::move(writer)};
}

}  // namespace deepmsg::archive::server::io
