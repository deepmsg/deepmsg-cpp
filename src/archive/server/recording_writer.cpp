#include "archive/server/recording_writer.h"

#include <cstdio>
#include <cstring>
#include <utility>

#include "archive/io/sync_writer.h"
#include "archive/server/frame_layout.h"
#include "archive/server/segment_files.h"
#include "core/time/clock.h"

namespace deepmsg::archive::server
{
namespace
{

[[nodiscard]] const NanoTimeSource& system_nano_clock() noexcept
{
    static const SystemNanoTime clock;
    return clock;
}

template <typename T>
[[nodiscard]] T read_value(const std::uint8_t* address) noexcept
{
    T value{};
    std::memcpy(&value, address, sizeof(T));
    return value;
}

template <typename T>
void write_value(std::uint8_t* address, T value) noexcept
{
    std::memcpy(address, &value, sizeof(T));
}

}  // namespace

std::int64_t SystemNanoTime::nano_time() const noexcept
{
    return axc::core::NanoClock::now();
}

RecordingWriter::RecordingWriter(std::int64_t recording_id, std::int64_t start_position, std::int32_t segment_length,
                                 std::int32_t term_length, std::int64_t join_position, RecordingWriterContext context,
                                 RecordingMetrics* metrics)
    : recording_id_(recording_id),
      segment_length_(segment_length),
      context_(std::move(context)),
      metrics_(metrics),
      join_position_(join_position)
{
    // `RecordingWriter.java:98-101`.
    segment_base_position_ = segment_file_base_position(start_position, join_position, term_length, segment_length);
    segment_offset_ = static_cast<std::int32_t>(join_position - segment_base_position_);
}

std::expected<void, axc::core::Error> RecordingWriter::init()
{
    // `RecordingWriter.init` (`RecordingWriter.java:188-196`). Positioning at
    // `segmentOffset` is implicit: every write carries its file offset.
    io::WriterConfig config;
    config.archive_dir = context_.archive_dir;
    config.recording_id = recording_id_;
    config.segment_base_position = segment_base_position_;
    config.segment_length = segment_length_;
    config.file_sync_level = context_.file_sync_level;
    config.archive_dir_fd = context_.archive_dir_fd;
    config.inflight_max = context_.io_inflight_max;
    config.copy_before_submit = context_.io_copy_before_submit;
    config.max_block_length = context_.max_block_length;
    config.start_position = join_position_;
    config.metrics = metrics_;
    const NanoTimeSource& nano_clock = nullptr != context_.nano_clock ? *context_.nano_clock : system_nano_clock();
    config.clock = &nano_clock;

    std::unique_ptr<io::SegmentWriter> selected;
    backend_name_ = "sync";

    if ("uring" == context_.io_backend)
    {
        auto uring = io::make_uring_writer(config);
        if (uring)
        {
            selected = std::move(*uring);
            backend_name_ = "uring";
        }
        else
        {
            // The plan's fallback: keep recording on the reference's file
            // channel, but never silently (`records/a6b.md`).
            const std::string_view reason = uring.error().message.view();
            std::fprintf(stderr, "WARNING: io_uring write backend unavailable (%.*s); falling back to sync\n",
                         static_cast<int>(reason.size()), reason.data());
            backend_name_ = "sync (fallback)";
        }
    }

    if (!selected)
    {
        auto sync = io::make_sync_writer(config);
        if (!sync)
        {
            return std::unexpected(sync.error());
        }
        selected = std::move(*sync);
    }

    writer_ = std::move(selected);
    return {};
}

std::expected<void, axc::core::Error> RecordingWriter::on_block(const std::uint8_t* term_buffer,
                                                                std::int32_t term_offset, std::int32_t length,
                                                                std::int32_t session_id, std::int32_t term_id)
{
    (void)session_id;
    (void)term_id;

    if (!writer_)
    {
        // The reference dereferences a null channel here (`RecordingWriter.java:134`).
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::internal_error, "recording writer used before init"));
    }

    // `RecordingWriter.java:113-129`.
    const bool is_padding_frame =
        PADDING_FRAME_TYPE == read_value<std::uint16_t>(term_buffer + term_offset + FRAME_TYPE_OFFSET);
    const std::int32_t data_length = is_padding_frame ? DATA_HEADER_LENGTH : length;

    const NanoTimeSource& clock = nullptr != context_.nano_clock ? *context_.nano_clock : system_nano_clock();
    const std::int64_t start_ns = clock.nano_time();

    io::BlockWrite block;
    block.len = data_length;
    block.file_offset = segment_offset_;
    block.end_position = segment_base_position_ + segment_offset_ + length;

    if (nullptr == context_.record_checksum || is_padding_frame)
    {
        block.src = term_buffer + term_offset;
    }
    else
    {
        if (context_.record_checksum_buffer.size() < static_cast<std::size_t>(data_length))
        {
            return std::unexpected(axc::core::make_error(axc::core::ErrorCode::invalid_argument,
                                                         "record checksum buffer is smaller than the block"));
        }
        std::memcpy(context_.record_checksum_buffer.data(), term_buffer + term_offset,
                    static_cast<std::size_t>(data_length));
        compute_checksum(context_.record_checksum_buffer.data(), data_length);
        block.src = context_.record_checksum_buffer.data();
    }

    auto submitted = writer_->submit(block);
    if (!submitted)
    {
        return std::unexpected(submitted.error());
    }
    if (!*submitted)
    {
        // The session checks `has_capacity()` before it polls the image, and
        // one poll is at most one block, so this is a contract violation and
        // not something to paper over: the image would move its subscriber
        // position past a block that was never handed to the backend.
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::internal_error,
                                                     "the recording writer ran out of in-flight capacity"));
    }

    // The synchronous backend writes and forces inside `submit`, so this is
    // its write time. An asynchronous one reports from `poll_completions`
    // instead (submit-to-completion, queueing included).
    if (!writer_->is_asynchronous())
    {
        const std::int64_t write_time_ns = clock.nano_time() - start_ns;
        if (nullptr != metrics_)
        {
            metrics_->bytes_written(data_length);
            metrics_->write_time_ns(write_time_ns);
        }
    }

    // `RecordingWriter.java:147-151`: the block advances the position by the
    // *frame* length, which for a padding frame is larger than what was written.
    segment_offset_ += length;
    if (segment_offset_ >= segment_length_)
    {
        return on_file_roll_over();
    }

    return {};
}

std::int64_t RecordingWriter::committed_position() const noexcept
{
    return writer_ ? writer_->committed_position() : segment_base_position_ + segment_offset_;
}

bool RecordingWriter::has_capacity() const noexcept
{
    return !writer_ || writer_->has_capacity();
}

int RecordingWriter::poll_completions() noexcept
{
    return writer_ ? writer_->poll_completions() : 0;
}

std::expected<void, axc::core::Error> RecordingWriter::take_error() const noexcept
{
    return writer_ ? writer_->take_error() : std::expected<void, axc::core::Error>{};
}

std::expected<void, axc::core::Error> RecordingWriter::close()
{
    if (!closed_)
    {
        closed_ = true;
        if (writer_)
        {
            return writer_->close();
        }
    }
    return {};
}

void RecordingWriter::compute_checksum(std::uint8_t* buffer, std::int32_t length) noexcept
{
    // `RecordingWriter.computeChecksum` (`RecordingWriter.java:198-211`): the
    // checksum of each frame body is stored in that frame's session-id field.
    std::int32_t frame_offset = 0;
    while (frame_offset < length)
    {
        const std::int32_t frame_length = read_value<std::int32_t>(buffer + frame_offset);
        const std::int32_t aligned_length = frame_align(frame_length, FRAME_ALIGNMENT);
        const std::int32_t computed = context_.record_checksum->compute(buffer, frame_offset + DATA_HEADER_LENGTH,
                                                                        aligned_length - DATA_HEADER_LENGTH);
        write_value<std::int32_t>(buffer + frame_offset + SESSION_ID_OFFSET, computed);
        frame_offset += aligned_length;
    }
}

std::expected<void, axc::core::Error> RecordingWriter::on_file_roll_over()
{
    // `RecordingWriter.onFileRollOver` (`RecordingWriter.java:234-247`).
    auto rolled = writer_->roll_segment(segment_base_position_ + segment_length_);
    if (!rolled)
    {
        return std::unexpected(rolled.error());
    }

    segment_offset_ = 0;
    segment_base_position_ += segment_length_;
    return {};
}

}  // namespace deepmsg::archive::server
