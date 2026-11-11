#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <span>
#include <vector>

#include "archive/io/segment_reader.h"

namespace deepmsg::archive::server::io
{

/// The `pread` backend (A6-5): one copy into an internal buffer, no mapping, so
/// the page-cache footprint is bounded by the buffer rather than by the segment.
class PreadReader final : public SegmentReader
{
public:
    explicit PreadReader(ReaderConfig config, std::size_t buffer_length = 1024 * 1024) noexcept;

    ~PreadReader() override;

    PreadReader(const PreadReader&) = delete;
    PreadReader& operator=(const PreadReader&) = delete;

    [[nodiscard]] std::expected<void, axc::core::Error> open() noexcept;

    [[nodiscard]] std::expected<std::span<std::uint8_t>, axc::core::Error> read(std::int64_t file_offset,
                                                                                std::int32_t max_len) noexcept override;

    [[nodiscard]] std::expected<void, axc::core::Error> roll_segment(std::int64_t next_base_position) noexcept override;

    [[nodiscard]] std::expected<void, axc::core::Error> close() noexcept override;

private:
    void close_fd() noexcept;

    ReaderConfig config_;
    std::vector<std::uint8_t> buffer_;
    int fd_{-1};
};

/// `buffer_length` is the caller's read window: `ReplaySession` passes the
/// per-session slice of `ctx.replayBuffer` so the copy buffer is no larger than
/// what the frame loop may ask for.
[[nodiscard]] std::expected<std::unique_ptr<SegmentReader>, axc::core::Error> make_pread_reader(
    const ReaderConfig& config, std::size_t buffer_length = 1024 * 1024);

}  // namespace deepmsg::archive::server::io
