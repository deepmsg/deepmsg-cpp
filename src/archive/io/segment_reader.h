#pragma once

#include <cstdint>
#include <expected>
#include <span>
#include <string>

#include "core/error/error.h"

namespace deepmsg::archive::server::io
{

struct ReaderConfig
{
    std::string archive_dir;
    std::int64_t recording_id{0};
    /// Base position of the segment the reader starts on.
    std::int64_t segment_base_position{0};
    std::int32_t segment_length{0};
};

/// `/home/gavin/trader/analysis/cpp/impl-2026-11-07-cpp-archive.md` §4.2: the
/// read side of a recording. `read()` hands back a view the caller may rewrite
/// (the replay session stamps the recording's own session and stream ids into
/// each frame header), so an `mmap` backend maps `MAP_PRIVATE` and a `pread`
/// backend copies into its own buffer first.
class SegmentReader
{
public:
    SegmentReader() = default;
    virtual ~SegmentReader() = default;

    SegmentReader(const SegmentReader&) = delete;
    SegmentReader& operator=(const SegmentReader&) = delete;

    /// Content of the segment at `[file_offset, file_offset + max_len)`, clipped
    /// to the segment length. Empty when the offset is past the end.
    [[nodiscard]] virtual std::expected<std::span<std::uint8_t>, axc::core::Error> read(
        std::int64_t file_offset, std::int32_t max_len) noexcept = 0;

    /// Switch to the next segment file. A missing file is an error, as in the
    /// reference (`RecordingReader.java:207-210`).
    [[nodiscard]] virtual std::expected<void, axc::core::Error> roll_segment(
        std::int64_t next_base_position) noexcept = 0;

    [[nodiscard]] virtual std::expected<void, axc::core::Error> close() noexcept = 0;
};

}  // namespace deepmsg::archive::server::io
