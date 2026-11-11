#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <span>

#include "archive/io/segment_reader.h"

namespace deepmsg::archive::server::io
{

/// Read-only backend used by the replay path: the segment file is mapped
/// `MAP_PRIVATE` (`RecordingReader.java:214`), so a frame-header rewrite made by
/// the replay session copies only the touched page and never reaches the file.
class MmapReader final : public SegmentReader
{
public:
    explicit MmapReader(ReaderConfig config) noexcept;

    ~MmapReader() override;

    MmapReader(const MmapReader&) = delete;
    MmapReader& operator=(const MmapReader&) = delete;

    [[nodiscard]] std::expected<void, axc::core::Error> open() noexcept;

    [[nodiscard]] std::expected<std::span<std::uint8_t>, axc::core::Error> read(std::int64_t file_offset,
                                                                                std::int32_t max_len) noexcept override;

    [[nodiscard]] std::expected<void, axc::core::Error> roll_segment(std::int64_t next_base_position) noexcept override;

    [[nodiscard]] std::expected<void, axc::core::Error> close() noexcept override;

private:
    void unmap() noexcept;

    ReaderConfig config_;
    std::uint8_t* mapping_{nullptr};
    std::size_t length_{0};
    /// Kept open for the mapping's lifetime so `DONTNEED` can be advised at
    /// unmap time (`posix_fadvise` needs a descriptor).
    int fd_{-1};
};

[[nodiscard]] std::expected<std::unique_ptr<SegmentReader>, axc::core::Error> make_mmap_reader(
    const ReaderConfig& config);

}  // namespace deepmsg::archive::server::io
