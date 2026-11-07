#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <string>

#include "archive/io/segment_reader.h"
#include "archive/server/catalog.h"
#include "core/error/error.h"

namespace deepmsg::archive::server
{

/// The reference's `SimpleFragmentHandler.onFragment` (`RecordingReader.java:162`):
/// the frame type, flags and reserved value come along with the payload.
class FragmentHandler
{
public:
    FragmentHandler() = default;
    virtual ~FragmentHandler() = default;

    FragmentHandler(const FragmentHandler&) = delete;
    FragmentHandler& operator=(const FragmentHandler&) = delete;

    virtual void on_fragment(const std::uint8_t* buffer, std::int32_t offset, std::int32_t length,
                             std::int32_t frame_type, std::uint8_t flags, std::int64_t reserved_value) = 0;
};

/// `RecordingReader` (`RecordingReader.java:41-221`): replays the frames of a
/// recording from a `(position, length)` window, mapping one segment file at a
/// time and stepping a term view through it.
class RecordingReader
{
public:
    static constexpr std::int64_t NULL_POSITION = -1;
    static constexpr std::int64_t NULL_LENGTH = -1;

    static std::expected<RecordingReader, axc::core::Error> create(const RecordingSummary& summary,
                                                                   std::string archive_dir, std::int64_t position,
                                                                   std::int64_t length);

    RecordingReader() = default;
    RecordingReader(RecordingReader&& other) noexcept;
    RecordingReader& operator=(RecordingReader&& other) noexcept;
    ~RecordingReader();

    RecordingReader(const RecordingReader&) = delete;
    RecordingReader& operator=(const RecordingReader&) = delete;

    [[nodiscard]] std::expected<void, axc::core::Error> close();

    [[nodiscard]] std::int64_t replay_position() const noexcept
    {
        return replay_position_;
    }

    [[nodiscard]] bool is_done() const noexcept
    {
        return is_done_;
    }

    /// `RecordingReader.poll` (`RecordingReader.java:133`). The reference throws
    /// when the next segment is missing; the error comes back here instead.
    [[nodiscard]] std::expected<int, axc::core::Error> poll(FragmentHandler& handler, int fragment_limit);

private:
    [[nodiscard]] std::expected<void, axc::core::Error> next_term();
    void close_recording_segment() noexcept;
    [[nodiscard]] std::expected<void, axc::core::Error> open_recording_segment();

    [[nodiscard]] std::span<std::uint8_t> term_buffer() const noexcept
    {
        if (term_base_segment_offset_ < 0 ||
            static_cast<std::size_t>(term_base_segment_offset_ + term_length_) > segment_.size())
        {
            return {};
        }
        return segment_.subspan(static_cast<std::size_t>(term_base_segment_offset_),
                                static_cast<std::size_t>(term_length_));
    }

    std::string archive_dir_;
    std::int64_t recording_id_{0};
    std::int32_t segment_length_{0};
    std::int32_t term_length_{0};
    std::int32_t stream_id_{0};

    std::unique_ptr<io::SegmentReader> reader_;
    std::span<std::uint8_t> segment_{};

    std::int64_t replay_limit_{0};
    std::int64_t replay_position_{0};
    std::int64_t segment_file_position_{0};
    std::int32_t term_offset_{0};
    std::int32_t term_base_segment_offset_{0};
    bool is_done_{false};
};

}  // namespace deepmsg::archive::server
