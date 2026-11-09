#include "archive/server/recording_reader.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <utility>

#include "archive/io/mmap_reader.h"
#include "archive/server/frame_layout.h"
#include "archive/server/segment_files.h"

namespace deepmsg::archive::server
{
namespace
{

template <typename T>
[[nodiscard]] T read_value(const std::uint8_t* address) noexcept
{
    T value{};
    std::memcpy(&value, address, sizeof(T));
    return value;
}

[[nodiscard]] axc::core::Error invalid(std::string_view message)
{
    return axc::core::make_error(axc::core::ErrorCode::invalid_argument, message);
}

}  // namespace

RecordingReader::RecordingReader(RecordingReader&& other) noexcept = default;

RecordingReader& RecordingReader::operator=(RecordingReader&& other) noexcept = default;

RecordingReader::~RecordingReader()
{
    close_recording_segment();
}

std::expected<RecordingReader, axc::core::Error> RecordingReader::create(const RecordingSummary& summary,
                                                                         std::string archive_dir, std::int64_t position,
                                                                         std::int64_t length)
{
    // `RecordingReader.java:63-70`.
    if (position < NULL_POSITION)
    {
        return std::unexpected(invalid("invalid position: " + std::to_string(position)));
    }
    if (length < NULL_LENGTH)
    {
        return std::unexpected(invalid("invalid length: " + std::to_string(length)));
    }

    RecordingReader reader;
    reader.archive_dir_ = std::move(archive_dir);
    reader.recording_id_ = summary.recording_id;
    reader.term_length_ = summary.term_buffer_length;
    reader.segment_length_ = summary.segment_file_length;
    reader.stream_id_ = summary.stream_id;

    // `RecordingReader.java:78-92`.
    const std::int64_t from_position = NULL_POSITION == position ? summary.start_position : position;
    const std::int64_t max_length = NULL_POSITION != summary.stop_position
                                        ? summary.stop_position - from_position
                                        : std::numeric_limits<std::int64_t>::max() - from_position;

    const std::int64_t replay_length = NULL_LENGTH == length ? max_length : std::min(length, max_length);
    if (replay_length < 0)
    {
        return std::unexpected(invalid("length must be positive"));
    }

    const std::int32_t position_bits = position_bits_to_shift(reader.term_length_);
    const std::int64_t start_term_base_position =
        summary.start_position - (summary.start_position & (reader.term_length_ - 1));
    const auto segment_offset =
        static_cast<std::int32_t>(from_position - start_term_base_position) & (reader.segment_length_ - 1);
    const auto term_id = static_cast<std::int32_t>(from_position >> position_bits) + summary.initial_term_id;

    reader.segment_file_position_ =
        segment_file_base_position(summary.start_position, from_position, reader.term_length_, reader.segment_length_);

    auto opened = reader.open_recording_segment();
    if (!opened)
    {
        return std::unexpected(opened.error());
    }

    reader.term_offset_ = static_cast<std::int32_t>(from_position & (reader.term_length_ - 1));
    reader.term_base_segment_offset_ = segment_offset - reader.term_offset_;

    // `RecordingReader.java:101-108`: a start position inside a recording has to
    // land on a frame boundary of a frame from this recording.
    if (from_position > summary.start_position)
    {
        const std::span<std::uint8_t> term = reader.term_buffer();
        const auto offset = static_cast<std::size_t>(reader.term_offset_);
        if (term.empty() ||
            read_value<std::int32_t>(term.data() + offset + TERM_OFFSET_FIELD_OFFSET) != reader.term_offset_ ||
            read_value<std::int32_t>(term.data() + offset + TERM_ID_OFFSET) != term_id ||
            read_value<std::int32_t>(term.data() + offset + STREAM_ID_OFFSET) != reader.stream_id_)
        {
            reader.close_recording_segment();
            return std::unexpected(invalid(std::to_string(from_position) + " position not aligned to valid fragment"));
        }
    }

    reader.replay_position_ = from_position;
    reader.replay_limit_ = from_position + replay_length;
    return reader;
}

std::expected<void, axc::core::Error> RecordingReader::close()
{
    close_recording_segment();
    return {};
}

std::expected<int, axc::core::Error> RecordingReader::poll(FragmentHandler& handler, int fragment_limit)
{
    // `RecordingReader.poll` (`RecordingReader.java:133-177`).
    int fragments = 0;

    while (replay_position_ < replay_limit_ && fragments < fragment_limit)
    {
        if (term_offset_ == term_length_)
        {
            if (auto advanced = next_term(); !advanced)
            {
                is_done_ = true;
                return std::unexpected(advanced.error());
            }
        }

        std::span<std::uint8_t> term = term_buffer();
        if (term.empty())
        {
            is_done_ = true;
            close_recording_segment();
            break;
        }

        const auto frame_offset = static_cast<std::size_t>(term_offset_);
        const std::int32_t frame_length = read_value<std::int32_t>(term.data() + frame_offset);
        if (frame_length <= 0)
        {
            is_done_ = true;
            close_recording_segment();
            break;
        }

        const auto frame_type =
            static_cast<std::int32_t>(read_value<std::uint16_t>(term.data() + frame_offset + FRAME_TYPE_OFFSET));
        const auto flags = term[frame_offset + FRAME_FLAGS_OFFSET];
        const std::int64_t reserved_value =
            read_value<std::int64_t>(term.data() + frame_offset + RESERVED_VALUE_OFFSET);

        const std::int32_t aligned_length = frame_align(frame_length, FRAME_ALIGNMENT);
        const std::int32_t data_offset = term_offset_ + DATA_HEADER_LENGTH;
        const std::int32_t data_length = frame_length - DATA_HEADER_LENGTH;

        handler.on_fragment(term.data(), data_offset, data_length, frame_type, flags, reserved_value);

        replay_position_ += aligned_length;
        term_offset_ += aligned_length;
        fragments++;

        if (replay_position_ >= replay_limit_)
        {
            is_done_ = true;
            close_recording_segment();
            break;
        }
    }

    return fragments;
}

std::expected<void, axc::core::Error> RecordingReader::next_term()
{
    // `RecordingReader.nextTerm` (`RecordingReader.java:179-193`).
    term_offset_ = 0;
    term_base_segment_offset_ += term_length_;

    if (term_base_segment_offset_ == segment_length_)
    {
        close_recording_segment();
        segment_file_position_ += segment_length_;

        // A missing successor segment means the recording ends here; the
        // reference throws from `openRecordingSegment`, which the caller sees on
        // the next poll as an empty term view.
        if (auto opened = open_recording_segment(); !opened)
        {
            is_done_ = true;
            return std::unexpected(opened.error());
        }
        term_base_segment_offset_ = 0;
    }

    return {};
}

void RecordingReader::close_recording_segment() noexcept
{
    // The backend's destructor unmaps the segment.
    reader_.reset();
    segment_ = {};
}

std::expected<void, axc::core::Error> RecordingReader::open_recording_segment()
{
    io::ReaderConfig config;
    config.archive_dir = archive_dir_;
    config.recording_id = recording_id_;
    config.segment_base_position = segment_file_position_;
    config.segment_length = segment_length_;

    auto reader = io::make_mmap_reader(config);
    if (!reader)
    {
        return std::unexpected(reader.error());
    }
    reader_ = std::move(*reader);

    auto segment = reader_->read(0, segment_length_);
    if (!segment)
    {
        return std::unexpected(segment.error());
    }
    segment_ = *segment;
    return {};
}

}  // namespace deepmsg::archive::server
