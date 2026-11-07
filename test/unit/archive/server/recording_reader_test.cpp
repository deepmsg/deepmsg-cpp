// Criterion for card A1b. The reference has no `RecordingReaderTest`, so this
// is our own: a recording written by `RecordingWriter` has to replay back frame
// for frame, across term and segment boundaries, and the error paths the
// reference throws (`RecordingReader.java:63-108`, `:207`) have to surface.

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include "archive/server/frame_layout.h"
#include "archive/server/recording_reader.h"
#include "archive/server/recording_writer.h"
#include "archive/server/segment_files.h"

namespace
{

using deepmsg::archive::server::DATA_FRAME_TYPE;
using deepmsg::archive::server::FragmentHandler;
using deepmsg::archive::server::RecordingReader;
using deepmsg::archive::server::RecordingSummary;
using deepmsg::archive::server::RecordingWriter;
using deepmsg::archive::server::RecordingWriterContext;
using deepmsg::archive::server::segment_file_name;

constexpr std::int32_t TERM_LENGTH = 64 * 1024;
constexpr std::int32_t SEGMENT_LENGTH = TERM_LENGTH * 4;
constexpr std::int32_t FRAME_LENGTH = 1024;
constexpr std::int64_t RECORDING_ID = 7;

class CollectingHandler final : public FragmentHandler
{
public:
    void on_fragment(const std::uint8_t* buffer, std::int32_t offset, std::int32_t length, std::int32_t frame_type,
                     std::uint8_t flags, std::int64_t reserved_value) override
    {
        (void)flags;
        (void)reserved_value;
        fragments++;
        last_frame_type = frame_type;
        last_length = length;
        if (length > 0)
        {
            last_first_byte = buffer[static_cast<std::size_t>(offset)];
        }
        markers.push_back(length > 0 ? buffer[static_cast<std::size_t>(offset)] : 0);
    }

    int fragments{0};
    std::int32_t last_frame_type{0};
    std::int32_t last_length{0};
    std::uint8_t last_first_byte{0};
    std::vector<std::uint8_t> markers;
};

class RecordingReaderTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::array<char, 64> pattern{};
        std::snprintf(pattern.data(), pattern.size(), "/tmp/axc-reader-XXXXXX");
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        archive_dir_ = created;
    }

    void TearDown() override
    {
        std::error_code error;
        std::filesystem::remove_all(archive_dir_, error);
    }

    /// Writes `count` data frames of `FRAME_LENGTH`, each tagged with its index.
    [[nodiscard]] std::int64_t write_frames(int count, std::int32_t frame_length = FRAME_LENGTH)
    {
        RecordingWriterContext ctx;
        ctx.archive_dir = archive_dir_;

        RecordingWriter writer{RECORDING_ID, 0, SEGMENT_LENGTH, TERM_LENGTH, 0, ctx, nullptr};
        EXPECT_TRUE(writer.init());

        std::vector<std::uint8_t> term_buffer(static_cast<std::size_t>(frame_length), 0);
        const auto type = static_cast<std::uint16_t>(DATA_FRAME_TYPE);
        std::memcpy(term_buffer.data() + deepmsg::archive::server::FRAME_TYPE_OFFSET, &type, sizeof(type));
        std::memcpy(term_buffer.data(), &frame_length, sizeof(frame_length));
        const std::int32_t stream_id = 3;
        std::memcpy(term_buffer.data() + deepmsg::archive::server::STREAM_ID_OFFSET, &stream_id, sizeof(stream_id));

        for (int index = 0; index < count; index++)
        {
            // Real frames carry their term offset and term id; the reader checks
            // both when it starts mid-recording (`RecordingReader.java:101-108`).
            const auto position = static_cast<std::int32_t>(index * frame_length);
            const std::int32_t term_offset = position & (TERM_LENGTH - 1);
            const std::int32_t term_id = position / TERM_LENGTH;
            std::memcpy(term_buffer.data() + deepmsg::archive::server::TERM_OFFSET_FIELD_OFFSET, &term_offset,
                        sizeof(term_offset));
            std::memcpy(term_buffer.data() + deepmsg::archive::server::TERM_ID_OFFSET, &term_id, sizeof(term_id));
            std::memset(term_buffer.data() + deepmsg::archive::server::DATA_HEADER_LENGTH, index & 0xFF,
                        static_cast<std::size_t>(frame_length - deepmsg::archive::server::DATA_HEADER_LENGTH));
            auto written = writer.on_block(term_buffer.data(), 0, frame_length, 1, 0);
            EXPECT_TRUE(written) << (written ? std::string{} : std::string{written.error().message.view()});
        }

        const std::int64_t position = writer.position();
        EXPECT_TRUE(writer.close());
        return position;
    }

    [[nodiscard]] RecordingSummary summary(std::int64_t start_position, std::int64_t stop_position) const
    {
        RecordingSummary summary;
        summary.recording_id = RECORDING_ID;
        summary.start_position = start_position;
        summary.stop_position = stop_position;
        summary.initial_term_id = 0;
        summary.segment_file_length = SEGMENT_LENGTH;
        summary.term_buffer_length = TERM_LENGTH;
        summary.mtu_length = 1408;
        summary.stream_id = 3;
        summary.session_id = 1;
        return summary;
    }

    std::string archive_dir_;
};

TEST_F(RecordingReaderTest, replaysEveryFrameAcrossTermsAndSegmentFiles)
{
    // 300 frames of 1 KiB = 300 KiB, i.e. past the 256 KiB segment boundary.
    constexpr int frame_count = 300;
    const std::int64_t stop_position = write_frames(frame_count);
    ASSERT_GT(stop_position, SEGMENT_LENGTH);

    auto reader = RecordingReader::create(summary(0, stop_position), archive_dir_, RecordingReader::NULL_POSITION,
                                          RecordingReader::NULL_LENGTH);
    ASSERT_TRUE(reader) << reader.error().message.view();

    CollectingHandler handler;
    int guard = 0;
    while (!reader->is_done() && guard++ < 10'000)
    {
        auto polled = reader->poll(handler, 7);
        ASSERT_TRUE(polled) << polled.error().message.view();
    }

    EXPECT_TRUE(reader->is_done());
    EXPECT_EQ(frame_count, handler.fragments);
    EXPECT_EQ(DATA_FRAME_TYPE, handler.last_frame_type);
    EXPECT_EQ(FRAME_LENGTH - deepmsg::archive::server::DATA_HEADER_LENGTH, handler.last_length);
    EXPECT_EQ(stop_position, reader->replay_position());
    ASSERT_EQ(static_cast<std::size_t>(frame_count), handler.markers.size());
    for (int index = 0; index < frame_count; index++)
    {
        ASSERT_EQ(static_cast<std::uint8_t>(index & 0xFF), handler.markers[static_cast<std::size_t>(index)])
            << "frame " << index;
    }
}

TEST_F(RecordingReaderTest, stopsAtTheReplayLimit)
{
    constexpr int frame_count = 10;
    const std::int64_t stop_position = write_frames(frame_count);

    // Three frames only.
    auto reader = RecordingReader::create(summary(0, stop_position), archive_dir_, RecordingReader::NULL_POSITION,
                                          3 * FRAME_LENGTH);
    ASSERT_TRUE(reader) << reader.error().message.view();

    CollectingHandler handler;
    while (!reader->is_done())
    {
        auto polled = reader->poll(handler, 16);
        ASSERT_TRUE(polled) << polled.error().message.view();
    }

    EXPECT_EQ(3, handler.fragments);
    EXPECT_EQ(3 * FRAME_LENGTH, reader->replay_position());
}

TEST_F(RecordingReaderTest, startsAtAGivenPosition)
{
    constexpr int frame_count = 10;
    const std::int64_t stop_position = write_frames(frame_count);

    auto reader = RecordingReader::create(summary(0, stop_position), archive_dir_, 4 * FRAME_LENGTH, 2 * FRAME_LENGTH);
    ASSERT_TRUE(reader) << reader.error().message.view();
    EXPECT_EQ(4 * FRAME_LENGTH, reader->replay_position());

    CollectingHandler handler;
    while (!reader->is_done())
    {
        auto polled = reader->poll(handler, 16);
        ASSERT_TRUE(polled) << polled.error().message.view();
    }

    EXPECT_EQ(2, handler.fragments);
    ASSERT_EQ(2u, handler.markers.size());
    EXPECT_EQ(4, handler.markers[0]);
    EXPECT_EQ(5, handler.markers[1]);
}

TEST_F(RecordingReaderTest, rejectsAPositionThatIsNotAFrameBoundary)
{
    const std::int64_t stop_position = write_frames(10);

    auto reader = RecordingReader::create(summary(0, stop_position), archive_dir_, 100, 10);
    ASSERT_FALSE(reader);
    EXPECT_EQ("100 position not aligned to valid fragment", reader.error().message.view());
}

TEST_F(RecordingReaderTest, rejectsAnInvalidPositionOrLength)
{
    const std::int64_t stop_position = write_frames(1);

    auto bad_position = RecordingReader::create(summary(0, stop_position), archive_dir_, -2, 10);
    ASSERT_FALSE(bad_position);
    EXPECT_EQ("invalid position: -2", bad_position.error().message.view());

    auto bad_length = RecordingReader::create(summary(0, stop_position), archive_dir_, 0, -2);
    ASSERT_FALSE(bad_length);
    EXPECT_EQ("invalid length: -2", bad_length.error().message.view());
}

TEST_F(RecordingReaderTest, reportsAMissingSegmentFile)
{
    // More than one segment, then the second one disappears.
    const std::int64_t stop_position = write_frames(300);
    ASSERT_GT(stop_position, SEGMENT_LENGTH);

    const std::string second_segment = archive_dir_ + "/" + segment_file_name(RECORDING_ID, SEGMENT_LENGTH);
    ASSERT_TRUE(std::filesystem::exists(second_segment));
    std::error_code error;
    ASSERT_TRUE(std::filesystem::remove(second_segment, error));

    auto reader = RecordingReader::create(summary(0, stop_position), archive_dir_, RecordingReader::NULL_POSITION,
                                          RecordingReader::NULL_LENGTH);
    ASSERT_TRUE(reader) << reader.error().message.view();

    CollectingHandler handler;
    std::expected<int, axc::core::Error> polled{0};
    int guard = 0;
    while (!reader->is_done() && guard++ < 10'000)
    {
        polled = reader->poll(handler, 16);
        if (!polled)
        {
            break;
        }
    }

    ASSERT_FALSE(polled);
    EXPECT_EQ("failed to open recording segment file " + segment_file_name(RECORDING_ID, SEGMENT_LENGTH),
              polled.error().message.view());
}

}  // namespace
