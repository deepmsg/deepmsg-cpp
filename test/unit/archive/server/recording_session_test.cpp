// A2b-3 criterion: the recording session's four states
// (`RecordingSession.java:131-278`) and the recorder's metric publication
// condition (`ArchiveConductor.java:2788-2796`), without a driver.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "archive/server/frame_layout.h"
#include "archive/server/recorder.h"
#include "archive/server/recording_session.h"

namespace
{

using deepmsg::archive::server::DATA_FRAME_TYPE;
using deepmsg::archive::server::DATA_HEADER_LENGTH;
using deepmsg::archive::server::FRAME_LENGTH_OFFSET;
using deepmsg::archive::server::FRAME_TYPE_OFFSET;
using deepmsg::archive::server::Recorder;
using deepmsg::archive::server::RecordingImage;
using deepmsg::archive::server::RecordingSession;
using deepmsg::archive::server::RecordingWriter;
using deepmsg::archive::server::RecordingWriterContext;
using deepmsg::archive::server::SESSION_ID_OFFSET;
using deepmsg::archive::server::STREAM_ID_OFFSET;
using deepmsg::archive::server::TERM_ID_OFFSET;

class FakeCounter final : public axc::core::CounterTarget
{
public:
    void set_value(std::int64_t value) noexcept override
    {
        value_ = value;
    }

    [[nodiscard]] std::int64_t value() const noexcept override
    {
        return value_;
    }

private:
    std::int64_t value_{0};
};

class FakeImage final : public RecordingImage
{
public:
    explicit FakeImage(std::int32_t session_id, std::int32_t term_id) : session_id_(session_id), term_id_(term_id) {}

    void add_block(std::size_t length)
    {
        std::vector<std::uint8_t> frame(length, 0);
        const auto frame_length = static_cast<std::int32_t>(length);
        const std::uint16_t type = DATA_FRAME_TYPE;
        std::memcpy(frame.data() + FRAME_LENGTH_OFFSET, &frame_length, sizeof(frame_length));
        std::memcpy(frame.data() + FRAME_TYPE_OFFSET, &type, sizeof(type));
        std::memcpy(frame.data() + SESSION_ID_OFFSET, &session_id_, sizeof(session_id_));
        std::memcpy(frame.data() + TERM_ID_OFFSET, &term_id_, sizeof(term_id_));
        blocks_.push_back(std::move(frame));
    }

    void fail_next_write()
    {
        fail_next_ = true;
    }

    void end_of_stream()
    {
        end_of_stream_ = true;
    }

    int block_poll(RecordingWriter& writer, std::int32_t) noexcept override
    {
        error_.reset();
        if (fail_next_)
        {
            fail_next_ = false;
            error_ = axc::core::make_error(axc::core::ErrorCode::io_error, "write failed");
            return 0;
        }
        if (blocks_.empty())
        {
            return 0;
        }

        auto block = std::move(blocks_.front());
        blocks_.erase(blocks_.begin());
        auto written = writer.on_block(block.data(), 0, static_cast<std::int32_t>(block.size()), session_id_, term_id_);
        if (!written)
        {
            error_ = written.error();
            return 0;
        }
        return static_cast<int>(block.size());
    }

    [[nodiscard]] bool is_end_of_stream() const noexcept override
    {
        return end_of_stream_;
    }

    [[nodiscard]] std::expected<void, axc::core::Error> take_error() override
    {
        if (!error_)
        {
            return {};
        }
        const axc::core::Error error = *error_;
        error_.reset();
        return std::unexpected(error);
    }

private:
    std::int32_t session_id_;
    std::int32_t term_id_;
    std::vector<std::vector<std::uint8_t>> blocks_;
    std::optional<axc::core::Error> error_;
    bool fail_next_{false};
    bool end_of_stream_{false};
};

class RecordingSessionTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::error_code error;
        archive_dir_ = std::filesystem::temp_directory_path(error) / "axc-recording-session-test";
        std::filesystem::remove_all(archive_dir_, error);
        std::filesystem::create_directories(archive_dir_, error);
        ASSERT_FALSE(error);
    }

    void TearDown() override
    {
        std::error_code error;
        std::filesystem::remove_all(archive_dir_, error);
    }

    std::filesystem::path archive_dir_;
};

TEST_F(RecordingSessionTest, recordsBlocksPublishesThePositionAndStopsAtEndOfStream)
{
    auto image_owner = std::make_unique<FakeImage>(7, 1);
    FakeImage& image = *image_owner;
    image.add_block(32);

    RecordingWriterContext context;
    context.archive_dir = archive_dir_.string();

    RecordingWriter writer(1, 0, 4096, 1024, 0, std::move(context), nullptr);
    RecordingSession::Config config;
    config.recording_id = 1;
    config.join_position = 0;
    config.start_position = 0;
    config.segment_length = 4096;
    config.term_length = 1024;
    config.block_length_limit = 1024;

    auto position_counter = std::make_unique<FakeCounter>();
    FakeCounter* position = position_counter.get();

    RecordingSession session(config, std::move(writer), std::move(position_counter), std::move(image_owner));

    // `RecordingSession.doWork` runs INIT and RECORDING in the same pass.
    EXPECT_EQ(1 + 32, session.do_work());
    EXPECT_EQ(RecordingSession::State::recording, session.state());
    EXPECT_EQ(32, position->value());
    EXPECT_EQ(32, session.recorded_position());

    // Nothing left, but the stream is still live.
    EXPECT_EQ(0, session.do_work());
    EXPECT_EQ(RecordingSession::State::recording, session.state());

    image.end_of_stream();
    EXPECT_EQ(1, session.do_work());
    EXPECT_TRUE(session.is_done());
}

TEST_F(RecordingSessionTest, aWriteErrorMovesTheSessionToStoppedWithAnError)
{
    auto image_owner = std::make_unique<FakeImage>(7, 1);
    FakeImage& image = *image_owner;
    RecordingWriterContext context;
    context.archive_dir = archive_dir_.string();

    RecordingWriter writer(1, 0, 4096, 1024, 0, std::move(context), nullptr);
    RecordingSession::Config config;
    config.recording_id = 1;
    config.segment_length = 4096;
    config.term_length = 1024;
    config.block_length_limit = 1024;

    RecordingSession session(config, std::move(writer), std::make_unique<FakeCounter>(), std::move(image_owner));

    EXPECT_EQ(1, session.do_work());
    image.fail_next_write();
    session.do_work();

    EXPECT_TRUE(session.is_done());
    EXPECT_EQ("write failed", session.error_message());
}

TEST_F(RecordingSessionTest, abortStopsTheSessionWithoutTouchingTheImage)
{
    auto image_owner = std::make_unique<FakeImage>(7, 1);
    RecordingWriterContext context;
    context.archive_dir = archive_dir_.string();

    RecordingWriter writer(1, 0, 4096, 1024, 0, std::move(context), nullptr);
    RecordingSession::Config config;
    config.recording_id = 1;
    config.segment_length = 4096;
    config.term_length = 1024;
    config.block_length_limit = 1024;

    RecordingSession session(config, std::move(writer), std::make_unique<FakeCounter>(), std::move(image_owner));

    session.abort("image unavailable");
    session.do_work();

    EXPECT_TRUE(session.is_done());
}

TEST_F(RecordingSessionTest, theRecorderPublishesOnlyOnAPassThatDidWork)
{
    FakeCounter bytes;
    FakeCounter total_time;
    FakeCounter max_time;
    Recorder recorder;
    recorder.set_counters(&bytes, &total_time, &max_time);

    EXPECT_EQ(0, recorder.do_work([](RecordingSession&) {}));
    EXPECT_EQ(0, bytes.value());

    recorder.bytes_written(128);
    recorder.write_time_ns(50);

    // No session, no work: the metrics stay unpublished (`AeronStat` does not
    // see them move on an idle pass).
    EXPECT_EQ(0, recorder.do_work([](RecordingSession&) {}));
    EXPECT_EQ(0, bytes.value());

    // A session's first pass (INIT -> RECORDING) counts as work.
    auto image_owner = std::make_unique<FakeImage>(7, 1);
    RecordingWriterContext context;
    context.archive_dir = archive_dir_.string();
    RecordingWriter writer(1, 0, 4096, 1024, 0, std::move(context), nullptr);
    RecordingSession::Config config;
    config.recording_id = 1;
    config.segment_length = 4096;
    config.term_length = 1024;
    config.block_length_limit = 1024;

    recorder.add_session(std::make_unique<RecordingSession>(std::move(config), std::move(writer),
                                                            std::make_unique<FakeCounter>(), std::move(image_owner)));

    EXPECT_EQ(1, recorder.do_work([](RecordingSession&) {}));
    EXPECT_EQ(128, bytes.value());
    EXPECT_EQ(50, total_time.value());
    EXPECT_EQ(50, max_time.value());
}

}  // namespace
