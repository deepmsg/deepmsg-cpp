// A3b-3 criteria: the replay session's four states, geometry, block read,
// frame rewrite, padding, limit counter and the revoke/close split
// (`ReplaySession.java:73-662`, 1.54.0 = 454da1e4e6), without a media driver.

#include <gtest/gtest.h>

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "archive/server/checksum.h"
#include "archive/server/frame_layout.h"
#include "archive/server/replay_session.h"

namespace
{

using deepmsg::archive::server::Checksum;
using deepmsg::archive::server::DATA_FRAME_TYPE;
using deepmsg::archive::server::DATA_HEADER_LENGTH;
using deepmsg::archive::server::FRAME_LENGTH_OFFSET;
using deepmsg::archive::server::FRAME_TYPE_OFFSET;
using deepmsg::archive::server::PADDING_FRAME_TYPE;
using deepmsg::archive::server::ReplayClock;
using deepmsg::archive::server::ReplayControlResponses;
using deepmsg::archive::server::ReplayLimitSource;
using deepmsg::archive::server::ReplayMetrics;
using deepmsg::archive::server::ReplayPublication;
using deepmsg::archive::server::ReplaySession;
using deepmsg::archive::server::ReplayWarningSink;
using deepmsg::archive::server::SESSION_ID_OFFSET;
using deepmsg::archive::server::STREAM_ID_OFFSET;
using deepmsg::archive::server::TERM_ID_OFFSET;
using deepmsg::archive::server::TERM_OFFSET_FIELD_OFFSET;
using deepmsg::archive::server::io::Backend;

constexpr std::int32_t TERM_LENGTH = 64 * 1024;
constexpr std::int32_t SEGMENT_LENGTH = 2 * TERM_LENGTH;
constexpr std::int32_t INITIAL_TERM_ID = 5;
constexpr std::int32_t RECORDING_STREAM_ID = 77;
constexpr std::int32_t RECORDING_SESSION_ID = 55;
constexpr std::int32_t REPLAY_SESSION_ID_SESSION = 777;
constexpr std::int64_t RECORDING_ID = 42;
constexpr std::int32_t FRAME_PAYLOAD = 992;
constexpr std::int32_t FRAME_LENGTH = DATA_HEADER_LENGTH + FRAME_PAYLOAD;
/// `min(request.fileIoMaxLength, ctx.replayBuffer)` for every fixture.
constexpr std::int32_t BUFFER_LENGTH = 1024 * 1024;

void write_int32(std::uint8_t* address, std::int32_t value)
{
    std::memcpy(address, &value, sizeof(value));
}

void write_int16(std::uint8_t* address, std::int16_t value)
{
    std::memcpy(address, &value, sizeof(value));
}

void write_data_frame(std::uint8_t* frame, std::int32_t length, std::int32_t term_offset, std::int32_t term_id,
                      std::int32_t session_id, std::int32_t stream_id, std::uint8_t seed)
{
    write_int32(frame + FRAME_LENGTH_OFFSET, length);
    write_int16(frame + FRAME_TYPE_OFFSET, DATA_FRAME_TYPE);
    write_int32(frame + TERM_OFFSET_FIELD_OFFSET, term_offset);
    write_int32(frame + SESSION_ID_OFFSET, session_id);
    write_int32(frame + STREAM_ID_OFFSET, stream_id);
    write_int32(frame + TERM_ID_OFFSET, term_id);
    std::memset(frame + DATA_HEADER_LENGTH, seed, static_cast<std::size_t>(length - DATA_HEADER_LENGTH));
}

void write_padding_frame(std::uint8_t* frame, std::int32_t length, std::int32_t term_offset, std::int32_t term_id,
                         std::int32_t session_id, std::int32_t stream_id)
{
    write_int32(frame + FRAME_LENGTH_OFFSET, length);
    write_int16(frame + FRAME_TYPE_OFFSET, PADDING_FRAME_TYPE);
    write_int32(frame + TERM_OFFSET_FIELD_OFFSET, term_offset);
    write_int32(frame + SESSION_ID_OFFSET, session_id);
    write_int32(frame + STREAM_ID_OFFSET, stream_id);
    write_int32(frame + TERM_ID_OFFSET, term_id);
}

struct Recording
{
    std::vector<std::uint8_t> bytes;
    std::int64_t stop_position{0};
    std::size_t data_frame_count{0};
};

/// A recording of `term_count` terms: 63 data frames of 1024 bytes, then a
/// padding frame that fills the term.
Recording build_recording(std::int32_t term_count, std::int32_t session_id = RECORDING_SESSION_ID)
{
    Recording recording;
    recording.bytes.assign(static_cast<std::size_t>(TERM_LENGTH) * static_cast<std::size_t>(term_count), 0);

    std::size_t term_base = 0;
    for (std::int32_t term = 0; term < term_count; term++)
    {
        std::int32_t offset = 0;
        while (offset + FRAME_LENGTH + 1024 <= TERM_LENGTH)
        {
            write_data_frame(recording.bytes.data() + term_base + static_cast<std::size_t>(offset), FRAME_LENGTH,
                             offset, INITIAL_TERM_ID + term, session_id, RECORDING_STREAM_ID,
                             static_cast<std::uint8_t>(term * 63 + offset / FRAME_LENGTH));
            offset += FRAME_LENGTH;
            recording.data_frame_count++;
        }

        write_padding_frame(recording.bytes.data() + term_base + static_cast<std::size_t>(offset), TERM_LENGTH - offset,
                            offset, INITIAL_TERM_ID + term, session_id, RECORDING_STREAM_ID);
        term_base += static_cast<std::size_t>(TERM_LENGTH);
    }

    recording.stop_position = static_cast<std::int64_t>(recording.bytes.size());
    return recording;
}

/// The bytes the replay is expected to publish for the data frames: every
/// frame's session/stream fields replaced by the publication's.
std::vector<std::uint8_t> expected_published_data(const Recording& recording, std::int32_t session_id,
                                                  std::int32_t stream_id)
{
    std::vector<std::uint8_t> expected;
    expected.reserve(recording.data_frame_count * FRAME_LENGTH);

    std::size_t offset = 0;
    while (offset < recording.bytes.size())
    {
        std::int32_t length = 0;
        std::memcpy(&length, recording.bytes.data() + offset + FRAME_LENGTH_OFFSET, sizeof(length));

        std::int16_t type = 0;
        std::memcpy(&type, recording.bytes.data() + offset + FRAME_TYPE_OFFSET, sizeof(type));

        const std::int32_t aligned = (length + 31) & ~31;
        if (DATA_FRAME_TYPE == type)
        {
            const std::size_t begin = expected.size();
            expected.insert(expected.end(), recording.bytes.begin() + static_cast<std::ptrdiff_t>(offset),
                            recording.bytes.begin() + static_cast<std::ptrdiff_t>(offset + aligned));
            write_int32(expected.data() + begin + SESSION_ID_OFFSET, session_id);
            write_int32(expected.data() + begin + STREAM_ID_OFFSET, stream_id);
        }

        offset += static_cast<std::size_t>(aligned);
    }

    return expected;
}

/// One term with a single unaligned data frame (1032 bytes, aligned to 1056)
/// followed by a padding frame: the aligned-vs-raw advance is observable.
Recording build_unaligned_recording()
{
    Recording recording;
    recording.bytes.assign(static_cast<std::size_t>(TERM_LENGTH), 0);

    constexpr std::int32_t UNALIGNED_FRAME_LENGTH = 1032;
    constexpr std::int32_t UNALIGNED_ALIGNED = 1056;

    write_data_frame(recording.bytes.data(), UNALIGNED_FRAME_LENGTH, 0, INITIAL_TERM_ID, RECORDING_SESSION_ID,
                     RECORDING_STREAM_ID, 0x5A);
    write_padding_frame(recording.bytes.data() + UNALIGNED_ALIGNED, TERM_LENGTH - UNALIGNED_ALIGNED, UNALIGNED_ALIGNED,
                        INITIAL_TERM_ID, RECORDING_SESSION_ID, RECORDING_STREAM_ID);
    recording.data_frame_count = 1;
    recording.stop_position = TERM_LENGTH;
    return recording;
}

/// One term with a single data frame written; the rest stays zero until the
/// recorder appends it (the live-replay shape).
Recording build_live_recording()
{
    Recording recording;
    recording.bytes.assign(static_cast<std::size_t>(TERM_LENGTH), 0);
    write_data_frame(recording.bytes.data(), FRAME_LENGTH, 0, INITIAL_TERM_ID, RECORDING_SESSION_ID,
                     RECORDING_STREAM_ID, 1);
    recording.data_frame_count = 1;
    recording.stop_position = TERM_LENGTH;
    return recording;
}

/// `pwrite` one frame into an existing segment file, the way the recorder
/// appends while a live replay is reading.
void append_frame_to_segment(const std::string& archive_dir, std::int64_t segment_base_position, std::size_t offset,
                             const std::vector<std::uint8_t>& frame)
{
    const std::string path =
        archive_dir + "/" + std::to_string(RECORDING_ID) + "-" + std::to_string(segment_base_position) + ".rec";
    const int fd = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0) << path;
    ASSERT_EQ(static_cast<ssize_t>(frame.size()), ::pwrite(fd, frame.data(), frame.size(), static_cast<off_t>(offset)));
    ::close(fd);
}

class TempArchiveDir
{
public:
    TempArchiveDir()
    {
        path_ = std::filesystem::temp_directory_path() /
                ("deepmsg-replay-" + std::to_string(::getpid()) + "-" + std::to_string(++counter_));
        std::filesystem::create_directories(path_);
    }

    ~TempArchiveDir()
    {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
    }

    TempArchiveDir(const TempArchiveDir&) = delete;
    TempArchiveDir& operator=(const TempArchiveDir&) = delete;

    [[nodiscard]] std::string path() const
    {
        return path_.string();
    }

private:
    static inline int counter_{0};
    std::filesystem::path path_;
};

void write_segments(const std::string& archive_dir, const Recording& recording, std::int64_t recording_id,
                    std::int32_t segment_length)
{
    for (std::size_t offset = 0, segment_base = 0; offset < recording.bytes.size();
         offset += static_cast<std::size_t>(segment_length), segment_base += static_cast<std::size_t>(segment_length))
    {
        const std::size_t length = std::min(static_cast<std::size_t>(segment_length), recording.bytes.size() - offset);
        std::ofstream file(
            archive_dir + "/" + std::to_string(recording_id) + "-" + std::to_string(segment_base) + ".rec",
            std::ios::binary | std::ios::trunc);
        file.write(reinterpret_cast<const char*>(recording.bytes.data() + offset),
                   static_cast<std::streamsize>(length));
    }
}

class FakePublication final : public ReplayPublication
{
public:
    bool connected{true};
    std::int64_t window{1024 * 1024};
    std::int32_t session{REPLAY_SESSION_ID_SESSION};
    std::int32_t stream{RECORDING_STREAM_ID};
    std::int32_t initial_term{INITIAL_TERM_ID};
    std::int32_t shift{16};
    std::string channel_string{"aeron:udp?endpoint=localhost:6666"};

    enum class Fail
    {
        none,
        back_pressured,
        error,
        closed
    };
    Fail fail{Fail::none};

    std::vector<std::uint8_t> offered;
    std::vector<std::int32_t> padding_lengths;
    bool closed{false};
    bool revoked{false};

    [[nodiscard]] bool is_connected() const noexcept override
    {
        return connected;
    }

    [[nodiscard]] std::int64_t available_window() const noexcept override
    {
        return window;
    }

    [[nodiscard]] std::int32_t session_id() const noexcept override
    {
        return session;
    }

    [[nodiscard]] std::int32_t stream_id() const noexcept override
    {
        return stream;
    }

    [[nodiscard]] std::int32_t initial_term_id() const noexcept override
    {
        return initial_term;
    }

    [[nodiscard]] std::int32_t position_bits_to_shift() const noexcept override
    {
        return shift;
    }

    [[nodiscard]] std::string_view channel() const noexcept override
    {
        return channel_string;
    }

    [[nodiscard]] std::int64_t offer_block(std::span<const std::uint8_t> buffer) noexcept override
    {
        if (Fail::back_pressured == fail)
        {
            return AERON_PUBLICATION_BACK_PRESSURED;
        }
        if (Fail::error == fail)
        {
            return AERON_PUBLICATION_ERROR;
        }
        if (Fail::closed == fail)
        {
            return AERON_PUBLICATION_CLOSED;
        }

        offered.insert(offered.end(), buffer.begin(), buffer.end());
        position_ += static_cast<std::int64_t>(buffer.size());
        return position_;
    }

    [[nodiscard]] std::int64_t append_padding(std::int32_t length) noexcept override
    {
        if (Fail::back_pressured == fail)
        {
            return AERON_PUBLICATION_BACK_PRESSURED;
        }
        if (Fail::error == fail)
        {
            return AERON_PUBLICATION_ERROR;
        }
        if (Fail::closed == fail)
        {
            return AERON_PUBLICATION_CLOSED;
        }

        padding_lengths.push_back(length);
        position_ += static_cast<std::int64_t>(length) + DATA_HEADER_LENGTH;
        return position_;
    }

    void close() noexcept override
    {
        closed = true;
    }

    void revoke() noexcept override
    {
        revoked = true;
    }

private:
    std::int64_t position_{0};
};

class FakeControl final : public ReplayControlResponses
{
public:
    struct Ok
    {
        std::int64_t control_session_id;
        std::int64_t correlation_id;
        std::int64_t replay_session_id;
    };

    struct Error
    {
        std::int64_t control_session_id;
        std::int64_t correlation_id;
        std::int64_t relevant_id;
        std::string message;
    };

    void async_send_ok(std::int64_t control_session_id, std::int64_t correlation_id,
                       std::int64_t replay_session_id) override
    {
        oks.push_back({control_session_id, correlation_id, replay_session_id});
    }

    void send_error(std::int64_t control_session_id, std::int64_t correlation_id, std::int64_t relevant_id,
                    std::string_view message) override
    {
        errors.push_back({control_session_id, correlation_id, relevant_id, std::string{message}});
    }

    std::vector<Ok> oks;
    std::vector<Error> errors;
};

class FakeMetrics final : public ReplayMetrics
{
public:
    void bytes_read(std::int64_t bytes) noexcept override
    {
        total_bytes += bytes;
    }

    void read_time_ns(std::int64_t nanos) noexcept override
    {
        total_time_ns += nanos;
        reads++;
    }

    std::int64_t total_bytes{0};
    std::int64_t total_time_ns{0};
    int reads{0};
};

class FakeClock final : public ReplayClock
{
public:
    [[nodiscard]] std::int64_t time_ms() const noexcept override
    {
        return ms;
    }

    [[nodiscard]] std::int64_t nano_time() const noexcept override
    {
        return ns;
    }

    std::int64_t ms{1000};
    std::int64_t ns{1'000'000'000};
};

class FakeLimit final : public ReplayLimitSource
{
public:
    [[nodiscard]] std::int64_t value() const noexcept override
    {
        return current;
    }

    [[nodiscard]] bool is_closed() const noexcept override
    {
        return closed;
    }

    [[nodiscard]] bool registration_matches() const noexcept override
    {
        return matches;
    }

    std::int64_t current{0};
    bool closed{false};
    bool matches{true};
};

class FakeWarnings final : public ReplayWarningSink
{
public:
    void unexpected_frame(std::int32_t frame_type, std::int64_t position) override
    {
        frames.push_back({frame_type, position});
    }

    struct Frame
    {
        std::int32_t frame_type;
        std::int64_t position;
    };

    std::vector<Frame> frames;
};

class FixedChecksum final : public Checksum
{
public:
    explicit FixedChecksum(std::int32_t value) : value_(value) {}

    [[nodiscard]] std::int32_t compute(const std::uint8_t*, std::int32_t, std::int32_t) const noexcept override
    {
        return value_;
    }

private:
    std::int32_t value_;
};

struct Fixture
{
    explicit Fixture(const Recording& recording, std::int64_t position = 0, std::int64_t length = 0,
                     std::int64_t start_position = 0, std::int64_t stop_position = 0, bool create_segments = true)
    {
        if (create_segments)
        {
            write_segments(dir.path(), recording, RECORDING_ID, SEGMENT_LENGTH);
        }

        config.correlation_id = 9;
        config.recording_id = RECORDING_ID;
        config.replay_position = position;
        config.replay_length = 0 == length ? recording.stop_position - position : length;
        config.start_position = start_position;
        config.stop_position = 0 == stop_position ? recording.stop_position : stop_position;
        config.segment_file_length = SEGMENT_LENGTH;
        config.term_buffer_length = TERM_LENGTH;
        config.stream_id = RECORDING_STREAM_ID;
        config.replay_session_id = (3LL << 32) | REPLAY_SESSION_ID_SESSION;
        config.control_session_id = 11;
        config.connect_timeout_ms = 5000;
        config.archive_dir = dir.path();

        publication = std::make_unique<FakePublication>();
        publication_raw = publication.get();
    }

    [[nodiscard]] std::unique_ptr<ReplaySession> make(std::unique_ptr<ReplayLimitSource> limit = nullptr,
                                                      const Checksum* checksum = nullptr,
                                                      ReplayWarningSink* warnings = nullptr)
    {
        return std::make_unique<ReplaySession>(config, std::move(publication), control, metrics, clock, checksum,
                                               std::move(limit), BUFFER_LENGTH, warnings);
    }

    TempArchiveDir dir;
    ReplaySession::Config config;
    FakeControl control;
    FakeMetrics metrics;
    FakeClock clock;
    std::unique_ptr<FakePublication> publication;
    FakePublication* publication_raw{nullptr};
};

int run_to_done(ReplaySession& session, int max_passes = 10000)
{
    int passes = 0;
    while (!session.is_done() && passes < max_passes)
    {
        session.do_work();
        passes++;
    }
    return passes;
}

TEST(ReplaySessionTest, replaysEveryTermAcrossSegmentsWithThePublicationIds)
{
    const Recording recording = build_recording(3);
    Fixture fixture(recording);
    auto session = fixture.make();

    ASSERT_LT(run_to_done(*session), 10000) << "replay did not finish";
    EXPECT_TRUE(session->is_done());
    session->close();

    // ★ One OK, carrying the replay session id, sent from init.
    ASSERT_EQ(1U, fixture.control.oks.size());
    EXPECT_EQ(11, fixture.control.oks[0].control_session_id);
    EXPECT_EQ(9, fixture.control.oks[0].correlation_id);
    EXPECT_EQ((3LL << 32) | REPLAY_SESSION_ID_SESSION, fixture.control.oks[0].replay_session_id);
    EXPECT_TRUE(fixture.control.errors.empty());

    // Data frames are published byte for byte except the two rewritten ids.
    EXPECT_EQ(expected_published_data(recording, REPLAY_SESSION_ID_SESSION, RECORDING_STREAM_ID),
              fixture.publication_raw->offered);

    // One padding frame per term, and the padding payload is the recorded frame
    // length minus the header.
    ASSERT_EQ(3U, fixture.publication_raw->padding_lengths.size());
    EXPECT_EQ(TERM_LENGTH - 63 * FRAME_LENGTH - DATA_HEADER_LENGTH, fixture.publication_raw->padding_lengths[0]);

    // Normal completion: close (linger), not revoke.
    EXPECT_TRUE(fixture.publication_raw->closed);
    EXPECT_FALSE(fixture.publication_raw->revoked);
    EXPECT_TRUE(session->error_message().empty());
    EXPECT_GT(fixture.metrics.total_bytes, 0);
    EXPECT_EQ(3, fixture.metrics.reads);
}

TEST(ReplaySessionTest, sendsTheOkBeforeThePublicationConnects)
{
    const Recording recording = build_recording(1);
    Fixture fixture(recording);
    fixture.publication_raw->connected = false;
    fixture.publication_raw->window = 0;

    auto session = fixture.make();
    session->do_work();

    // INIT opened the segment and queued the OK even though the publication is
    // not connected yet (`ReplaySession.java:338`).
    ASSERT_EQ(1U, fixture.control.oks.size());
    EXPECT_EQ(ReplaySession::State::init, session->state());
    EXPECT_TRUE(fixture.publication_raw->offered.empty());

    fixture.publication_raw->connected = true;
    fixture.publication_raw->window = 1024 * 1024;
    session->do_work();
    EXPECT_EQ(ReplaySession::State::done, session->state());
    EXPECT_FALSE(fixture.publication_raw->offered.empty());
}

TEST(ReplaySessionTest, failsWhenNoSubscriberConnectsBeforeTheDeadline)
{
    const Recording recording = build_recording(1);
    Fixture fixture(recording);
    fixture.publication_raw->connected = false;

    auto session = fixture.make();
    session->do_work();
    EXPECT_EQ(ReplaySession::State::init, session->state());

    fixture.clock.ms += 6000;
    session->do_work();
    session->send_pending_error();

    ASSERT_EQ(1U, fixture.control.errors.size());
    EXPECT_NE(std::string::npos,
              fixture.control.errors[0].message.find("no connection established for replayChannel="));
    session->close();
    EXPECT_TRUE(fixture.publication_raw->revoked);
    EXPECT_EQ(ReplaySession::State::inactive, session->state());
}

TEST(ReplaySessionTest, waitsForAMissingSegmentFileUntilTheDeadline)
{
    const Recording recording = build_recording(1);
    Fixture fixture(recording, 0, 0, 0, 0, false);

    auto session = fixture.make();
    session->do_work();
    EXPECT_EQ(ReplaySession::State::init, session->state());
    EXPECT_TRUE(fixture.control.errors.empty());

    fixture.clock.ms += 6000;
    session->do_work();
    session->send_pending_error();
    ASSERT_EQ(1U, fixture.control.errors.size());
    EXPECT_NE(std::string::npos, fixture.control.errors[0].message.find("recording segment file not created"));
}

TEST(ReplaySessionTest, emptyReplayEndsWithoutOfferingAnything)
{
    const Recording recording = build_recording(1);
    Fixture fixture(recording, 0, 0, 0, 0);
    fixture.config.start_position = 0;
    fixture.config.stop_position = 0;
    fixture.config.replay_length = 0;

    auto session = fixture.make();
    ASSERT_LT(run_to_done(*session), 10);
    session->close();
    EXPECT_TRUE(fixture.publication_raw->offered.empty());
    EXPECT_TRUE(fixture.publication_raw->padding_lengths.empty());
    EXPECT_TRUE(fixture.publication_raw->closed);
    EXPECT_FALSE(fixture.publication_raw->revoked);
}

TEST(ReplaySessionTest, zeroWindowDoesNotReadOrOffer)
{
    const Recording recording = build_recording(1);
    Fixture fixture(recording);
    fixture.publication_raw->window = 0;

    auto session = fixture.make();
    session->do_work();
    EXPECT_EQ(ReplaySession::State::replay, session->state());
    EXPECT_EQ(0, fixture.metrics.reads);
    EXPECT_TRUE(fixture.publication_raw->offered.empty());

    fixture.publication_raw->window = 1024 * 1024;
    session->do_work();
    EXPECT_FALSE(fixture.publication_raw->offered.empty());
}

TEST(ReplaySessionTest, backPressureDropsTheWholeBatchIncludingItsPadding)
{
    const Recording recording = build_recording(1);
    Fixture fixture(recording);
    fixture.publication_raw->fail = FakePublication::Fail::back_pressured;

    auto session = fixture.make();
    session->do_work();
    EXPECT_EQ(ReplaySession::State::replay, session->state());
    EXPECT_TRUE(fixture.publication_raw->offered.empty());
    EXPECT_TRUE(fixture.publication_raw->padding_lengths.empty());

    fixture.publication_raw->fail = FakePublication::Fail::none;
    ASSERT_LT(run_to_done(*session), 10000);
    EXPECT_EQ(expected_published_data(recording, REPLAY_SESSION_ID_SESSION, RECORDING_STREAM_ID),
              fixture.publication_raw->offered);
    ASSERT_EQ(1U, fixture.publication_raw->padding_lengths.size());
}

TEST(ReplaySessionTest, followsALiveRecordingLimitAndFreezesWhenTheCounterIsClosed)
{
    const Recording recording = build_recording(3);
    Fixture fixture(recording, 0, std::numeric_limits<std::int64_t>::max(), 0, TERM_LENGTH);

    auto limit = std::make_unique<FakeLimit>();
    auto* limit_raw = limit.get();
    limit->current = TERM_LENGTH;

    auto session = fixture.make(std::move(limit));
    session->do_work();
    session->do_work();
    EXPECT_EQ(ReplaySession::State::replay, session->state());
    EXPECT_EQ(static_cast<std::size_t>(63 * FRAME_LENGTH), fixture.publication_raw->offered.size());

    // The recording extends: the limit counter moves and the replay follows.
    limit_raw->current = 2 * TERM_LENGTH;
    session->do_work();
    EXPECT_GT(fixture.publication_raw->offered.size(), static_cast<std::size_t>(TERM_LENGTH));

    // The recorder closes the counter at its final position: the replay ends at
    // that position (same registration ⇒ the last value is the limit).
    limit_raw->closed = true;
    limit_raw->matches = true;
    limit_raw->current = 2 * TERM_LENGTH;
    session->do_work();
    EXPECT_TRUE(session->is_done());
    session->close();
    EXPECT_TRUE(fixture.publication_raw->closed);
    EXPECT_FALSE(fixture.publication_raw->revoked);
}

TEST(ReplaySessionTest, freezesAtTheStopPositionWhenTheLimitSlotWasReused)
{
    const Recording recording = build_recording(2);
    Fixture fixture(recording, 0, std::numeric_limits<std::int64_t>::max(), 0, TERM_LENGTH);

    auto limit = std::make_unique<FakeLimit>();
    auto* limit_raw = limit.get();
    limit->current = TERM_LENGTH;

    auto session = fixture.make(std::move(limit));
    session->do_work();
    session->do_work();
    EXPECT_EQ(ReplaySession::State::replay, session->state());

    // Closed and the slot no longer belongs to us: freeze at the old stop.
    limit_raw->closed = true;
    limit_raw->matches = false;
    limit_raw->current = 9999999;

    session->do_work();
    EXPECT_TRUE(session->is_done());
    EXPECT_LT(fixture.publication_raw->offered.size(), static_cast<std::size_t>(recording.stop_position));
}

TEST(ReplaySessionTest, aBoundedClientCounterFollowsItsValueWithoutClosing)
{
    const Recording recording = build_recording(2);
    Fixture fixture(recording, 0, std::numeric_limits<std::int64_t>::max(), 0, TERM_LENGTH);

    auto limit = std::make_unique<FakeLimit>();
    auto* limit_raw = limit.get();
    limit->current = TERM_LENGTH;
    limit->closed = false;

    auto session = fixture.make(std::move(limit));
    session->do_work();
    session->do_work();
    EXPECT_EQ(ReplaySession::State::replay, session->state());

    limit_raw->current = 2 * TERM_LENGTH;
    session->do_work();
    EXPECT_GT(fixture.publication_raw->offered.size(), static_cast<std::size_t>(TERM_LENGTH));
}

TEST(ReplaySessionTest, anUnexpectedFrameTypeStallsAndThenFailsWithTheBound)
{
    Recording recording = build_recording(1);
    // Turn the first data frame into a frame type neither DATA nor PAD.
    write_int16(recording.bytes.data() + FRAME_TYPE_OFFSET, 3);
    Fixture fixture(recording);
    FakeWarnings warnings;

    auto session = fixture.make(nullptr, nullptr, &warnings);
    session->do_work();
    EXPECT_EQ(ReplaySession::State::replay, session->state());
    ASSERT_EQ(1U, warnings.frames.size());
    EXPECT_EQ(3, warnings.frames[0].frame_type);
    EXPECT_EQ(0, warnings.frames[0].position);
    EXPECT_TRUE(fixture.publication_raw->offered.empty());

    // Still inside the 1 s stall: no progress, no error.
    fixture.clock.ns += 500'000'000;
    session->do_work();
    EXPECT_EQ(ReplaySession::State::replay, session->state());
    EXPECT_TRUE(fixture.publication_raw->offered.empty());
    EXPECT_EQ(1U, warnings.frames.size());

    // Past the bound: error, revoke, no offer.
    fixture.clock.ns += 600'000'000;
    session->do_work();
    session->send_pending_error();
    ASSERT_EQ(1U, fixture.control.errors.size());
    EXPECT_NE(std::string::npos, fixture.control.errors[0].message.find("unexpected frame type 3 at position=0"));
    EXPECT_NE(std::string::npos, fixture.control.errors[0].message.find("no progress for"));
    session->close();
    EXPECT_TRUE(fixture.publication_raw->revoked);
    EXPECT_TRUE(fixture.publication_raw->offered.empty());
    EXPECT_EQ(ReplaySession::State::inactive, session->state());
}

TEST(ReplaySessionTest, anErroredSessionReachesDoneOnTheNextPass)
{
    const Recording recording = build_recording(1, 99);
    Fixture fixture(recording);
    FixedChecksum checksum{100};

    auto session = fixture.make(nullptr, &checksum);
    session->do_work();
    ASSERT_EQ(ReplaySession::State::inactive, session->state());
    ASSERT_FALSE(session->error_message().empty());

    // The reference's throw only skips the rest of that pass; the next pass must
    // still close the segment and report DONE (otherwise the conductor never
    // sends the pending ERROR response).
    session->do_work();
    EXPECT_TRUE(session->is_done());
}

TEST(ReplaySessionTest, aNormalPassRaisesNoUnexpectedFrameWarning)
{
    const Recording recording = build_recording(1);
    Fixture fixture(recording);
    FakeWarnings warnings;
    auto session = fixture.make(nullptr, nullptr, &warnings);

    ASSERT_LT(run_to_done(*session), 10000);
    EXPECT_TRUE(warnings.frames.empty());
}

TEST(ReplaySessionTest, advancesByTheAlignedFrameLength)
{
    const Recording recording = build_unaligned_recording();
    Fixture fixture(recording);

    auto session = fixture.make();
    ASSERT_LT(run_to_done(*session), 10000) << "replay did not finish";
    session->close();

    // A raw frameLength advance would land inside the padding slot and fail the
    // next frame read.
    EXPECT_TRUE(fixture.control.errors.empty());
    EXPECT_EQ(expected_published_data(recording, REPLAY_SESSION_ID_SESSION, RECORDING_STREAM_ID),
              fixture.publication_raw->offered);
    ASSERT_EQ(1U, fixture.publication_raw->padding_lengths.size());
    EXPECT_EQ(TERM_LENGTH - 1056 - DATA_HEADER_LENGTH, fixture.publication_raw->padding_lengths[0]);
    EXPECT_TRUE(fixture.publication_raw->closed);
    EXPECT_FALSE(fixture.publication_raw->revoked);
}

TEST(ReplaySessionTest, aProtocolErrorFromOfferEndsTheReplay)
{
    const Recording recording = build_recording(1);
    Fixture fixture(recording);
    fixture.publication_raw->fail = FakePublication::Fail::error;

    auto session = fixture.make();
    session->do_work();
    session->send_pending_error();
    session->close();

    ASSERT_EQ(1U, fixture.control.errors.size());
    EXPECT_NE(std::string::npos, fixture.control.errors[0].message.find("failed to offer replay block"));
    EXPECT_TRUE(fixture.publication_raw->revoked);
}

TEST(ReplaySessionTest, bothReadBackendsPublishTheSameBytes)
{
    const Recording recording = build_recording(3);

    for (const Backend backend : {Backend::pread, Backend::mmap})
    {
        Fixture fixture(recording);
        fixture.config.backend = backend;

        auto session = fixture.make();
        ASSERT_LT(run_to_done(*session), 10000) << "backend " << static_cast<int>(backend);
        session->close();

        EXPECT_TRUE(fixture.control.errors.empty());
        EXPECT_EQ(expected_published_data(recording, REPLAY_SESSION_ID_SESSION, RECORDING_STREAM_ID),
                  fixture.publication_raw->offered);

        // ★ Every segment still reads byte for byte: `MAP_PRIVATE` means the
        // rewritten headers never reach the recorded files (A6d-2).
        std::size_t offset = 0;
        for (std::int64_t base = 0; offset < recording.bytes.size(); base += SEGMENT_LENGTH)
        {
            const std::string path =
                fixture.dir.path() + "/" + std::to_string(RECORDING_ID) + "-" + std::to_string(base) + ".rec";
            std::ifstream file(path, std::ios::binary);
            const std::vector<std::uint8_t> bytes{std::istreambuf_iterator<char>{file},
                                                  std::istreambuf_iterator<char>{}};
            const std::size_t length =
                std::min(static_cast<std::size_t>(SEGMENT_LENGTH), recording.bytes.size() - offset);
            ASSERT_EQ(length, bytes.size()) << path;
            EXPECT_TRUE(
                std::equal(bytes.begin(), bytes.end(), recording.bytes.begin() + static_cast<std::ptrdiff_t>(offset)))
                << path;
            offset += length;
        }
    }
}

TEST(ReplaySessionTest, aShortReadStillReturnsTheFullLimitSoTheFrameLoopFails)
{
    // A truncated file is the reference's short-read path: `readRecording`
    // returns the limit anyway and the frame loop trips over the stale tail
    // (`ReplaySession.java:510-530`). Returning only the bytes actually read
    // would leave the session spinning on a complete prefix with no progress.
    const Recording recording = build_recording(1);

    for (const Backend backend : {Backend::pread, Backend::mmap})
    {
        Fixture fixture(recording);
        fixture.config.backend = backend;
        const std::string path = fixture.dir.path() + "/" + std::to_string(RECORDING_ID) + "-0.rec";
        ASSERT_EQ(0, ::truncate(path.c_str(), 100));

        auto session = fixture.make();
        EXPECT_LT(run_to_done(*session), 10000) << "backend " << static_cast<int>(backend);

        session->send_pending_error();
        ASSERT_EQ(1U, fixture.control.errors.size());
        EXPECT_NE(std::string::npos, fixture.control.errors[0].message.find("unexpected end of recording"));
        session->close();
    }
}

TEST(ReplaySessionTest, aLiveReplayKeepsReadingTheGrowingPage)
{
    // ★ A6d-4 finding: the first frame-header rewrite copy-on-writes the
    // mapping's 4 KiB page; a frame the recorder appends into the same page
    // afterwards must still be seen. A `MAP_PRIVATE` view would freeze the page
    // as zeros and the frame loop would end the replay with `unexpected end of
    // recording`, so a live replay (limit counter present) reads through
    // `pread` even when the mmap backend is configured.
    const Recording recording = build_live_recording();
    Fixture fixture(recording, 0, recording.stop_position, 0, FRAME_LENGTH);
    fixture.config.backend = Backend::mmap;

    auto limit = std::make_unique<FakeLimit>();
    limit->current = FRAME_LENGTH;
    FakeLimit* limit_raw = limit.get();

    auto session = fixture.make(std::move(limit));
    session->do_work();
    ASSERT_TRUE(session->error_message().empty()) << session->error_message();
    ASSERT_EQ(static_cast<std::size_t>(FRAME_LENGTH), fixture.publication_raw->offered.size());

    // The recorder appends the next frame into the same page and advances the
    // live limit the way the position counter does.
    std::vector<std::uint8_t> frame(static_cast<std::size_t>(FRAME_LENGTH), 0);
    write_data_frame(frame.data(), FRAME_LENGTH, FRAME_LENGTH, INITIAL_TERM_ID, RECORDING_SESSION_ID,
                     RECORDING_STREAM_ID, 2);
    append_frame_to_segment(fixture.dir.path(), 0, static_cast<std::size_t>(FRAME_LENGTH), frame);
    limit_raw->current = 2 * FRAME_LENGTH;

    session->do_work();
    ASSERT_TRUE(session->error_message().empty()) << session->error_message();
    ASSERT_EQ(static_cast<std::size_t>(2 * FRAME_LENGTH), fixture.publication_raw->offered.size());

    std::int32_t published_session_id = 0;
    std::memcpy(&published_session_id, fixture.publication_raw->offered.data() + FRAME_LENGTH + SESSION_ID_OFFSET,
                sizeof(published_session_id));
    EXPECT_EQ(REPLAY_SESSION_ID_SESSION, published_session_id);
    EXPECT_EQ(2, fixture.publication_raw->offered.back());

    limit_raw->closed = true;
    limit_raw->current = 2 * FRAME_LENGTH;
    for (int pass = 0; !session->is_done() && pass < 10; pass++)
    {
        session->do_work();
    }
    EXPECT_TRUE(session->is_done());
    session->close();
}

TEST(ReplaySessionTest, anUnalignedReplayPositionIsRejected)
{
    const Recording recording = build_recording(1);

    for (const Backend backend : {Backend::pread, Backend::mmap})
    {
        Fixture fixture(recording, 32);
        fixture.config.backend = backend;

        auto session = fixture.make();
        session->do_work();
        session->send_pending_error();

        ASSERT_EQ(1U, fixture.control.errors.size());
        EXPECT_EQ(16, fixture.control.errors[0].relevant_id);
        EXPECT_NE(std::string::npos, fixture.control.errors[0].message.find("does not point to a valid frame"));
        session->close();
        EXPECT_TRUE(fixture.publication_raw->revoked);
    }
}

TEST(ReplaySessionTest, verifiesTheChecksumBeforeRewritingTheHeader)
{
    // The recorded session id field carries the checksum (the reference reads it
    // with `frameSessionId`); a matching checksum proves the order: rewriting
    // first would compare against the replay publication's session id.
    Recording recording = build_recording(1, 99);
    Fixture fixture(recording);
    FixedChecksum checksum{99};

    auto session = fixture.make(nullptr, &checksum);
    ASSERT_LT(run_to_done(*session), 10000);
    EXPECT_TRUE(fixture.control.errors.empty());
    EXPECT_EQ(expected_published_data(recording, REPLAY_SESSION_ID_SESSION, RECORDING_STREAM_ID),
              fixture.publication_raw->offered);
}

TEST(ReplaySessionTest, aChecksumMismatchEndsTheReplayWithAnError)
{
    const Recording recording = build_recording(1, 99);
    Fixture fixture(recording);
    FixedChecksum checksum{100};

    auto session = fixture.make(nullptr, &checksum);
    session->do_work();
    session->send_pending_error();

    ASSERT_EQ(1U, fixture.control.errors.size());
    EXPECT_NE(std::string::npos, fixture.control.errors[0].message.find("CRC checksum mismatch"));
    session->close();
    EXPECT_TRUE(fixture.publication_raw->revoked);
    EXPECT_TRUE(fixture.publication_raw->offered.empty());
}

TEST(ReplaySessionTest, aClosedPublicationEndsTheReplayByRevoking)
{
    const Recording recording = build_recording(1);
    Fixture fixture(recording);
    fixture.publication_raw->fail = FakePublication::Fail::closed;

    auto session = fixture.make();
    session->do_work();
    session->do_work();

    EXPECT_TRUE(session->is_done());
    session->close();
    EXPECT_TRUE(fixture.publication_raw->revoked);
    EXPECT_TRUE(fixture.control.errors.empty());
}

}  // namespace
