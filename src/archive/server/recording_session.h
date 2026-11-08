#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <aeronc.h>

#include "archive/server/recording_writer.h"
#include "core/counters/counter_target.h"
#include "core/error/error.h"

namespace deepmsg::archive::server
{

/// `Image.blockPoll(BlockHandler, limit)` as the recording session uses it; the
/// C implementation is `aeron_image_block_poll` (`AeronRecordingImage`), and a
/// unit test can feed blocks without a driver.
class RecordingImage
{
public:
    RecordingImage() = default;
    virtual ~RecordingImage() = default;

    RecordingImage(const RecordingImage&) = delete;
    RecordingImage& operator=(const RecordingImage&) = delete;

    /// Bytes consumed this pass, or a negative error.
    virtual int block_poll(RecordingWriter& writer, std::int32_t block_length_limit) noexcept = 0;
    [[nodiscard]] virtual bool is_end_of_stream() const noexcept = 0;
    /// The first write error the poll hit, cleared by the caller.
    [[nodiscard]] virtual std::expected<void, axc::core::Error> take_error() = 0;
};

/// `aeron_image_t` behind the `RecordingImage` seam.
///
/// The C block handler has no `termOffset` (the Java `BlockHandler` does), so the
/// block start is passed as the base with an offset of 0: a block always starts
/// on a frame boundary and the writer only uses the pair as "base + offset".
class AeronRecordingImage final : public RecordingImage
{
public:
    explicit AeronRecordingImage(aeron_image_t* image) noexcept : image_(image) {}

    int block_poll(RecordingWriter& writer, std::int32_t block_length_limit) noexcept override;
    [[nodiscard]] bool is_end_of_stream() const noexcept override;
    [[nodiscard]] std::expected<void, axc::core::Error> take_error() override;

    [[nodiscard]] aeron_image_t* handle() const noexcept
    {
        return image_;
    }

private:
    static void block_handler(void* clientd, const std::uint8_t* buffer, std::size_t length, std::int32_t session_id,
                              std::int32_t term_id);

    aeron_image_t* image_{nullptr};
    RecordingWriter* writer_{nullptr};
    std::optional<axc::core::Error> error_{};
};

/// `RecordingSession` (`RecordingSession.java:53-301`): one recording's four
/// states. The conductor owns it through the `Recorder` (the reference's
/// `SessionWorker`).
class RecordingSession
{
public:
    /// `RecordingSession.State`.
    enum class State
    {
        init,
        recording,
        inactive,
        stopped
    };

    /// Everything `startRecordingSession` reads from the image and the request
    /// (`ArchiveConductor.java:2013-2044`).
    struct Config
    {
        std::int64_t correlation_id{0};
        std::int64_t recording_id{0};
        std::int64_t control_session_id{0};
        std::int64_t subscription_registration_id{0};
        std::int64_t start_position{0};
        std::int64_t join_position{0};
        std::int32_t segment_length{0};
        std::int32_t term_length{0};
        std::int32_t block_length_limit{0};
        std::int32_t session_id{0};
        std::int32_t stream_id{0};
        /// Identity only: compared by the conductor, never dereferenced after
        /// `abort`.
        aeron_image_t* image{nullptr};
        bool auto_stop{false};
        std::string original_channel;
        std::string stripped_channel;
        std::string source_identity;
    };

    RecordingSession(Config config, RecordingWriter writer, std::unique_ptr<axc::core::CounterTarget> position,
                     std::unique_ptr<RecordingImage> image) noexcept;

    RecordingSession(const RecordingSession&) = delete;
    RecordingSession& operator=(const RecordingSession&) = delete;

    /// Closing is idempotent; the conductor's close path and the worker's
    /// teardown both land here.
    ~RecordingSession();

    /// `RecordingSession.doWork` (`RecordingSession.java:131-163`).
    int do_work();

    /// `RecordingSession.abort` (`:108-113`).
    void abort(std::string_view reason);

    /// `RecordingSession.isDone` (`:98-101`).
    [[nodiscard]] bool is_done() const noexcept
    {
        return State::stopped == state_;
    }

    /// `RecordingSession.close` (`:118-122`): close the writer, then the counter.
    void close();

    [[nodiscard]] State state() const noexcept
    {
        return state_;
    }

    [[nodiscard]] std::int64_t recording_id() const noexcept
    {
        return config_.recording_id;
    }

    [[nodiscard]] std::int64_t correlation_id() const noexcept
    {
        return config_.correlation_id;
    }

    [[nodiscard]] std::int64_t control_session_id() const noexcept
    {
        return config_.control_session_id;
    }

    [[nodiscard]] std::int64_t subscription_registration_id() const noexcept
    {
        return config_.subscription_registration_id;
    }

    [[nodiscard]] std::int64_t join_position() const noexcept
    {
        return config_.join_position;
    }

    /// `RecordingSession.recordedPosition` (`:174-181`): `NULL_POSITION` once the
    /// counter is closed.
    [[nodiscard]] std::int64_t recorded_position() const noexcept;

    [[nodiscard]] bool auto_stop() const noexcept
    {
        return config_.auto_stop;
    }

    [[nodiscard]] std::string_view error_message() const noexcept
    {
        return error_message_;
    }

    [[nodiscard]] RecordingImage* image() const noexcept
    {
        return image_.get();
    }

    [[nodiscard]] aeron_image_t* image_handle() const noexcept
    {
        return config_.image;
    }

private:
    int init();
    int record();
    void set_state(State next, std::string_view reason);

    Config config_;
    RecordingWriter writer_;
    std::unique_ptr<axc::core::CounterTarget> position_;
    std::unique_ptr<RecordingImage> image_;
    State state_{State::init};
    std::string abort_reason_;
    std::string error_message_;
    bool aborted_{false};
};

}  // namespace deepmsg::archive::server
