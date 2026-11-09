#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <span>
#include <string>
#include <string_view>

#include "archive/server/checksum.h"
#include "archive/server/epoch_clock_source.h"
#include "archive/server/frame_layout.h"
#include "archive/server/replay_publication.h"
#include "core/error/error.h"

namespace deepmsg::archive::server
{

/// The two responses a replay session sends through its control session
/// (`ReplaySession.java:288-295`, `:338`). The conductor implements this by id
/// lookup: Java keeps the `ControlSession` object alive, C++ must not hold a
/// pointer to a session the `SessionWorker` may destroy.
class ReplayControlResponses
{
public:
    ReplayControlResponses() = default;
    virtual ~ReplayControlResponses() = default;

    ReplayControlResponses(const ReplayControlResponses&) = delete;
    ReplayControlResponses& operator=(const ReplayControlResponses&) = delete;

    /// `ControlSession.asyncSendOkResponse` (`ControlSession.java:733-745`).
    virtual void async_send_ok(std::int64_t control_session_id, std::int64_t correlation_id,
                               std::int64_t replay_session_id) = 0;
    /// `ControlSession.sendErrorResponse(correlationId, errorCode, message)`.
    virtual void send_error(std::int64_t control_session_id, std::int64_t correlation_id, std::int64_t relevant_id,
                            std::string_view message) = 0;
};

/// `ReplaySession`'s view of its limit position counter
/// (`ReplaySession.java:544-578`). Java holds a `Counter` whose `isClosed()`
/// flips when the owner closes it; the conductor's `CounterState` backs this.
class ReplayLimitSource
{
public:
    ReplayLimitSource() = default;
    virtual ~ReplayLimitSource() = default;

    ReplayLimitSource(const ReplayLimitSource&) = delete;
    ReplayLimitSource& operator=(const ReplayLimitSource&) = delete;

    [[nodiscard]] virtual std::int64_t value() const noexcept = 0;
    [[nodiscard]] virtual bool is_closed() const noexcept = 0;
    /// `countersReader.getCounterRegistrationId(id) == counter.registrationId()`.
    [[nodiscard]] virtual bool registration_matches() const noexcept = 0;
};

/// `Replayer.bytesRead` / `readTimeNs` (`ArchiveConductor.java:2795-2804`).
class ReplayMetrics
{
public:
    ReplayMetrics() = default;
    virtual ~ReplayMetrics() = default;

    ReplayMetrics(const ReplayMetrics&) = delete;
    ReplayMetrics& operator=(const ReplayMetrics&) = delete;

    virtual void bytes_read(std::int64_t bytes) noexcept = 0;
    virtual void read_time_ns(std::int64_t nanos) noexcept = 0;
};

/// The archive-wide warning sink (`ArchiveConductor.logWarning`). Used once when
/// a frame type the reference cannot handle is first seen; `ArchiveTracing`
/// lands with A6.
class ReplayWarningSink
{
public:
    ReplayWarningSink() = default;
    virtual ~ReplayWarningSink() = default;

    ReplayWarningSink(const ReplayWarningSink&) = delete;
    ReplayWarningSink& operator=(const ReplayWarningSink&) = delete;

    virtual void unexpected_frame(std::int32_t frame_type, std::int64_t position) = 0;
};

/// `EpochClock` + `NanoClock` as the replay session reads them
/// (`ReplaySession.java:103-105`).
class ReplayClock
{
public:
    ReplayClock() = default;
    virtual ~ReplayClock() = default;

    ReplayClock(const ReplayClock&) = delete;
    ReplayClock& operator=(const ReplayClock&) = delete;

    [[nodiscard]] virtual std::int64_t time_ms() const noexcept = 0;
    [[nodiscard]] virtual std::int64_t nano_time() const noexcept = 0;
};

/// Production clock: the conductor's cached epoch time and `NanoClock`.
class SystemReplayClock final : public ReplayClock
{
public:
    explicit SystemReplayClock(const EpochClockSource& epoch) noexcept : epoch_(epoch) {}

    [[nodiscard]] std::int64_t time_ms() const noexcept override
    {
        return epoch_.time_ms();
    }
    [[nodiscard]] std::int64_t nano_time() const noexcept override;

private:
    const EpochClockSource& epoch_;
};

/// `ReplaySession` (`ReplaySession.java:73-662`): one replay's four states.
///
/// The session owns the replay publication (Java's `ExclusivePublication`) and
/// is owned by the `Replayer` (the reference's `SessionWorker`).
class ReplaySession final
{
public:
    /// `ReplaySession.State`.
    enum class State
    {
        init,
        replay,
        inactive,
        done
    };

    /// Everything `newReplaySession` passes in (`ArchiveConductor.java:968-990`).
    struct Config
    {
        std::int64_t correlation_id{0};
        std::int64_t recording_id{0};
        std::int64_t replay_position{0};
        std::int64_t replay_length{0};
        std::int64_t start_position{0};
        std::int64_t stop_position{0};
        std::int32_t segment_file_length{0};
        std::int32_t term_buffer_length{0};
        std::int32_t stream_id{0};
        std::int64_t replay_session_id{0};
        std::int64_t control_session_id{0};
        std::int64_t connect_timeout_ms{0};
        std::string archive_dir;
    };

    ReplaySession(Config config, std::unique_ptr<ReplayPublication> publication, ReplayControlResponses& control,
                  ReplayMetrics& metrics, ReplayClock& clock, const Checksum* checksum,
                  std::unique_ptr<ReplayLimitSource> limit, std::span<std::uint8_t> buffer,
                  ReplayWarningSink* warnings = nullptr) noexcept;

    ~ReplaySession();

    ReplaySession(const ReplaySession&) = delete;
    ReplaySession& operator=(const ReplaySession&) = delete;

    /// `ReplaySession.doWork` (`:209-243`).
    int do_work();

    /// `ReplaySession.abort` (`:248-252`).
    void abort(std::string_view reason) noexcept;

    [[nodiscard]] bool is_done() const noexcept
    {
        return State::done == state_;
    }

    [[nodiscard]] State state() const noexcept
    {
        return state_;
    }

    /// `ReplaySession.close` (`:174-194`): revoke on error/abort, close otherwise.
    void close() noexcept;

    /// `ReplaySession.sendPendingError` (`:288-295`); called by the conductor's
    /// close path before the session is destroyed.
    void send_pending_error();

    [[nodiscard]] std::int64_t recording_id() const noexcept
    {
        return config_.recording_id;
    }

    [[nodiscard]] std::int64_t session_id() const noexcept
    {
        return config_.replay_session_id;
    }

    [[nodiscard]] std::int64_t segment_file_base_position() const noexcept
    {
        return segment_file_base_position_;
    }

    [[nodiscard]] std::string_view replay_channel() const noexcept
    {
        return publication_->channel();
    }

    [[nodiscard]] std::int32_t replay_stream_id() const noexcept
    {
        return publication_->stream_id();
    }

    [[nodiscard]] std::string_view error_message() const noexcept
    {
        return error_message_;
    }

    /// The replay buffer slice (`min(request.fileIoMaxLength, ctx.replayBuffer)`).
    static constexpr std::int32_t DEFAULT_FILE_IO_MAX_LENGTH = 1024 * 1024;
    /// `UNEXPECTED_FRAME_TIMEOUT_NS`: the reference spins forever on a frame that
    /// is neither DATA nor PAD (`ReplaySession.java:397-430`); the stall is kept
    /// but bounded (A3b plan §3.3).
    static constexpr std::int64_t UNEXPECTED_FRAME_TIMEOUT_NS = 1'000'000'000;

private:
    [[nodiscard]] int init();
    [[nodiscard]] int replay();
    [[nodiscard]] bool not_extended(std::int64_t replay_position, std::int64_t old_stop_position);
    [[nodiscard]] bool has_publication_advanced(std::int64_t position, std::int32_t aligned_length);
    [[nodiscard]] int read_recording(std::int64_t available_replay);
    void verify_checksum(std::int32_t frame_offset, std::int32_t aligned_length);
    [[nodiscard]] bool not_header_aligned(std::int32_t segment_offset, std::int32_t term_offset, std::int32_t term_id);
    void next_term();
    [[nodiscard]] bool open_recording_segment();
    void close_recording_segment() noexcept;
    void raise_error(std::string_view message, std::int64_t error_code);
    void set_state(State next, std::string_view reason);
    [[nodiscard]] std::string frame_position(std::int32_t frame_offset) const;

    Config config_;
    std::unique_ptr<ReplayPublication> publication_;
    ReplayControlResponses& control_;
    ReplayMetrics& metrics_;
    ReplayClock& clock_;
    const Checksum* checksum_{nullptr};
    std::unique_ptr<ReplayLimitSource> limit_{};
    ReplayWarningSink* warnings_{nullptr};
    std::span<std::uint8_t> buffer_{};

    std::int64_t segment_file_base_position_{0};
    std::int32_t term_base_segment_offset_{0};
    std::int32_t term_offset_{0};
    std::int64_t replay_position_{0};
    std::int64_t stop_position_{0};
    std::int64_t replay_limit_{0};
    std::int64_t connect_deadline_ms_{0};

    int fd_{-1};

    State state_{State::init};
    std::string error_message_;
    std::int64_t error_code_{0};
    std::string abort_reason_;
    bool revoke_publication_{false};
    bool aborted_{false};
    bool closed_{false};

    /// Stall tracking for a frame type the reference cannot handle.
    std::int64_t stuck_position_{-1};
    std::int64_t stuck_deadline_ns_{0};
};

}  // namespace deepmsg::archive::server
