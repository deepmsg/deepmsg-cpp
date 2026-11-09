#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "archive/server/replay_publication.h"
#include "core/deepmsg/async_add.h"
#include "core/deepmsg/handles.h"

namespace deepmsg::archive::server
{

class CreateReplayPublicationSessionHandler;

/// `CreateReplayPublicationSession` (`CreateReplayPublicationSession.java:26-184`):
/// adds the replay publication asynchronously and hands it to a `ReplaySession`.
///
/// `RESOURCE_TEMPORARILY_UNAVAILABLE` is retried on a later pass (the reference
/// clears the registration id and returns 0); any other failure ends the replay
/// with an ERROR response.
class CreateReplayPublicationSession
{
public:
    /// Everything `startReplay` passes (`ArchiveConductor.java:903-920`).
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
        std::int32_t replay_stream_id{0};
        std::string replay_channel;
        std::int32_t file_io_max_length{0};
        std::int64_t control_session_id{0};
        /// `AERON_NULL_COUNTER_ID` when the recording's own position counter is
        /// the limit; otherwise the client's bounded counter.
        std::int32_t limit_counter_id{-1};
        bool limit_client_supplied{false};
    };

    CreateReplayPublicationSession(Config config, CreateReplayPublicationSessionHandler& handler) noexcept;
    ~CreateReplayPublicationSession();

    CreateReplayPublicationSession(const CreateReplayPublicationSession&) = delete;
    CreateReplayPublicationSession& operator=(const CreateReplayPublicationSession&) = delete;

    /// `CreateReplayPublicationSession.doWork` (`:126-183`).
    int do_work();

    [[nodiscard]] bool is_done() const noexcept
    {
        return done_;
    }

    /// Cancel a pending add (the worker's teardown path).
    void close() noexcept;

    [[nodiscard]] const Config& config() const noexcept
    {
        return config_;
    }

private:
    Config config_;
    CreateReplayPublicationSessionHandler& handler_;
    std::optional<axc::core::AsyncAdd<axc::core::ExPubPtr>> add_;
    bool done_{false};
};

/// The conductor side of the session (`ArchiveConductor`).
class CreateReplayPublicationSessionHandler
{
public:
    CreateReplayPublicationSessionHandler() = default;
    virtual ~CreateReplayPublicationSessionHandler() = default;

    CreateReplayPublicationSessionHandler(const CreateReplayPublicationSessionHandler&) = delete;
    CreateReplayPublicationSessionHandler& operator=(const CreateReplayPublicationSessionHandler&) = delete;

    /// `aeron.asyncAddExclusivePublication(replayChannel, replayStreamId)`.
    virtual axc::core::AsyncAdd<axc::core::ExPubPtr> add_replay_publication(std::string_view channel,
                                                                            std::int32_t stream_id) = 0;
    /// `conductor.onReplayEnd()` + `controlSession.sendErrorResponse(...)` + rethrow.
    virtual void replay_publication_failed(std::int64_t control_session_id, std::int64_t correlation_id,
                                           std::string_view message) = 0;
    /// `conductor.newReplaySession(...)` (`ArchiveConductor.java:941-994`).
    virtual void new_replay_session(const CreateReplayPublicationSession::Config& config,
                                    std::unique_ptr<ReplayPublication> publication) = 0;
};

}  // namespace deepmsg::archive::server
