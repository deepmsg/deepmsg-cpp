#pragma once

#include <cstdint>
#include <expected>
#include <memory>
#include <string>
#include <string_view>

#include <aeronc.h>

#include <client/aeron_archive.h>

#include "cluster/consensus/consensus_constants.h"
#include "cluster/consensus/log_adapter.h"
#include "cluster/election/election_host.h"
#include "cluster/service/role.h"
#include "core/deepmsg/async_add.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/deepmsg/handles.h"
#include "core/error/error.h"

namespace deepmsg::cluster::consensus
{

using axc::core::Error;

/// What `LogReplay` asks of the consensus module (`LogReplay.java:94-109`).
class LogReplayHost
{
public:
    LogReplayHost() = default;
    LogReplayHost(const LogReplayHost&) = delete;
    LogReplayHost& operator=(const LogReplayHost&) = delete;
    virtual ~LogReplayHost() = default;

    /// `consensusModuleAgent.awaitServicesReady(...)` (`:94-101`); blocking.
    [[nodiscard]] virtual std::expected<void, Error> await_services_ready(
        std::string_view log_channel, std::int32_t stream_id, std::int32_t log_session_id, std::int64_t log_position,
        std::int64_t max_log_position, bool is_startup, service::Role role) = 0;

    /// `consensusModuleAgent.replayLogPoll(logAdapter, stopPosition)`
    /// (`:108`).
    [[nodiscard]] virtual std::expected<int, Error> replay_log_poll(LogAdapter& log_adapter,
                                                                    std::int64_t stop_position) = 0;

    /// `ConsensusModule.State.SNAPSHOT == state` (`LogReplay.java:117`).
    [[nodiscard]] virtual bool is_snapshot_state() const = 0;

    /// The reference throws out of `LogReplay`; the C++ seam has no error
    /// channel, so a terminal replay failure is reported through the host.
    virtual void on_log_replay_error(const Error& error) = 0;
};

/// `LogReplay` (`LogReplay.java`): replays our own log tail into the state
/// during a leader election. The archive replay is anchored to an existing
/// subscription (`subscription_registration_id` in
/// `aeron_archive_replay_params_t`) — the C client's replay would otherwise
/// create the subscription too late and lose the first fragments (C2b's
/// finding).
class LogReplay final : public election::LogReplay
{
public:
    LogReplay(axc::core::DeepmsgClient& client, aeron_archive_t* archive, std::int64_t recording_id,
              std::int64_t start_position, std::int64_t stop_position, LogAdapter& log_adapter,
              std::string replay_channel, std::int32_t replay_stream_id, LogReplayHost& host);
    ~LogReplay() override;

    LogReplay(const LogReplay&) = delete;
    LogReplay& operator=(const LogReplay&) = delete;

    /// `doWork()` (`:77-112`).
    [[nodiscard]] int do_work() override;
    /// `isDone()` (`:114-118`).
    [[nodiscard]] bool is_done() const override;
    /// `position()` (`:120-123`).
    [[nodiscard]] std::int64_t position() const override;
    /// `close()` (`:62-75`).
    void close() override;

private:
    axc::core::DeepmsgClient* client_;
    aeron_archive_t* archive_;
    std::int64_t recording_id_;
    std::int64_t start_position_;
    std::int64_t stop_position_;
    LogAdapter* log_adapter_;
    std::string replay_channel_;
    std::int32_t replay_stream_id_;
    LogReplayHost* host_;

    axc::core::SubPtr subscription_{};
    std::int64_t replay_session_id_{NULL_VALUE};
    std::int32_t log_session_id_{0};
    bool closed_{false};
};

}  // namespace deepmsg::cluster::consensus
