#pragma once

#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>

#include <aeronc.h>

#include "cluster/common/cluster_member.h"
#include "cluster/election/election_state.h"
#include "cluster/service/role.h"
#include "core/error/error.h"

namespace deepmsg::cluster::common
{
class NodeStateFile;
class RecordingLog;
}  // namespace deepmsg::cluster::common

namespace deepmsg::cluster::election
{

using axc::core::Error;

/// `Aeron.NULL_VALUE` / `AeronArchive.NULL_POSITION` / `CommonContext.NULL_SESSION_ID`.
inline constexpr std::int64_t NULL_VALUE = -1;
inline constexpr std::int64_t NULL_POSITION = -1;
inline constexpr std::int32_t NULL_SESSION_ID = -1;
/// `ConsensusModuleAgent.APPEND_POSITION_FLAG_NONE`.
inline constexpr std::int16_t APPEND_POSITION_FLAG_NONE = 0;

/// `ConsensusModule.State` (`ConsensusModule.java:146-181`). The CM itself is
/// C4a's; the election only reads it, once (`Election.java:1104`).
enum class ModuleState : std::int32_t
{
    init = 0,
    active = 1,
    suspended = 2,
    snapshot = 3,
    quitting = 4,
    terminating = 5,
    closed = 6
};

/// `LogReplay` (`LogReplay.java`): replaying our own log tail into the state
/// during a leader election. The election drives it and closes it; C4d owns the
/// implementation.
class LogReplay
{
public:
    virtual ~LogReplay() = default;

    [[nodiscard]] virtual int do_work() = 0;
    [[nodiscard]] virtual bool is_done() const = 0;
    /// `LogReplay.position()` (`Election.java:963`).
    [[nodiscard]] virtual std::int64_t position() const = 0;
    virtual void close() = 0;
};

/// `RecordingReplication` (`RecordingReplication.java`): catching up a follower
/// from another archive. Driven through its recording signals; C6b owns the
/// implementation.
class RecordingReplication
{
public:
    virtual ~RecordingReplication() = default;

    virtual void on_signal(std::int64_t correlation_id, std::int64_t recording_id, std::int64_t position,
                           std::int32_t signal) = 0;
    /// `RecordingReplication.poll(long)` (`Election.java:925`). The reference
    /// throws out of `poll` when the replication cannot progress; the error is
    /// returned so it ends this `doWork` pass the same way.
    [[nodiscard]] virtual std::expected<int, Error> poll(std::int64_t now_ns) = 0;
    /// `hasReplicationEnded()` (`Election.java:927`).
    [[nodiscard]] virtual bool has_replication_ended() const = 0;
    /// `hasStopped()` (`Election.java:927`).
    [[nodiscard]] virtual bool has_stopped() const = 0;
    /// `hasSynced()` (`Election.java:945`).
    [[nodiscard]] virtual bool has_synced() const = 0;
    /// `srcArchiveChannel()` (`Election.java:941`).
    [[nodiscard]] virtual std::string_view src_archive_channel() const = 0;
    [[nodiscard]] virtual std::int64_t recording_id() const = 0;
    [[nodiscard]] virtual std::int64_t position() const = 0;
    virtual void close() = 0;
};

/// An image on the follower's log subscription, with the position the
/// subscriber joined it at: the C client exposes the join position as a
/// constant (`aeron_image_constants_t`), not on the image itself.
struct LogImage
{
    aeron_image_t* image{nullptr};
    std::int64_t join_position{0};
};

/// One `Election.state(...)` transition, as the reference's
/// `ClusterTracing.traceElectionStateChange` sees it (`Election.java:1540-1570`).
struct ElectionStateChange
{
    std::int32_t member_id{static_cast<std::int32_t>(NULL_VALUE)};
    ElectionState old_state{ElectionState::init};
    ElectionState new_state{ElectionState::init};
    std::int32_t leader_id{static_cast<std::int32_t>(NULL_VALUE)};
    std::int64_t candidate_term_id{NULL_VALUE};
    std::int64_t leadership_term_id{NULL_VALUE};
    std::int64_t log_position{NULL_POSITION};
    std::int64_t log_leadership_term_id{NULL_VALUE};
    std::int64_t append_position{NULL_POSITION};
    std::int64_t catchup_position{NULL_POSITION};
    std::string_view reason{};
};

/// The `ConsensusModule.Context` accessors the election reads, as one value
/// object rather than 14 more methods on the frozen host seam (X0-6's
/// `election-host-table.md`, §「不在 ElectionHost 里的 ctx 访问器」).
///
/// The defaults are the reference's own (`ConsensusModule.Configuration`), so a
/// test only overrides what it drives.
struct ElectionContext
{
    /// `ctx.nodeStateFile()` (`Election.java:357`, `:376`, `:681`, `:687`).
    common::NodeStateFile* node_state_file{nullptr};
    /// `ctx.recordingLog()` (`Election.java:1509`).
    common::RecordingLog* recording_log{nullptr};
    /// `ctx.clusterClock()` in nanoseconds (`Election.java:1598-1601`).
    std::function<std::int64_t()> now_ns{[] { return 0; }};
    /// `ctx.epochClock().time()` (`Election.java:688`).
    std::function<std::int64_t()> epoch_ms{[] { return 0; }};
    /// `ctx.clusterClock().timeUnit()` (`Election.java:1601-1604`).
    service::TimeUnit time_unit{service::TimeUnit::milliseconds};
    /// `ctx.fileSyncLevel()` (`Election.java:1520`).
    std::int32_t file_sync_level{0};
    /// `ctx.appointedLeaderId()` (`Election.java:715`).
    std::int32_t appointed_leader_id{static_cast<std::int32_t>(NULL_VALUE)};
    /// `ctx.startupCanvassTimeoutNs()` (`Election.java:704`).
    std::int64_t startup_canvass_timeout_ns{20'000'000'000};
    /// `ctx.leaderHeartbeatTimeoutNs()`.
    std::int64_t leader_heartbeat_timeout_ns{10'000'000'000};
    /// `ctx.leaderHeartbeatIntervalNs()`.
    std::int64_t leader_heartbeat_interval_ns{100'000'000};
    /// `ctx.electionTimeoutNs()`.
    std::int64_t election_timeout_ns{1'000'000'000};
    /// `ctx.electionStatusIntervalNs()`.
    std::int64_t election_status_interval_ns{100'000'000};
    /// `ctx.appVersion()` (`Election.java:1295`).
    std::int32_t app_version{0};
    /// `ctx.consensusStreamId()` (`Election.java:1242`), for a member's
    /// consensus publication.
    std::int32_t consensus_stream_id{0};
    /// `ctx.logChannel()` (`Election.java:1346`, `:1360`).
    std::string log_channel{};
    /// `ctx.logStreamId()` (`Election.java:1382`).
    std::int32_t log_stream_id{0};
    /// `ctx.isLogMdc()` (`Election.java:1354`).
    bool log_is_mdc{false};
    /// `ctx.commitPositionCounter().getPlain()` (`Election.java:265`, `:456`).
    std::function<std::int64_t()> commit_position{[] { return 0; }};
    /// `ctx.random().nextDouble()` (`Election.java:723`).
    std::function<double()> random{[] { return 0.0; }};
    /// `ctx.electionStateCounter().setRelease(newState.code())`
    /// (`Election.java:1433`): counter 207.
    std::function<void(ElectionState)> set_election_state{[](ElectionState) {}};
    /// `ctx.electionCounter().incrementRelease()` (`Election.java:148`).
    std::function<void()> increment_election_counter{[] {}};
    /// `ClusterTracing.traceElectionStateChange(...)` (`Election.java:1540-1570`).
    std::function<void(const ElectionStateChange&)> trace_state_change{[](const ElectionStateChange&) {}};

    /// `nanosToTimestamp(ctx, nowNs)` (`Election.java:1601-1604`): nanosecond
    /// time in the cluster clock's own unit, which is what `RecordingLog`
    /// stores.
    [[nodiscard]] std::int64_t timestamp_from_nanos(std::int64_t nanos) const noexcept;
};

/// What the election asks of the consensus module: the exact
/// `consensusModuleAgent.*` call surface of `Election.java` at 1.54.0 —
/// **30 signatures / 44 calls** (`X0-6/election-host-table.md`).
///
/// **This seam is frozen** (the card's definition of done). Methods may be
/// added when a later card discovers a gap; they are not changed or removed.
///
/// Departure: the reference hands over its `ExclusivePublication` and its
/// `ClusterMember` objects, because they live in the same JVM. Here the member
/// table belongs to the host and the publications belong to the consensus
/// sender, so the host takes `ClusterMember&` for the two calls that name a
/// member and the sender takes a member id.
class ElectionHost
{
public:
    virtual ~ElectionHost() = default;

    // ---- the two counter-backed axes ------------------------------------

    /// `role(Cluster.Role)` (`ConsensusModuleAgent.java:1337`).
    virtual void set_role(service::Role role) = 0;
    /// `role()` (`ConsensusModuleAgent.java:1355`).
    [[nodiscard]] virtual service::Role role() const = 0;
    /// `state()` (`ConsensusModuleAgent.java:1323`). The reference only reads
    /// it from the election (`Election.java:1104`); the plan's table listed a
    /// setter by mistake, which X0-6 caught.
    [[nodiscard]] virtual ModuleState state() const = 0;

    // ---- log recording identity -----------------------------------------

    /// `logRecordingId()` (`ConsensusModuleAgent.java:1885`).
    [[nodiscard]] virtual std::int64_t log_recording_id() const = 0;
    /// `logRecordingId(long)` (`ConsensusModuleAgent.java:1890`), called from
    /// `Election.java:280` when a replication produces a recording.
    virtual void set_log_recording_id(std::int64_t recording_id) = 0;
    /// `timeOfLastLeaderUpdateNs()` (`ConsensusModuleAgent.java:2926`).
    [[nodiscard]] virtual std::int64_t time_of_last_leader_update_ns() const = 0;

    // ---- positions -------------------------------------------------------

    /// `quorumPositionBoundedByLeaderLog(long, long)`
    /// (`ConsensusModuleAgent.java:2915`).
    [[nodiscard]] virtual std::int64_t quorum_position_bounded_by_leader_log(std::int64_t leader_append_position,
                                                                             std::int64_t now_ns) = 0;

    /// `updateMemberLogPosition(ClusterMember, long, long)`
    /// (`ConsensusModuleAgent.java:1100`).
    virtual void update_member_log_position(common::ClusterMember& member, std::int64_t leadership_term_id,
                                            std::int64_t log_position) = 0;

    /// `updateLeaderPosition(long, long, long)` (`ConsensusModuleAgent.java:2931`)
    /// — the three-argument overload the election calls; the one-argument form
    /// at `:2897` is private and not on this seam.
    [[nodiscard]] virtual std::expected<int, Error> update_leader_position(std::int64_t now_ns,
                                                                           std::int64_t append_position,
                                                                           std::int64_t quorum_position) = 0;

    /// `publishCommitPosition(long, long)` (`ConsensusModuleAgent.java:2958`).
    [[nodiscard]] virtual std::expected<void, Error> publish_commit_position(std::int64_t commit_position,
                                                                             std::int64_t leadership_term_id) = 0;

    // ---- joining the log -------------------------------------------------

    /// `joinLogAsLeader(long, long, int, boolean)` (`ConsensusModuleAgent.java:1704`).
    /// **Blocks** (creates the recording and waits for the append-position
    /// counter and the services).
    [[nodiscard]] virtual std::expected<void, Error> join_log_as_leader(std::int64_t leadership_term_id,
                                                                        std::int64_t log_position,
                                                                        std::int32_t log_session_id,
                                                                        bool is_startup) = 0;

    /// `tryJoinLogAsFollower(Image, boolean, long)` (`ConsensusModuleAgent.java:1787`).
    /// **Blocks**.
    [[nodiscard]] virtual std::expected<bool, Error> try_join_log_as_follower(aeron_image_t* image,
                                                                              bool is_leader_startup,
                                                                              std::int64_t now_ns) = 0;

    /// `addLogPublication(long)` (`ConsensusModuleAgent.java:1655`). Returns the
    /// log publication's session id, which `Election.java:1401` keeps as
    /// `logSessionId`.
    [[nodiscard]] virtual std::expected<std::int32_t, Error> add_log_publication(std::int64_t append_position) = 0;

    /// `newLogReplay(long, long)` (`ConsensusModuleAgent.java:1856`): replay
    /// our own log tail. Polled through the returned handle.
    [[nodiscard]] virtual std::expected<std::unique_ptr<LogReplay>, Error> new_log_replay(
        std::int64_t log_position, std::int64_t append_position) = 0;

    /// `newLogReplication(String, String, long, long, long)`
    /// (`ConsensusModuleAgent.java:2969`). The second endpoint is the member's
    /// `archiveResponseEndpoint`; the reference null-checks it to choose a
    /// plain or a response-mode replication channel (`:2982-2990`).
    [[nodiscard]] virtual std::expected<std::unique_ptr<RecordingReplication>, Error> new_log_replication(
        std::string_view leader_archive_endpoint, std::optional<std::string> response_archive_endpoint,
        std::int64_t leader_recording_id, std::int64_t stop_position, std::int64_t now_ns) = 0;

    /// `truncateLogEntry(long, long)` (`ConsensusModuleAgent.java:1898`). The
    /// caller always raises a `ClusterEvent` afterwards
    /// (`Election.java:634-645`) — that is the caller's job, not this one's.
    [[nodiscard]] virtual std::expected<void, Error> truncate_log_entry(std::int64_t leadership_term_id,
                                                                        std::int64_t log_position) = 0;

    /// `appendNewLeadershipTermEvent(long)` (`ConsensusModuleAgent.java:1909`).
    [[nodiscard]] virtual std::expected<bool, Error> append_new_leadership_term_event(std::int64_t now_ns) = 0;

    /// `prepareForNewLeadership(long, long)` (`ConsensusModuleAgent.java:1360`):
    /// stop recording, close the log, roll back uncommitted state and report
    /// the position to start from (`NULL_POSITION` when unchanged).
    [[nodiscard]] virtual std::expected<std::int64_t, Error> prepare_for_new_leadership(std::int64_t log_position,
                                                                                        std::int64_t now_ns) = 0;

    // ---- catchup ---------------------------------------------------------

    /// `catchupInitiated(long)` (`ConsensusModuleAgent.java:1986`).
    [[nodiscard]] virtual std::expected<void, Error> catchup_initiated(std::int64_t now_ns) = 0;

    /// `catchupPoll(long, long)` (`ConsensusModuleAgent.java:1992`): poll the
    /// catchup up to `limit_position`.
    [[nodiscard]] virtual std::expected<int, Error> catchup_poll(std::int64_t limit_position, std::int64_t now_ns) = 0;

    /// `isCatchupNearLive(long)` (`ConsensusModuleAgent.java:2039`).
    [[nodiscard]] virtual bool is_catchup_near_live(std::int64_t position) = 0;

    /// `trackCatchupCompletion(ClusterMember, long, short)`
    /// (`ConsensusModuleAgent.java:1962`).
    [[nodiscard]] virtual std::expected<void, Error> track_catchup_completion(common::ClusterMember& member,
                                                                              std::int64_t leadership_term_id,
                                                                              std::int16_t append_position_flags) = 0;

    /// `stopAllCatchups()` (`ConsensusModuleAgent.java:2053`).
    [[nodiscard]] virtual std::expected<void, Error> stop_all_catchups() = 0;

    /// `liveLogDestination()` (`ConsensusModuleAgent.java:1762`), `""` when unset.
    [[nodiscard]] virtual std::string_view live_log_destination() const = 0;
    /// `liveLogDestination(String)` (`ConsensusModuleAgent.java:1757`).
    [[nodiscard]] virtual std::expected<void, Error> set_live_log_destination(std::string_view destination) = 0;
    /// `catchupLogDestination()` (`ConsensusModuleAgent.java:1772`), `""` when unset.
    [[nodiscard]] virtual std::string_view catchup_log_destination() const = 0;
    /// `catchupLogDestination(String)` (`ConsensusModuleAgent.java:1767`).
    [[nodiscard]] virtual std::expected<void, Error> set_catchup_log_destination(std::string_view destination) = 0;

    // ---- lifecycle -------------------------------------------------------

    /// `awaitLocalSocketsClosed(long)` (`ConsensusModuleAgent.java:3006`).
    /// **Blocks**; a follower cannot rebind a UDP port until the old image's
    /// sockets are really free.
    [[nodiscard]] virtual std::expected<void, Error> await_local_sockets_closed(std::int64_t registration_id) = 0;

    /// `electionComplete(long)` (`ConsensusModuleAgent.java:1921`): hand control
    /// back to the steady state, having recorded the term and the leader this
    /// election settled on (`:1907`, `:1930`). ★ The term and the leader are
    /// the election's, not the agent's — they are handed over here (lesson 3).
    [[nodiscard]] virtual std::expected<void, Error> election_complete(std::int64_t leadership_term_id,
                                                                       std::int32_t leader_member_id,
                                                                       std::int64_t log_position,
                                                                       std::int64_t now_ns) = 0;

    /// `pollArchiveEvents()` (`ConsensusModuleAgent.java:2077`).
    [[nodiscard]] virtual std::expected<int, Error> poll_archive_events() = 0;

    // ---- added by C5b: the member publications and the log subscription ----
    //
    // Add-only, as the freeze allows: the frozen 30 above are untouched. The
    // reference reaches these through `ClusterMember.publication()` and its
    // own `Aeron` client; here the member table is ours to hold but the
    // publications and the subscription belong to the host.

    /// `null == member.publication()` (`Election.java:1238`).
    [[nodiscard]] virtual bool member_has_publication(std::int32_t member_id) const = 0;

    /// `ClusterMember.tryAddPublication(...)` (`ClusterMember.java:1073-1098`),
    /// called for a member with no publication in `publishCanvassPosition`
    /// (`Election.java:1238-1245`).
    [[nodiscard]] virtual std::expected<void, Error> try_add_member_publication(std::int32_t member_id) = 0;

    /// `addFollowerSubscription()` (`Election.java:1366-1383`): the manual-MDC
    /// log subscription a follower joins through, pinned to the leader's
    /// `logSessionId`. Answers its registration id.
    [[nodiscard]] virtual std::expected<std::int64_t, Error> add_log_subscription(std::int32_t log_session_id) = 0;

    /// `CloseHelper.close(logSubscription)` (`Election.java:674-676`).
    [[nodiscard]] virtual std::expected<void, Error> remove_log_subscription(std::int64_t registration_id) = 0;

    /// `logSubscription.addDestination(destination)` (`Election.java:1347`,
    /// `:1362`).
    [[nodiscard]] virtual std::expected<void, Error> add_log_destination(std::int64_t registration_id,
                                                                         std::string_view destination) = 0;

    /// `logSubscription.resolvedEndpoint()` (`Election.java:1032`).
    [[nodiscard]] virtual std::optional<std::string> log_subscription_resolved_endpoint(
        std::int64_t registration_id) const = 0;

    /// `ChannelEndpointStatus.ERRORED == logSubscription.channelStatus()`
    /// (`Election.java:1071`, `:1153`).
    [[nodiscard]] virtual bool log_subscription_channel_errored(std::int64_t registration_id) const = 0;

    /// `logSubscription.channel()`, for the two failure messages
    /// (`Election.java:1075`, `:1157`).
    [[nodiscard]] virtual std::string_view log_subscription_channel(std::int64_t registration_id) const = 0;

    /// `logSubscription.imageBySessionId(logSessionId)` (`Election.java:1062`,
    /// `:1138`), with the join position the reference reads off the image.
    [[nodiscard]] virtual std::optional<LogImage> log_image_by_session(std::int64_t registration_id,
                                                                       std::int32_t session_id) = 0;
};

/// What the election sends to other members: the exact
/// `consensusPublisher.*` call surface of `Election.java` — the same six
/// methods as `ConsensusPublisher.java:53-308`.
///
/// Departure: the publication is named by the member id, not handed over.
/// Publications belong to the consensus module's member table, and this seam
/// is implemented by the module.
///
/// Return values follow the reference's own signal: `request_vote`,
/// `append_position` and `catchup_position` answer whether the send happened
/// and the election acts on it (`Election.java:787`, `:1168`, `:1340`); the
/// other three retry on the next duty cycle. A failed send is not an error.
class ConsensusSender
{
public:
    virtual ~ConsensusSender() = default;

    /// `canvassPosition(..)` (`ConsensusPublisher.java:53`), called at
    /// `Election.java:1247`.
    virtual void canvass_position(std::int32_t member_id, std::int64_t log_leadership_term_id,
                                  std::int64_t log_position, std::int64_t leadership_term_id,
                                  std::int32_t follower_member_id) = 0;

    /// `requestVote(..)` (`ConsensusPublisher.java:91`), called at
    /// `Election.java:787`.
    [[nodiscard]] virtual bool request_vote(std::int32_t member_id, std::int64_t log_leadership_term_id,
                                            std::int64_t log_position, std::int64_t candidate_term_id,
                                            std::int32_t candidate_member_id) = 0;

    /// `placeVote(..)` (`ConsensusPublisher.java:131`), called at
    /// `Election.java:1187`.
    virtual void place_vote(std::int32_t member_id, std::int64_t candidate_term_id, std::int64_t log_leadership_term_id,
                            std::int64_t log_position, std::int32_t candidate_member_id,
                            std::int32_t follower_member_id, bool vote) = 0;

    /// `newLeadershipTerm(..)` (`ConsensusPublisher.java:172`), called at
    /// `Election.java:1281`. The argument order is the encoder's
    /// (`NewLeadershipTerm` in schema 111).
    // NOLINTNEXTLINE(readability-function-size): the wire message has fifteen fields
    virtual void new_leadership_term(std::int32_t member_id, std::int64_t log_leadership_term_id,
                                     std::int64_t next_leadership_term_id, std::int64_t next_term_base_log_position,
                                     std::int64_t next_log_position, std::int64_t leadership_term_id,
                                     std::int64_t term_base_log_position, std::int64_t log_position,
                                     std::int64_t commit_position, std::int64_t leader_recording_id,
                                     std::int64_t timestamp, std::int32_t leader_member_id, std::int32_t log_session_id,
                                     std::int32_t app_version, bool is_startup) = 0;

    /// `appendPosition(..)` (`ConsensusPublisher.java:234`), called three
    /// times. Also the follower's heartbeat: there is no separate frame.
    [[nodiscard]] virtual bool append_position(std::int32_t member_id, std::int64_t leadership_term_id,
                                               std::int64_t log_position, std::int32_t follower_member_id,
                                               std::int16_t flags) = 0;

    /// `catchupPosition(..)` (`ConsensusPublisher.java:308`), called at
    /// `Election.java:1340`.
    [[nodiscard]] virtual bool catchup_position(std::int32_t member_id, std::int64_t leadership_term_id,
                                                std::int64_t log_position, std::int32_t follower_member_id,
                                                std::string_view catchup_endpoint) = 0;
};

}  // namespace deepmsg::cluster::election
