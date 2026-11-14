#pragma once

#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "cluster/consensus/authenticator.h"
#include "cluster/consensus/cluster_session.h"
#include "cluster/consensus/consensus_constants.h"
#include "cluster/consensus/consensus_module_context.h"
#include "cluster/consensus/egress_publisher.h"
#include "cluster/consensus/log_publisher.h"
#include "cluster/service/role.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/error/error.h"

namespace deepmsg::cluster::consensus
{

using axc::core::Error;

class ConsensusModuleSnapshotTaker;

struct SessionManagerConfig
{
    std::int32_t member_id{0};
    std::int32_t cluster_id{0};
    std::int64_t session_timeout_ns{10'000'000'000};
    std::int32_t max_concurrent_sessions{10};
    std::int32_t service_count{1};
    service::TimeUnit cluster_time_unit{service::TimeUnit::milliseconds};
};

/// `SessionManager` (`SessionManager.java`) for the user-session paths of C4b:
/// connect/authenticate/open, ingress messages, keep-alive, close, timeout,
/// redirect and rejection. Backup/heartbeat/standby sessions and the
/// persistence/snapshot paths are C9/C4d and are refused loudly by the agent
/// before they reach this class.
class SessionManager
{
public:
    SessionManager(SessionManagerConfig config, axc::core::DeepmsgClient& client, LogAppender& log_publisher,
                   EgressSender& egress_publisher, Authenticator& authenticator,
                   AuthorisationService& authorisation_service, std::function<std::int64_t()> nano_clock,
                   std::function<std::int64_t()> epoch_clock_ms, std::function<void()> on_timed_out,
                   std::function<void(const Error&)> report);

    SessionManager(const SessionManager&) = delete;
    SessionManager& operator=(const SessionManager&) = delete;
    SessionManager(SessionManager&&) = delete;
    SessionManager& operator=(SessionManager&&) = delete;
    ~SessionManager() = default;

    [[nodiscard]] ClusterSession* find_by_session_id(std::int64_t cluster_session_id) noexcept;

    /// Test seam: a pending session is not in `sessions()` until it opens.
    [[nodiscard]] ClusterSession* find_pending_session_by_id(std::int64_t cluster_session_id) noexcept;

    [[nodiscard]] const std::map<std::int64_t, std::unique_ptr<ClusterSession>>& sessions() const noexcept
    {
        return sessions_;
    }

    [[nodiscard]] std::int64_t next_committed_session_id() const noexcept
    {
        return next_committed_session_id_;
    }

    /// `onSessionConnect` (`SessionManager.java:240-289`). `role` is the
    /// consensus module's role; only the leader path admits a session.
    void on_session_connect(std::int64_t correlation_id, std::int32_t response_stream_id, std::int32_t version,
                            std::string response_channel, std::span<const std::uint8_t> encoded_credentials,
                            std::string_view client_info, std::int64_t image_correlation_id,
                            std::string_view image_source_identity, std::int32_t image_session_id, ClusterRole role,
                            std::string_view ingress_endpoints);

    /// `onSessionClose` (`:291-309`).
    void on_session_close(std::int64_t leadership_term_id, std::int64_t cluster_session_id);

    /// `onIngressMessage` (`ConsensusModuleAgent.java:828-853`): append to the
    /// log; false means the ingress poll must ABORT.
    [[nodiscard]] bool on_session_message(std::int64_t leadership_term_id, std::int64_t cluster_session_id,
                                          const std::uint8_t* payload, std::size_t payload_length);

    /// `onSessionKeepAlive` (`:856-867`).
    void on_session_keep_alive(std::int64_t leadership_term_id, std::int64_t cluster_session_id,
                               std::int64_t image_correlation_id);

    /// `onChallengeResponseForUserSession` (`:408-419`).
    void on_challenge_response(std::int64_t correlation_id, std::int64_t cluster_session_id,
                               std::span<const std::uint8_t> encoded_credentials);

    /// `onServiceCloseSession` (`:454-476`): the service asked for the session
    /// to close. The uncommitted-closed-session tracking is C4d.
    void on_service_close_session(std::int64_t cluster_session_id, bool is_active_leader,
                                  std::int64_t leadership_term_id);

    /// `processAllPendingSessions` (`:550-570`).
    int process_all_pending_sessions(std::int64_t now_ns, std::int32_t leader_member_id,
                                     std::int64_t leadership_term_id);

    /// `checkSessions` (`:869-967`).
    int check_sessions(std::int64_t now_ns, std::int64_t leadership_term_id, std::int32_t leader_member_id,
                       std::string_view ingress_endpoints);

    int send_redirects(std::int64_t leadership_term_id, std::int32_t leader_member_id, std::int64_t now_ns);
    int send_rejections(std::int64_t leadership_term_id, std::int32_t leader_member_id, std::int64_t now_ns);

    /// The session is removed from the table and handed back so the caller
    /// can keep it alive: `uncommittedClosedSessions` holds it until the commit
    /// position passes its close (`SessionManager.java:454-476`,
    /// `:1057-1096`).
    [[nodiscard]] std::unique_ptr<ClusterSession> close_session(ClusterSession& session);
    void close_sessions();
    void disconnect_sessions();

    // ---- C4d: replay, snapshot load and the uncommitted sweep ----------

    /// `onReplaySessionOpen` (`:478-505`).
    void on_replay_session_open(std::int64_t log_position, std::int64_t correlation_id, std::int64_t cluster_session_id,
                                std::int64_t timestamp, std::int32_t response_stream_id, std::string response_channel);

    /// `onReplaySessionClose` (`:507-515`).
    void on_replay_session_close(std::int64_t cluster_session_id, codecs::CloseReason::Value close_reason);

    /// `onLoadClusterSession` (`:540-561`).
    void on_load_cluster_session(std::int64_t cluster_session_id, std::int64_t correlation_id,
                                 std::int64_t opened_log_position, std::int64_t time_of_last_activity_ns,
                                 codecs::CloseReason::Value close_reason, std::int32_t response_stream_id,
                                 std::string response_channel);

    /// `loadNextSessionId` (`:563-567`).
    void load_next_session_id(std::int64_t next_session_id);

    /// `prepareSessionsForNewTerm` (`:932-967`).
    void prepare_sessions_for_new_term(bool is_startup);

    /// `clearSessionsAfter` (`:1028-1047`).
    void clear_sessions_after(std::int64_t log_position, std::int64_t leadership_term_id);

    /// `sweepUncommittedSessions` (`:1057-1075`).
    void sweep_uncommitted_sessions(std::int64_t commit_position);

    /// `restoreUncommittedSessions` (`:1077-1096`).
    void restore_uncommitted_sessions(std::int64_t commit_position);

    /// `updateTimeOfLastActivity` (`:1098-1105`).
    void update_time_of_last_activity();

    /// `timeoutOnUnavailableImage` (`:1121-1132`): an ingress image that went
    /// away closes every open session that was linked to it.
    void timeout_on_unavailable_image(std::int64_t image_correlation_id) noexcept;

    /// `snapshotSessions` (`:1107-1119`): OPEN and CLOSING sessions only.
    [[nodiscard]] std::expected<void, Error> snapshot_sessions(ConsensusModuleSnapshotTaker& snapshot_taker);

    [[nodiscard]] std::size_t pending_session_count() const noexcept
    {
        return pending_sessions_.size();
    }

private:
    [[nodiscard]] std::int64_t now_ns() const;
    [[nodiscard]] std::int64_t now_ms() const;

    int process_pending_sessions(std::vector<std::unique_ptr<ClusterSession>>& pending, std::int64_t now_ns,
                                 std::int32_t leader_member_id, std::int64_t leadership_term_id);
    int send_for_all(std::vector<std::unique_ptr<ClusterSession>>& sessions, std::int64_t leadership_term_id,
                     std::int32_t leader_member_id, std::int64_t now_ns);
    void add_session(std::unique_ptr<ClusterSession> session);
    static void fast_unordered_remove(std::vector<std::unique_ptr<ClusterSession>>& sessions,
                                      std::size_t index) noexcept;
    static std::string session_info(std::string_view client_info, std::string_view image_source_identity,
                                    std::int32_t image_session_id);

    SessionManagerConfig config_;
    axc::core::DeepmsgClient& client_;
    LogAppender& log_publisher_;
    EgressSender& egress_publisher_;
    Authenticator& authenticator_;
    [[maybe_unused]] AuthorisationService& authorisation_service_;
    std::function<std::int64_t()> nano_clock_;
    std::function<std::int64_t()> epoch_clock_ms_;
    std::function<void()> on_timed_out_;
    [[maybe_unused]] std::function<void(const Error&)> report_;

    std::map<std::int64_t, std::unique_ptr<ClusterSession>> sessions_{};
    std::vector<std::unique_ptr<ClusterSession>> pending_sessions_{};
    std::vector<std::unique_ptr<ClusterSession>> rejected_sessions_{};
    std::vector<std::unique_ptr<ClusterSession>> redirect_sessions_{};
    std::int64_t next_session_id_{1};
    std::int64_t next_committed_session_id_{1};
    std::deque<std::unique_ptr<ClusterSession>> uncommitted_closed_sessions_{};
    std::deque<std::pair<std::int64_t, std::int64_t>> uncommitted_next_committed_session_ids_{};
};

}  // namespace deepmsg::cluster::consensus
