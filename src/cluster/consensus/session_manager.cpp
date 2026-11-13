#include "cluster/consensus/session_manager.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <utility>

#include "cluster/consensus/cm_snapshot.h"

namespace deepmsg::cluster::consensus
{
namespace
{

using axc::core::Error;

/// `SemanticVersion.toString` (`SemanticVersion.java`): major.minor.patch.
[[nodiscard]] std::string semantic_version_to_string(std::int32_t version)
{
    return std::to_string(version >> 16) + "." + std::to_string((version >> 8) & 0xFF) + "." +
           std::to_string(version & 0xFF);
}

/// `ClusterSessionProxy` (`ClusterSessionProxy.java`): the authenticator's view
/// of a session.
class ClusterSessionProxy final : public SessionProxy
{
public:
    ClusterSessionProxy(EgressSender& egress_publisher, ClusterSession& session)
        : egress_publisher_(egress_publisher), session_(session)
    {
    }

    [[nodiscard]] std::int64_t session_id() override
    {
        return session_.id();
    }

    [[nodiscard]] bool challenge(std::span<const std::uint8_t> encoded_challenge) override
    {
        if (egress_publisher_.send_challenge(session_, encoded_challenge))
        {
            session_.state(SessionState::challenged, "challenged");
            return true;
        }
        return false;
    }

    [[nodiscard]] bool authenticate(std::span<const std::uint8_t> encoded_principal) override
    {
        session_.authenticate(encoded_principal);
        return true;
    }

    void reject() override
    {
        session_.reject(codecs::EventCode::AUTHENTICATION_REJECTED, std::string{SESSION_REJECTED_MSG});
    }

private:
    EgressSender& egress_publisher_;
    ClusterSession& session_;
};

}  // namespace

SessionManager::SessionManager(SessionManagerConfig config, axc::core::DeepmsgClient& client,
                               LogAppender& log_publisher, EgressSender& egress_publisher, Authenticator& authenticator,
                               AuthorisationService& authorisation_service, std::function<std::int64_t()> nano_clock,
                               std::function<std::int64_t()> epoch_clock_ms, std::function<void()> on_timed_out,
                               std::function<void(const Error&)> report)
    : config_(config),
      client_(client),
      log_publisher_(log_publisher),
      egress_publisher_(egress_publisher),
      authenticator_(authenticator),
      authorisation_service_(authorisation_service),
      nano_clock_(std::move(nano_clock)),
      epoch_clock_ms_(std::move(epoch_clock_ms)),
      on_timed_out_(std::move(on_timed_out)),
      report_(std::move(report))
{
}

std::int64_t SessionManager::now_ns() const
{
    return nano_clock_ ? nano_clock_() : 0;
}

std::int64_t SessionManager::now_ms() const
{
    return epoch_clock_ms_ ? epoch_clock_ms_() : 0;
}

ClusterSession* SessionManager::find_by_session_id(std::int64_t cluster_session_id) noexcept
{
    const auto it = sessions_.find(cluster_session_id);
    return sessions_.end() == it ? nullptr : it->second.get();
}

ClusterSession* SessionManager::find_pending_session_by_id(std::int64_t cluster_session_id) noexcept
{
    for (auto& session : pending_sessions_)
    {
        if (session->id() == cluster_session_id)
        {
            return session.get();
        }
    }
    return nullptr;
}

std::string SessionManager::session_info(std::string_view client_info, std::string_view image_source_identity,
                                         std::int32_t image_session_id)
{
    const std::string image_info =
        "sourceIdentity=" + std::string{image_source_identity} + " sessionId=" + std::to_string(image_session_id);
    if (client_info.empty())
    {
        return image_info;
    }
    return std::string{client_info} + " " + image_info;
}

void SessionManager::fast_unordered_remove(std::vector<std::unique_ptr<ClusterSession>>& sessions,
                                           std::size_t index) noexcept
{
    if (index != sessions.size() - 1)
    {
        sessions[index] = std::move(sessions.back());
    }
    sessions.pop_back();
}

void SessionManager::on_session_connect(std::int64_t correlation_id, std::int32_t response_stream_id,
                                        std::int32_t version, std::string response_channel,
                                        std::span<const std::uint8_t> encoded_credentials, std::string_view client_info,
                                        std::int64_t image_correlation_id, std::string_view image_source_identity,
                                        std::int32_t image_session_id, ClusterRole role,
                                        std::string_view ingress_endpoints)
{
    const std::int64_t cluster_session_id = ClusterRole::leader == role ? next_session_id_++ : NULL_VALUE;
    auto session = std::make_unique<ClusterSession>(config_.member_id, cluster_session_id, response_stream_id,
                                                    std::move(response_channel),
                                                    session_info(client_info, image_source_identity, image_session_id));
    session->async_connect(client_, config_.cluster_id);
    session->last_activity_ns(now_ns(), correlation_id);
    session->link_ingress_image(image_correlation_id);

    if (ClusterRole::leader != role)
    {
        session->redirect(std::string{ingress_endpoints});
        redirect_sessions_.push_back(std::move(session));
        return;
    }

    if ((version >> 16) != CLUSTER_PROTOCOL_MAJOR_VERSION)
    {
        session->reject(codecs::EventCode::ERROR, std::string{SESSION_INVALID_VERSION_MSG} + " " +
                                                      semantic_version_to_string(version) + ", cluster is " +
                                                      semantic_version_to_string(CLUSTER_PROTOCOL_SEMANTIC_VERSION));
        rejected_sessions_.push_back(std::move(session));
        return;
    }

    if (static_cast<std::int32_t>(pending_sessions_.size() + sessions_.size()) >= config_.max_concurrent_sessions)
    {
        session->reject(codecs::EventCode::ERROR, std::string{SESSION_LIMIT_MSG});
        rejected_sessions_.push_back(std::move(session));
        return;
    }

    authenticator_.on_connect_request(session->id(), encoded_credentials, now_ms());
    pending_sessions_.push_back(std::move(session));
}

void SessionManager::on_session_close(std::int64_t leadership_term_id, std::int64_t cluster_session_id)
{
    ClusterSession* session = find_by_session_id(cluster_session_id);
    if (nullptr == session || !session->is_open())
    {
        return;
    }

    session->closing(codecs::CloseReason::CLIENT_ACTION);
    session->disconnect(client_);
    if (log_publisher_.append_session_close(*session, leadership_term_id, now_ms()))
    {
        session->closed_log_position(log_publisher_.position());
        (void)close_session(*session);
    }
}

bool SessionManager::on_session_message(std::int64_t leadership_term_id, std::int64_t cluster_session_id,
                                        const std::uint8_t* payload, std::size_t payload_length)
{
    ClusterSession* session = find_by_session_id(cluster_session_id);
    if (nullptr == session || !session->is_open())
    {
        return true;
    }

    const long position =
        log_publisher_.append_message(leadership_term_id, cluster_session_id, now_ms(), payload, payload_length);
    if (position > 0)
    {
        session->time_of_last_activity_ns(now_ns());
        return true;
    }
    return false;
}

void SessionManager::on_session_keep_alive(std::int64_t leadership_term_id, std::int64_t cluster_session_id,
                                           std::int64_t image_correlation_id)
{
    (void)leadership_term_id;
    ClusterSession* session = find_by_session_id(cluster_session_id);
    if (nullptr != session && SessionState::open == session->state())
    {
        session->link_ingress_image(image_correlation_id);
        session->time_of_last_activity_ns(now_ns());
    }
}

void SessionManager::on_challenge_response(std::int64_t correlation_id, std::int64_t cluster_session_id,
                                           std::span<const std::uint8_t> encoded_credentials)
{
    for (auto& pending : pending_sessions_)
    {
        if (pending->id() == cluster_session_id && SessionState::challenged == pending->state())
        {
            pending->last_activity_ns(now_ns(), correlation_id);
            authenticator_.on_challenge_response(cluster_session_id, encoded_credentials, now_ms());
            break;
        }
    }
}

int SessionManager::process_all_pending_sessions(std::int64_t now_ns_value, std::int32_t leader_member_id,
                                                 std::int64_t leadership_term_id)
{
    return process_pending_sessions(pending_sessions_, now_ns_value, leader_member_id, leadership_term_id);
}

int SessionManager::process_pending_sessions(std::vector<std::unique_ptr<ClusterSession>>& pending,
                                             std::int64_t now_ns_value, std::int32_t leader_member_id,
                                             std::int64_t leadership_term_id)
{
    int work_count = 0;

    for (std::size_t i = pending.size(); i-- > 0;)
    {
        ClusterSession& session = *pending[i];

        if (SessionState::invalid == session.state())
        {
            session.close(client_, "invalid session");
            fast_unordered_remove(pending, i);
            continue;
        }

        if (SessionState::init != session.state() &&
            session.time_of_last_activity_ns() + config_.session_timeout_ns < now_ns_value)
        {
            session.close(client_, "session timed out");
            fast_unordered_remove(pending, i);
            on_timed_out_();
            continue;
        }

        if (SessionState::init == session.state() || SessionState::connecting == session.state() ||
            SessionState::connected == session.state())
        {
            if (session.is_response_publication_connected(client_, now_ns_value))
            {
                session.state(SessionState::connected, "connected");
                ClusterSessionProxy proxy(egress_publisher_, session);
                authenticator_.on_connected_session(proxy, now_ms());
            }
        }

        if (SessionState::challenged == session.state())
        {
            if (session.is_response_publication_connected(client_, now_ns_value))
            {
                ClusterSessionProxy proxy(egress_publisher_, session);
                authenticator_.on_challenged_session(proxy, now_ms());
            }
        }

        if (SessionState::authenticated == session.state())
        {
            const long position = log_publisher_.append_session_open(session, leadership_term_id, now_ms());
            if (position > 0)
            {
                session.open(position);
                session.time_of_last_activity_ns(now_ns_value);
                const bool sent = egress_publisher_.send_event(session, leadership_term_id, leader_member_id,
                                                               codecs::EventCode::OK, {});
                if (sent)
                {
                    session.clear_open_event_pending();
                }

                uncommitted_next_committed_session_ids_.emplace_back(log_publisher_.position(), session.id() + 1);
                auto opened = std::move(pending[i]);
                fast_unordered_remove(pending, i);
                add_session(std::move(opened));
                work_count += 1;
            }
        }
    }

    return work_count;
}

void SessionManager::add_session(std::unique_ptr<ClusterSession> session)
{
    sessions_.emplace(session->id(), std::move(session));
}

int SessionManager::check_sessions(std::int64_t now_ns_value, std::int64_t leadership_term_id,
                                   std::int32_t leader_member_id, std::string_view ingress_endpoints)
{
    int work_count = 0;
    std::vector<std::int64_t> to_close{};

    for (auto& [id, session] : sessions_)
    {
        if (session->time_of_last_activity_ns() + config_.session_timeout_ns < now_ns_value)
        {
            switch (session->state())
            {
                case SessionState::open:
                    session->closing(codecs::CloseReason::TIMEOUT);
                    [[fallthrough]];
                case SessionState::closing:
                {
                    if (log_publisher_.append_session_close(*session, leadership_term_id, now_ms()))
                    {
                        session->closed_log_position(log_publisher_.position());
                        (void)egress_publisher_.send_event(*session, leadership_term_id, leader_member_id,
                                                           codecs::EventCode::CLOSED,
                                                           session_state_name(SessionState::closing));
                        if (codecs::CloseReason::TIMEOUT == session->close_reason())
                        {
                            on_timed_out_();
                        }
                        to_close.push_back(id);
                        work_count += 1;
                    }
                    break;
                }
                default:
                    to_close.push_back(id);
                    work_count += 1;
                    break;
            }
        }
        else if (session->has_open_event_pending())
        {
            if (egress_publisher_.send_event(*session, leadership_term_id, leader_member_id, codecs::EventCode::OK, {}))
            {
                session->clear_open_event_pending();
                work_count += 1;
            }
        }
        else if (session->has_new_leader_event_pending())
        {
            if (egress_publisher_.new_leader(*session, leadership_term_id, leader_member_id, ingress_endpoints))
            {
                session->has_new_leader_event_pending(false);
                work_count += 1;
            }
        }
    }

    for (const std::int64_t id : to_close)
    {
        const auto it = sessions_.find(id);
        if (sessions_.end() != it)
        {
            it->second->close(client_, "closed");
            sessions_.erase(it);
        }
    }

    return work_count;
}

int SessionManager::send_redirects(std::int64_t leadership_term_id, std::int32_t leader_member_id,
                                   std::int64_t now_ns_value)
{
    return send_for_all(redirect_sessions_, leadership_term_id, leader_member_id, now_ns_value);
}

int SessionManager::send_rejections(std::int64_t leadership_term_id, std::int32_t leader_member_id,
                                    std::int64_t now_ns_value)
{
    return send_for_all(rejected_sessions_, leadership_term_id, leader_member_id, now_ns_value);
}

int SessionManager::send_for_all(std::vector<std::unique_ptr<ClusterSession>>& sessions,
                                 std::int64_t leadership_term_id, std::int32_t leader_member_id,
                                 std::int64_t now_ns_value)
{
    int work_count = 0;

    for (std::size_t i = sessions.size(); i-- > 0;)
    {
        ClusterSession& session = *sessions[i];
        if (session.is_response_publication_connected(client_, now_ns_value) &&
            egress_publisher_.send_event(session, leadership_term_id, leader_member_id, session.event_code(),
                                         session.response_detail()))
        {
            session.close(client_, "event sent");
            fast_unordered_remove(sessions, i);
            work_count += 1;
        }
        else if (session.has_timed_out(now_ns_value, config_.session_timeout_ns))
        {
            session.close(client_, "session timed out");
            fast_unordered_remove(sessions, i);
            work_count += 1;
        }
    }

    return work_count;
}

std::unique_ptr<ClusterSession> SessionManager::close_session(ClusterSession& session)
{
    const std::int64_t id = session.id();
    session.close(client_, "closed");
    const auto node = sessions_.extract(id);
    return node.empty() ? nullptr : std::move(node.mapped());
}

void SessionManager::on_service_close_session(std::int64_t cluster_session_id, bool is_active_leader,
                                              std::int64_t leadership_term_id)
{
    ClusterSession* session = find_by_session_id(cluster_session_id);
    if (nullptr == session)
    {
        return;
    }

    session->closing(codecs::CloseReason::SERVICE_ACTION);

    if (is_active_leader)
    {
        const std::int64_t timestamp = now_ms();
        if (log_publisher_.append_session_close(*session, leadership_term_id, timestamp))
        {
            egress_publisher_.send_event(*session, leadership_term_id, config_.member_id, codecs::EventCode::CLOSED,
                                         "SERVICE_ACTION");
            session->closed_log_position(log_publisher_.position());
            // `uncommittedClosedSessions`: kept until the commit position
            // passes the close (`SessionManager.java:471`, `:1057-1075`).
            uncommitted_closed_sessions_.push_back(close_session(*session));
        }
    }
}

void SessionManager::on_replay_session_open(std::int64_t log_position, std::int64_t correlation_id,
                                            std::int64_t cluster_session_id, std::int64_t timestamp,
                                            std::int32_t response_stream_id, std::string response_channel)
{
    const std::string session_info = response_channel;
    auto session = std::make_unique<ClusterSession>(config_.member_id, cluster_session_id, response_stream_id,
                                                    std::move(response_channel), session_info);
    session->open(log_position);
    session->last_activity_ns(service::time_to_nanos(config_.cluster_time_unit, timestamp), correlation_id);

    add_session(std::move(session));

    if (cluster_session_id >= next_session_id_)
    {
        next_session_id_ = cluster_session_id + 1;
    }
    if (cluster_session_id >= next_committed_session_id_)
    {
        next_committed_session_id_ = cluster_session_id + 1;
    }
}

void SessionManager::on_replay_session_close(std::int64_t cluster_session_id, codecs::CloseReason::Value close_reason)
{
    ClusterSession* session = find_by_session_id(cluster_session_id);
    if (nullptr != session)
    {
        session->closing(close_reason);
        (void)close_session(*session);
    }
}

void SessionManager::on_load_cluster_session(std::int64_t cluster_session_id, std::int64_t correlation_id,
                                             std::int64_t opened_log_position, std::int64_t time_of_last_activity_ns,
                                             codecs::CloseReason::Value close_reason, std::int32_t response_stream_id,
                                             std::string response_channel)
{
    const std::string session_info = response_channel;
    auto session = std::make_unique<ClusterSession>(config_.member_id, cluster_session_id, response_stream_id,
                                                    std::move(response_channel), session_info);
    session->load_snapshot_state(correlation_id, opened_log_position, time_of_last_activity_ns, close_reason);

    add_session(std::move(session));

    if (cluster_session_id >= next_session_id_)
    {
        next_session_id_ = cluster_session_id + 1;
        next_committed_session_id_ = next_session_id_;
    }
}

void SessionManager::load_next_session_id(std::int64_t next_session_id)
{
    next_session_id_ = next_session_id;
    next_committed_session_id_ = next_session_id;
}

void SessionManager::prepare_sessions_for_new_term(bool is_startup)
{
    if (is_startup)
    {
        for (auto& [id, session] : sessions_)
        {
            if (SessionState::open == session->state())
            {
                session->closing(codecs::CloseReason::TIMEOUT);
            }
        }
        return;
    }

    for (auto& [id, session] : sessions_)
    {
        if (SessionState::open == session->state())
        {
            session->async_connect(client_, config_.cluster_id);
        }
    }

    const std::int64_t now = now_ns();
    for (auto& [id, session] : sessions_)
    {
        if (SessionState::open == session->state())
        {
            session->time_of_last_activity_ns(now);
            session->has_new_leader_event_pending(true);
        }
    }
}

void SessionManager::clear_sessions_after(std::int64_t log_position, std::int64_t leadership_term_id)
{
    std::vector<std::int64_t> to_close{};
    for (auto& [id, session] : sessions_)
    {
        if (session->opened_log_position() > log_position)
        {
            (void)egress_publisher_.send_event(*session, leadership_term_id, config_.member_id,
                                               codecs::EventCode::CLOSED, "election");
            to_close.push_back(id);
        }
    }
    for (const std::int64_t id : to_close)
    {
        ClusterSession* session = find_by_session_id(id);
        if (nullptr != session)
        {
            (void)close_session(*session);
        }
    }

    for (auto& session : pending_sessions_)
    {
        (void)egress_publisher_.send_event(*session, leadership_term_id, config_.member_id, codecs::EventCode::CLOSED,
                                           "election");
        session->close(client_, "election");
    }
    pending_sessions_.clear();
}

void SessionManager::sweep_uncommitted_sessions(std::int64_t commit_position)
{
    while (!uncommitted_closed_sessions_.empty() &&
           uncommitted_closed_sessions_.front()->closed_log_position() <= commit_position)
    {
        uncommitted_closed_sessions_.pop_front();
    }

    while (!uncommitted_next_committed_session_ids_.empty() &&
           uncommitted_next_committed_session_ids_.front().first <= commit_position)
    {
        next_committed_session_id_ =
            std::max(next_committed_session_id_, uncommitted_next_committed_session_ids_.front().second);
        uncommitted_next_committed_session_ids_.pop_front();
    }
}

void SessionManager::restore_uncommitted_sessions(std::int64_t commit_position)
{
    while (!uncommitted_closed_sessions_.empty())
    {
        std::unique_ptr<ClusterSession> session = std::move(uncommitted_closed_sessions_.front());
        uncommitted_closed_sessions_.pop_front();
        if (nullptr != session && session->closed_log_position() > commit_position)
        {
            session->closed_log_position(NULL_POSITION);
            session->state(SessionState::closing, "uncommitted session");
            add_session(std::move(session));
        }
    }

    while (!uncommitted_next_committed_session_ids_.empty() &&
           uncommitted_next_committed_session_ids_.front().first <= commit_position)
    {
        next_committed_session_id_ =
            std::max(next_committed_session_id_, uncommitted_next_committed_session_ids_.front().second);
        uncommitted_next_committed_session_ids_.pop_front();
    }
    uncommitted_next_committed_session_ids_.clear();
}

void SessionManager::update_time_of_last_activity()
{
    const std::int64_t now = now_ns();
    for (auto& [id, session] : sessions_)
    {
        session->time_of_last_activity_ns(now);
    }
}

std::expected<void, Error> SessionManager::snapshot_sessions(ConsensusModuleSnapshotTaker& snapshot_taker)
{
    for (auto& [id, session] : sessions_)
    {
        const SessionState state = session->state();
        if (SessionState::open == state || SessionState::closing == state)
        {
            auto written = snapshot_taker.snapshot_session(*session);
            if (!written)
            {
                return written;
            }
        }
    }
    return {};
}

void SessionManager::close_sessions()
{
    for (auto& [id, session] : sessions_)
    {
        session->close(client_, "Cluster node terminated");
    }
    sessions_.clear();
    for (auto& session : pending_sessions_)
    {
        session->close(client_, "Cluster node terminated");
    }
    pending_sessions_.clear();
    for (auto& session : redirect_sessions_)
    {
        session->close(client_, "Cluster node terminated");
    }
    redirect_sessions_.clear();
    for (auto& session : rejected_sessions_)
    {
        session->close(client_, "Cluster node terminated");
    }
    rejected_sessions_.clear();
}

void SessionManager::disconnect_sessions()
{
    for (auto& [id, session] : sessions_)
    {
        session->unlink_ingress_image();
        session->disconnect(client_);
    }
}

}  // namespace deepmsg::cluster::consensus
