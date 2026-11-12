#include "apps/services/echo-service/echo_service.h"

#include <cstring>
#include <string>
#include <string_view>

namespace deepmsg::apps
{
namespace
{

constexpr std::string_view ECHO_SERVICE_IPC_INGRESS_MSG = "Echo as Service IPC ingress";
constexpr std::int32_t TIMER_SCHEDULE = 1;
constexpr std::int32_t TIMER_CANCEL = 2;
constexpr std::size_t TIMER_COMMAND_LENGTH = 16;

}  // namespace

std::expected<void, EchoService::Error> EchoService::on_start(deepmsg::cluster::service::Cluster&,
                                                              deepmsg::cluster::service::SnapshotImage*)
{
    return {};
}

std::expected<void, EchoService::Error> EchoService::on_session_open(deepmsg::cluster::service::ClientSession&,
                                                                     std::int64_t)
{
    return {};
}

std::expected<void, EchoService::Error> EchoService::on_session_close(deepmsg::cluster::service::ClientSession&,
                                                                      std::int64_t,
                                                                      deepmsg::cluster::service::CloseReason)
{
    return {};
}

std::expected<void, EchoService::Error> EchoService::echo_to_session(Cluster& cluster, SessionId session,
                                                                     std::span<const std::uint8_t> payload)
{
    while (true)
    {
        auto view = cluster.session(session);
        if (!view.has_value())
        {
            return {};
        }

        auto offered = view->offer(payload);
        if (!offered)
        {
            return std::unexpected(offered.error());
        }
        if (*offered > 0)
        {
            return {};
        }
        if (deepmsg::cluster::service::OFFER_BACK_PRESSURED == *offered ||
            deepmsg::cluster::service::OFFER_ADMIN_ACTION == *offered)
        {
            auto idled = cluster.idle(0);
            if (!idled)
            {
                return std::unexpected(idled.error());
            }
            continue;
        }
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::no_capacity,
                                                     "egress publication error: result=" + std::to_string(*offered) +
                                                         ", clusterSessionId=" + std::to_string(session.value)));
    }
}

std::expected<void, EchoService::Error> EchoService::send_service_message(Cluster& cluster, ClientSession* session,
                                                                          std::span<const std::uint8_t> payload)
{
    if (nullptr != session)
    {
        while (true)
        {
            auto offered = cluster.offer(payload);
            if (!offered)
            {
                return std::unexpected(offered.error());
            }
            if (*offered > 0)
            {
                return {};
            }
            auto idled = cluster.idle(0);
            if (!idled)
            {
                return std::unexpected(idled.error());
            }
        }
    }

    std::vector<SessionId> sessions;
    sessions.reserve(cluster.sessions().size());
    for (const auto& info : cluster.sessions())
    {
        sessions.push_back(SessionId{info.id()});
    }
    for (const SessionId id : sessions)
    {
        auto echoed = echo_to_session(cluster, id, payload);
        if (!echoed)
        {
            return std::unexpected(echoed.error());
        }
    }
    return {};
}

std::expected<void, EchoService::Error> EchoService::on_session_message(Cluster& cluster, ClientSession* session,
                                                                        std::int64_t timestamp,
                                                                        std::span<const std::uint8_t> payload,
                                                                        const deepmsg::cluster::service::LogHeader&)
{
    if (payload.size() == ECHO_SERVICE_IPC_INGRESS_MSG.size() &&
        0 == std::memcmp(payload.data(), ECHO_SERVICE_IPC_INGRESS_MSG.data(), payload.size()))
    {
        return send_service_message(cluster, session, payload);
    }

    if (TIMER_COMMAND_LENGTH == payload.size())
    {
        std::int32_t command = 0;
        std::int64_t correlation_id = 0;
        std::int32_t delay_millis = 0;
        std::memcpy(&command, payload.data(), sizeof(command));
        std::memcpy(&correlation_id, payload.data() + 4, sizeof(correlation_id));
        std::memcpy(&delay_millis, payload.data() + 12, sizeof(delay_millis));

        if (TIMER_SCHEDULE == command)
        {
            auto scheduled = cluster.schedule_timer(correlation_id, timestamp + delay_millis);
            if (!scheduled)
            {
                return std::unexpected(scheduled.error());
            }
            if (*scheduled)
            {
                pending_.push_back(
                    PendingTimer{correlation_id, nullptr != session, nullptr == session ? -1 : session->id().value});
            }
        }
        else if (TIMER_CANCEL == command)
        {
            auto cancelled = cluster.cancel_timer(correlation_id);
            if (!cancelled)
            {
                return std::unexpected(cancelled.error());
            }
        }
        return {};
    }

    if (nullptr != session)
    {
        return echo_to_session(cluster, session->id(), payload);
    }
    return {};
}

std::expected<void, EchoService::Error> EchoService::on_timer_event(Cluster& cluster, std::int64_t correlation_id,
                                                                    std::int64_t)
{
    for (auto it = pending_.begin(); it != pending_.end(); ++it)
    {
        if (it->correlation_id == correlation_id)
        {
            const bool has_session = it->has_session;
            const std::int64_t session_id = it->session_id;
            pending_.erase(it);
            if (has_session)
            {
                std::uint8_t bytes[8]{};
                std::memcpy(bytes, &correlation_id, sizeof(correlation_id));
                return echo_to_session(cluster, SessionId{session_id},
                                       std::span<const std::uint8_t>(bytes, sizeof(bytes)));
            }
            return {};
        }
    }
    return {};
}

std::expected<void, EchoService::Error> EchoService::on_take_snapshot(deepmsg::cluster::service::SnapshotPublication&)
{
    return {};
}

std::expected<void, EchoService::Error> EchoService::on_role_change(deepmsg::cluster::service::Role)
{
    return {};
}

std::expected<void, EchoService::Error> EchoService::on_terminate(deepmsg::cluster::service::Cluster&)
{
    return {};
}

}  // namespace deepmsg::apps
