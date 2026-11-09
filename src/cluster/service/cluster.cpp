#include "cluster/service/cluster.h"

#include <utility>

namespace deepmsg::cluster::service
{

Cluster::Cluster(axc::core::DeepmsgClient& client, SessionTable& sessions, ConsensusModuleProxy& proxy,
                 ContainerContext& context, const ServiceContainerConfig& config, TimeUnit time_unit, std::int64_t time,
                 std::int64_t log_position, Role role, std::int32_t member_id) noexcept
    : client_(client),
      sessions_(sessions),
      proxy_(proxy),
      context_(context),
      config_(config),
      time_unit_(time_unit),
      time_(time),
      log_position_(log_position),
      role_(role),
      member_id_(member_id)
{
}

std::optional<ClientSession> Cluster::session(SessionId id) noexcept
{
    ContainerClientSession* session = sessions_.get(id.value);
    if (nullptr == session)
    {
        return std::nullopt;
    }
    return ClientSession{*this, *session};
}

std::expected<bool, Error> Cluster::close_client_session(SessionId id)
{
    auto valid = check_for_valid_invocation();
    if (!valid)
    {
        return std::unexpected(valid.error());
    }

    ContainerClientSession* session = sessions_.get(id.value);
    if (nullptr == session)
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::not_found,
                                                     "unknown clusterSessionId: " + std::to_string(id.value)));
    }
    if (session->is_closing())
    {
        return true;
    }
    return close_session(*session);
}

std::expected<bool, Error> Cluster::close_session(ContainerClientSession& session)
{
    int attempts = 3;
    while (true)
    {
        auto sent = proxy_.close_session(session.id());
        if (!sent)
        {
            return std::unexpected(sent.error());
        }
        if (*sent)
        {
            session.mark_closing();
            return true;
        }

        auto idled = context_.idle(client_, 0);
        if (!idled)
        {
            return std::unexpected(idled.error());
        }

        if (0 == --attempts)
        {
            return false;
        }
    }
}

std::expected<bool, Error> Cluster::schedule_timer(std::int64_t correlation_id, std::int64_t deadline)
{
    auto valid = check_for_valid_invocation();
    if (!valid)
    {
        return std::unexpected(valid.error());
    }
    return proxy_.schedule_timer(correlation_id, deadline);
}

std::expected<bool, Error> Cluster::cancel_timer(std::int64_t correlation_id)
{
    auto valid = check_for_valid_invocation();
    if (!valid)
    {
        return std::unexpected(valid.error());
    }
    return proxy_.cancel_timer(correlation_id);
}

std::expected<std::int64_t, Error> Cluster::offer(std::span<const std::uint8_t> payload)
{
    auto valid = check_for_valid_invocation();
    if (!valid)
    {
        return std::unexpected(valid.error());
    }

    const std::int64_t position = proxy_.offer_service_message(payload);
    if (OFFER_NOT_CONNECTED == position || OFFER_MAX_POSITION_EXCEEDED == position ||
        AERON_PUBLICATION_CLOSED == position)
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::no_capacity,
                                                     "publication refused the message: " + std::to_string(position)));
    }
    return position;
}

std::expected<std::int64_t, Error> Cluster::offer_vectored(std::span<const std::span<const std::uint8_t>> parts)
{
    auto valid = check_for_valid_invocation();
    if (!valid)
    {
        return std::unexpected(valid.error());
    }
    const std::int64_t position = proxy_.offer_service_message_vectored(parts);
    if (OFFER_NOT_CONNECTED == position || OFFER_MAX_POSITION_EXCEEDED == position ||
        AERON_PUBLICATION_CLOSED == position)
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::no_capacity,
                                                     "publication refused the message: " + std::to_string(position)));
    }
    return position;
}

std::expected<TryClaim, Error> Cluster::try_claim(std::size_t length)
{
    auto valid = check_for_valid_invocation();
    if (!valid)
    {
        return std::unexpected(valid.error());
    }

    auto claimed = proxy_.claim_service_message(length);
    if (claimed.has_value())
    {
        return TryClaim{*claimed, 0};
    }
    if (ConsensusModuleProxy::is_retryable_claim(claimed.error()))
    {
        return TryClaim{std::nullopt, claimed.error()};
    }
    return std::unexpected(axc::core::make_error(axc::core::ErrorCode::no_capacity,
                                                 "publication refused the claim: " + std::to_string(claimed.error())));
}

std::expected<bool, Error> Cluster::cluster_members_query(std::int64_t correlation_id)
{
    auto valid = check_for_valid_invocation();
    if (!valid)
    {
        return std::unexpected(valid.error());
    }
    return proxy_.cluster_members_query(correlation_id);
}

std::expected<void, Error> Cluster::idle(int work)
{
    return context_.idle(client_, work);
}

std::expected<void, Error> Cluster::check_for_valid_invocation() const
{
    if (LifecycleCallback::none == context_.lifecycle())
    {
        return {};
    }
    return std::unexpected(axc::core::make_error(axc::core::ErrorCode::invalid_argument,
                                                 "sending messages or scheduling timers is not allowed from " +
                                                     std::string{lifecycle_name(context_.lifecycle())}));
}

SessionId ClientSession::id() const noexcept
{
    return SessionId{session_->id()};
}

std::int32_t ClientSession::response_stream_id() const noexcept
{
    return session_->response_stream_id();
}

const std::string& ClientSession::response_channel() const noexcept
{
    return session_->response_channel();
}

const std::vector<std::uint8_t>& ClientSession::encoded_principal() const noexcept
{
    return session_->encoded_principal();
}

bool ClientSession::is_closing() const noexcept
{
    return session_->is_closing();
}

std::expected<bool, Error> ClientSession::close()
{
    auto valid = check_for_valid_invocation();
    if (!valid)
    {
        return std::unexpected(valid.error());
    }
    if (session_->is_closing())
    {
        return true;
    }
    return cluster_->close_session(*session_);
}

std::expected<std::int64_t, Error> ClientSession::offer(std::span<const std::uint8_t> payload)
{
    auto valid = check_for_valid_invocation();
    if (!valid)
    {
        return std::unexpected(valid.error());
    }

    if (cluster_->role() != Role::leader)
    {
        return MOCKED_OFFER;
    }
    if (!session_->has_publication())
    {
        return OFFER_NOT_CONNECTED;
    }

    return cluster_->proxy().offer_session_message(session_->publication(), session_->id(), cluster_->time(), payload);
}

std::expected<std::int64_t, Error> ClientSession::offer_vectored(std::span<const std::span<const std::uint8_t>> parts)
{
    if (cluster_->role() != Role::leader)
    {
        return MOCKED_OFFER;
    }
    if (!session_->has_publication())
    {
        return OFFER_NOT_CONNECTED;
    }

    return cluster_->proxy().offer_session_message_vectored(session_->publication(), session_->id(), cluster_->time(),
                                                            parts);
}

std::expected<TryClaim, Error> ClientSession::try_claim(std::size_t length)
{
    auto valid = check_for_valid_invocation();
    if (!valid)
    {
        return std::unexpected(valid.error());
    }

    if (cluster_->role() != Role::leader)
    {
        auto mocked = cluster_->proxy().mock_session_message(session_->id(), cluster_->time(), length);
        if (!mocked)
        {
            return std::unexpected(mocked.error());
        }
        return TryClaim{*mocked, 0};
    }

    if (!session_->has_publication())
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::no_capacity, "publication is not connected"));
    }

    auto claimed =
        cluster_->proxy().claim_session_message(session_->publication(), session_->id(), cluster_->time(), length);
    if (claimed.has_value())
    {
        return TryClaim{*claimed, 0};
    }
    if (ConsensusModuleProxy::is_retryable_claim(claimed.error()))
    {
        return TryClaim{std::nullopt, claimed.error()};
    }
    return std::unexpected(axc::core::make_error(axc::core::ErrorCode::no_capacity,
                                                 "publication refused the claim: " + std::to_string(claimed.error())));
}

std::expected<void, Error> ClientSession::check_for_valid_invocation() const
{
    return cluster_->check_for_valid_invocation();
}

}  // namespace deepmsg::cluster::service
