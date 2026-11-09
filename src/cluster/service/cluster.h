#pragma once

#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "cluster/service/clustered_service.h"
#include "cluster/service/consensus_module_proxy.h"
#include "cluster/service/container_config.h"
#include "cluster/service/container_context.h"
#include "cluster/service/role.h"
#include "cluster/service/session.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/error/error.h"

namespace deepmsg::cluster::service
{

using axc::core::Error;

/// What `Cluster::try_claim` / `ClientSession::try_claim` answered: a claim (or
/// a follower's mocked window), or one of the two retryable codes.
struct TryClaim
{
    std::optional<ClaimedMessage> claimed{};
    std::int64_t code{0};

    [[nodiscard]] bool is_claimed() const noexcept
    {
        return claimed.has_value();
    }
};

class Cluster;

/// `ClientSession` (`ClientSession.java:30-134`): one session, for one callback.
class ClientSession
{
public:
    ClientSession(Cluster& cluster, ContainerClientSession& session) noexcept : cluster_(&cluster), session_(&session)
    {
    }

    [[nodiscard]] SessionId id() const noexcept;
    [[nodiscard]] std::int32_t response_stream_id() const noexcept;
    [[nodiscard]] const std::string& response_channel() const noexcept;
    [[nodiscard]] const std::vector<std::uint8_t>& encoded_principal() const noexcept;
    [[nodiscard]] bool is_closing() const noexcept;

    [[nodiscard]] std::expected<bool, Error> close();
    [[nodiscard]] std::expected<std::int64_t, Error> offer(std::span<const std::uint8_t> payload);
    [[nodiscard]] std::expected<std::int64_t, Error> offer_vectored(
        std::span<const std::span<const std::uint8_t>> parts);
    [[nodiscard]] std::expected<TryClaim, Error> try_claim(std::size_t length);

    [[nodiscard]] std::expected<void, Error> check_for_valid_invocation() const;

private:
    Cluster* cluster_;
    ContainerClientSession* session_;
};

/// `Cluster` (`Cluster.java:130-387`): the context a business callback is given.
///
/// It is assembled per callback from the container's state; the values below
/// are copied from the log event that triggered the callback.
class Cluster
{
public:
    Cluster(axc::core::DeepmsgClient& client, SessionTable& sessions, ConsensusModuleProxy& proxy,
            ContainerContext& context, const ServiceContainerConfig& config, TimeUnit time_unit, std::int64_t time,
            std::int64_t log_position, Role role, std::int32_t member_id) noexcept;

    [[nodiscard]] std::int32_t member_id() const noexcept
    {
        return member_id_;
    }

    [[nodiscard]] Role role() const noexcept
    {
        return role_;
    }

    [[nodiscard]] std::int64_t log_position() const noexcept
    {
        return log_position_;
    }

    [[nodiscard]] std::int64_t time() const noexcept
    {
        return time_;
    }

    [[nodiscard]] TimeUnit time_unit() const noexcept
    {
        return time_unit_;
    }

    [[nodiscard]] std::int64_t time_micros() const noexcept
    {
        return time_to_micros(time_unit_, time_);
    }

    [[nodiscard]] std::int64_t time_nanos() const noexcept
    {
        return time_to_nanos(time_unit_, time_);
    }

    [[nodiscard]] std::optional<ClientSession> session(SessionId id) noexcept;
    [[nodiscard]] const std::vector<ContainerClientSession>& sessions() const noexcept
    {
        return sessions_.sessions();
    }

    template <class F>
    void for_each_client_session(F&& action);

    [[nodiscard]] axc::core::DeepmsgClient& client() noexcept
    {
        return client_;
    }

    [[nodiscard]] const ServiceContainerConfig& context() const noexcept
    {
        return config_;
    }

    [[nodiscard]] std::expected<bool, Error> close_client_session(SessionId id);
    [[nodiscard]] std::expected<bool, Error> schedule_timer(std::int64_t correlation_id, std::int64_t deadline);
    [[nodiscard]] std::expected<bool, Error> cancel_timer(std::int64_t correlation_id);
    [[nodiscard]] std::expected<std::int64_t, Error> offer(std::span<const std::uint8_t> payload);
    [[nodiscard]] std::expected<std::int64_t, Error> offer_vectored(
        std::span<const std::span<const std::uint8_t>> parts);
    [[nodiscard]] std::expected<TryClaim, Error> try_claim(std::size_t length);
    [[nodiscard]] std::expected<bool, Error> cluster_members_query(std::int64_t correlation_id);
    [[nodiscard]] std::expected<void, Error> idle(int work);

    [[nodiscard]] std::expected<void, Error> check_for_valid_invocation() const;

    // Used by ClientSession.
    [[nodiscard]] ConsensusModuleProxy& proxy() noexcept
    {
        return proxy_;
    }

    [[nodiscard]] ContainerContext& container_context() noexcept
    {
        return context_;
    }

    [[nodiscard]] std::expected<bool, Error> close_session(ContainerClientSession& session);

private:
    axc::core::DeepmsgClient& client_;
    SessionTable& sessions_;
    ConsensusModuleProxy& proxy_;
    ContainerContext& context_;
    const ServiceContainerConfig& config_;
    TimeUnit time_unit_;
    std::int64_t time_;
    std::int64_t log_position_;
    Role role_;
    std::int32_t member_id_;
};

template <class F>
void Cluster::for_each_client_session(F&& action)
{
    for (ContainerClientSession& session : sessions_.sessions())
    {
        ClientSession view{*this, session};
        action(view);
    }
}

}  // namespace deepmsg::cluster::service
