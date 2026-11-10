#include "cluster/client/async_connect.h"

#include <array>
#include <charconv>
#include <utility>

#include <aeronc.h>

#include "cluster/client/channel_util.h"
#include "cluster/common/cluster_codecs.h"
#include "core/time/clock.h"
#include "io_aeron_cluster_codecs/ChallengeResponse.h"
#include "io_aeron_cluster_codecs/SessionConnectRequest.h"

namespace deepmsg::cluster::client
{
namespace
{

namespace codecs = common::codecs;

/// `FRAGMENT_LIMIT` (`AeronCluster.java:103`).
constexpr std::size_t FRAGMENT_LIMIT = 10;
/// `AERON_CLIENT_MAX_CHANNEL_STR_LEN` is private to the C client; the same
/// bound as `DeepmsgClient::MAX_CHANNEL_LENGTH`.
constexpr std::size_t MAX_CHANNEL_LENGTH = 4096;

using axc::core::ErrorCode;
using axc::core::make_error;

/// `AERON_ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE`
/// (`aeron_client_error.h`): the `RegistrationException` code the reference
/// treats as "try again" (`AeronCluster.java:2368-2372`).
constexpr std::int32_t RESOURCE_TEMPORARILY_UNAVAILABLE = 10;

[[nodiscard]] bool is_temporarily_unavailable(const axc::core::Error& error) noexcept
{
    return RESOURCE_TEMPORARILY_UNAVAILABLE == error.errcode;
}

}  // namespace

AsyncConnect::AsyncConnect(const ClientConfig& config, std::int64_t deadline_ns)
    : config_(config), deadline_ns_(deadline_ns)
{
}

std::optional<AsyncConnect::State> AsyncConnect::state_from_step(std::int32_t step) noexcept
{
    if (step < static_cast<std::int32_t>(State::create_egress_subscription) ||
        step > static_cast<std::int32_t>(State::done))
    {
        return std::nullopt;
    }
    return static_cast<State>(step);
}

std::string_view AsyncConnect::step_name(std::int32_t step) noexcept
{
    const auto state = state_from_step(step);
    if (!state.has_value())
    {
        return "<unknown>";
    }

    switch (*state)
    {
        case State::create_egress_subscription:
            return "CREATE_EGRESS_SUBSCRIPTION";
        case State::create_ingress_publications:
            return "CREATE_INGRESS_PUBLICATIONS";
        case State::await_publication_connected:
            return "AWAIT_PUBLICATION_CONNECTED";
        case State::send_message:
            return "SEND_MESSAGE";
        case State::poll_response:
            return "POLL_RESPONSE";
        case State::conclude_connect:
            return "CONCLUDE_CONNECT";
        case State::done:
            return "DONE";
    }
    return "<unknown>";
}

std::expected<std::vector<AsyncConnect::MemberIngress>, Error> AsyncConnect::parse_ingress_endpoints(
    std::string_view endpoints)
{
    std::vector<MemberIngress> members;

    // Java's `String.split(",")`: an empty string yields one empty token, which
    // then fails the '=' check.
    std::size_t begin = 0;
    while (true)
    {
        const std::size_t comma = endpoints.find(',', begin);
        const std::string_view token =
            std::string_view::npos == comma ? endpoints.substr(begin) : endpoints.substr(begin, comma - begin);

        const std::size_t equals = token.find('=');
        if (std::string_view::npos == equals)
        {
            // `AeronCluster.java:744-748`.
            return std::unexpected(
                make_error(ErrorCode::config_error, "endpoint missing '=' separator: " + std::string{endpoints}));
        }

        std::int32_t member_id = 0;
        const std::string_view id = token.substr(0, equals);
        const auto parsed = std::from_chars(id.begin(), id.end(), member_id);
        if (std::errc{} != parsed.ec || parsed.ptr != id.end())
        {
            return std::unexpected(
                make_error(ErrorCode::config_error, "endpoint member id is not a number: " + std::string{endpoints}));
        }

        MemberIngress member;
        member.member_id = member_id;
        member.endpoint = std::string{token.substr(equals + 1)};
        members.push_back(std::move(member));

        if (std::string_view::npos == comma)
        {
            break;
        }
        begin = comma + 1;
    }

    return members;
}

std::expected<AsyncConnect, Error> AsyncConnect::create(const ClientConfig& config)
{
    if (!config.is_concluded())
    {
        return std::unexpected(
            make_error(ErrorCode::config_error, "the client context must be concluded before a connect"));
    }
    if (!config.ingress_channel.has_value() || !config.egress_channel.has_value())
    {
        // `conclude()` already refused this; kept so the state machine can use
        // the channels without dereferencing an optional.
        return std::unexpected(make_error(ErrorCode::config_error, "ingress and egress channels must be specified"));
    }

    // `AeronCluster.java:2202`: the member table is parsed in the constructor.
    std::vector<MemberIngress> members;
    if (config.ingress_endpoints.has_value())
    {
        auto parsed = parse_ingress_endpoints(*config.ingress_endpoints);
        if (!parsed)
        {
            return std::unexpected(parsed.error());
        }
        members = std::move(*parsed);
    }

    // `AeronCluster.java:2195`: one deadline for the whole connect, challenges
    // and redirects included.
    AsyncConnect connect(config, axc::core::NanoClock::now() + config.message_timeout_ns);
    connect.ingress_channel_ = *config.ingress_channel;
    connect.egress_channel_ = *config.egress_channel;
    connect.members_ = std::move(members);
    return connect;
}

std::expected<std::optional<ClusterClient>, Error> AsyncConnect::poll(axc::core::DeepmsgClient& client)
{
    // `AeronCluster.java:2285`.
    if (axc::core::NanoClock::now() > deadline_ns_)
    {
        return std::unexpected(deadline_error(client));
    }

    // `AeronCluster.java:2288-2315`: one state per call.
    switch (state_)
    {
        case State::create_egress_subscription:
        {
            auto result = create_egress_subscription(client);
            return result ? std::expected<std::optional<ClusterClient>, Error>{std::nullopt}
                          : std::unexpected(result.error());
        }

        case State::create_ingress_publications:
        {
            auto result = create_ingress_publications(client);
            return result ? std::expected<std::optional<ClusterClient>, Error>{std::nullopt}
                          : std::unexpected(result.error());
        }

        case State::await_publication_connected:
        {
            auto result = await_publication_connected(client);
            return result ? std::expected<std::optional<ClusterClient>, Error>{std::nullopt}
                          : std::unexpected(result.error());
        }

        case State::send_message:
        {
            auto result = send_message();
            return result ? std::expected<std::optional<ClusterClient>, Error>{std::nullopt}
                          : std::unexpected(result.error());
        }

        case State::poll_response:
        {
            auto result = poll_response(client);
            return result ? std::expected<std::optional<ClusterClient>, Error>{std::nullopt}
                          : std::unexpected(result.error());
        }

        case State::conclude_connect:
            return conclude_connect();

        case State::done:
            break;
    }

    return std::optional<ClusterClient>{};
}

std::expected<void, Error> AsyncConnect::create_egress_subscription(axc::core::DeepmsgClient& client)
{
    // `AeronCluster.java:2353-2380`.
    if (!egress_add_.has_value())
    {
        egress_add_.emplace(client.add_subscription(egress_channel_, config_.egress_stream_id));
    }

    auto polled = egress_add_->poll();
    if (!polled)
    {
        if (is_temporarily_unavailable(polled.error()))
        {
            egress_add_.reset();
            return {};
        }
        return std::unexpected(polled.error());
    }

    if (!polled->has_value())
    {
        return {};
    }

    egress_subscription_ = std::move(**polled);
    egress_add_.reset();
    egress_poller_ = std::make_unique<EgressPoller>(FRAGMENT_LIMIT);
    state_ = State::create_ingress_publications;
    return {};
}

std::expected<void, Error> AsyncConnect::create_ingress_publications(axc::core::DeepmsgClient& client)
{
    // `AeronCluster.java:2382-2417`.
    if (!config_.ingress_endpoints.has_value())
    {
        if (!ingress_publication_.valid())
        {
            if (config_.is_ingress_exclusive)
            {
                if (!ingress_exclusive_add_.has_value())
                {
                    ingress_exclusive_add_.emplace(
                        client.add_exclusive_publication(ingress_channel_, config_.ingress_stream_id));
                }

                auto polled = ingress_exclusive_add_->poll();
                if (!polled)
                {
                    if (is_temporarily_unavailable(polled.error()))
                    {
                        ingress_exclusive_add_.reset();
                        return {};
                    }
                    return std::unexpected(polled.error());
                }
                if (!polled->has_value())
                {
                    return {};
                }

                ingress_publication_ = IngressPublication::from_exclusive(std::move(**polled));
                ingress_exclusive_add_.reset();
            }
            else
            {
                if (!ingress_concurrent_add_.has_value())
                {
                    ingress_concurrent_add_.emplace(
                        client.add_publication(ingress_channel_, config_.ingress_stream_id));
                }

                auto polled = ingress_concurrent_add_->poll();
                if (!polled)
                {
                    if (is_temporarily_unavailable(polled.error()))
                    {
                        ingress_concurrent_add_.reset();
                        return {};
                    }
                    return std::unexpected(polled.error());
                }
                if (!polled->has_value())
                {
                    return {};
                }

                ingress_publication_ = IngressPublication::from_concurrent(std::move(**polled));
                ingress_concurrent_add_.reset();
            }
        }
        else
        {
            ingress_concurrent_add_.reset();
            ingress_exclusive_add_.reset();
            state_ = State::await_publication_connected;
        }
    }
    else
    {
        state_ = State::await_publication_connected;
    }

    return {};
}

std::expected<std::optional<std::string>, Error> AsyncConnect::resolved_response_channel() const
{
    // `tryResolveChannelEndpointPort` (`AeronCluster.java:2421`): null while a
    // wildcard port is unresolved.
    std::array<char, MAX_CHANNEL_LENGTH> buffer{};
    const int result =
        aeron_subscription_try_resolve_channel_endpoint_port(egress_subscription_.get(), buffer.data(), buffer.size());
    if (result < 0)
    {
        return std::unexpected(
            make_error(ErrorCode::internal_error, "failed to resolve the egress channel", aeron_errcode()));
    }
    if (0 == result)
    {
        return std::optional<std::string>{};
    }

    return std::optional<std::string>{std::string{buffer.data(), static_cast<std::size_t>(result)}};
}

std::expected<void, Error> AsyncConnect::await_publication_connected(axc::core::DeepmsgClient& client)
{
    // `AeronCluster.java:2419-2450`.
    auto response_channel = resolved_response_channel();
    if (!response_channel)
    {
        return std::unexpected(response_channel.error());
    }
    if (!response_channel->has_value())
    {
        return {};
    }

    if (!ingress_publication_.valid())
    {
        for (MemberIngress& member : members_)
        {
            if (!member.publication.valid() && !member.publication_exception.has_value())
            {
                // Only start an add once; `asyncGetPublication` is the poll.
                if (!member.concurrent_add.has_value() && !member.exclusive_add.has_value())
                {
                    async_add_member_publication(client, member);
                }
                async_get_member_publication(member);
            }

            if (member.publication.valid() && member.publication.is_connected())
            {
                ingress_publication_ = std::move(member.publication);
                prepare_connect_request(client, **response_channel);
                break;
            }
        }
    }
    else if (ingress_publication_.is_connected())
    {
        prepare_connect_request(client, **response_channel);
    }

    return {};
}

void AsyncConnect::async_add_member_publication(axc::core::DeepmsgClient& client, MemberIngress& member)
{
    // `MemberIngress.asyncAddPublication` (`AeronCluster.java:2617-2627`): a
    // UDP ingress channel gets the member's endpoint substituted.
    std::string channel = ingress_channel_;
    if ("udp" == detail::channel_media(channel))
    {
        channel = detail::with_parameter(channel, "endpoint", member.endpoint);
    }

    if (config_.is_ingress_exclusive)
    {
        member.exclusive_add.emplace(client.add_exclusive_publication(channel, config_.ingress_stream_id));
    }
    else
    {
        member.concurrent_add.emplace(client.add_publication(channel, config_.ingress_stream_id));
    }
}

void AsyncConnect::async_get_member_publication(MemberIngress& member)
{
    // `MemberIngress.asyncGetPublication` (`AeronCluster.java:2629-2647`).
    if (config_.is_ingress_exclusive)
    {
        if (!member.exclusive_add.has_value())
        {
            return;
        }

        auto polled = member.exclusive_add->poll();
        if (!polled)
        {
            if (is_temporarily_unavailable(polled.error()))
            {
                // `AERON_ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE` is
                // retried on the next pass (`AeronCluster.java:2639-2646`).
                member.exclusive_add.reset();
            }
            else
            {
                member.publication_exception = polled.error();
                member.exclusive_add.reset();
            }
            return;
        }
        if (!polled->has_value())
        {
            return;
        }

        member.publication = IngressPublication::from_exclusive(std::move(**polled));
        member.exclusive_add.reset();
        return;
    }

    if (!member.concurrent_add.has_value())
    {
        return;
    }

    auto polled = member.concurrent_add->poll();
    if (!polled)
    {
        if (is_temporarily_unavailable(polled.error()))
        {
            member.concurrent_add.reset();
        }
        else
        {
            member.publication_exception = polled.error();
            member.concurrent_add.reset();
        }
        return;
    }
    if (!polled->has_value())
    {
        return;
    }

    member.publication = IngressPublication::from_concurrent(std::move(**polled));
    member.concurrent_add.reset();
}

void AsyncConnect::prepare_connect_request(axc::core::DeepmsgClient& client, std::string_view response_channel)
{
    // `AeronCluster.java:2452-2471`.
    correlation_id_ = client.next_correlation_id();
    const std::vector<std::uint8_t> encoded_credentials = config_.credentials.encoded();
    const std::string info = client_info(config_);

    const std::size_t length =
        common::MESSAGE_HEADER_LENGTH +
        codecs::SessionConnectRequest::computeLength(response_channel.size(), encoded_credentials.size(), info.size());
    buffer_.resize(length);

    codecs::SessionConnectRequest encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer_.data()), 0, buffer_.size());
    encoder.correlationId(correlation_id_)
        .responseStreamId(config_.egress_stream_id)
        .version(PROTOCOL_SEMANTIC_VERSION);
    encoder.putResponseChannel(response_channel);
    const char* credentials_data =
        encoded_credentials.empty() ? "" : reinterpret_cast<const char*>(encoded_credentials.data());
    encoder.putEncodedCredentials(credentials_data, static_cast<std::uint32_t>(encoded_credentials.size()));
    encoder.putClientInfo(info);

    message_length_ = common::MESSAGE_HEADER_LENGTH + encoder.encodedLength();
    state_ = State::send_message;
}

void AsyncConnect::prepare_challenge_response(axc::core::DeepmsgClient& client,
                                              std::span<const std::uint8_t> credentials)
{
    // `AeronCluster.java:2528-2541`.
    correlation_id_ = client.next_correlation_id();

    const std::size_t length =
        common::MESSAGE_HEADER_LENGTH + codecs::ChallengeResponse::computeLength(credentials.size());
    buffer_.resize(length);

    codecs::ChallengeResponse encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer_.data()), 0, buffer_.size());
    encoder.correlationId(correlation_id_).clusterSessionId(cluster_session_id_);
    const char* credentials_data = credentials.empty() ? "" : reinterpret_cast<const char*>(credentials.data());
    encoder.putEncodedCredentials(credentials_data, static_cast<std::uint32_t>(credentials.size()));

    message_length_ = common::MESSAGE_HEADER_LENGTH + encoder.encodedLength();
    state_ = State::send_message;
}

std::expected<void, Error> AsyncConnect::send_message()
{
    // `AeronCluster.java:2473-2484`.
    const std::int64_t position =
        ingress_publication_.offer(std::span<const std::uint8_t>(buffer_.data(), message_length_));
    if (position > 0)
    {
        state_ = State::poll_response;
        return {};
    }

    if (AERON_PUBLICATION_CLOSED == position || AERON_PUBLICATION_NOT_CONNECTED == position)
    {
        return std::unexpected(make_error(ErrorCode::no_capacity, "unexpected loss of connection to cluster"));
    }

    // Back-pressure: stay in SEND_MESSAGE and retry on the next pass.
    return {};
}

std::expected<void, Error> AsyncConnect::poll_response(axc::core::DeepmsgClient& client)
{
    // `AeronCluster.java:2486-2526`.
    const int fragments = egress_poller_->poll(egress_subscription_.get());
    if (fragments < 0)
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "egress poll failed", aeron_errcode()));
    }

    if (fragments <= 0 || !egress_poller_->is_poll_complete() || egress_poller_->correlation_id() != correlation_id_)
    {
        return {};
    }

    if (egress_poller_->is_challenged())
    {
        correlation_id_ = NULL_VALUE;
        cluster_session_id_ = egress_poller_->cluster_session_id();
        const std::vector<std::uint8_t> response =
            config_.credentials.on_challenge(egress_poller_->encoded_challenge());
        prepare_challenge_response(client, response);
        return {};
    }

    switch (egress_poller_->event_code().value_or(codecs::EventCode::NULL_VALUE))
    {
        case codecs::EventCode::OK:
        {
            // `AeronCluster.java:2502-2509`.
            leadership_term_id_ = egress_poller_->leadership_term_id();
            leader_member_id_ = egress_poller_->leader_member_id();
            cluster_session_id_ = egress_poller_->cluster_session_id();
            leader_heartbeat_timeout_ns_ = egress_poller_->leader_heartbeat_timeout_ns();
            state_ = State::conclude_connect;
            break;
        }

        case codecs::EventCode::ERROR:
            // `AeronCluster.java:2511-2512`.
            return std::unexpected(make_error(ErrorCode::protocol_error, std::string{egress_poller_->detail()}));

        case codecs::EventCode::REDIRECT:
            // `AeronCluster.java:2514-2516`.
            update_members();
            break;

        case codecs::EventCode::AUTHENTICATION_REJECTED:
            // `AeronCluster.java:2518-2519`.
            return std::unexpected(make_error(ErrorCode::protocol_error, std::string{egress_poller_->detail()}));

        case codecs::EventCode::CLOSED:
        case codecs::EventCode::NULL_VALUE:
            // `AeronCluster.java:2521-2523`: nothing to do.
            break;
    }

    return {};
}

void AsyncConnect::update_members()
{
    // `AeronCluster.java:2543-2570`.
    leader_member_id_ = egress_poller_->leader_member_id();

    std::optional<MemberIngress> old_leader;
    for (auto iterator = members_.begin(); iterator != members_.end(); ++iterator)
    {
        if (iterator->member_id == leader_member_id_)
        {
            old_leader = std::move(*iterator);
            members_.erase(iterator);
            break;
        }
    }

    ingress_publication_.close();

    // `CloseHelper.closeAll(memberByIdMap.values())`.
    close_members();

    auto parsed = parse_ingress_endpoints(egress_poller_->detail());
    members_ = parsed.has_value() ? std::move(*parsed) : std::vector<MemberIngress>{};

    for (MemberIngress& member : members_)
    {
        if (member.member_id != leader_member_id_)
        {
            continue;
        }

        if (old_leader.has_value() && !old_leader->publication_exception.has_value() &&
            old_leader->endpoint == member.endpoint)
        {
            // Same endpoint: keep the publication the client already has.
            member.publication = std::move(old_leader->publication);
        }
        else if (old_leader.has_value())
        {
            // `CloseHelper.close(oldLeader)`: destroyed when the optional goes.
            old_leader.reset();
        }
        break;
    }

    state_ = State::await_publication_connected;
}

std::expected<std::optional<ClusterClient>, Error> AsyncConnect::conclude_connect()
{
    // `AeronCluster.java:2572-2598`.
    if (!config_.new_leader_timeout_ns.has_value())
    {
        const std::int64_t heartbeat = NULL_VALUE != leader_heartbeat_timeout_ns_ ? leader_heartbeat_timeout_ns_
                                                                                  : LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS;
        config_.new_leader_timeout_ns = 2 * heartbeat;
    }

    ClusterClient cluster(config_, std::move(egress_subscription_), std::move(ingress_publication_),
                          egress_poller_->egress_image(), cluster_session_id_, leadership_term_id_, leader_member_id_);

    // The leader's ingress publication went into the cluster above; the other
    // members are closed (`AeronCluster.java:2591-2593`).
    close_members();
    state_ = State::done;

    return std::optional<ClusterClient>{std::move(cluster)};
}

void AsyncConnect::close_members() noexcept
{
    for (MemberIngress& member : members_)
    {
        member.publication.close();
        member.concurrent_add.reset();
        member.exclusive_add.reset();
    }
    members_.clear();
}

void AsyncConnect::close(axc::core::DeepmsgClient& client) noexcept
{
    // `AeronCluster.java:2210-2236`. The `AsyncAdd` handles cancel a pending
    // add when destroyed, which is the C equivalent of `asyncRemoveX`.
    (void)client;

    if (State::done == state_)
    {
        return;
    }

    ingress_publication_.close();
    ingress_concurrent_add_.reset();
    ingress_exclusive_add_.reset();
    egress_poller_.reset();
    egress_subscription_.reset();
    egress_add_.reset();
    close_members();
}

Error AsyncConnect::deadline_error(axc::core::DeepmsgClient& client) const
{
    // `AeronCluster.java:2320-2345`: the state plus every resource fact that
    // explains the stall.
    (void)client;

    const bool egress_connected =
        nullptr != egress_subscription_ && aeron_subscription_is_connected(egress_subscription_.get());
    std::string response_channel = "<unknown>";
    std::array<char, MAX_CHANNEL_LENGTH> buffer{};
    if (nullptr != egress_subscription_ && 0 < aeron_subscription_try_resolve_channel_endpoint_port(
                                                   egress_subscription_.get(), buffer.data(), buffer.size()))
    {
        response_channel = buffer.data();
    }

    std::string message = "cluster connect timeout: state=";
    message += step_name(step());
    message += " messageTimeout=" + std::to_string(config_.message_timeout_ns) + "ns";
    message += " ingressChannel=" + config_.ingress_channel.value_or("<null>");
    message += " ingressEndpoints=" + config_.ingress_endpoints.value_or("<null>");
    message += " ingressPublication=";
    message +=
        !ingress_publication_.valid() ? "<null>" : (ingress_publication_.is_connected() ? "connected" : "disconnected");
    message += " egress.isConnected=";
    message += egress_connected ? "true" : "false";
    message += " responseChannel=" + response_channel;

    for (const MemberIngress& member : members_)
    {
        if (member.publication_exception.has_value())
        {
            message += " member[" + std::to_string(member.member_id) +
                       "]=" + std::string{member.publication_exception->message.view()};
        }
    }

    return make_error(ErrorCode::timed_out, message);
}

}  // namespace deepmsg::cluster::client
