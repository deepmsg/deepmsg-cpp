#include "cluster/client/cluster_client.h"

#include <algorithm>
#include <charconv>
#include <cstring>
#include <utility>

#include "cluster/client/channel_util.h"
#include "core/error/error.h"
#include "core/time/clock.h"
#include "io_aeron_cluster_codecs/AdminResponse.h"
#include "io_aeron_cluster_codecs/NewLeaderEvent.h"
#include "io_aeron_cluster_codecs/SessionCloseRequest.h"
#include "io_aeron_cluster_codecs/SessionEvent.h"
#include "io_aeron_cluster_codecs/SessionKeepAlive.h"
#include "io_aeron_cluster_codecs/SessionMessageHeader.h"

namespace deepmsg::cluster::client
{
namespace
{

namespace codecs = common::codecs;

/// `SEND_ATTEMPTS` (`AeronCluster.java:102`).
constexpr int SEND_ATTEMPTS = 3;
using axc::core::ErrorCode;
using axc::core::make_error;

/// `AERON_ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE` (`aeron_client_error.h`).
constexpr std::int32_t RESOURCE_TEMPORARILY_UNAVAILABLE = 10;

[[nodiscard]] bool is_temporarily_unavailable(const axc::core::Error& error) noexcept
{
    return RESOURCE_TEMPORARILY_UNAVAILABLE == error.errcode;
}

/// `SessionMessageHeaderEncoder.wrapAndApplyHeader(...)` (`AeronCluster.java:259-262`).
void encode_session_header(std::span<std::uint8_t> header, std::int64_t leadership_term_id,
                           std::int64_t cluster_session_id)
{
    codecs::SessionMessageHeader encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(header.data()), 0, header.size());
    encoder.clusterSessionId(cluster_session_id).leadershipTermId(leadership_term_id);
}

/// `parseIngressEndpoints` (`AeronCluster.java:736-756`) for the session's
/// failover path; member ids are small and the list is short.
struct ParsedMember
{
    std::int32_t member_id{-1};
    std::string endpoint{};
};

[[nodiscard]] std::expected<std::vector<ParsedMember>, axc::core::Error> parse_endpoints(std::string_view endpoints)
{
    std::vector<ParsedMember> members;
    std::size_t begin = 0;

    while (true)
    {
        const std::size_t comma = endpoints.find(',', begin);
        const std::string_view token =
            std::string_view::npos == comma ? endpoints.substr(begin) : endpoints.substr(begin, comma - begin);

        const std::size_t equals = token.find('=');
        if (std::string_view::npos == equals)
        {
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

        members.push_back(ParsedMember{member_id, std::string{token.substr(equals + 1)}});
        if (std::string_view::npos == comma)
        {
            break;
        }
        begin = comma + 1;
    }

    return members;
}

}  // namespace

std::int64_t SessionClaim::commit() noexcept
{
    if (0 != aeron_buffer_claim_commit(&claim_))
    {
        return AERON_PUBLICATION_ERROR;
    }
    return position_;
}

void SessionClaim::abort() noexcept
{
    (void)aeron_buffer_claim_abort(&claim_);
}

bool IngressPublication::is_connected() const noexcept
{
    if (nullptr != exclusive_)
    {
        return aeron_exclusive_publication_is_connected(exclusive_.get());
    }
    if (nullptr != concurrent_)
    {
        return aeron_publication_is_connected(concurrent_.get());
    }
    return false;
}

std::int64_t IngressPublication::offer(std::span<const std::uint8_t> message) const noexcept
{
    if (nullptr != exclusive_)
    {
        return aeron_exclusive_publication_offer(exclusive_.get(), message.data(), message.size(), nullptr, nullptr);
    }
    if (nullptr != concurrent_)
    {
        return aeron_publication_offer(concurrent_.get(), message.data(), message.size(), nullptr, nullptr);
    }
    return AERON_PUBLICATION_NOT_CONNECTED;
}

std::int64_t IngressPublication::offer_two(std::span<const std::uint8_t> first,
                                           std::span<const std::uint8_t> second) const noexcept
{
    aeron_iovec_t iov[2]{};
    iov[0].iov_base = const_cast<std::uint8_t*>(first.data());
    iov[0].iov_len = first.size();
    iov[1].iov_base = const_cast<std::uint8_t*>(second.data());
    iov[1].iov_len = second.size();

    if (nullptr != exclusive_)
    {
        return aeron_exclusive_publication_offerv(exclusive_.get(), iov, 2, nullptr, nullptr);
    }
    if (nullptr != concurrent_)
    {
        return aeron_publication_offerv(concurrent_.get(), iov, 2, nullptr, nullptr);
    }
    return AERON_PUBLICATION_NOT_CONNECTED;
}

std::int32_t IngressPublication::term_buffer_length() const noexcept
{
    aeron_publication_constants_t constants{};
    if (nullptr != exclusive_)
    {
        if (0 != aeron_exclusive_publication_constants(exclusive_.get(), &constants))
        {
            return 0;
        }
        return static_cast<std::int32_t>(constants.term_buffer_length);
    }
    if (nullptr != concurrent_)
    {
        if (0 != aeron_publication_constants(concurrent_.get(), &constants))
        {
            return 0;
        }
        return static_cast<std::int32_t>(constants.term_buffer_length);
    }
    return 0;
}

std::int64_t IngressPublication::try_claim(std::size_t length, aeron_buffer_claim_t& claim) const noexcept
{
    if (nullptr != exclusive_)
    {
        return aeron_exclusive_publication_try_claim(exclusive_.get(), length, &claim);
    }
    if (nullptr != concurrent_)
    {
        return aeron_publication_try_claim(concurrent_.get(), length, &claim);
    }
    return AERON_PUBLICATION_NOT_CONNECTED;
}

ClusterClient::ClusterClient(axc::core::DeepmsgClient& client, ClientConfig config,
                             axc::core::SubPtr egress_subscription, IngressPublication ingress_publication,
                             aeron_image_t* egress_image, std::int64_t cluster_session_id,
                             std::int64_t leadership_term_id, std::int32_t leader_member_id)
    : client_(&client),
      config_(std::move(config)),
      egress_subscription_(std::move(egress_subscription)),
      ingress_publication_(std::move(ingress_publication)),
      egress_image_(nullptr),
      cluster_session_id_(cluster_session_id),
      leadership_term_id_(leadership_term_id),
      leader_member_id_(leader_member_id)
{
    encode_session_header(header_, leadership_term_id_, cluster_session_id_);
    ingress_channel_ = config_.ingress_channel.value_or(std::string{});
    // The hand-off from `AsyncConnect` is the first image the session owns.
    set_egress_image(egress_image);

    if (0 != aeron_fragment_assembler_create(&assembler_, &ClusterClient::on_fragment_dispatch, this))
    {
        assembler_ = nullptr;
    }
    if (0 != aeron_controlled_fragment_assembler_create(&controlled_assembler_, &ClusterClient::on_fragment, this))
    {
        controlled_assembler_ = nullptr;
    }

    state(State::connected, 0);
}

ClusterClient::ClusterClient(ClusterClient&& other) noexcept
    : client_(other.client_),
      config_(std::move(other.config_)),
      ingress_channel_(std::move(other.ingress_channel_)),
      pending_error_(other.pending_error_),
      egress_subscription_(std::move(other.egress_subscription_)),
      ingress_publication_(std::move(other.ingress_publication_)),
      egress_image_(other.egress_image_),
      cluster_session_id_(other.cluster_session_id_),
      leadership_term_id_(other.leadership_term_id_),
      leader_member_id_(other.leader_member_id_),
      state_(other.state_),
      state_deadline_ns_(other.state_deadline_ns_),
      header_(other.header_),
      members_(std::move(other.members_)),
      assembler_(nullptr),
      controlled_assembler_(nullptr),
      egress_listener_(other.egress_listener_),
      controlled_egress_listener_(other.controlled_egress_listener_)
{
    if (nullptr != other.assembler_)
    {
        (void)aeron_fragment_assembler_delete(other.assembler_);
        other.assembler_ = nullptr;
    }
    if (nullptr != other.controlled_assembler_)
    {
        (void)aeron_controlled_fragment_assembler_delete(other.controlled_assembler_);
        other.controlled_assembler_ = nullptr;
    }
    (void)aeron_fragment_assembler_create(&assembler_, &ClusterClient::on_fragment_dispatch, this);
    (void)aeron_controlled_fragment_assembler_create(&controlled_assembler_, &ClusterClient::on_fragment, this);
    other.client_ = nullptr;
    other.egress_image_ = nullptr;
}

ClusterClient& ClusterClient::operator=(ClusterClient&& other) noexcept
{
    if (this != &other)
    {
        if (nullptr != assembler_)
        {
            (void)aeron_fragment_assembler_delete(assembler_);
        }
        if (nullptr != controlled_assembler_)
        {
            (void)aeron_controlled_fragment_assembler_delete(controlled_assembler_);
        }

        release_egress_image();
        client_ = other.client_;
        config_ = std::move(other.config_);
        ingress_channel_ = std::move(other.ingress_channel_);
        pending_error_ = other.pending_error_;
        egress_subscription_ = std::move(other.egress_subscription_);
        ingress_publication_ = std::move(other.ingress_publication_);
        egress_image_ = other.egress_image_;
        cluster_session_id_ = other.cluster_session_id_;
        leadership_term_id_ = other.leadership_term_id_;
        leader_member_id_ = other.leader_member_id_;
        state_ = other.state_;
        state_deadline_ns_ = other.state_deadline_ns_;
        header_ = other.header_;
        members_ = std::move(other.members_);
        egress_listener_ = other.egress_listener_;
        controlled_egress_listener_ = other.controlled_egress_listener_;
        assembler_ = nullptr;
        controlled_assembler_ = nullptr;
        if (nullptr != other.assembler_)
        {
            (void)aeron_fragment_assembler_delete(other.assembler_);
            other.assembler_ = nullptr;
        }
        if (nullptr != other.controlled_assembler_)
        {
            (void)aeron_controlled_fragment_assembler_delete(other.controlled_assembler_);
            other.controlled_assembler_ = nullptr;
        }
        (void)aeron_fragment_assembler_create(&assembler_, &ClusterClient::on_fragment_dispatch, this);
        (void)aeron_controlled_fragment_assembler_create(&controlled_assembler_, &ClusterClient::on_fragment, this);
        other.client_ = nullptr;
        other.egress_image_ = nullptr;
    }
    return *this;
}

ClusterClient::~ClusterClient()
{
    release_egress_image();
    if (nullptr != assembler_)
    {
        (void)aeron_fragment_assembler_delete(assembler_);
    }
    if (nullptr != controlled_assembler_)
    {
        (void)aeron_controlled_fragment_assembler_delete(controlled_assembler_);
    }
}

std::int64_t ClusterClient::now_ns() const
{
    return config_.clock ? config_.clock() : axc::core::NanoClock::now();
}

void ClusterClient::set_egress_image(aeron_image_t* image) noexcept
{
    if (image == egress_image_)
    {
        return;
    }
    release_egress_image();
    egress_image_ = image;
    if (nullptr != egress_image_ && nullptr != egress_subscription_)
    {
        (void)aeron_subscription_image_retain(egress_subscription_.get(), egress_image_);
    }
}

void ClusterClient::release_egress_image() noexcept
{
    if (nullptr != egress_image_ && nullptr != egress_subscription_)
    {
        (void)aeron_subscription_image_release(egress_subscription_.get(), egress_image_);
    }
    egress_image_ = nullptr;
}

void ClusterClient::state(State new_state, std::int64_t deadline_ns) noexcept
{
    state_ = new_state;
    state_deadline_ns_ = deadline_ns;
}

std::int64_t ClusterClient::offer(std::span<const std::uint8_t> payload)
{
    const std::int64_t result = ingress_publication_.offer_two(header_, payload);
    track_ingress_publication_result(result);
    return result;
}

std::int64_t ClusterClient::offer_vectored(std::span<const std::span<const std::uint8_t>> parts)
{
    // The reference replaces vector 0 with the session header
    // (`AeronCluster.java:468-477`); the C offerv takes the same shape.
    std::vector<aeron_iovec_t> iov;
    iov.reserve(parts.size() + 1);
    iov.push_back(aeron_iovec_t{const_cast<std::uint8_t*>(header_.data()), header_.size()});
    for (const std::span<const std::uint8_t> part : parts)
    {
        iov.push_back(aeron_iovec_t{const_cast<std::uint8_t*>(part.data()), part.size()});
    }

    std::int64_t result = AERON_PUBLICATION_NOT_CONNECTED;
    if (nullptr != ingress_publication_.exclusive())
    {
        result = aeron_exclusive_publication_offerv(ingress_publication_.exclusive(), iov.data(),
                                                    static_cast<int>(iov.size()), nullptr, nullptr);
    }
    else if (nullptr != ingress_publication_.concurrent())
    {
        result = aeron_publication_offerv(ingress_publication_.concurrent(), iov.data(), static_cast<int>(iov.size()),
                                          nullptr, nullptr);
    }

    track_ingress_publication_result(result);
    return result;
}

std::expected<SessionClaim, std::int64_t> ClusterClient::try_claim(std::int32_t length)
{
    aeron_buffer_claim_t claim{};
    const std::int64_t position = ingress_publication_.try_claim(SESSION_HEADER_LENGTH + length, claim);
    track_ingress_publication_result(position);
    if (position <= 0)
    {
        return std::unexpected(position);
    }

    std::memcpy(claim.data, header_.data(), SESSION_HEADER_LENGTH);
    return SessionClaim(claim, std::span<std::uint8_t>(claim.data + SESSION_HEADER_LENGTH, length), position);
}

std::expected<bool, Error> ClusterClient::send_keep_alive()
{
    const std::size_t length = common::MESSAGE_HEADER_LENGTH + codecs::SessionKeepAlive::sbeBlockLength();

    for (int attempt = 0; attempt < SEND_ATTEMPTS; ++attempt)
    {
        aeron_buffer_claim_t claim{};
        const std::int64_t position = ingress_publication_.try_claim(length, claim);
        track_ingress_publication_result(position);

        if (position > 0)
        {
            codecs::SessionKeepAlive encoder;
            encoder.wrapAndApplyHeader(reinterpret_cast<char*>(claim.data), 0, claim.length);
            encoder.leadershipTermId(leadership_term_id_).clusterSessionId(cluster_session_id_);
            (void)aeron_buffer_claim_commit(&claim);
            return true;
        }

        if (AERON_PUBLICATION_MAX_POSITION_EXCEEDED == position)
        {
            return std::unexpected(make_error(
                ErrorCode::no_capacity,
                "max position exceeded: term-length=" + std::to_string(ingress_publication_.term_buffer_length())));
        }
    }

    return false;
}

void ClusterClient::close_session()
{
    const std::size_t length = common::MESSAGE_HEADER_LENGTH + codecs::SessionCloseRequest::sbeBlockLength();

    for (int attempt = 0; attempt < SEND_ATTEMPTS; ++attempt)
    {
        aeron_buffer_claim_t claim{};
        const std::int64_t position = ingress_publication_.try_claim(length, claim);
        track_ingress_publication_result(position);

        if (position > 0)
        {
            codecs::SessionCloseRequest encoder;
            encoder.wrapAndApplyHeader(reinterpret_cast<char*>(claim.data), 0, claim.length);
            encoder.leadershipTermId(leadership_term_id_).clusterSessionId(cluster_session_id_);
            (void)aeron_buffer_claim_commit(&claim);
            return;
        }
    }
}

std::expected<int, Error> ClusterClient::poll_egress(std::size_t fragment_limit)
{
    if (nullptr == egress_subscription_ || State::closed == state_)
    {
        return 0;
    }

    const int fragments =
        aeron_subscription_poll(egress_subscription_.get(), &ClusterClient::on_fragment_dispatch, this, fragment_limit);
    if (fragments < 0)
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "egress poll failed", aeron_errcode()));
    }

    int work = fragments;

    // `AeronCluster.java:603-607`: a closed image while connected means the
    // leader is gone.
    if (nullptr != egress_image_ && aeron_image_is_closed(egress_image_) &&
        (State::connected == state_ || State::await_new_leader_connection == state_))
    {
        on_disconnected();
        ++work;
    }

    work += poll_state_changes();
    if (pending_error_.has_value())
    {
        Error error = *pending_error_;
        pending_error_.reset();
        return std::unexpected(error);
    }

    return work;
}

std::expected<int, Error> ClusterClient::controlled_poll_egress(std::size_t fragment_limit)
{
    if (nullptr == egress_subscription_ || State::closed == state_)
    {
        return 0;
    }

    const int fragments = aeron_subscription_controlled_poll(egress_subscription_.get(), &ClusterClient::on_fragment,
                                                             this, fragment_limit);
    if (fragments < 0)
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "egress poll failed", aeron_errcode()));
    }

    int work = fragments;
    if (nullptr != egress_image_ && aeron_image_is_closed(egress_image_) &&
        (State::connected == state_ || State::await_new_leader_connection == state_))
    {
        on_disconnected();
        ++work;
    }

    work += poll_state_changes();
    if (pending_error_.has_value())
    {
        Error error = *pending_error_;
        pending_error_.reset();
        return std::unexpected(error);
    }

    return work;
}

int ClusterClient::poll_state_changes()
{
    if (State::pending_close == state_ ||
        ((State::await_new_leader == state_ || State::await_new_leader_connection == state_) &&
         0 <= now_ns() - state_deadline_ns_))
    {
        close();
        return 1;
    }
    return 0;
}

void ClusterClient::track_ingress_publication_result(std::int64_t result)
{
    if (State::connected == state_)
    {
        if (AERON_PUBLICATION_NOT_CONNECTED == result || AERON_PUBLICATION_CLOSED == result)
        {
            on_disconnected();
        }
        else if (AERON_PUBLICATION_MAX_POSITION_EXCEEDED == result)
        {
            ingress_publication_.close();
            state(State::pending_close, 0);
        }
    }
    else if (State::await_new_leader_connection == state_)
    {
        if (0 < result)
        {
            state(State::connected, 0);
        }
    }
}

void ClusterClient::on_disconnected()
{
    ingress_publication_.close();
    const std::int64_t timeout = config_.new_leader_timeout_ns.value_or(2 * LEADER_HEARTBEAT_TIMEOUT_DEFAULT_NS);
    state(State::await_new_leader, now_ns() + timeout);
}

std::expected<void, Error> ClusterClient::on_new_leader(std::int64_t cluster_session_id,
                                                        std::int64_t leadership_term_id, std::int32_t leader_member_id,
                                                        std::string_view ingress_endpoints)
{
    // `AeronCluster.java:677-681`.
    if (cluster_session_id != cluster_session_id_)
    {
        return std::unexpected(
            make_error(ErrorCode::protocol_error, "invalid clusterSessionId=" + std::to_string(cluster_session_id) +
                                                      " expected=" + std::to_string(cluster_session_id_)));
    }

    // `AeronCluster.java:683-687`.
    state(State::await_new_leader_connection, now_ns() + config_.message_timeout_ns);
    leadership_term_id_ = leadership_term_id;
    leader_member_id_ = leader_member_id;
    encode_session_header(header_, leadership_term_id_, cluster_session_id_);

    ingress_publication_.close();
    // The C close is a conductor command; let it run before the replacement
    // add, or an exclusive publication on the same stream can race it.
    (void)client_->do_work();

    if (!config_.ingress_endpoints.has_value())
    {
        auto publication =
            add_new_leader_ingress_publication(ingress_channel_, config_.ingress_stream_id, leader_member_id_);
        if (!publication)
        {
            return std::unexpected(publication.error());
        }
        ingress_publication_ = std::move(*publication);
    }
    else
    {
        config_.ingress_endpoints = std::string{ingress_endpoints};
        auto updated = update_member_endpoints(ingress_endpoints, leader_member_id_);
        if (!updated)
        {
            return std::unexpected(updated.error());
        }
    }

    // `fragmentAssembler.clear()`: the C assembler has no clear, so the only
    // equivalent is a fresh one.
    if (nullptr != assembler_)
    {
        (void)aeron_fragment_assembler_delete(assembler_);
    }
    if (nullptr != controlled_assembler_)
    {
        (void)aeron_controlled_fragment_assembler_delete(controlled_assembler_);
    }
    (void)aeron_fragment_assembler_create(&assembler_, &ClusterClient::on_fragment_dispatch, this);
    (void)aeron_controlled_fragment_assembler_create(&controlled_assembler_, &ClusterClient::on_fragment, this);

    if (nullptr != egress_listener_)
    {
        egress_listener_->on_new_leader(cluster_session_id_, leadership_term_id_, leader_member_id_, ingress_endpoints);
    }
    if (nullptr != controlled_egress_listener_)
    {
        controlled_egress_listener_->on_new_leader(cluster_session_id_, leadership_term_id_, leader_member_id_,
                                                   ingress_endpoints);
    }

    return {};
}

std::expected<IngressPublication, Error> ClusterClient::add_new_leader_ingress_publication(
    std::string_view channel, std::int32_t stream_id, std::int32_t leader_member_id)
{
    // `AeronCluster.java:758-793`: a bounded loop that drives the client
    // itself; RESOURCE_TEMPORARILY_UNAVAILABLE is retried. The async add has to
    // outlive each iteration -- destroying it cancels the pending registration.
    const std::int64_t deadline_ns = now_ns() + config_.message_timeout_ns;

    std::optional<axc::core::AsyncAdd<axc::core::PubPtr>> concurrent_add{};
    std::optional<axc::core::AsyncAdd<axc::core::ExPubPtr>> exclusive_add{};

    if (config_.is_ingress_exclusive)
    {
        exclusive_add.emplace(client_->add_exclusive_publication(channel, stream_id));
    }
    else
    {
        concurrent_add.emplace(client_->add_publication(channel, stream_id));
    }

    do
    {
        if (exclusive_add.has_value())
        {
            auto polled = exclusive_add->poll();
            if (!polled)
            {
                if (is_temporarily_unavailable(polled.error()))
                {
                    exclusive_add.emplace(client_->add_exclusive_publication(channel, stream_id));
                }
                else
                {
                    return std::unexpected(polled.error());
                }
            }
            else if (polled->has_value())
            {
                return IngressPublication::from_exclusive(std::move(**polled));
            }
        }
        else
        {
            auto polled = concurrent_add->poll();
            if (!polled)
            {
                if (is_temporarily_unavailable(polled.error()))
                {
                    concurrent_add.emplace(client_->add_publication(channel, stream_id));
                }
                else
                {
                    return std::unexpected(polled.error());
                }
            }
            else if (polled->has_value())
            {
                return IngressPublication::from_concurrent(std::move(**polled));
            }
        }

        (void)client_->do_work();
    } while (now_ns() < deadline_ns);

    return std::unexpected(
        make_error(ErrorCode::timed_out,
                   "failed to add new leader ingress publication (leaderMemberId=" + std::to_string(leader_member_id) +
                       ", leadershipTermId=" + std::to_string(leadership_term_id_) +
                       ", channel=" + std::string{channel} + ", streamId=" + std::to_string(stream_id) + ")"));
}

std::expected<void, Error> ClusterClient::update_member_endpoints(std::string_view ingress_endpoints,
                                                                  std::int32_t leader_member_id)
{
    // `AeronCluster.java:819-834`.
    for (MemberIngress& member : members_)
    {
        member.publication.close();
    }
    members_.clear();

    auto parsed = parse_endpoints(ingress_endpoints);
    if (!parsed)
    {
        return std::unexpected(parsed.error());
    }

    std::string channel{ingress_channel_};
    const auto leader = std::find_if(parsed->begin(), parsed->end(), [leader_member_id](const ParsedMember& m)
                                     { return m.member_id == leader_member_id; });
    if (leader == parsed->end())
    {
        return std::unexpected(make_error(
            ErrorCode::config_error, "ingress endpoints have no leader member " + std::to_string(leader_member_id)));
    }

    if ("udp" == detail::channel_media(channel))
    {
        channel = detail::with_parameter(channel, "endpoint", leader->endpoint);
    }

    auto publication = add_new_leader_ingress_publication(channel, config_.ingress_stream_id, leader_member_id);
    if (!publication)
    {
        return std::unexpected(publication.error());
    }
    ingress_publication_ = std::move(*publication);

    members_.clear();
    for (ParsedMember& member : *parsed)
    {
        MemberIngress entry;
        entry.member_id = member.member_id;
        entry.endpoint = std::move(member.endpoint);
        if (entry.member_id == leader_member_id)
        {
            // The leader's publication is the session's ingress publication;
            // the member table only keeps the endpoint for a later redirect.
            continue;
        }
        members_.push_back(std::move(entry));
    }

    return {};
}

void ClusterClient::close()
{
    // `AeronCluster.java:290-311`.
    if (State::closed == state_)
    {
        return;
    }

    if (State::connected == state_ && ingress_publication_.valid() && ingress_publication_.is_connected())
    {
        close_session();
    }

    ingress_publication_.close();
    release_egress_image();
    egress_subscription_.reset();
    for (MemberIngress& member : members_)
    {
        member.publication.close();
    }
    members_.clear();
    state(State::closed, 0);
}

aeron_controlled_fragment_handler_action_t ClusterClient::on_fragment(void* clientd, const std::uint8_t* buffer,
                                                                      std::size_t length, aeron_header_t* header)
{
    auto* client = static_cast<ClusterClient*>(clientd);
    return client->dispatch(std::span<const std::uint8_t>(buffer, length),
                            static_cast<aeron_image_t*>(aeron_header_context(header)), true);
}

void ClusterClient::on_fragment_dispatch(void* clientd, const std::uint8_t* buffer, std::size_t length,
                                         aeron_header_t* header)
{
    auto* client = static_cast<ClusterClient*>(clientd);
    (void)client->dispatch(std::span<const std::uint8_t>(buffer, length),
                           static_cast<aeron_image_t*>(aeron_header_context(header)), false);
}

aeron_controlled_fragment_handler_action_t ClusterClient::dispatch(std::span<const std::uint8_t> message,
                                                                   aeron_image_t* image, bool controlled)
{
    auto header = common::message_header(message);
    if (!header)
    {
        // `AeronCluster.java:850-868`: a foreign schema is an error (the
        // extension hooks are not part of C3b).
        pending_error_ = header.error();
        return AERON_ACTION_ABORT;
    }

    const std::int32_t template_id = header->templateId();
    switch (template_id)
    {
        case codecs::SessionMessageHeader::sbeTemplateId():
        {
            auto decoder = common::wrap_body<codecs::SessionMessageHeader>(message, *header);
            const std::int64_t session_id = decoder.clusterSessionId();
            if (session_id != cluster_session_id_)
            {
                return AERON_ACTION_CONTINUE;
            }

            const std::span<const std::uint8_t> payload = message.subspan(SESSION_HEADER_LENGTH);
            const std::int64_t timestamp = decoder.timestamp();

            if (controlled)
            {
                if (nullptr == controlled_egress_listener_)
                {
                    pending_error_ = make_error(ErrorCode::config_error,
                                                "controlledEgressListener must be specified on the cluster client");
                    return AERON_ACTION_ABORT;
                }
                return controlled_egress_listener_->on_message(session_id, timestamp, payload);
            }

            if (nullptr == egress_listener_)
            {
                pending_error_ =
                    make_error(ErrorCode::config_error, "egressListener must be specified on the cluster client");
                return AERON_ACTION_ABORT;
            }
            egress_listener_->on_message(session_id, timestamp, payload);
            return AERON_ACTION_CONTINUE;
        }

        case codecs::SessionEvent::sbeTemplateId():
        {
            auto decoder = common::wrap_body<codecs::SessionEvent>(message, *header);
            const std::int64_t session_id = decoder.clusterSessionId();
            if (session_id != cluster_session_id_)
            {
                return AERON_ACTION_CONTINUE;
            }

            const codecs::EventCode::Value code = decoder.code();
            const std::string detail = decoder.getDetailAsString();
            if (codecs::EventCode::CLOSED == code)
            {
                state(State::pending_close, 0);
            }

            if (controlled)
            {
                if (nullptr != controlled_egress_listener_)
                {
                    controlled_egress_listener_->on_session_event(decoder.correlationId(), session_id,
                                                                  decoder.leadershipTermId(), decoder.leaderMemberId(),
                                                                  code, detail);
                }
            }
            else if (nullptr != egress_listener_)
            {
                egress_listener_->on_session_event(decoder.correlationId(), session_id, decoder.leadershipTermId(),
                                                   decoder.leaderMemberId(), code, detail);
            }
            return AERON_ACTION_CONTINUE;
        }

        case codecs::NewLeaderEvent::sbeTemplateId():
        {
            auto decoder = common::wrap_body<codecs::NewLeaderEvent>(message, *header);
            const std::int64_t session_id = decoder.clusterSessionId();
            if (session_id != cluster_session_id_)
            {
                return AERON_ACTION_CONTINUE;
            }

            set_egress_image(image);
            const std::string endpoints = decoder.getIngressEndpointsAsString();
            auto promoted = on_new_leader(session_id, decoder.leadershipTermId(), decoder.leaderMemberId(), endpoints);
            if (!promoted)
            {
                pending_error_ = promoted.error();
            }
            return controlled ? AERON_ACTION_COMMIT : AERON_ACTION_CONTINUE;
        }

        case codecs::AdminResponse::sbeTemplateId():
        {
            auto decoder = common::wrap_body<codecs::AdminResponse>(message, *header);
            const std::int64_t session_id = decoder.clusterSessionId();
            if (session_id != cluster_session_id_)
            {
                return AERON_ACTION_CONTINUE;
            }

            // `AeronCluster.java:958-963`: the payload follows the message's
            // var-data, i.e. the decoder's position after the message string.
            const std::uint32_t message_length = decoder.messageLength();
            const std::uint32_t payload_length = decoder.payloadLength();
            const std::size_t offset = common::MESSAGE_HEADER_LENGTH + header->blockLength() + 4 + message_length + 4;
            const std::span<const std::uint8_t> payload =
                offset <= message.size()
                    ? message.subspan(offset, std::min<std::size_t>(payload_length, message.size() - offset))
                    : std::span<const std::uint8_t>{};

            const std::string detail = decoder.getMessageAsString();
            if (controlled)
            {
                if (nullptr != controlled_egress_listener_)
                {
                    controlled_egress_listener_->on_admin_response(session_id, decoder.correlationId(),
                                                                   decoder.requestType(), decoder.responseCode(),
                                                                   detail, payload);
                }
            }
            else if (nullptr != egress_listener_)
            {
                egress_listener_->on_admin_response(session_id, decoder.correlationId(), decoder.requestType(),
                                                    decoder.responseCode(), detail, payload);
            }
            return AERON_ACTION_CONTINUE;
        }

        default:
            return AERON_ACTION_CONTINUE;
    }
}

EgressAdapter::EgressAdapter(EgressListener& listener, std::int64_t cluster_session_id, std::size_t fragment_limit)
    : EgressAdapter(listener, nullptr, cluster_session_id, fragment_limit)
{
}

EgressAdapter::EgressAdapter(EgressListener& listener, EgressListenerExtension* listener_extension,
                             std::int64_t cluster_session_id, std::size_t fragment_limit)
    : listener_(listener),
      listener_extension_(listener_extension),
      cluster_session_id_(cluster_session_id),
      fragment_limit_(fragment_limit)
{
    if (0 != aeron_fragment_assembler_create(&assembler_, &EgressAdapter::on_fragment, this))
    {
        assembler_ = nullptr;
    }
}

EgressAdapter::~EgressAdapter()
{
    if (nullptr != assembler_)
    {
        (void)aeron_fragment_assembler_delete(assembler_);
    }
}

std::expected<int, Error> EgressAdapter::poll(aeron_subscription_t* subscription)
{
    const int fragments = aeron_subscription_poll(subscription, &EgressAdapter::on_fragment, this, fragment_limit_);
    if (fragments < 0)
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "egress poll failed", aeron_errcode()));
    }
    if (pending_error_.has_value())
    {
        Error error = *pending_error_;
        pending_error_.reset();
        return std::unexpected(error);
    }
    return fragments;
}

void EgressAdapter::on_fragment(void* clientd, const std::uint8_t* buffer, std::size_t length, aeron_header_t*)
{
    static_cast<EgressAdapter*>(clientd)->dispatch(std::span<const std::uint8_t>(buffer, length));
}

std::optional<Error> EgressAdapter::take_error() noexcept
{
    std::optional<Error> error = pending_error_;
    pending_error_.reset();
    return error;
}

void EgressAdapter::dispatch(std::span<const std::uint8_t> message)
{
    // `EgressAdapter.java:99-113`: a foreign schema goes to the extension
    // listener, and without one it is an error (the reference throws
    // `ClusterException`; our `poll()` carries the same text as an error).
    auto header = common::raw_message_header(message);
    if (!header)
    {
        pending_error_ = header.error();
        return;
    }

    if (header->schemaId() != common::SBE_SCHEMA_ID)
    {
        if (nullptr != listener_extension_)
        {
            listener_extension_->on_extension_message(header->blockLength(), header->templateId(), header->schemaId(),
                                                      header->version(),
                                                      message.subspan(common::MESSAGE_HEADER_LENGTH));
            return;
        }
        pending_error_ =
            make_error(ErrorCode::protocol_error, "expected schemaId=" + std::to_string(common::SBE_SCHEMA_ID) +
                                                      ", actual=" + std::to_string(header->schemaId()));
        return;
    }

    switch (header->templateId())
    {
        case codecs::SessionMessageHeader::sbeTemplateId():
        {
            auto decoder = common::wrap_body<codecs::SessionMessageHeader>(message, *header);
            if (decoder.clusterSessionId() == cluster_session_id_)
            {
                listener_.on_message(decoder.clusterSessionId(), decoder.timestamp(),
                                     message.subspan(SESSION_HEADER_LENGTH));
            }
            break;
        }

        case codecs::SessionEvent::sbeTemplateId():
        {
            auto decoder = common::wrap_body<codecs::SessionEvent>(message, *header);
            if (decoder.clusterSessionId() == cluster_session_id_)
            {
                listener_.on_session_event(decoder.correlationId(), decoder.clusterSessionId(),
                                           decoder.leadershipTermId(), decoder.leaderMemberId(), decoder.code(),
                                           decoder.getDetailAsString());
            }
            break;
        }

        case codecs::NewLeaderEvent::sbeTemplateId():
        {
            auto decoder = common::wrap_body<codecs::NewLeaderEvent>(message, *header);
            if (decoder.clusterSessionId() == cluster_session_id_)
            {
                listener_.on_new_leader(decoder.clusterSessionId(), decoder.leadershipTermId(),
                                        decoder.leaderMemberId(), decoder.getIngressEndpointsAsString());
            }
            break;
        }

        case codecs::AdminResponse::sbeTemplateId():
        {
            auto decoder = common::wrap_body<codecs::AdminResponse>(message, *header);
            if (decoder.clusterSessionId() == cluster_session_id_)
            {
                const std::string detail = decoder.getMessageAsString();
                const std::uint32_t payload_length = decoder.payloadLength();
                const std::size_t offset =
                    common::MESSAGE_HEADER_LENGTH + header->blockLength() + 4 + detail.size() + 4;
                const std::span<const std::uint8_t> payload =
                    offset <= message.size()
                        ? message.subspan(offset, std::min<std::size_t>(payload_length, message.size() - offset))
                        : std::span<const std::uint8_t>{};
                listener_.on_admin_response(decoder.clusterSessionId(), decoder.correlationId(), decoder.requestType(),
                                            decoder.responseCode(), detail, payload);
            }
            break;
        }

        default:
            break;
    }
}

ControlledEgressAdapter::ControlledEgressAdapter(ControlledEgressListener& listener, std::int64_t cluster_session_id,
                                                 std::size_t fragment_limit)
    : ControlledEgressAdapter(listener, nullptr, cluster_session_id, fragment_limit)
{
}

ControlledEgressAdapter::ControlledEgressAdapter(ControlledEgressListener& listener,
                                                 ControlledEgressListenerExtension* listener_extension,
                                                 std::int64_t cluster_session_id, std::size_t fragment_limit)
    : listener_(listener),
      listener_extension_(listener_extension),
      cluster_session_id_(cluster_session_id),
      fragment_limit_(fragment_limit)
{
    if (0 != aeron_controlled_fragment_assembler_create(&assembler_, &ControlledEgressAdapter::on_fragment, this))
    {
        assembler_ = nullptr;
    }
}

ControlledEgressAdapter::~ControlledEgressAdapter()
{
    if (nullptr != assembler_)
    {
        (void)aeron_controlled_fragment_assembler_delete(assembler_);
    }
}

std::expected<int, Error> ControlledEgressAdapter::poll(aeron_subscription_t* subscription)
{
    const int fragments =
        aeron_subscription_controlled_poll(subscription, &ControlledEgressAdapter::on_fragment, this, fragment_limit_);
    if (fragments < 0)
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "egress poll failed", aeron_errcode()));
    }
    if (pending_error_.has_value())
    {
        Error error = *pending_error_;
        pending_error_.reset();
        return std::unexpected(error);
    }
    return fragments;
}

aeron_controlled_fragment_handler_action_t ControlledEgressAdapter::on_fragment(void* clientd,
                                                                                const std::uint8_t* buffer,
                                                                                std::size_t length, aeron_header_t*)
{
    return static_cast<ControlledEgressAdapter*>(clientd)->dispatch(std::span<const std::uint8_t>(buffer, length));
}

std::optional<Error> ControlledEgressAdapter::take_error() noexcept
{
    std::optional<Error> error = pending_error_;
    pending_error_.reset();
    return error;
}

aeron_controlled_fragment_handler_action_t ControlledEgressAdapter::dispatch(std::span<const std::uint8_t> message)
{
    // `ControlledEgressAdapter.java:89-113`: same schema rule as the
    // unsolicited adapter, with the listener's action as the return.
    auto header = common::raw_message_header(message);
    if (!header)
    {
        pending_error_ = header.error();
        return AERON_ACTION_ABORT;
    }

    if (header->schemaId() != common::SBE_SCHEMA_ID)
    {
        if (nullptr != listener_extension_)
        {
            return listener_extension_->on_extension_message(header->blockLength(), header->templateId(),
                                                             header->schemaId(), header->version(),
                                                             message.subspan(common::MESSAGE_HEADER_LENGTH));
        }
        pending_error_ =
            make_error(ErrorCode::protocol_error, "expected schemaId=" + std::to_string(common::SBE_SCHEMA_ID) +
                                                      ", actual=" + std::to_string(header->schemaId()));
        return AERON_ACTION_ABORT;
    }

    switch (header->templateId())
    {
        case codecs::SessionMessageHeader::sbeTemplateId():
        {
            auto decoder = common::wrap_body<codecs::SessionMessageHeader>(message, *header);
            if (decoder.clusterSessionId() == cluster_session_id_)
            {
                return listener_.on_message(decoder.clusterSessionId(), decoder.timestamp(),
                                            message.subspan(SESSION_HEADER_LENGTH));
            }
            break;
        }

        case codecs::SessionEvent::sbeTemplateId():
        {
            auto decoder = common::wrap_body<codecs::SessionEvent>(message, *header);
            if (decoder.clusterSessionId() == cluster_session_id_)
            {
                listener_.on_session_event(decoder.correlationId(), decoder.clusterSessionId(),
                                           decoder.leadershipTermId(), decoder.leaderMemberId(), decoder.code(),
                                           decoder.getDetailAsString());
            }
            break;
        }

        case codecs::NewLeaderEvent::sbeTemplateId():
        {
            auto decoder = common::wrap_body<codecs::NewLeaderEvent>(message, *header);
            if (decoder.clusterSessionId() == cluster_session_id_)
            {
                listener_.on_new_leader(decoder.clusterSessionId(), decoder.leadershipTermId(),
                                        decoder.leaderMemberId(), decoder.getIngressEndpointsAsString());
                return AERON_ACTION_COMMIT;
            }
            break;
        }

        case codecs::AdminResponse::sbeTemplateId():
        {
            auto decoder = common::wrap_body<codecs::AdminResponse>(message, *header);
            if (decoder.clusterSessionId() == cluster_session_id_)
            {
                const std::string detail = decoder.getMessageAsString();
                const std::uint32_t payload_length = decoder.payloadLength();
                const std::size_t offset =
                    common::MESSAGE_HEADER_LENGTH + header->blockLength() + 4 + detail.size() + 4;
                const std::span<const std::uint8_t> payload =
                    offset <= message.size()
                        ? message.subspan(offset, std::min<std::size_t>(payload_length, message.size() - offset))
                        : std::span<const std::uint8_t>{};
                listener_.on_admin_response(decoder.clusterSessionId(), decoder.correlationId(), decoder.requestType(),
                                            decoder.responseCode(), detail, payload);
            }
            break;
        }

        default:
            break;
    }

    return AERON_ACTION_CONTINUE;
}

}  // namespace deepmsg::cluster::client
