#include "cluster/consensus/service_proxy.h"

#include <sched.h>

#include <string>
#include <tuple>
#include <utility>

#include "cluster/common/cluster_codecs.h"
#include "io_aeron_cluster_codecs/BooleanType.h"
#include "io_aeron_cluster_codecs/ClusterMembersExtendedResponse.h"
#include "io_aeron_cluster_codecs/ClusterMembersResponse.h"
#include "io_aeron_cluster_codecs/JoinLog.h"
#include "io_aeron_cluster_codecs/RequestServiceAck.h"
#include "io_aeron_cluster_codecs/ServiceTerminationPosition.h"

namespace deepmsg::cluster::consensus
{
namespace
{

using axc::core::ErrorCode;
using axc::core::make_error;
namespace codecs = deepmsg::cluster::common::codecs;
using deepmsg::cluster::common::MESSAGE_HEADER_LENGTH;

/// Every variable-length field in schema 111 is a 4-byte length and the bytes.
constexpr std::size_t VAR_DATA_HEADER_LENGTH = 4;

[[nodiscard]] codecs::BooleanType::Value boolean(bool value) noexcept
{
    return value ? codecs::BooleanType::TRUE : codecs::BooleanType::FALSE;
}

}  // namespace

std::int64_t PublicationSend::try_claim(std::size_t length, std::span<std::uint8_t>& window)
{
    window = {};
    if (nullptr == publication_)
    {
        return AERON_PUBLICATION_NOT_CONNECTED;
    }

    const std::int64_t position = static_cast<std::int64_t>(aeron_publication_try_claim(publication_, length, &claim_));
    if (position > 0)
    {
        claimed_ = true;
        window = std::span<std::uint8_t>{claim_.data, claim_.length};
    }
    return position;
}

void PublicationSend::commit() noexcept
{
    if (claimed_)
    {
        (void)aeron_buffer_claim_commit(&claim_);
        claimed_ = false;
    }
}

std::int64_t PublicationSend::offer(std::span<const std::uint8_t> message)
{
    if (nullptr == publication_)
    {
        return AERON_PUBLICATION_NOT_CONNECTED;
    }
    return static_cast<std::int64_t>(
        aeron_publication_offer(publication_, message.data(), message.size(), nullptr, nullptr));
}

bool PublicationSend::is_closed() const noexcept
{
    if (nullptr == publication_)
    {
        return true;
    }
    return aeron_publication_is_closed(publication_);
}

std::int32_t PublicationSend::term_buffer_length() const noexcept
{
    if (nullptr == publication_)
    {
        return 0;
    }
    aeron_publication_constants_t constants{};
    if (0 != aeron_publication_constants(publication_, &constants))
    {
        return 0;
    }
    return static_cast<std::int32_t>(constants.term_buffer_length);
}

std::string_view ServiceProxy::publication_error_string(std::int64_t result) noexcept
{
    switch (result)
    {
        case AERON_PUBLICATION_NOT_CONNECTED:
            return "not connected";
        case AERON_PUBLICATION_BACK_PRESSURED:
            return "back pressured";
        case AERON_PUBLICATION_ADMIN_ACTION:
            return "admin action";
        case AERON_PUBLICATION_CLOSED:
            return "closed";
        case AERON_PUBLICATION_MAX_POSITION_EXCEEDED:
            return "max position exceeded";
        case AERON_PUBLICATION_ERROR:
            return "error";
        default:
            return "unknown";
    }
}

std::expected<void, Error> ServiceProxy::check_result(std::int64_t position, const ServiceSend& send)
{
    if (AERON_PUBLICATION_NOT_CONNECTED == position)
    {
        return std::unexpected(make_error(ErrorCode::protocol_error, "publication is not connected"));
    }

    if (AERON_PUBLICATION_CLOSED == position)
    {
        return std::unexpected(make_error(ErrorCode::protocol_error, "publication is closed"));
    }

    if (AERON_PUBLICATION_MAX_POSITION_EXCEEDED == position)
    {
        return std::unexpected(make_error(ErrorCode::protocol_error, "publication at max position: term-length=" +
                                                                         std::to_string(send.term_buffer_length())));
    }

    return {};
}

ServiceProxy::ServiceProxy(ServiceSend& send, ErrorHandler& error_handler) : send_(send), error_handler_(error_handler)
{
}

std::expected<void, Error> ServiceProxy::join_log(std::int64_t log_position, std::int64_t max_log_position,
                                                  std::int32_t member_id, std::int32_t log_session_id,
                                                  std::int32_t log_stream_id, bool is_startup, ClusterRole role,
                                                  std::string_view channel, bool is_standby)
{
    const std::size_t length =
        MESSAGE_HEADER_LENGTH + codecs::JoinLog::sbeBlockLength() + VAR_DATA_HEADER_LENGTH + channel.size();

    std::int64_t position = 0;
    int attempts = SEND_ATTEMPTS;
    do
    {
        std::span<std::uint8_t> window{};
        position = send_.try_claim(length, window);
        if (position > 0)
        {
            codecs::JoinLog encoder;
            encoder.wrapAndApplyHeader(reinterpret_cast<char*>(window.data()), 0, window.size());
            encoder.logPosition(log_position)
                .maxLogPosition(max_log_position)
                .memberId(member_id)
                .logSessionId(log_session_id)
                .logStreamId(log_stream_id)
                .isStartup(boolean(is_startup))
                .role(role_code(role))
                .isStandby(boolean(is_standby));
            encoder.putLogChannel(channel);
            send_.commit();
            return {};
        }

        auto checked = check_result(position, send_);
        if (!checked)
        {
            return checked;
        }
        if (AERON_PUBLICATION_BACK_PRESSURED == position)
        {
            ::sched_yield();
        }
    } while (--attempts > 0);

    return std::unexpected(make_error(ErrorCode::protocol_error, "failed to send join log request: " +
                                                                     std::string{publication_error_string(position)}));
}

void ServiceProxy::cluster_members_response(std::int64_t correlation_id, std::int32_t leader_member_id,
                                            std::string_view active_members)
{
    const std::string_view passive_followers{};
    const std::size_t length = MESSAGE_HEADER_LENGTH + codecs::ClusterMembersResponse::sbeBlockLength() +
                               VAR_DATA_HEADER_LENGTH + active_members.size() + VAR_DATA_HEADER_LENGTH +
                               passive_followers.size();

    std::int64_t result = 0;
    int attempts = SEND_ATTEMPTS;
    do
    {
        std::span<std::uint8_t> window{};
        result = send_.try_claim(length, window);
        if (result > 0)
        {
            codecs::ClusterMembersResponse encoder;
            encoder.wrapAndApplyHeader(reinterpret_cast<char*>(window.data()), 0, window.size());
            encoder.correlationId(correlation_id)
                .leaderMemberId(leader_member_id)
                .putActiveMembers(active_members)
                .putPassiveFollowers(passive_followers);
            send_.commit();
            return;
        }

        if (AERON_PUBLICATION_BACK_PRESSURED == result)
        {
            ::sched_yield();
        }
    } while (--attempts > 0);

    error_handler_.on_error(make_error(ErrorCode::protocol_error, "failed to send cluster members response: result=" +
                                                                      std::string{publication_error_string(result)}));
}

void ServiceProxy::cluster_members_extended_response(std::int64_t correlation_id, std::int64_t current_time_ns,
                                                     std::int32_t leader_member_id, std::int32_t member_id,
                                                     std::span<const common::ClusterMember> active_members)
{
    std::vector<std::tuple<std::size_t, std::size_t, std::size_t, std::size_t, std::size_t>> item_lengths;
    item_lengths.reserve(active_members.size());
    for (const common::ClusterMember& member : active_members)
    {
        item_lengths.emplace_back(member.ingress_endpoint.size(), member.consensus_endpoint.size(),
                                  member.log_endpoint.size(), member.catchup_endpoint.size(),
                                  member.archive_endpoint.size());
    }

    const std::size_t length =
        MESSAGE_HEADER_LENGTH + codecs::ClusterMembersExtendedResponse::computeLength(item_lengths);
    buffer_.resize(length);

    codecs::ClusterMembersExtendedResponse encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer_.data()), 0, buffer_.size());
    encoder.correlationId(correlation_id)
        .currentTimeNs(current_time_ns)
        .leaderMemberId(leader_member_id)
        .memberId(member_id);

    auto active = encoder.activeMembersCount(static_cast<std::uint16_t>(active_members.size()));
    for (const common::ClusterMember& member : active_members)
    {
        active.next()
            .leadershipTermId(member.leadership_term_id)
            .logPosition(member.log_position)
            .timeOfLastAppendNs(member.time_of_last_append_position_ns)
            .memberId(member.id)
            .putIngressEndpoint(member.ingress_endpoint)
            .putConsensusEndpoint(member.consensus_endpoint)
            .putLogEndpoint(member.log_endpoint)
            .putCatchupEndpoint(member.catchup_endpoint)
            .putArchiveEndpoint(member.archive_endpoint);
    }

    encoder.passiveMembersCount(0);

    std::int64_t result = 0;
    int attempts = SEND_ATTEMPTS;
    do
    {
        result = send_.offer(std::span<const std::uint8_t>{buffer_.data(), length});
        if (result > 0)
        {
            return;
        }

        if (AERON_PUBLICATION_BACK_PRESSURED == result)
        {
            ::sched_yield();
        }
    } while (--attempts > 0);

    error_handler_.on_error(make_error(
        ErrorCode::protocol_error,
        "failed to send cluster members extended response: result=" + std::string{publication_error_string(result)}));
}

void ServiceProxy::termination_position(std::int64_t log_position, bool ack_required)
{
    if (send_.is_closed())
    {
        return;
    }

    const std::size_t length = MESSAGE_HEADER_LENGTH + codecs::ServiceTerminationPosition::sbeBlockLength();
    std::int64_t result = 0;

    int attempts = SEND_ATTEMPTS;
    do
    {
        std::span<std::uint8_t> window{};
        result = send_.try_claim(length, window);
        if (result > 0)
        {
            codecs::ServiceTerminationPosition encoder;
            encoder.wrapAndApplyHeader(reinterpret_cast<char*>(window.data()), 0, window.size());
            encoder.logPosition(log_position).ackRequired(boolean(ack_required));
            send_.commit();
            return;
        }

        if (AERON_PUBLICATION_BACK_PRESSURED == result)
        {
            ::sched_yield();
        }
    } while (--attempts > 0);

    error_handler_.on_error(make_error(
        ErrorCode::protocol_error,
        "failed to send service termination position: result=" + std::string{publication_error_string(result)}));
}

std::expected<void, Error> ServiceProxy::request_service_ack(std::int64_t log_position)
{
    const std::size_t length = MESSAGE_HEADER_LENGTH + codecs::RequestServiceAck::sbeBlockLength();

    std::int64_t result = 0;
    int attempts = SEND_ATTEMPTS;
    do
    {
        std::span<std::uint8_t> window{};
        result = send_.try_claim(length, window);
        if (result > 0)
        {
            codecs::RequestServiceAck encoder;
            encoder.wrapAndApplyHeader(reinterpret_cast<char*>(window.data()), 0, window.size());
            encoder.logPosition(log_position);
            send_.commit();
            return {};
        }

        if (AERON_PUBLICATION_BACK_PRESSURED == result)
        {
            ::sched_yield();
        }
    } while (--attempts > 0);

    return std::unexpected(make_error(ErrorCode::protocol_error, "failed to send request for service ack: result=" +
                                                                     std::string{publication_error_string(result)}));
}

}  // namespace deepmsg::cluster::consensus
