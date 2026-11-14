#include "cluster/consensus/egress_publisher.h"

#include <cstdio>

#include "io_aeron_cluster_codecs/AdminRequestType.h"
#include "io_aeron_cluster_codecs/AdminResponse.h"
#include "io_aeron_cluster_codecs/AdminResponseCode.h"
#include "io_aeron_cluster_codecs/Challenge.h"
#include "io_aeron_cluster_codecs/EventCode.h"
#include "io_aeron_cluster_codecs/MessageHeader.h"
#include "io_aeron_cluster_codecs/NewLeaderEvent.h"
#include "io_aeron_cluster_codecs/SessionEvent.h"

namespace deepmsg::cluster::consensus
{
namespace
{

constexpr int SEND_ATTEMPTS = 3;

}  // namespace

EgressPublisher::EgressPublisher(std::int64_t leader_heartbeat_timeout_ns)
    : leader_heartbeat_timeout_ns_(leader_heartbeat_timeout_ns)
{
}

bool EgressPublisher::send_event(ClusterSession& session, std::int64_t leadership_term_id,
                                 std::int32_t leader_member_id, codecs::EventCode::Value code, std::string_view detail)
{
    if (!session.has_response_publication())
    {
        return false;
    }

    const std::size_t length =
        codecs::MessageHeader::encodedLength() + codecs::SessionEvent::computeLength(detail.size());

    const auto encode = [&](char* buffer, std::size_t buffer_length)
    {
        codecs::SessionEvent encoder;
        encoder.wrapAndApplyHeader(buffer, 0, buffer_length);
        encoder.clusterSessionId(session.id())
            .correlationId(session.correlation_id())
            .leadershipTermId(leadership_term_id)
            .leaderMemberId(leader_member_id)
            .code(code)
            .version(CLUSTER_PROTOCOL_SEMANTIC_VERSION)
            .leaderHeartbeatTimeoutNs(leader_heartbeat_timeout_ns_);
        encoder.putDetail(detail);
    };

    if (static_cast<std::int64_t>(length) <= session.max_payload_length())
    {
        int attempts = SEND_ATTEMPTS;
        do
        {
            aeron_buffer_claim_t claim{};
            const long position = session.try_claim(length, claim);
            if (position > 0)
            {
                encode(reinterpret_cast<char*>(claim.data), claim.length);
                aeron_buffer_claim_commit(&claim);
                return true;
            }
        } while (--attempts > 0);
        return false;
    }

    buffer_.resize(length);
    encode(reinterpret_cast<char*>(buffer_.data()), buffer_.size());
    return offer_message(session, length);
}

bool EgressPublisher::send_challenge(ClusterSession& session, std::span<const std::uint8_t> encoded_challenge)
{
    if (!session.has_response_publication())
    {
        return false;
    }

    // Reference shape (`EgressPublisher.java:105-128`): the length calculation
    // uses `ChallengeResponseEncoder` while the body is `ChallengeEncoder`.
    const std::size_t length =
        codecs::MessageHeader::encodedLength() + codecs::Challenge::computeLength(encoded_challenge.size());

    const auto encode = [&](char* buffer, std::size_t buffer_length)
    {
        codecs::Challenge encoder;
        encoder.wrapAndApplyHeader(buffer, 0, buffer_length);
        encoder.clusterSessionId(session.id()).correlationId(session.correlation_id());
        encoder.putEncodedChallenge(reinterpret_cast<const char*>(encoded_challenge.data()),
                                    static_cast<std::uint32_t>(encoded_challenge.size()));
    };

    if (static_cast<std::int64_t>(length) <= session.max_payload_length())
    {
        int attempts = SEND_ATTEMPTS;
        do
        {
            aeron_buffer_claim_t claim{};
            const long position = session.try_claim(length, claim);
            if (position > 0)
            {
                encode(reinterpret_cast<char*>(claim.data), claim.length);
                aeron_buffer_claim_commit(&claim);
                return true;
            }
        } while (--attempts > 0);
        return false;
    }

    buffer_.resize(length);
    encode(reinterpret_cast<char*>(buffer_.data()), buffer_.size());
    return offer_message(session, length);
}

bool EgressPublisher::new_leader(ClusterSession& session, std::int64_t leadership_term_id,
                                 std::int32_t leader_member_id, std::string_view ingress_endpoints)
{
    if (!session.has_response_publication())
    {
        return false;
    }

    const std::size_t length =
        codecs::MessageHeader::encodedLength() + codecs::NewLeaderEvent::computeLength(ingress_endpoints.size());

    const auto encode = [&](char* buffer, std::size_t buffer_length)
    {
        codecs::NewLeaderEvent encoder;
        encoder.wrapAndApplyHeader(buffer, 0, buffer_length);
        encoder.clusterSessionId(session.id()).leadershipTermId(leadership_term_id).leaderMemberId(leader_member_id);
        encoder.putIngressEndpoints(ingress_endpoints);
    };

    if (static_cast<std::int64_t>(length) <= session.max_payload_length())
    {
        int attempts = SEND_ATTEMPTS;
        do
        {
            aeron_buffer_claim_t claim{};
            const long position = session.try_claim(length, claim);
            if (position > 0)
            {
                encode(reinterpret_cast<char*>(claim.data), claim.length);
                aeron_buffer_claim_commit(&claim);
                return true;
            }
        } while (--attempts > 0);
        return false;
    }

    buffer_.resize(length);
    encode(reinterpret_cast<char*>(buffer_.data()), buffer_.size());
    return offer_message(session, length);
}

bool EgressPublisher::send_admin_response(ClusterSession& session, std::int64_t correlation_id,
                                          codecs::AdminRequestType::Value request_type,
                                          codecs::AdminResponseCode::Value response_code, std::string_view message)
{
    if (!session.has_response_publication())
    {
        return false;
    }

    const std::size_t length =
        codecs::MessageHeader::encodedLength() + codecs::AdminResponse::computeLength(message.size(), 0);

    const auto encode = [&](char* buffer, std::size_t buffer_length)
    {
        codecs::AdminResponse encoder;
        encoder.wrapAndApplyHeader(buffer, 0, buffer_length);
        encoder.clusterSessionId(session.id())
            .correlationId(correlation_id)
            .requestType(request_type)
            .responseCode(response_code);
        encoder.putMessage(message);
        encoder.putPayload("", 0);
    };

    if (static_cast<std::int64_t>(length) <= session.max_payload_length())
    {
        int attempts = SEND_ATTEMPTS;
        do
        {
            aeron_buffer_claim_t claim{};
            const long position = session.try_claim(length, claim);
            if (position > 0)
            {
                encode(reinterpret_cast<char*>(claim.data), claim.length);
                aeron_buffer_claim_commit(&claim);
                return true;
            }
        } while (--attempts > 0);
        return false;
    }

    buffer_.resize(length);
    encode(reinterpret_cast<char*>(buffer_.data()), buffer_.size());
    return offer_message(session, length);
}

bool EgressPublisher::offer_message(ClusterSession& session, std::size_t length)
{
    int attempts = SEND_ATTEMPTS;
    do
    {
        if (session.offer(buffer_.data(), length) > 0)
        {
            return true;
        }
    } while (--attempts > 0);
    return false;
}

}  // namespace deepmsg::cluster::consensus
