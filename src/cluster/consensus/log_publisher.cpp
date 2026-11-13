#include "cluster/consensus/log_publisher.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <utility>

#include "io_aeron_cluster_codecs/ClusterActionRequest.h"
#include "io_aeron_cluster_codecs/MessageHeader.h"
#include "io_aeron_cluster_codecs/NewLeadershipTermEvent.h"
#include "io_aeron_cluster_codecs/SessionCloseEvent.h"
#include "io_aeron_cluster_codecs/SessionMessageHeader.h"
#include "io_aeron_cluster_codecs/SessionOpenEvent.h"
#include "io_aeron_cluster_codecs/TimerEvent.h"

namespace deepmsg::cluster::consensus
{
namespace
{

constexpr int SEND_ATTEMPTS = 3;

using axc::core::ExPubPtr;

}  // namespace

LogPublisher::LogPublisher(std::string destination_channel) : destination_channel_(std::move(destination_channel))
{
    codecs::SessionMessageHeader encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(session_header_.data()), 0, session_header_.size());
}

std::int64_t LogPublisher::position() const noexcept
{
    if (nullptr == publication_)
    {
        return 0;
    }
    return aeron_exclusive_publication_position(publication_.get());
}

std::int32_t LogPublisher::session_id() const noexcept
{
    if (nullptr == publication_)
    {
        return NULL_VALUE;
    }
    aeron_publication_constants_t constants{};
    if (0 != aeron_exclusive_publication_constants(publication_.get(), &constants))
    {
        return NULL_VALUE;
    }
    return constants.session_id;
}

bool LogPublisher::is_fatal(long position) noexcept
{
    return AERON_PUBLICATION_CLOSED == position || AERON_PUBLICATION_MAX_POSITION_EXCEEDED == position;
}

long LogPublisher::append_message(std::int64_t leadership_term_id, std::int64_t cluster_session_id,
                                  std::int64_t timestamp, const std::uint8_t* payload, std::size_t payload_length)
{
    if (nullptr == publication_)
    {
        return AERON_PUBLICATION_NOT_CONNECTED;
    }

    codecs::SessionMessageHeader encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(session_header_.data()), 0, session_header_.size());
    encoder.leadershipTermId(leadership_term_id).clusterSessionId(cluster_session_id).timestamp(timestamp);

    aeron_iovec_t iov[2] = {{session_header_.data(), SESSION_HEADER_LENGTH},
                            {const_cast<std::uint8_t*>(payload), payload_length}};

    long position = 0;
    int attempts = SEND_ATTEMPTS;
    do
    {
        position = static_cast<long>(aeron_exclusive_publication_offerv(publication_.get(), iov, 2, nullptr, nullptr));
        if (position > 0 || is_fatal(position))
        {
            break;
        }
    } while (--attempts > 0);

    return position;
}

long LogPublisher::append_session_open(const ClusterSession& session, std::int64_t leadership_term_id,
                                       std::int64_t timestamp)
{
    if (nullptr == publication_)
    {
        return AERON_PUBLICATION_NOT_CONNECTED;
    }

    const auto encoded_principal = session.encoded_principal();
    const std::string& response_channel = session.response_channel();
    const std::size_t length =
        codecs::MessageHeader::encodedLength() +
        codecs::SessionOpenEvent::computeLength(response_channel.size(), encoded_principal.size());
    encode_buffer_.resize(length);

    codecs::SessionOpenEvent encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(encode_buffer_.data()), 0, encode_buffer_.size());
    encoder.leadershipTermId(leadership_term_id)
        .clusterSessionId(session.id())
        .correlationId(session.correlation_id())
        .timestamp(timestamp)
        .responseStreamId(session.response_stream_id());
    encoder.putResponseChannel(response_channel);
    encoder.putEncodedPrincipal(reinterpret_cast<const char*>(encoded_principal.data()),
                                static_cast<std::uint32_t>(encoded_principal.size()));

    long position = 0;
    int attempts = SEND_ATTEMPTS;
    do
    {
        position = static_cast<long>(
            aeron_exclusive_publication_offer(publication_.get(), encode_buffer_.data(), length, nullptr, nullptr));
        if (position > 0 || is_fatal(position))
        {
            break;
        }
    } while (--attempts > 0);

    return position;
}

bool LogPublisher::append_session_close(ClusterSession& session, std::int64_t leadership_term_id,
                                        std::int64_t timestamp)
{
    if (nullptr == publication_)
    {
        return false;
    }

    const std::size_t length = codecs::MessageHeader::encodedLength() + codecs::SessionCloseEvent::sbeBlockLength();

    int attempts = SEND_ATTEMPTS;
    do
    {
        aeron_buffer_claim_t claim{};
        const long position =
            static_cast<long>(aeron_exclusive_publication_try_claim(publication_.get(), length, &claim));
        if (position > 0)
        {
            codecs::SessionCloseEvent encoder;
            encoder.wrapAndApplyHeader(reinterpret_cast<char*>(claim.data), 0, claim.length);
            encoder.leadershipTermId(leadership_term_id)
                .clusterSessionId(session.id())
                .timestamp(timestamp)
                .closeReason(session.close_reason());
            aeron_buffer_claim_commit(&claim);
            return true;
        }

        if (is_fatal(position))
        {
            return false;
        }
    } while (--attempts > 0);

    return false;
}

long LogPublisher::append_timer(std::int64_t correlation_id, std::int64_t leadership_term_id, std::int64_t timestamp)
{
    if (nullptr == publication_)
    {
        return AERON_PUBLICATION_NOT_CONNECTED;
    }

    const std::size_t length = codecs::MessageHeader::encodedLength() + codecs::TimerEvent::sbeBlockLength();

    long position = 0;
    int attempts = SEND_ATTEMPTS;
    do
    {
        aeron_buffer_claim_t claim{};
        position = static_cast<long>(aeron_exclusive_publication_try_claim(publication_.get(), length, &claim));
        if (position > 0)
        {
            codecs::TimerEvent encoder;
            encoder.wrapAndApplyHeader(reinterpret_cast<char*>(claim.data), 0, claim.length);
            encoder.leadershipTermId(leadership_term_id).correlationId(correlation_id).timestamp(timestamp);
            aeron_buffer_claim_commit(&claim);
            break;
        }

        if (is_fatal(position))
        {
            break;
        }
    } while (--attempts > 0);

    return position;
}

bool LogPublisher::append_new_leadership_term_event(std::int64_t leadership_term_id, std::int64_t timestamp,
                                                    std::int64_t term_base_log_position, std::int32_t leader_member_id,
                                                    std::int32_t log_session_id, std::int32_t time_unit_code,
                                                    std::int32_t app_version)
{
    if (nullptr == publication_)
    {
        return false;
    }

    const std::size_t length =
        codecs::MessageHeader::encodedLength() + codecs::NewLeadershipTermEvent::sbeBlockLength();

    for (int attempts = SEND_ATTEMPTS; attempts > 0; attempts--)
    {
        aeron_buffer_claim_t claim{};
        const long position =
            static_cast<long>(aeron_exclusive_publication_try_claim(publication_.get(), length, &claim));
        if (position > 0)
        {
            const long aligned_frame_length = (32 + static_cast<long>(length) + 31) & ~31L;
            codecs::NewLeadershipTermEvent encoder;
            encoder.wrapAndApplyHeader(reinterpret_cast<char*>(claim.data), 0, claim.length);
            encoder.leadershipTermId(leadership_term_id)
                .logPosition(position + aligned_frame_length)
                .timestamp(timestamp)
                .termBaseLogPosition(term_base_log_position)
                .leaderMemberId(leader_member_id)
                .logSessionId(log_session_id)
                .timeUnit(static_cast<codecs::ClusterTimeUnit::Value>(time_unit_code))
                .appVersion(app_version);
            last_append_position_ = position + aligned_frame_length;
            aeron_buffer_claim_commit(&claim);
            return true;
        }
        if (is_fatal(position))
        {
            return false;
        }
    }
    return false;
}

bool LogPublisher::append_cluster_action(std::int64_t leadership_term_id, std::int64_t timestamp, std::int32_t action,
                                         std::int32_t flags)
{
    if (nullptr == publication_)
    {
        return false;
    }

    const std::size_t length = codecs::MessageHeader::encodedLength() + codecs::ClusterActionRequest::sbeBlockLength();
    const long aligned_frame_length = (32 + static_cast<long>(length) + 31) & ~31L;
    const long log_position =
        static_cast<long>(aeron_exclusive_publication_position(publication_.get())) + aligned_frame_length;

    for (int attempts = SEND_ATTEMPTS; attempts > 0; attempts--)
    {
        aeron_buffer_claim_t claim{};
        const long position =
            static_cast<long>(aeron_exclusive_publication_try_claim(publication_.get(), length, &claim));
        if (position > 0)
        {
            codecs::ClusterActionRequest encoder;
            encoder.wrapAndApplyHeader(reinterpret_cast<char*>(claim.data), 0, claim.length);
            encoder.leadershipTermId(leadership_term_id)
                .logPosition(log_position)
                .timestamp(timestamp)
                .action(static_cast<codecs::ClusterAction::Value>(action))
                .flags(static_cast<std::uint8_t>(flags));
            last_append_position_ = log_position;
            aeron_buffer_claim_commit(&claim);
            return true;
        }
        if (is_fatal(position))
        {
            return false;
        }
    }
    return false;
}

}  // namespace deepmsg::cluster::consensus
