#include "cluster/service/consensus_module_proxy.h"

#include <algorithm>
#include <cstring>
#include <utility>

#include "io_aeron_cluster_codecs/CancelTimer.h"
#include "io_aeron_cluster_codecs/CloseSession.h"
#include "io_aeron_cluster_codecs/ClusterMembersQuery.h"
#include "io_aeron_cluster_codecs/MessageHeader.h"
#include "io_aeron_cluster_codecs/ScheduleTimer.h"
#include "io_aeron_cluster_codecs/ServiceAck.h"
#include "io_aeron_cluster_codecs/SessionMessageHeader.h"

namespace deepmsg::cluster::service
{
namespace
{

namespace codecs = ::io::aeron::cluster::codecs;

constexpr std::size_t MESSAGE_HEADER_LENGTH = codecs::MessageHeader::encodedLength();

void encode_session_header(std::span<std::uint8_t> slot, std::int64_t leadership_term_id,
                           std::int64_t cluster_session_id, std::int64_t timestamp)
{
    codecs::SessionMessageHeader header;
    header.wrapAndApplyHeader(reinterpret_cast<char*>(slot.data()), 0, slot.size());
    header.leadershipTermId(leadership_term_id).clusterSessionId(cluster_session_id).timestamp(timestamp);
}

}  // namespace

void ConsensusModuleProxy::resize(std::size_t length)
{
    if (buffer_.size() < length)
    {
        buffer_.resize(length);
    }
}

void ConsensusModuleProxy::encode_header(std::span<std::uint8_t> slot) const
{
    encode_session_header(slot, header_.leadership_term_id, header_.cluster_session_id, header_.timestamp);
}

std::expected<bool, Error> ConsensusModuleProxy::check_error(std::int64_t code) const
{
    switch (code)
    {
        case OFFER_NOT_CONNECTED:
            return std::unexpected(
                axc::core::make_error(axc::core::ErrorCode::no_capacity, "publication is not connected"));
        case AERON_PUBLICATION_CLOSED:
            return std::unexpected(axc::core::make_error(axc::core::ErrorCode::no_capacity, "publication is closed"));
        case OFFER_MAX_POSITION_EXCEEDED:
        case AERON_PUBLICATION_ERROR:
            return std::unexpected(
                axc::core::make_error(axc::core::ErrorCode::no_capacity, "publication at max position"));
        default:
            return true;
    }
}

std::int64_t ConsensusModuleProxy::offer_on(aeron_publication_t* publication, std::span<const std::uint8_t> message)
{
    for (int attempt = 0; attempt < SEND_ATTEMPTS; ++attempt)
    {
        const std::int64_t result =
            aeron_publication_offer(publication, message.data(), message.size(), nullptr, nullptr);
        if (result > 0)
        {
            return result;
        }
        if (OFFER_BACK_PRESSURED != result && OFFER_ADMIN_ACTION != result &&
            AERON_PUBLICATION_MAX_POSITION_EXCEEDED != result)
        {
            return result;
        }
        if (AERON_PUBLICATION_MAX_POSITION_EXCEEDED == result)
        {
            return result;
        }
    }

    return OFFER_BACK_PRESSURED;
}

std::int64_t ConsensusModuleProxy::offer_with(aeron_publication_t* publication, std::span<const std::uint8_t> first,
                                              std::span<const std::span<const std::uint8_t>> parts)
{
    if (parts.empty())
    {
        return offer_on(publication, first);
    }

    if (parts.size() + 1 <= 8)
    {
        aeron_iovec_t iov[9]{};
        iov[0].iov_base = const_cast<std::uint8_t*>(first.data());
        iov[0].iov_len = first.size();
        for (std::size_t i = 0; i < parts.size(); ++i)
        {
            iov[i + 1].iov_base = const_cast<std::uint8_t*>(parts[i].data());
            iov[i + 1].iov_len = parts[i].size();
        }

        for (int attempt = 0; attempt < SEND_ATTEMPTS; ++attempt)
        {
            const std::int64_t result = aeron_publication_offerv(publication, iov, parts.size() + 1, nullptr, nullptr);
            if (result > 0)
            {
                return result;
            }
            if (OFFER_BACK_PRESSURED != result && OFFER_ADMIN_ACTION != result)
            {
                return result;
            }
        }
        return OFFER_BACK_PRESSURED;
    }

    std::size_t total = first.size();
    for (const auto& part : parts)
    {
        total += part.size();
    }
    resize(total);
    std::memcpy(buffer_.data(), first.data(), first.size());
    std::size_t offset = first.size();
    for (const auto& part : parts)
    {
        std::memcpy(buffer_.data() + offset, part.data(), part.size());
        offset += part.size();
    }
    return offer_on(publication, std::span<const std::uint8_t>(buffer_.data(), total));
}

std::expected<bool, Error> ConsensusModuleProxy::send(std::span<const std::uint8_t> message)
{
    if (nullptr == publication_)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::no_capacity, "publication is not connected"));
    }

    const std::int64_t result = offer_on(publication_, message);
    if (result > 0)
    {
        return true;
    }

    auto checked = check_error(result);
    if (!checked)
    {
        return std::unexpected(checked.error());
    }
    return false;
}

std::expected<bool, Error> ConsensusModuleProxy::schedule_timer(std::int64_t correlation_id, std::int64_t deadline)
{
    constexpr std::size_t LENGTH = MESSAGE_HEADER_LENGTH + codecs::ScheduleTimer::SBE_BLOCK_LENGTH;
    resize(LENGTH);

    codecs::ScheduleTimer encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer_.data()), 0, buffer_.size());
    encoder.correlationId(correlation_id).deadline(deadline);

    return send(std::span<const std::uint8_t>(buffer_.data(), LENGTH));
}

std::expected<bool, Error> ConsensusModuleProxy::cancel_timer(std::int64_t correlation_id)
{
    constexpr std::size_t LENGTH = MESSAGE_HEADER_LENGTH + codecs::CancelTimer::SBE_BLOCK_LENGTH;
    resize(LENGTH);

    codecs::CancelTimer encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer_.data()), 0, buffer_.size());
    encoder.correlationId(correlation_id);

    return send(std::span<const std::uint8_t>(buffer_.data(), LENGTH));
}

std::expected<bool, Error> ConsensusModuleProxy::ack(std::int64_t log_position, std::int64_t timestamp,
                                                     std::int64_t ack_id, std::int64_t relevant_id)
{
    constexpr std::size_t LENGTH = MESSAGE_HEADER_LENGTH + codecs::ServiceAck::SBE_BLOCK_LENGTH;
    resize(LENGTH);

    codecs::ServiceAck encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer_.data()), 0, buffer_.size());
    encoder.logPosition(log_position).timestamp(timestamp).ackId(ack_id).relevantId(relevant_id).serviceId(service_id_);

    return send(std::span<const std::uint8_t>(buffer_.data(), LENGTH));
}

std::expected<bool, Error> ConsensusModuleProxy::close_session(std::int64_t cluster_session_id)
{
    constexpr std::size_t LENGTH = MESSAGE_HEADER_LENGTH + codecs::CloseSession::SBE_BLOCK_LENGTH;
    resize(LENGTH);

    codecs::CloseSession encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer_.data()), 0, buffer_.size());
    encoder.clusterSessionId(cluster_session_id);

    return send(std::span<const std::uint8_t>(buffer_.data(), LENGTH));
}

std::expected<bool, Error> ConsensusModuleProxy::cluster_members_query(std::int64_t correlation_id)
{
    constexpr std::size_t LENGTH = MESSAGE_HEADER_LENGTH + codecs::ClusterMembersQuery::SBE_BLOCK_LENGTH;
    resize(LENGTH);

    codecs::ClusterMembersQuery encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer_.data()), 0, buffer_.size());
    encoder.correlationId(correlation_id).extended(codecs::BooleanType::TRUE);

    return send(std::span<const std::uint8_t>(buffer_.data(), LENGTH));
}

std::int64_t ConsensusModuleProxy::offer_service_message(std::span<const std::uint8_t> payload)
{
    header_.cluster_session_id = service_id_;
    resize(SESSION_HEADER_LENGTH);
    encode_header(std::span<std::uint8_t>(buffer_.data(), SESSION_HEADER_LENGTH));

    if (nullptr == publication_)
    {
        return OFFER_NOT_CONNECTED;
    }

    // The reference offers the sticky header and the caller's buffer as two
    // slices (`CSA:382-397`); no copy of the payload.
    const std::span<const std::uint8_t> header{buffer_.data(), SESSION_HEADER_LENGTH};
    if (payload.empty())
    {
        return offer_on(publication_, header);
    }
    const std::span<const std::uint8_t> parts[]{payload};
    return offer_with(publication_, header, parts);
}

std::int64_t ConsensusModuleProxy::offer_session_message(aeron_publication_t* publication,
                                                         std::int64_t cluster_session_id, std::int64_t timestamp,
                                                         std::span<const std::uint8_t> payload)
{
    header_.cluster_session_id = cluster_session_id;
    header_.timestamp = timestamp;
    resize(SESSION_HEADER_LENGTH);
    encode_header(std::span<std::uint8_t>(buffer_.data(), SESSION_HEADER_LENGTH));

    if (nullptr == publication)
    {
        return OFFER_NOT_CONNECTED;
    }

    const std::span<const std::uint8_t> header{buffer_.data(), SESSION_HEADER_LENGTH};
    if (payload.empty())
    {
        return offer_on(publication, header);
    }
    const std::span<const std::uint8_t> parts[]{payload};
    return offer_with(publication, header, parts);
}

std::int64_t ConsensusModuleProxy::offer_service_message_vectored(std::span<const std::span<const std::uint8_t>> parts)
{
    header_.cluster_session_id = service_id_;
    resize(SESSION_HEADER_LENGTH);
    encode_header(std::span<std::uint8_t>(buffer_.data(), SESSION_HEADER_LENGTH));

    if (nullptr == publication_)
    {
        return OFFER_NOT_CONNECTED;
    }
    return offer_with(publication_, std::span<const std::uint8_t>(buffer_.data(), SESSION_HEADER_LENGTH), parts);
}

std::int64_t ConsensusModuleProxy::offer_session_message_vectored(aeron_publication_t* publication,
                                                                  std::int64_t cluster_session_id,
                                                                  std::int64_t timestamp,
                                                                  std::span<const std::span<const std::uint8_t>> parts)
{
    header_.cluster_session_id = cluster_session_id;
    header_.timestamp = timestamp;
    encode_header(std::span<std::uint8_t>(buffer_.data(), SESSION_HEADER_LENGTH));

    if (nullptr == publication)
    {
        return OFFER_NOT_CONNECTED;
    }
    return offer_with(publication, std::span<const std::uint8_t>(buffer_.data(), SESSION_HEADER_LENGTH), parts);
}

std::expected<ClaimedMessage, std::int64_t> ConsensusModuleProxy::claim_on(aeron_publication_t* publication,
                                                                           std::size_t length)
{
    if (nullptr == publication)
    {
        return std::unexpected(OFFER_NOT_CONNECTED);
    }

    const std::size_t total = SESSION_HEADER_LENGTH + length;
    aeron_buffer_claim_t claim{};
    const std::int64_t position = aeron_publication_try_claim(publication, total, &claim);
    if (position < 0)
    {
        return std::unexpected(position);
    }

    encode_header(std::span<std::uint8_t>(claim.data, SESSION_HEADER_LENGTH));
    return ClaimedMessage(claim, position, std::span<std::uint8_t>(claim.data + SESSION_HEADER_LENGTH, length), false);
}

std::expected<ClaimedMessage, std::int64_t> ConsensusModuleProxy::claim_service_message(std::size_t length)
{
    header_.cluster_session_id = service_id_;
    return claim_on(publication_, length);
}

std::expected<ClaimedMessage, std::int64_t> ConsensusModuleProxy::claim_session_message(
    aeron_publication_t* publication, std::int64_t cluster_session_id, std::int64_t timestamp, std::size_t length)
{
    header_.cluster_session_id = cluster_session_id;
    header_.timestamp = timestamp;
    return claim_on(publication, length);
}

std::expected<ClaimedMessage, Error> ConsensusModuleProxy::mock_session_message(std::int64_t cluster_session_id,
                                                                                std::int64_t timestamp,
                                                                                std::size_t length)
{
    constexpr std::size_t MAX_CLAIMED_PAYLOAD = 1408 - 32 - SESSION_HEADER_LENGTH;
    if (length > MAX_CLAIMED_PAYLOAD)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::invalid_argument,
                                  "claim exceeds maxPayloadLength=" + std::to_string(MAX_CLAIMED_PAYLOAD) +
                                      ", length=" + std::to_string(length)));
    }

    header_.cluster_session_id = cluster_session_id;
    header_.timestamp = timestamp;
    resize(SESSION_HEADER_LENGTH + length);
    encode_header(std::span<std::uint8_t>(buffer_.data(), SESSION_HEADER_LENGTH));

    return ClaimedMessage(aeron_buffer_claim_t{}, MOCKED_OFFER,
                          std::span<std::uint8_t>(buffer_.data() + SESSION_HEADER_LENGTH, length), true);
}

bool ConsensusModuleProxy::is_retryable_claim(std::int64_t code) noexcept
{
    return OFFER_BACK_PRESSURED == code || OFFER_ADMIN_ACTION == code;
}

std::expected<std::int64_t, Error> ClaimedMessage::commit() noexcept
{
    if (mocked_)
    {
        return position_;
    }

    if (0 != aeron_buffer_claim_commit(&claim_))
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::internal_error, "failed to commit the claimed frame"));
    }
    return position_;
}

}  // namespace deepmsg::cluster::service
