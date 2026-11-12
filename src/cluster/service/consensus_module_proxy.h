#pragma once

#include <cstdint>
#include <expected>
#include <span>
#include <vector>

#include <aeronc.h>

#include "core/error/error.h"

namespace deepmsg::cluster::service
{

using axc::core::Error;

/// `ServiceProxy.SEND_ATTEMPTS`.
inline constexpr int SEND_ATTEMPTS = 5;

/// `AeronCluster.SESSION_HEADER_LENGTH`: SBE header (8) + `SessionMessageHeader`
/// block (24).
inline constexpr std::size_t SESSION_HEADER_LENGTH = 32;

/// The `Publication` return codes the reference distinguishes.
inline constexpr std::int64_t OFFER_NOT_CONNECTED = AERON_PUBLICATION_NOT_CONNECTED;
inline constexpr std::int64_t OFFER_BACK_PRESSURED = AERON_PUBLICATION_BACK_PRESSURED;
inline constexpr std::int64_t OFFER_ADMIN_ACTION = AERON_PUBLICATION_ADMIN_ACTION;
inline constexpr std::int64_t OFFER_MAX_POSITION_EXCEEDED = AERON_PUBLICATION_MAX_POSITION_EXCEEDED;
/// `ClientSession.MOCKED_OFFER`.
inline constexpr std::int64_t MOCKED_OFFER = 1;

/// The sticky `SessionMessageHeader` values (`CSA:191-193`).
struct SessionHeader
{
    std::int64_t leadership_term_id{0};
    std::int64_t cluster_session_id{0};
    std::int64_t timestamp{0};
};

/// A claimed message; the sticky header is already written.
class ClaimedMessage
{
public:
    ClaimedMessage() = default;
    ClaimedMessage(aeron_buffer_claim_t claim, std::int64_t position, std::span<std::uint8_t> payload,
                   bool mocked) noexcept
        : claim_(claim), position_(position), payload_(payload), mocked_(mocked)
    {
    }

    [[nodiscard]] std::int64_t position() const noexcept
    {
        return position_;
    }

    [[nodiscard]] std::span<std::uint8_t> payload() const noexcept
    {
        return payload_;
    }

    [[nodiscard]] std::expected<std::int64_t, Error> commit() noexcept;

private:
    aeron_buffer_claim_t claim_{};
    std::int64_t position_{0};
    std::span<std::uint8_t> payload_{};
    bool mocked_{false};
};

/// `ConsensusModuleProxy` (`ConsensusModuleProxy.java`): the container → CM
/// direction of S2, plus the sticky session header every egress message uses.
class ConsensusModuleProxy
{
public:
    explicit ConsensusModuleProxy(std::int32_t service_id) : service_id_(service_id) {}

    void set_publication(aeron_publication_t* publication) noexcept
    {
        publication_ = publication;
    }

    [[nodiscard]] const SessionHeader& header() const noexcept
    {
        return header_;
    }

    void set_leadership_term_id(std::int64_t leadership_term_id) noexcept
    {
        header_.leadership_term_id = leadership_term_id;
    }

    [[nodiscard]] std::expected<bool, Error> schedule_timer(std::int64_t correlation_id, std::int64_t deadline);
    [[nodiscard]] std::expected<bool, Error> cancel_timer(std::int64_t correlation_id);
    [[nodiscard]] std::expected<bool, Error> ack(std::int64_t log_position, std::int64_t timestamp, std::int64_t ack_id,
                                                 std::int64_t relevant_id);
    [[nodiscard]] std::expected<bool, Error> close_session(std::int64_t cluster_session_id);
    [[nodiscard]] std::expected<bool, Error> cluster_members_query(std::int64_t correlation_id);

    /// A service message (`clusterSessionId = serviceId`, timestamp untouched).
    [[nodiscard]] std::int64_t offer_service_message(std::span<const std::uint8_t> payload);
    /// The same, with the parts offered back to back after the sticky header.
    [[nodiscard]] std::int64_t offer_service_message_vectored(std::span<const std::span<const std::uint8_t>> parts);

    /// An egress message on the session's own publication.
    [[nodiscard]] std::int64_t offer_session_message(aeron_publication_t* publication, std::int64_t cluster_session_id,
                                                     std::int64_t timestamp, std::span<const std::uint8_t> payload);
    [[nodiscard]] std::int64_t offer_session_message_vectored(aeron_publication_t* publication,
                                                              std::int64_t cluster_session_id, std::int64_t timestamp,
                                                              std::span<const std::span<const std::uint8_t>> parts);

    /// Claim a service message; the caller writes `payload()` and commits.
    [[nodiscard]] std::expected<ClaimedMessage, std::int64_t> claim_service_message(std::size_t length);
    [[nodiscard]] std::expected<ClaimedMessage, std::int64_t> claim_session_message(aeron_publication_t* publication,
                                                                                    std::int64_t cluster_session_id,
                                                                                    std::int64_t timestamp,
                                                                                    std::size_t length);
    /// A follower's claim: a window over the resident buffer, nothing sent.
    [[nodiscard]] std::expected<ClaimedMessage, Error> mock_session_message(std::int64_t cluster_session_id,
                                                                            std::int64_t timestamp, std::size_t length);

    [[nodiscard]] static bool is_retryable_claim(std::int64_t code) noexcept;

private:
    [[nodiscard]] std::expected<bool, Error> send(std::span<const std::uint8_t> message);
    [[nodiscard]] std::int64_t offer_on(aeron_publication_t* publication, std::span<const std::uint8_t> message);
    [[nodiscard]] std::int64_t offer_with(aeron_publication_t* publication, std::span<const std::uint8_t> first,
                                          std::span<const std::span<const std::uint8_t>> parts);
    void encode_header(std::span<std::uint8_t> slot) const;
    void resize(std::size_t length);
    [[nodiscard]] std::expected<ClaimedMessage, std::int64_t> claim_on(aeron_publication_t* publication,
                                                                       std::size_t length);
    [[nodiscard]] std::expected<bool, Error> check_error(std::int64_t code) const;

    aeron_publication_t* publication_{nullptr};
    std::int32_t service_id_{0};
    SessionHeader header_{};
    std::vector<std::uint8_t> buffer_{};
};

}  // namespace deepmsg::cluster::service
