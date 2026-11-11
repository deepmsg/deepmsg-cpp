#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <aeronc.h>

#include "cluster/common/cluster_member.h"
#include "cluster/consensus/consensus_module_context.h"
#include "core/error/error.h"
#include "core/error/error_handler.h"

namespace deepmsg::cluster::consensus
{

using axc::core::Error;
using axc::core::ErrorHandler;

/// `ServiceProxy.SEND_ATTEMPTS` (`ServiceProxy.java:28`).
inline constexpr int SEND_ATTEMPTS = 5;

/// The publication surface `ServiceProxy` needs. Kept as a seam (the same
/// shape as `LogAppender`/`EgressSender`) so the reference's failure paths --
/// not connected, closed, max position, back pressure -- can be injected
/// without a real publication.
class ServiceSend
{
public:
    ServiceSend() = default;
    ServiceSend(const ServiceSend&) = delete;
    ServiceSend& operator=(const ServiceSend&) = delete;
    virtual ~ServiceSend() = default;

    /// `Publication.tryClaim(length, claim)`: a positive position on success,
    /// with `window` the claimed region to fill; the caller then calls
    /// `commit()`.
    [[nodiscard]] virtual std::int64_t try_claim(std::size_t length, std::span<std::uint8_t>& window) = 0;
    virtual void commit() noexcept = 0;

    /// `Publication.offer(buffer, offset, length)`.
    [[nodiscard]] virtual std::int64_t offer(std::span<const std::uint8_t> message) = 0;

    [[nodiscard]] virtual bool is_closed() const noexcept = 0;
    /// `Publication.termBufferLength()`.
    [[nodiscard]] virtual std::int32_t term_buffer_length() const noexcept = 0;
};

/// The production `ServiceSend` over a regular `aeron_publication_t`.
class PublicationSend final : public ServiceSend
{
public:
    explicit PublicationSend(aeron_publication_t* publication) : publication_(publication) {}

    [[nodiscard]] std::int64_t try_claim(std::size_t length, std::span<std::uint8_t>& window) override;
    void commit() noexcept override;
    [[nodiscard]] std::int64_t offer(std::span<const std::uint8_t> message) override;
    [[nodiscard]] bool is_closed() const noexcept override;
    [[nodiscard]] std::int32_t term_buffer_length() const noexcept override;

private:
    aeron_publication_t* publication_{nullptr};
    aeron_buffer_claim_t claim_{};
    bool claimed_{false};
};

/// `ServiceProxy` (`ServiceProxy.java`): the consensus module -> container
/// direction of S2, five messages on the control channel's service stream.
///
/// Failure semantics are the reference's and are not uniform: `join_log` and
/// `request_service_ack` return an error when the attempts are gone (the
/// startup wait cannot proceed), `termination_position` silently ignores a
/// closed publication, and the two membership responses report through the
/// error handler and carry on.
class ServiceProxy
{
public:
    ServiceProxy(ServiceSend& send, ErrorHandler& error_handler);

    ServiceProxy(const ServiceProxy&) = delete;
    ServiceProxy& operator=(const ServiceProxy&) = delete;

    /// `joinLog(...)` (`:54-101`).
    [[nodiscard]] std::expected<void, Error> join_log(std::int64_t log_position, std::int64_t max_log_position,
                                                      std::int32_t member_id, std::int32_t log_session_id,
                                                      std::int32_t log_stream_id, bool is_startup, ClusterRole role,
                                                      std::string_view channel, bool is_standby);

    /// `clusterMembersResponse(...)` (`:103-139`).
    void cluster_members_response(std::int64_t correlation_id, std::int32_t leader_member_id,
                                  std::string_view active_members);

    /// `clusterMembersExtendedResponse(...)` (`:141-194`).
    void cluster_members_extended_response(std::int64_t correlation_id, std::int64_t current_time_ns,
                                           std::int32_t leader_member_id, std::int32_t member_id,
                                           std::span<const common::ClusterMember> active_members);

    /// `terminationPosition(...)` (`:196-229`).
    void termination_position(std::int64_t log_position, bool ack_required);

    /// `requestServiceAck(...)` (`:231-259`).
    [[nodiscard]] std::expected<void, Error> request_service_ack(std::int64_t log_position);

    [[nodiscard]] static std::string_view publication_error_string(std::int64_t result) noexcept;

private:
    [[nodiscard]] static std::expected<void, Error> check_result(std::int64_t position, const ServiceSend& send);

    ServiceSend& send_;
    ErrorHandler& error_handler_;
    std::vector<std::uint8_t> buffer_{};
};

}  // namespace deepmsg::cluster::consensus
