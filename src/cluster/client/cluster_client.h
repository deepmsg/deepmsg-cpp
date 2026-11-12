#pragma once

#include <array>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <aeronc.h>

#include "cluster/client/client_config.h"
#include "cluster/common/cluster_codecs.h"
#include "core/deepmsg/async_add.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/deepmsg/handles.h"
#include "io_aeron_cluster_codecs/AdminRequestType.h"
#include "io_aeron_cluster_codecs/AdminResponseCode.h"
#include "io_aeron_cluster_codecs/SessionMessageHeader.h"

namespace deepmsg::cluster::client
{

class ClusterClient;

/// `AeronCluster.SESSION_HEADER_LENGTH`: the SBE header plus the block
/// (`AeronCluster.java:99-100`).
inline constexpr std::size_t SESSION_HEADER_LENGTH =
    common::codecs::MessageHeader::encodedLength() + common::codecs::SessionMessageHeader::sbeBlockLength();

/// `FRAGMENT_LIMIT` (`AeronCluster.java:103`).
inline constexpr std::size_t FRAGMENT_LIMIT = 10;

/// `EgressListener` (`AeronCluster.java:843-981`). Payloads are spans into the
/// polled buffer; they are valid only for the call.
class EgressListener
{
public:
    virtual ~EgressListener() = default;

    virtual void on_message(std::int64_t cluster_session_id, std::int64_t timestamp,
                            std::span<const std::uint8_t> payload) = 0;

    virtual void on_session_event(std::int64_t correlation_id, std::int64_t cluster_session_id,
                                  std::int64_t leadership_term_id, std::int32_t leader_member_id,
                                  common::codecs::EventCode::Value code, std::string_view detail) = 0;

    virtual void on_new_leader(std::int64_t cluster_session_id, std::int64_t leadership_term_id,
                               std::int32_t leader_member_id, std::string_view ingress_endpoints) = 0;

    virtual void on_admin_response(std::int64_t cluster_session_id, std::int64_t correlation_id,
                                   common::codecs::AdminRequestType::Value request_type,
                                   common::codecs::AdminResponseCode::Value response_code, std::string_view message,
                                   std::span<const std::uint8_t> payload) = 0;
};

/// `ControlledEgressListener` (`AeronCluster.java:984-1127`): `on_message`
/// returns the poller's action, everything else is fire-and-forget.
class ControlledEgressListener
{
public:
    virtual ~ControlledEgressListener() = default;

    virtual aeron_controlled_fragment_handler_action_t on_message(std::int64_t cluster_session_id,
                                                                  std::int64_t timestamp,
                                                                  std::span<const std::uint8_t> payload) = 0;

    virtual void on_session_event(std::int64_t correlation_id, std::int64_t cluster_session_id,
                                  std::int64_t leadership_term_id, std::int32_t leader_member_id,
                                  common::codecs::EventCode::Value code, std::string_view detail) = 0;

    virtual void on_new_leader(std::int64_t cluster_session_id, std::int64_t leadership_term_id,
                               std::int32_t leader_member_id, std::string_view ingress_endpoints) = 0;

    virtual void on_admin_response(std::int64_t cluster_session_id, std::int64_t correlation_id,
                                   common::codecs::AdminRequestType::Value request_type,
                                   common::codecs::AdminResponseCode::Value response_code, std::string_view message,
                                   std::span<const std::uint8_t> payload) = 0;
};

/// A claimed ingress range (`BufferClaim`): the session header is already
/// written, the caller fills `payload()` and calls `commit()`.
class SessionClaim
{
public:
    SessionClaim() = default;

    SessionClaim(aeron_buffer_claim_t claim, std::span<std::uint8_t> payload, std::int64_t position) noexcept
        : claim_(claim), payload_(payload), position_(position)
    {
    }

    [[nodiscard]] std::span<std::uint8_t> payload() const noexcept
    {
        return payload_;
    }

    [[nodiscard]] std::int64_t commit() noexcept;
    void abort() noexcept;

private:
    aeron_buffer_claim_t claim_{};
    std::span<std::uint8_t> payload_{};
    std::int64_t position_{0};
};

/// `Publication` in the reference: a concurrent or an exclusive publication.
/// The C API has two incompatible types, so the choice is a member.
class IngressPublication
{
public:
    IngressPublication() = default;

    static IngressPublication from_concurrent(axc::core::PubPtr publication)
    {
        IngressPublication result;
        result.concurrent_ = std::move(publication);
        return result;
    }

    static IngressPublication from_exclusive(axc::core::ExPubPtr publication)
    {
        IngressPublication result;
        result.exclusive_ = std::move(publication);
        return result;
    }

    IngressPublication(IngressPublication&&) noexcept = default;
    IngressPublication& operator=(IngressPublication&&) noexcept = default;
    IngressPublication(const IngressPublication&) = delete;
    IngressPublication& operator=(const IngressPublication&) = delete;

    [[nodiscard]] bool valid() const noexcept
    {
        return nullptr != concurrent_ || nullptr != exclusive_;
    }

    [[nodiscard]] bool is_connected() const noexcept;

    /// `Publication.offer(buffer, offset, length)` for the connect request.
    [[nodiscard]] std::int64_t offer(std::span<const std::uint8_t> message) const noexcept;

    /// `Publication.offer(header, payload)` (`AeronCluster.java:451-458`): two
    /// regions, no copy.
    [[nodiscard]] std::int64_t offer_two(std::span<const std::uint8_t> first,
                                         std::span<const std::uint8_t> second) const noexcept;

    /// `Publication.tryClaim` (`AeronCluster.java:427-439`).
    [[nodiscard]] std::int64_t try_claim(std::size_t length, aeron_buffer_claim_t& claim) const noexcept;

    /// `Publication.termBufferLength()` for the max-position message.
    [[nodiscard]] std::int32_t term_buffer_length() const noexcept;

    [[nodiscard]] aeron_publication_t* concurrent() const noexcept
    {
        return concurrent_.get();
    }

    [[nodiscard]] aeron_exclusive_publication_t* exclusive() const noexcept
    {
        return exclusive_.get();
    }

    /// `Publication.close()`.
    void close() noexcept
    {
        concurrent_.reset();
        exclusive_.reset();
    }

private:
    axc::core::PubPtr concurrent_{};
    axc::core::ExPubPtr exclusive_{};
};

/// `AeronCluster` (`AeronCluster.java:95-1200`), the connected session: send
/// (offer / offerv / tryClaim / keepAlive / close), receive (egress dispatch
/// and the adapters) and failover (NewLeaderEvent).
class ClusterClient
{
public:
    /// `State` (`AeronCluster.java:1170-1193`).
    enum class State
    {
        connected,
        await_new_leader,
        await_new_leader_connection,
        pending_close,
        closed
    };

    /// `client` must outlive the session and must not move after this call:
    /// the session keeps a pointer to it for failover registrations.
    ClusterClient(axc::core::DeepmsgClient& client, ClientConfig config, axc::core::SubPtr egress_subscription,
                  IngressPublication ingress_publication, aeron_image_t* egress_image, std::int64_t cluster_session_id,
                  std::int64_t leadership_term_id, std::int32_t leader_member_id);

    ClusterClient(ClusterClient&&) noexcept;
    ClusterClient& operator=(ClusterClient&&) noexcept;
    ClusterClient(const ClusterClient&) = delete;
    ClusterClient& operator=(const ClusterClient&) = delete;
    ~ClusterClient();

    [[nodiscard]] const ClientConfig& config() const noexcept
    {
        return config_;
    }

    [[nodiscard]] std::int64_t cluster_session_id() const noexcept
    {
        return cluster_session_id_;
    }

    [[nodiscard]] std::int64_t leadership_term_id() const noexcept
    {
        return leadership_term_id_;
    }

    [[nodiscard]] std::int32_t leader_member_id() const noexcept
    {
        return leader_member_id_;
    }

    [[nodiscard]] std::int64_t new_leader_timeout_ns() const noexcept
    {
        return config_.new_leader_timeout_ns.value_or(NULL_VALUE);
    }

    [[nodiscard]] aeron_image_t* egress_image() const noexcept
    {
        return egress_image_;
    }

    [[nodiscard]] aeron_subscription_t* egress_subscription() const noexcept
    {
        return egress_subscription_.get();
    }

    [[nodiscard]] const IngressPublication& ingress_publication() const noexcept
    {
        return ingress_publication_;
    }

    [[nodiscard]] State state() const noexcept
    {
        return state_;
    }

    [[nodiscard]] bool is_closed() const noexcept
    {
        return State::closed == state_;
    }

    void set_egress_listener(EgressListener* listener) noexcept
    {
        egress_listener_ = listener;
    }

    void set_controlled_egress_listener(ControlledEgressListener* listener) noexcept
    {
        controlled_egress_listener_ = listener;
    }

    /// `offer(buffer, offset, length)` (`AeronCluster.java:451-458`).
    [[nodiscard]] std::int64_t offer(std::span<const std::uint8_t> payload);

    /// The `DirectBufferVector[]` form (`AeronCluster.java:468-477`): the
    /// session header is the first region.
    [[nodiscard]] std::int64_t offer_vectored(std::span<const std::span<const std::uint8_t>> parts);

    /// `tryClaim(length, bufferClaim)` (`AeronCluster.java:427-439`). On
    /// success the returned claim's payload is `length` bytes after the session
    /// header; `commit()` publishes it.
    [[nodiscard]] std::expected<SessionClaim, std::int64_t> try_claim(std::int32_t length);

    /// `sendKeepAlive()` (`AeronCluster.java:487-526`).
    [[nodiscard]] std::expected<bool, Error> send_keep_alive();

    /// `pollEgress()` (`AeronCluster.java:599-612`).
    [[nodiscard]] std::expected<int, Error> poll_egress(std::size_t fragment_limit = FRAGMENT_LIMIT);

    /// `controlledPollEgress()` (`AeronCluster.java:626-639`).
    [[nodiscard]] std::expected<int, Error> controlled_poll_egress(std::size_t fragment_limit = FRAGMENT_LIMIT);

    /// `pollStateChanges()` (`AeronCluster.java:648-660`).
    [[nodiscard]] int poll_state_changes();

    /// `onNewLeader(...)` (`AeronCluster.java:671-704`); public because the
    /// reference exposes it for `EgressAdapter` / `EgressPoller` users.
    [[nodiscard]] std::expected<void, Error> on_new_leader(std::int64_t cluster_session_id,
                                                           std::int64_t leadership_term_id,
                                                           std::int32_t leader_member_id,
                                                           std::string_view ingress_endpoints);

    /// `trackIngressPublicationResult` (`AeronCluster.java:713-734`).
    void track_ingress_publication_result(std::int64_t result);

    /// `close()` (`AeronCluster.java:290-311`): a connected publication gets a
    /// `SessionCloseRequest` first.
    void close();

private:
    struct MemberIngress
    {
        std::int32_t member_id{-1};
        std::string endpoint{};
        IngressPublication publication{};
    };

    static aeron_controlled_fragment_handler_action_t on_fragment(void* clientd, const std::uint8_t* buffer,
                                                                  std::size_t length, aeron_header_t* header);
    static void on_fragment_dispatch(void* clientd, const std::uint8_t* buffer, std::size_t length,
                                     aeron_header_t* header);

    [[nodiscard]] aeron_controlled_fragment_handler_action_t dispatch(std::span<const std::uint8_t> message,
                                                                      aeron_image_t* image, bool controlled);
    void on_disconnected();
    void close_session();
    void state(State new_state, std::int64_t deadline_ns) noexcept;
    [[nodiscard]] std::int64_t now_ns() const;

    [[nodiscard]] std::expected<IngressPublication, Error> add_new_leader_ingress_publication(
        std::string_view channel, std::int32_t stream_id, std::int32_t leader_member_id);
    [[nodiscard]] std::expected<void, Error> update_member_endpoints(std::string_view ingress_endpoints,
                                                                     std::int32_t leader_member_id);

    axc::core::DeepmsgClient* client_{nullptr};
    ClientConfig config_;
    std::string ingress_channel_{};
    std::optional<Error> pending_error_{};
    axc::core::SubPtr egress_subscription_{};
    IngressPublication ingress_publication_{};
    aeron_image_t* egress_image_{nullptr};
    std::int64_t cluster_session_id_{NULL_VALUE};
    std::int64_t leadership_term_id_{NULL_VALUE};
    std::int32_t leader_member_id_{NULL_VALUE};
    State state_{State::connected};
    std::int64_t state_deadline_ns_{0};

    std::array<std::uint8_t, SESSION_HEADER_LENGTH> header_{};

    std::vector<MemberIngress> members_{};

    aeron_fragment_assembler_t* assembler_{nullptr};
    aeron_controlled_fragment_assembler_t* controlled_assembler_{nullptr};
    EgressListener* egress_listener_{nullptr};
    ControlledEgressListener* controlled_egress_listener_{nullptr};
};

/// The two adapters (`EgressAdapter.java`, `ControlledEgressAdapter.java`):
/// the same dispatch as the session, for callers that own the subscription.
class EgressAdapter
{
public:
    EgressAdapter(EgressListener& listener, std::int64_t cluster_session_id, std::size_t fragment_limit);
    ~EgressAdapter();

    EgressAdapter(const EgressAdapter&) = delete;
    EgressAdapter& operator=(const EgressAdapter&) = delete;

    [[nodiscard]] std::expected<int, Error> poll(aeron_subscription_t* subscription);

private:
    static void on_fragment(void* clientd, const std::uint8_t* buffer, std::size_t length, aeron_header_t* header);

    EgressListener& listener_;
    std::int64_t cluster_session_id_;
    std::size_t fragment_limit_;
    aeron_fragment_assembler_t* assembler_{nullptr};
    std::optional<Error> pending_error_{};
};

class ControlledEgressAdapter
{
public:
    ControlledEgressAdapter(ControlledEgressListener& listener, std::int64_t cluster_session_id,
                            std::size_t fragment_limit);
    ~ControlledEgressAdapter();

    ControlledEgressAdapter(const ControlledEgressAdapter&) = delete;
    ControlledEgressAdapter& operator=(const ControlledEgressAdapter&) = delete;

    [[nodiscard]] std::expected<int, Error> poll(aeron_subscription_t* subscription);

private:
    static aeron_controlled_fragment_handler_action_t on_fragment(void* clientd, const std::uint8_t* buffer,
                                                                  std::size_t length, aeron_header_t* header);

    ControlledEgressListener& listener_;
    std::int64_t cluster_session_id_;
    std::size_t fragment_limit_;
    aeron_controlled_fragment_assembler_t* assembler_{nullptr};
    std::optional<Error> pending_error_{};
};

}  // namespace deepmsg::cluster::client
