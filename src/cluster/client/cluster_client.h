#pragma once

#include <cstdint>
#include <span>

#include <aeronc.h>

#include "cluster/client/client_config.h"
#include "core/deepmsg/handles.h"

namespace deepmsg::cluster::client
{

/// `Publication` in the reference (`AeronCluster.java:1407-1421`: a
/// concurrent or an exclusive publication). The C API has two incompatible
/// types, so the choice is a member, not a base class.
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

    /// `Publication.offer` (`AeronCluster.java:2475`).
    [[nodiscard]] std::int64_t offer(std::span<const std::uint8_t> message) const noexcept;

    [[nodiscard]] aeron_publication_t* concurrent() const noexcept
    {
        return concurrent_.get();
    }

    [[nodiscard]] aeron_exclusive_publication_t* exclusive() const noexcept
    {
        return exclusive_.get();
    }

    /// Release the reference the client holds (`Publication.close()`).
    void close() noexcept
    {
        concurrent_.reset();
        exclusive_.reset();
    }

private:
    axc::core::PubPtr concurrent_{};
    axc::core::ExPubPtr exclusive_{};
};

/// The connected session, in the C3a shape: field accessors and `close`. Send
/// / poll / new-leader handling arrive with C3b (`AeronCluster.java:112-118`).
class ClusterClient
{
public:
    ClusterClient(ClientConfig config, axc::core::SubPtr egress_subscription, IngressPublication ingress_publication,
                  aeron_image_t* egress_image, std::int64_t cluster_session_id, std::int64_t leadership_term_id,
                  std::int32_t leader_member_id);

    ClusterClient(ClusterClient&&) noexcept = default;
    ClusterClient& operator=(ClusterClient&&) noexcept = default;
    ClusterClient(const ClusterClient&) = delete;
    ClusterClient& operator=(const ClusterClient&) = delete;

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

    [[nodiscard]] bool is_closed() const noexcept
    {
        return closed_;
    }

    /// C3a releases the resources only; the reference first sends a
    /// `SessionCloseRequest` (`AeronCluster.java:290-311`), which is C3b's send
    /// path. Recorded as a deviation in `records/c3a.md`.
    void close() noexcept;

private:
    ClientConfig config_;
    axc::core::SubPtr egress_subscription_{};
    IngressPublication ingress_publication_{};
    aeron_image_t* egress_image_{nullptr};
    std::int64_t cluster_session_id_{NULL_VALUE};
    std::int64_t leadership_term_id_{NULL_VALUE};
    std::int32_t leader_member_id_{NULL_VALUE};
    bool closed_{false};
};

}  // namespace deepmsg::cluster::client
