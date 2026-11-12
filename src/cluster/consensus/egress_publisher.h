#pragma once

#include <cstdint>
#include <span>
#include <string_view>
#include <vector>

#include "cluster/consensus/cluster_session.h"
#include "io_aeron_cluster_codecs/AdminRequestType.h"
#include "io_aeron_cluster_codecs/AdminResponseCode.h"

namespace deepmsg::cluster::consensus
{

/// The egress half of the session seam (`EgressPublisher.java`). Kept as an
/// interface so `SessionManager` can be exercised without a driver; the
/// concrete `EgressPublisher` writes into the session's response publication.
class EgressSender
{
public:
    EgressSender() = default;
    EgressSender(const EgressSender&) = delete;
    EgressSender& operator=(const EgressSender&) = delete;
    virtual ~EgressSender() = default;

    virtual bool send_event(ClusterSession& session, std::int64_t leadership_term_id, std::int32_t leader_member_id,
                            codecs::EventCode::Value code, std::string_view detail) = 0;
    virtual bool send_challenge(ClusterSession& session, std::span<const std::uint8_t> encoded_challenge) = 0;
    virtual bool new_leader(ClusterSession& session, std::int64_t leadership_term_id, std::int32_t leader_member_id,
                            std::string_view ingress_endpoints) = 0;
    virtual bool send_admin_response(ClusterSession& session, std::int64_t correlation_id,
                                     codecs::AdminRequestType::Value request_type,
                                     codecs::AdminResponseCode::Value response_code, std::string_view message) = 0;
};

/// `EgressPublisher` (`EgressPublisher.java:34-331`).
class EgressPublisher final : public EgressSender
{
public:
    explicit EgressPublisher(std::int64_t leader_heartbeat_timeout_ns);

    bool send_event(ClusterSession& session, std::int64_t leadership_term_id, std::int32_t leader_member_id,
                    codecs::EventCode::Value code, std::string_view detail) override;

    bool send_challenge(ClusterSession& session, std::span<const std::uint8_t> encoded_challenge) override;

    bool new_leader(ClusterSession& session, std::int64_t leadership_term_id, std::int32_t leader_member_id,
                    std::string_view ingress_endpoints) override;

    bool send_admin_response(ClusterSession& session, std::int64_t correlation_id,
                             codecs::AdminRequestType::Value request_type,
                             codecs::AdminResponseCode::Value response_code, std::string_view message) override;

private:
    [[nodiscard]] bool offer_message(ClusterSession& session, std::size_t length);

    std::int64_t leader_heartbeat_timeout_ns_{0};
    std::vector<std::uint8_t> buffer_{};
};

}  // namespace deepmsg::cluster::consensus
