#include "cluster/client/cluster_client.h"

#include <utility>

namespace deepmsg::cluster::client
{

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

ClusterClient::ClusterClient(ClientConfig config, axc::core::SubPtr egress_subscription,
                             IngressPublication ingress_publication, aeron_image_t* egress_image,
                             std::int64_t cluster_session_id, std::int64_t leadership_term_id,
                             std::int32_t leader_member_id)
    : config_(std::move(config)),
      egress_subscription_(std::move(egress_subscription)),
      ingress_publication_(std::move(ingress_publication)),
      egress_image_(egress_image),
      cluster_session_id_(cluster_session_id),
      leadership_term_id_(leadership_term_id),
      leader_member_id_(leader_member_id)
{
}

void ClusterClient::close() noexcept
{
    if (closed_)
    {
        return;
    }

    ingress_publication_.close();
    egress_subscription_.reset();
    closed_ = true;
}

}  // namespace deepmsg::cluster::client
