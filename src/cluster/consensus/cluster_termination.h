#pragma once

#include <cstdint>
#include <span>

#include "cluster/common/cluster_member.h"
#include "cluster/consensus/consensus_publisher.h"

namespace deepmsg::cluster::consensus
{

/// `ClusterTermination` (`ClusterTermination.java:22-85`): the deadline and the
/// per-member termination bookkeeping used by SHUTDOWN/ABORT.
class ClusterTermination
{
public:
    explicit ClusterTermination(std::int64_t deadline_ns) noexcept : deadline_ns_(deadline_ns) {}

    void deadline_ns(std::int64_t deadline_ns) noexcept
    {
        deadline_ns_ = deadline_ns;
    }
    [[nodiscard]] std::int64_t deadline_ns() const noexcept
    {
        return deadline_ns_;
    }

    /// `canTerminate` (`:37-56`): false until the services have acknowledged,
    /// then true when every non-leader member has terminated or the deadline
    /// has passed.
    [[nodiscard]] bool can_terminate(std::span<const common::ClusterMember> members, std::int64_t now_ns) const;

    /// `onServicesTerminated` (`:58-61`).
    void on_services_terminated() noexcept
    {
        is_awaiting_services_ = false;
    }

    /// `terminationPosition` (`:63-84`): tell every other member; a refused
    /// send is the caller's WARN.
    void termination_position(ConsensusPublisher& consensus_publisher, std::span<common::ClusterMember> members,
                              const common::ClusterMember& this_member, std::int64_t leadership_term_id,
                              std::int64_t position);

private:
    std::int64_t deadline_ns_;
    bool is_awaiting_services_{true};
};

}  // namespace deepmsg::cluster::consensus
