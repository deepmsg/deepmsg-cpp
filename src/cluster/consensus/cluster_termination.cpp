#include "cluster/consensus/cluster_termination.h"

namespace deepmsg::cluster::consensus
{

bool ClusterTermination::can_terminate(std::span<const common::ClusterMember> members, std::int64_t now_ns) const
{
    if (is_awaiting_services_)
    {
        return false;
    }

    bool result = true;
    for (const common::ClusterMember& member : members)
    {
        if (!member.is_leader && !member.has_terminated)
        {
            result = false;
            break;
        }
    }

    return result || now_ns >= deadline_ns_;
}

void ClusterTermination::termination_position(ConsensusPublisher& consensus_publisher,
                                              std::span<common::ClusterMember> members,
                                              const common::ClusterMember& this_member, std::int64_t leadership_term_id,
                                              std::int64_t position)
{
    for (common::ClusterMember& member : members)
    {
        member.has_terminated = false;
        if (member.id != this_member.id)
        {
            (void)consensus_publisher.termination_position(member.id, leadership_term_id, position);
        }
    }
}

}  // namespace deepmsg::cluster::consensus
