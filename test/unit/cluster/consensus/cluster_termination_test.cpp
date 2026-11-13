// C4d: `ClusterTermination.canTerminate` (`ClusterTermination.java:37-56`)
// without a driver. The services' ack gate comes first, the leader is not
// waited for, and the deadline is the backstop. The sending half
// (`terminationPosition`, `:63-84`) needs a real `ConsensusPublisher` and is
// covered by the SHUTDOWN/ABORT interop cases instead.

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "cluster/common/cluster_member.h"
#include "cluster/consensus/cluster_termination.h"

namespace
{

using deepmsg::cluster::common::ClusterMember;
using deepmsg::cluster::consensus::ClusterTermination;

[[nodiscard]] ClusterMember member(std::int32_t id, bool is_leader, bool has_terminated)
{
    ClusterMember result;
    result.id = id;
    result.is_leader = is_leader;
    result.has_terminated = has_terminated;
    return result;
}

TEST(ClusterTerminationTest, cannotTerminateUntilTheServicesAck)
{
    ClusterTermination termination(1'000);
    std::vector<ClusterMember> members{member(0, true, false)};

    EXPECT_FALSE(termination.can_terminate(members, 2'000)) << "the deadline must not bypass the services";

    termination.on_services_terminated();
    EXPECT_TRUE(termination.can_terminate(members, 0));
}

TEST(ClusterTerminationTest, theLeaderIsNotWaitedFor)
{
    ClusterTermination termination(1'000);
    termination.on_services_terminated();
    std::vector<ClusterMember> members{member(0, true, false), member(1, false, true)};

    EXPECT_TRUE(termination.can_terminate(members, 0));
}

TEST(ClusterTerminationTest, aLiveFollowerBlocksUntilItTerminates)
{
    ClusterTermination termination(1'000);
    termination.on_services_terminated();
    std::vector<ClusterMember> members{member(0, true, false), member(1, false, false)};

    EXPECT_FALSE(termination.can_terminate(members, 999));

    members[1].has_terminated = true;
    EXPECT_TRUE(termination.can_terminate(members, 999));
}

TEST(ClusterTerminationTest, theDeadlineIsTheBackstop)
{
    ClusterTermination termination(1'000);
    termination.on_services_terminated();
    std::vector<ClusterMember> members{member(0, true, false), member(1, false, false)};

    EXPECT_TRUE(termination.can_terminate(members, 1'000));
}

}  // namespace
