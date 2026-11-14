// C6a: three nodes elect, commit, and survive the loss of one follower. The
// rig (three `deepmsg-cluster-node --embedded-driver` processes, three C++
// echo containers, one Java client driver) is `fixture/three_node_rig.h`.

#include <gtest/gtest.h>

#include "fixture/three_node_rig.h"

namespace
{

using namespace axc::interop;

class Xc2ThreeNodesTest : public ThreeNodeRig
{
};

TEST_F(Xc2ThreeNodesTest, theClusterElectsCommitsAndSurvivesAFollowerLoss)
{
    for (int round = 0; round < rounds_to_run(); ++round)
    {
        ASSERT_NO_FATAL_FAILURE(run_round(round)) << "round " << round;
    }
}

}  // namespace
