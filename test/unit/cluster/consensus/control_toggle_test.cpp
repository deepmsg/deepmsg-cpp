// C4e: the cluster and node control-toggle values, their guarded CAS and the
// activation states the reference uses around an election.

#include <gtest/gtest.h>

#include <cstdint>

#include "cluster/consensus/cluster_control.h"
#include "cluster/consensus/node_control.h"

using deepmsg::cluster::consensus::cluster_control_code;
using deepmsg::cluster::consensus::cluster_control_compare_and_set;
using deepmsg::cluster::consensus::cluster_control_state;
using deepmsg::cluster::consensus::cluster_control_store;
using deepmsg::cluster::consensus::cluster_control_toggle;
using deepmsg::cluster::consensus::ClusterControlState;
using deepmsg::cluster::consensus::node_control_code;
using deepmsg::cluster::consensus::node_control_compare_and_set;
using deepmsg::cluster::consensus::node_control_state;
using deepmsg::cluster::consensus::node_control_store;
using deepmsg::cluster::consensus::node_control_toggle;
using deepmsg::cluster::consensus::NodeControlState;

TEST(ClusterControlTest, theCodesFollowTheReferenceOrdinals)
{
    EXPECT_EQ(0, cluster_control_code(ClusterControlState::inactive));
    EXPECT_EQ(1, cluster_control_code(ClusterControlState::neutral));
    EXPECT_EQ(2, cluster_control_code(ClusterControlState::suspend));
    EXPECT_EQ(3, cluster_control_code(ClusterControlState::resume));
    EXPECT_EQ(4, cluster_control_code(ClusterControlState::snapshot));
    EXPECT_EQ(5, cluster_control_code(ClusterControlState::shutdown));
    EXPECT_EQ(6, cluster_control_code(ClusterControlState::abort));
    EXPECT_EQ(7, cluster_control_code(ClusterControlState::standby_snapshot));
}

TEST(ClusterControlTest, anOutOfRangeValueIsAnError)
{
    EXPECT_TRUE(cluster_control_state(0).has_value());
    EXPECT_TRUE(cluster_control_state(7).has_value());
    EXPECT_FALSE(cluster_control_state(-1).has_value());
    EXPECT_FALSE(cluster_control_state(8).has_value());
}

TEST(ClusterControlTest, aToggleOnlyLeavesNeutralOrSuspendsForResume)
{
    std::int64_t neutral = 1;
    EXPECT_TRUE(cluster_control_toggle(&neutral, ClusterControlState::snapshot));
    EXPECT_EQ(4, neutral);

    std::int64_t snapshot = 4;
    EXPECT_FALSE(cluster_control_toggle(&snapshot, ClusterControlState::suspend));
    EXPECT_EQ(4, snapshot);

    // `ClusterControl.ToggleState.toggle` (`:118-126`): RESUME also accepts
    // SUSPEND; from NEUTRAL it goes through the ordinary CAS.
    std::int64_t suspended = 2;
    EXPECT_TRUE(cluster_control_toggle(&suspended, ClusterControlState::resume));
    EXPECT_EQ(3, suspended);

    std::int64_t neutral_resume = 1;
    EXPECT_TRUE(cluster_control_toggle(&neutral_resume, ClusterControlState::resume));
    EXPECT_EQ(3, neutral_resume);
}

TEST(ClusterControlTest, activateDeactivateAndResetWriteTheReferenceStates)
{
    std::int64_t value = 99;
    cluster_control_store(&value, ClusterControlState::inactive);
    EXPECT_EQ(0, value);
    cluster_control_store(&value, ClusterControlState::neutral);
    EXPECT_EQ(1, value);

    EXPECT_TRUE(cluster_control_compare_and_set(&value, 1, 2));
    EXPECT_EQ(2, value);
    EXPECT_FALSE(cluster_control_compare_and_set(&value, 1, 3));
    EXPECT_EQ(2, value);
}

TEST(NodeControlTest, theCodesFollowTheReferenceOrdinals)
{
    EXPECT_EQ(0, node_control_code(NodeControlState::inactive));
    EXPECT_EQ(1, node_control_code(NodeControlState::neutral));
    EXPECT_EQ(2, node_control_code(NodeControlState::replicate_standby_snapshot));
    EXPECT_EQ(3, node_control_code(NodeControlState::validate_recording_log));
}

TEST(NodeControlTest, anOutOfRangeValueIsAnError)
{
    EXPECT_TRUE(node_control_state(0).has_value());
    EXPECT_TRUE(node_control_state(3).has_value());
    EXPECT_FALSE(node_control_state(-1).has_value());
    EXPECT_FALSE(node_control_state(4).has_value());
}

TEST(NodeControlTest, aToggleOnlyLeavesNeutral)
{
    std::int64_t value = 1;
    EXPECT_TRUE(node_control_toggle(&value, NodeControlState::replicate_standby_snapshot));
    EXPECT_EQ(2, value);
    EXPECT_FALSE(node_control_toggle(&value, NodeControlState::validate_recording_log));
    EXPECT_EQ(2, value);

    std::int64_t inactive = 0;
    EXPECT_FALSE(node_control_toggle(&inactive, NodeControlState::validate_recording_log));
    EXPECT_EQ(0, inactive);
}

TEST(NodeControlTest, resetAndActivateWriteNeutral)
{
    std::int64_t value = 3;
    node_control_store(&value, NodeControlState::neutral);
    EXPECT_EQ(1, value);
    node_control_store(&value, NodeControlState::inactive);
    EXPECT_EQ(0, value);
}
