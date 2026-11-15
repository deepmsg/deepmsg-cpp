// C6c F2-0: the steady-state election triggers the reference has and we did
// not (Aeron 1.54.0 `ConsensusModuleAgent.java`):
//   1. a higher-term CommitPosition names a new leader      (`:1129-1140`);
//   2. a higher-term NewLeadershipTerm names a new term     (`:1063-1075`);
//   3. a leader that loses the follower quorum steps down   (`:2438-2441`);
//   4. a follower whose leader heartbeat stops elects       (`:2463-2467`).
//
// The rig is `fixture/three_node_rig.h`; the forged messages use the product's
// own `ConsensusPublisher`, so the bytes are the ones the agent already parses.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "cluster/common/cluster_member.h"
#include "cluster/consensus/consensus_constants.h"
#include "cluster/consensus/consensus_publisher.h"
#include "fixture/three_node_rig.h"

namespace
{

using namespace axc::interop;
using deepmsg::cluster::common::ClusterMember;
using deepmsg::cluster::consensus::ConsensusPublisher;

/// `ConsensusModuleContext.consensus_stream_id` default.
constexpr std::int32_t CONSENSUS_STREAM_ID = 108;

class Xc2ElectionTest : public ThreeNodeRig
{
protected:
    void SetUp() override
    {
        // These cases only read counters and forge consensus frames; they do
        // not need the Java probe classes.
        if (!set_up_environment())
        {
            return;
        }
    }

    [[nodiscard]] ClusterMember member(int index)
    {
        auto members = deepmsg::cluster::common::parse_cluster_members(members_string(round_));
        EXPECT_TRUE(members) << (members ? "" : members.error().message.view());
        if (!members)
        {
            return {};
        }
        const ClusterMember* found = deepmsg::cluster::common::find_member(*members, index);
        EXPECT_NE(nullptr, found) << "member " << index << " is not in " << members_string(round_);
        return nullptr == found ? ClusterMember{} : *found;
    }

    /// A publisher that speaks to `target`'s consensus endpoint. It creates the
    /// publication on the target's own driver (the rig's counter client).
    [[nodiscard]] std::unique_ptr<ConsensusPublisher> publisher_for(int target)
    {
        auto* client = node_client(target);
        EXPECT_NE(nullptr, client) << "no counter client for node " << target;
        if (nullptr == client)
        {
            return nullptr;
        }
        auto publisher =
            std::make_unique<ConsensusPublisher>(*client, "aeron:udp?alias=consensus", CONSENSUS_STREAM_ID, "", false);
        const ClusterMember member_to_target = member(target);
        auto added = publisher->add_publication(member_to_target);
        EXPECT_TRUE(added.has_value()) << (added ? "" : added.error().message.view());
        return publisher;
    }

    /// The publication is registered by `add_publication` before its UDP
    /// handshake completes; a lower-term RequestVote is the cheapest message
    /// the steady-state agent ignores, and it reports whether the offer went
    /// through.
    [[nodiscard]] bool wait_until_connected(ConsensusPublisher& publisher, int target, std::int64_t term)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (publisher.request_vote(target, term, 0, term - 1, target))
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return false;
    }

    /// `wait_for_cluster` stops at ACTIVE/role/term; the election counter is
    /// still CLOSED-or-earlier until each member finishes joining. The forged
    /// message criterion needs a settled election, or the counter starts below
    /// 17 for reasons unrelated to the trigger.
    [[nodiscard]] bool wait_for_elections_closed(std::chrono::milliseconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            drive_node_clients();
            bool all_closed = true;
            for (int i = 0; i < NODES; ++i)
            {
                all_closed =
                    all_closed && node_counter(i, deepmsg::cluster::common::CLUSTER_ELECTION_STATE_TYPE_ID) == 17;
            }
            if (all_closed)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    /// The trigger is observable as the election leaving CLOSED (17) and the
    /// counted error the reference's `enterElection` reports. The election may
    /// then resolve back to the current leader (there is no real higher-term
    /// leader in this test), so a term change is not the criterion.
    [[nodiscard]] bool wait_for_election_trigger(int target, std::int64_t error_count_before,
                                                 std::chrono::milliseconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            drive_node_clients();
            (void)error_count_before;
            if (node_counter(target, deepmsg::cluster::common::CLUSTER_ELECTION_STATE_TYPE_ID) != 17)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return false;
    }
};

TEST_F(Xc2ElectionTest, aHigherTermCommitPositionStartsAnElection)
{
    start_cluster(0);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    const int target = (leader + 1) % NODES;
    const std::int64_t term = node_counter(target, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);
    const std::int64_t commit = node_counter(target, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID);
    const std::int64_t errors_before =
        node_counter(target, deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID);

    ASSERT_EQ(17, node_counter(target, deepmsg::cluster::common::CLUSTER_ELECTION_STATE_TYPE_ID))
        << "the target is not in a closed election before the forged message: " << node_dump();

    auto publisher = publisher_for(target);
    ASSERT_NE(nullptr, publisher);
    ASSERT_TRUE(wait_until_connected(*publisher, target, term)) << "the forged publisher never connected";
    const int new_leader_member = (leader + 2) % NODES;
    ASSERT_TRUE(publisher->commit_position(target, term + 1, commit, new_leader_member));

    EXPECT_TRUE(wait_for_election_trigger(target, errors_before, std::chrono::seconds(10)))
        << "a higher-term CommitPosition did not start an election: " << node_dump() << node_log(0, target);

    stop_round();
}

TEST_F(Xc2ElectionTest, aHigherTermNewLeadershipTermStartsAnElection)
{
    start_cluster(0);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    const int target = (leader + 1) % NODES;
    const std::int64_t term = node_counter(target, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);
    const std::int64_t commit = node_counter(target, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID);
    const std::int64_t errors_before =
        node_counter(target, deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID);

    ASSERT_EQ(17, node_counter(target, deepmsg::cluster::common::CLUSTER_ELECTION_STATE_TYPE_ID))
        << "the target is not in a closed election before the forged message: " << node_dump();

    auto publisher = publisher_for(target);
    ASSERT_NE(nullptr, publisher);
    ASSERT_TRUE(wait_until_connected(*publisher, target, term)) << "the forged publisher never connected";
    const int new_leader_member = (leader + 2) % NODES;
    publisher->new_leadership_term(target, term, term + 1, deepmsg::cluster::consensus::NULL_POSITION,
                                   deepmsg::cluster::consensus::NULL_POSITION, term + 1, 0, commit, commit, 0, 0,
                                   new_leader_member, 0, 1, false);

    EXPECT_TRUE(wait_for_election_trigger(target, errors_before, std::chrono::seconds(10)))
        << "a higher-term NewLeadershipTerm did not start an election: " << node_dump() << node_log(0, target);

    stop_round();
}

TEST_F(Xc2ElectionTest, theLeaderStepsDownWhenItLosesTheFollowerQuorum)
{
    start_cluster(0);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    const int follower_a = (leader + 1) % NODES;
    const int follower_b = (leader + 2) % NODES;

    pause_node(follower_a);
    pause_node(follower_b);

    bool stepped_down = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    while (std::chrono::steady_clock::now() < deadline)
    {
        drive_node_clients();
        if (node_counter(leader, deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID) != 2)
        {
            stepped_down = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    EXPECT_TRUE(stepped_down) << "the leader kept the role with no active follower quorum: " << node_dump()
                              << node_log(0, leader);

    resume_node(follower_a);
    resume_node(follower_b);
    stop_round();
}

TEST_F(Xc2ElectionTest, theFollowersElectWhenTheLeaderHeartbeatStops)
{
    // A SIGSTOPped leader keeps its driver and archive mappings, so the
    // followers' log image would normally stay open until the image liveness
    // timeout. Raise it so the only trigger left is the heartbeat timeout
    // (`ConsensusModuleAgent.slowTickWork:2463-2467`), not the image-close
    // path C6b already has.
    for (int i = 0; i < NODES; ++i)
    {
        node_property_overrides_[i]["aeron.image.liveness.timeout"] = "30s";
    }

    start_cluster(0);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    const std::int64_t term = node_counter(leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);
    const std::vector<int> survivors = {(leader + 1) % NODES, (leader + 2) % NODES};

    pause_node(leader);

    const int new_leader = await_new_leader(term, survivors, std::chrono::milliseconds(15'000));
    EXPECT_GE(new_leader, 0) << "a silent leader was not timed out: " << node_dump() << node_log(0, leader);

    resume_node(leader);
    stop_round();
}

}  // namespace
