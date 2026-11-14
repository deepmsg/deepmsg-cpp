// C6b: the follower path — a leader dying, a follower rejoining, a follower
// several terms behind (log replication), a truncated recording log, and a
// follower terminating on SHUTDOWN. The rig is `fixture/three_node_rig.h`.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include "fixture/three_node_rig.h"

namespace
{

using namespace axc::interop;

class Xc2FollowerPathTest : public ThreeNodeRig
{
};

TEST_F(Xc2FollowerPathTest, aRestartedFollowerRejoins)
{
    start_cluster(0);

    const auto before = run_echo(0, ECHO_MESSAGES, std::chrono::seconds(60));
    ASSERT_EQ(0, before.exit_code) << before.output;
    ASSERT_NE(std::string::npos, before.output.find("RECEIVED " + std::to_string(ECHO_MESSAGES))) << before.output;

    const int leader = leader_index();
    ASSERT_GE(leader, 0);
    const int follower = (leader + 1) % NODES;
    kill_hard(containers_[follower]);
    kill_hard(nodes_[follower]);
    node_clients_[follower].reset();

    pause_for(std::chrono::milliseconds(11'000));
    start_node(0, follower);
    // The container must be subscribing before the CM's leader replay finishes
    // and it sends JoinLog, or the single JoinLog is lost and the service never
    // acks. Start it with the node, then let the driver replace the old CnC
    // before any counter client connects.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    start_container(0, follower);
    std::this_thread::sleep_for(std::chrono::seconds(2));

    bool rejoined = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (std::chrono::steady_clock::now() < deadline)
    {
        drive_node_clients();
        const std::int64_t leader_commit =
            node_counter(leader, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID);
        const std::int64_t follower_commit =
            node_counter(follower, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID);
        if (1 == node_counter(follower, deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID) &&
            node_counter(follower, deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID) == 0 &&
            follower_commit >= leader_commit - 64)
        {
            rejoined = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_TRUE(rejoined) << node_dump() << node_log(0, follower);

    const auto after = run_echo(0, ECHO_MESSAGES, std::chrono::seconds(60));
    EXPECT_EQ(0, after.exit_code) << after.output;
    EXPECT_NE(std::string::npos, after.output.find("RECEIVED " + std::to_string(ECHO_MESSAGES))) << after.output;
    stop_round();
}

TEST_F(Xc2FollowerPathTest, theLeaderDiesAndTheSurvivorsElectANewTerm)
{
    start_cluster(0);

    const auto before = run_echo(0, ECHO_MESSAGES, std::chrono::seconds(60));
    ASSERT_EQ(0, before.exit_code) << before.output;
    ASSERT_NE(std::string::npos, before.output.find("RECEIVED " + std::to_string(ECHO_MESSAGES))) << before.output;

    const int old_leader = leader_index();
    ASSERT_GE(old_leader, 0);
    const std::int64_t old_term =
        node_counter(old_leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    kill_hard(containers_[old_leader]);
    kill_hard(nodes_[old_leader]);
    node_clients_[old_leader].reset();

    bool elected = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (std::chrono::steady_clock::now() < deadline)
    {
        drive_node_clients();
        int leaders = 0;
        int new_leader = -1;
        for (int i = 0; i < NODES; ++i)
        {
            if (i == old_leader)
            {
                continue;
            }
            if (node_counter(i, deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID) == 2 &&
                node_counter(i, deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID) == 1)
            {
                ++leaders;
                new_leader = i;
            }
        }
        if (1 == leaders &&
            node_counter(new_leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID) > old_term)
        {
            elected = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_TRUE(elected) << node_dump();

    // A second election can start while the first new leader is still joining;
    // wait until both survivors agree on one closed-term leader before the
    // client connects.
    bool stable = false;
    const auto stable_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(45);
    while (std::chrono::steady_clock::now() < stable_deadline)
    {
        drive_node_clients();
        int leaders = 0;
        std::int64_t term = -2;
        std::int64_t commit = -2;
        bool same = true;
        for (int i = 0; i < NODES; ++i)
        {
            if (i == old_leader)
            {
                continue;
            }
            const std::int64_t node_term =
                node_counter(i, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);
            const std::int64_t node_commit = node_counter(i, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID);
            const std::int64_t election = node_counter(i, deepmsg::cluster::common::CLUSTER_ELECTION_STATE_TYPE_ID);
            leaders += 2 == node_counter(i, deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID) ? 1 : 0;
            if (-2 == term)
            {
                term = node_term;
                commit = node_commit;
            }
            same = same && (node_term == term) && (node_commit == commit) && 17 == election;
        }
        if (1 == leaders && same)
        {
            stable = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_TRUE(stable) << node_dump();

    const auto after = run_echo(0, ECHO_MESSAGES, std::chrono::seconds(60));
    EXPECT_EQ(0, after.exit_code) << after.output;
    EXPECT_NE(std::string::npos, after.output.find("RECEIVED " + std::to_string(ECHO_MESSAGES))) << after.output;
    stop_round();
}

TEST_F(Xc2FollowerPathTest, aFollowerMultipleTermsBehindReplicatesTheLog)
{
    // The shape of the reference's
    // `ClusterTest.shouldRecoverWhenFollowerIsMultipleTermsBehind`: the log
    // overtakes a node by more than one leadership term, so it comes back
    // through `FOLLOWER_LOG_REPLICATION` rather than a replay.
    start_cluster(0);

    const auto first = run_echo(0, ECHO_MESSAGES, std::chrono::seconds(60));
    ASSERT_EQ(0, first.exit_code) << first.output;
    ASSERT_NE(std::string::npos, first.output.find("RECEIVED " + std::to_string(ECHO_MESSAGES))) << first.output;

    const int original_leader = leader_index();
    ASSERT_GE(original_leader, 0);
    const std::int64_t first_term =
        node_counter(original_leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    // Term 2: the original leader is gone (a clean stop, as the reference's
    // `stopNode`).
    stop_node_gracefully(original_leader);

    std::vector<int> live;
    for (int i = 0; i < NODES; ++i)
    {
        if (i != original_leader)
        {
            live.push_back(i);
        }
    }
    const int second_leader = await_new_leader(first_term, live, std::chrono::seconds(60));
    ASSERT_GE(second_leader, 0) << node_dump();

    const auto second = run_echo(0, ECHO_MESSAGES, std::chrono::seconds(60));
    EXPECT_EQ(0, second.exit_code) << second.output;
    EXPECT_NE(std::string::npos, second.output.find("RECEIVED " + std::to_string(ECHO_MESSAGES))) << second.output;

    const std::int64_t second_term =
        node_counter(second_leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    // Term 3: the second leader restarts at once, so the other survivor and it
    // still hold a quorum.
    stop_node_gracefully(second_leader);
    start_node(0, second_leader);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    start_container(0, second_leader);

    const int third_leader = await_new_leader(second_term, live, std::chrono::seconds(60));
    ASSERT_GE(third_leader, 0) << node_dump();

    const auto third = run_echo(0, ECHO_MESSAGES, std::chrono::seconds(60));
    EXPECT_EQ(0, third.exit_code) << third.output;
    EXPECT_NE(std::string::npos, third.output.find("RECEIVED " + std::to_string(ECHO_MESSAGES))) << third.output;

    // The original leader returns several terms behind: it must replicate the
    // leader's log rather than run a replay of its own tail.
    start_node(0, original_leader);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    start_container(0, original_leader);

    bool saw_replication = false;
    bool caught_up = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(90);
    while (std::chrono::steady_clock::now() < deadline)
    {
        drive_node_clients();
        saw_replication =
            saw_replication ||
            (9 == node_counter(original_leader, deepmsg::cluster::common::CLUSTER_ELECTION_STATE_TYPE_ID));
        const std::int64_t leader_commit =
            node_counter(third_leader, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID);
        if (leader_commit > 0)
        {
            bool all_active = true;
            bool same_term = true;
            bool same_commit = true;
            bool elections_closed = true;
            int leaders = 0;
            std::int64_t term = -2;
            for (int i = 0; i < NODES; ++i)
            {
                const std::int64_t state =
                    node_counter(i, deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID);
                const std::int64_t node_term =
                    node_counter(i, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);
                const std::int64_t node_commit =
                    node_counter(i, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID);
                const std::int64_t election =
                    node_counter(i, deepmsg::cluster::common::CLUSTER_ELECTION_STATE_TYPE_ID);
                all_active = all_active && (1 == state);
                // A node finishing its catchup still runs an Election, and an
                // Election is all its CM does: it answers no ingress until the
                // election closes. Wait for that, not just the counters.
                elections_closed = elections_closed && 17 == election;
                if (-2 == term)
                {
                    term = node_term;
                }
                same_term = same_term && node_term == term;
                same_commit = same_commit && node_commit == leader_commit;
                leaders += 2 == node_counter(i, deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID) ? 1 : 0;
            }
            caught_up = all_active && 1 == leaders && same_term && same_commit && elections_closed;
        }
        if (caught_up)
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_TRUE(saw_replication) << node_dump();
    EXPECT_TRUE(caught_up) << node_dump();

    const auto after = run_echo_until_served(0, ECHO_MESSAGES, std::chrono::seconds(60));
    EXPECT_EQ(0, after.exit_code) << after.output << node_dump();
    EXPECT_NE(std::string::npos, after.output.find("RECEIVED " + std::to_string(ECHO_MESSAGES)))
        << after.output << node_dump();
    stop_round();
}

TEST_F(Xc2FollowerPathTest, aFollowerTerminatesOnShutdown)
{
    // `ClusterControl.ToggleState.SHUTDOWN`: the leader appends the shutdown
    // action and sends the termination position; every follower acknowledges
    // it and leaves, and the leader leaves only once every ack is in.
    start_cluster(0);

    const auto before = run_echo(0, ECHO_MESSAGES, std::chrono::seconds(60));
    ASSERT_EQ(0, before.exit_code) << before.output;

    const int leader = leader_index();
    ASSERT_GE(leader, 0);

    constexpr std::int32_t CLUSTER_CONTROL_SHUTDOWN = 5;
    set_control_toggle(leader, CLUSTER_CONTROL_SHUTDOWN);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    bool all_exited = false;
    while (std::chrono::steady_clock::now() < deadline)
    {
        for (auto& client : node_clients_)
        {
            if (client)
            {
                (void)client->do_work();
            }
        }
        all_exited = true;
        for (int i = 0; i < NODES; ++i)
        {
            if (nodes_[i] && nodes_[i]->running())
            {
                all_exited = false;
            }
        }
        if (all_exited)
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    ASSERT_TRUE(all_exited) << "not every node terminated:" << node_dump();
    for (int i = 0; i < NODES; ++i)
    {
        EXPECT_EQ(0, nodes_[i] ? nodes_[i]->exit_code() : -1)
            << "node " << i << " did not exit cleanly" << node_log(0, i);
    }
    // The containers leave with their nodes.
    for (int i = 0; i < NODES; ++i)
    {
        if (containers_[i])
        {
            const auto container_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
            while (containers_[i]->running() && std::chrono::steady_clock::now() < container_deadline)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
            EXPECT_FALSE(containers_[i]->running()) << "container " << i << " did not die";
        }
    }
    stop_round();
}

TEST_F(Xc2FollowerPathTest, aTruncatedRecordingLogStillRecovers)
{
    // The first arm of the reference's `StartFromTruncatedRecordingLogTest`:
    // after a snapshot, each node's `recording.log` is rewritten to the last
    // TERM entry plus the latest service and configuration snapshots, the
    // mark files and the unreferenced segments are dropped, and the cluster
    // must elect and serve again. Two rounds.
    start_cluster(0);

    const auto initial = run_echo(0, ECHO_MESSAGES, std::chrono::seconds(60));
    ASSERT_EQ(0, initial.exit_code) << initial.output;
    ASSERT_NE(std::string::npos, initial.output.find("RECEIVED " + std::to_string(ECHO_MESSAGES))) << initial.output;

    constexpr std::int32_t CLUSTER_CONTROL_SNAPSHOT = 4;
    for (int round = 0; round < 2; ++round)
    {
        const int leader = leader_index();
        ASSERT_GE(leader, 0) << "round " << round << node_dump();
        const std::int64_t snapshots_before =
            node_counter(leader, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);
        set_control_toggle(leader, CLUSTER_CONTROL_SNAPSHOT);

        bool taken = false;
        const auto snapshot_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (std::chrono::steady_clock::now() < snapshot_deadline)
        {
            drive_node_clients();
            if (node_counter(leader, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID) > snapshots_before)
            {
                taken = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        ASSERT_TRUE(taken) << "round " << round << ": no snapshot was taken" << node_dump();

        stop_round();
        for (int i = 0; i < NODES; ++i)
        {
            truncate_recording_log(0, i);
        }
        start_round(0);

        const auto echo = run_echo(0, ECHO_MESSAGES, std::chrono::seconds(60));
        EXPECT_EQ(0, echo.exit_code) << "round " << round << ": " << echo.output << node_dump();
        EXPECT_NE(std::string::npos, echo.output.find("RECEIVED " + std::to_string(ECHO_MESSAGES)))
            << "round " << round << ": " << echo.output;
    }
    stop_round();
}

}  // namespace
