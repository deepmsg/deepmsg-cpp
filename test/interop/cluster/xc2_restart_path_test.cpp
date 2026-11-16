// C6c F1 batch 1: restart and catch-up paths that need only the three-node
// rig and the in-process client. The reference cases are
// `ClusterTest.shouldStopFollowerAndRestartFollowerGracefully`,
// `shouldStopLeaderAndRestartAsFollowerWithSendingAfter{ThenStopLeader}`,
// `shouldAcceptMessagesAfterTwoNodeCleanRestart`,
// `shouldTolerateMultipleLeaderFailures`,
// `shouldRecoverAfterTwoLeaderNodesFailAndComeBackUpAtSameTime`,
// `shouldRecoverQuicklyAfterKillingFollowersThenRestartingOne` and the C6c
// catch-up batch, including
// `shouldRecoverWhenFollowerIsMultipleTermsBehindFromEmptyLog`.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include "fixture/cluster_client_session.h"
#include "fixture/three_node_rig.h"

namespace
{

using namespace axc::interop;

[[nodiscard]] bool send_echoes(ClusterClientSession& session, int count, int base = 0)
{
    for (int i = 0; i < count; ++i)
    {
        const std::string payload = "f1-" + std::to_string(base + i);
        if (!session.offer_and_await(payload, static_cast<std::size_t>(base + i + 1)))
        {
            return false;
        }
    }
    return true;
}

class Xc2RestartPathTest : public ThreeNodeRig
{
protected:
    void SetUp() override
    {
        if (!set_up_environment())
        {
            return;
        }
    }

    [[nodiscard]] std::string ingress_endpoints_for(int round)
    {
        const Ports ports = ports_for(round);
        std::string endpoints;
        for (int i = 0; i < NODES; ++i)
        {
            if (0 != i)
            {
                endpoints.push_back(',');
            }
            endpoints.append(std::to_string(i)).append("=localhost:").append(std::to_string(ports.ingress[i]));
        }
        return endpoints;
    }

    [[nodiscard]] std::string egress_channel_for(int round)
    {
        return "aeron:udp?endpoint=localhost:" + std::to_string(ports_for(round).client_egress);
    }

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

    [[nodiscard]] bool connect_udp_retry(ClusterClientSession& session)
    {
        for (int attempt = 0; attempt < 3; ++attempt)
        {
            if (session.connect_udp(client_aeron_dir_, ingress_endpoints_for(round_), egress_channel_for(round_)))
            {
                return true;
            }
            session.close();
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        return false;
    }

    [[nodiscard]] bool wait_for_not_leader(int index, std::chrono::milliseconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            drive_node_clients();
            if (node_counter(index, deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID) != 2)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    /// Wait until the live members have settled: every live member is ACTIVE
    /// and its election is CLOSED (17), all live members are at one term, and
    /// exactly one of them is LEADER. `min_term` requires that term to be above
    /// the one before the event under test. `trace` keeps the distinct snapshots
    /// so a failure says whether the members were moving or parked.
    [[nodiscard]] bool wait_for_settled(std::chrono::milliseconds timeout, std::int64_t min_term, std::string& trace)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        std::string last;
        while (std::chrono::steady_clock::now() < deadline)
        {
            drive_node_clients();
            int live = 0;
            int leaders = 0;
            bool all_active = true;
            std::int64_t term = -1;
            std::string line;
            for (int i = 0; i < NODES; ++i)
            {
                line.append(" n").append(std::to_string(i)).append("=");
                if (!nodes_[i])
                {
                    line.append("stopped");
                    continue;
                }
                ++live;
                const std::int64_t state =
                    node_counter(i, deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID);
                const std::int64_t role = node_counter(i, deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID);
                const std::int64_t member_term =
                    node_counter(i, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);
                const std::int64_t election = node_counter(i, deepmsg::cluster::common::CLUSTER_ELECTION_STATE_TYPE_ID);
                line.append(std::to_string(state))
                    .append("/")
                    .append(std::to_string(role))
                    .append("/")
                    .append(std::to_string(member_term))
                    .append("/")
                    .append(std::to_string(election));
                all_active = all_active && 1 == state && 17 == election;
                leaders += 2 == role ? 1 : 0;
                if (-1 == term)
                {
                    term = member_term;
                }
                else if (term != member_term)
                {
                    all_active = false;
                }
            }
            if (line != last)
            {
                if (trace.size() < 8192)
                {
                    trace.append(line).append("\n");
                }
                last = line;
            }
            if (live > 0 && all_active && 1 == leaders && term > min_term)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    [[nodiscard]] bool wait_for_role(int index, std::int32_t role, std::chrono::milliseconds timeout)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            drive_node_clients();
            if (node_counter(index, deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID) == role &&
                node_counter(index, deepmsg::cluster::common::CLUSTER_ELECTION_STATE_TYPE_ID) == 17)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }
};

TEST_F(Xc2RestartPathTest, shouldStopFollowerAndRestartFollowerGracefully)
{
    start_cluster(20);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    const int follower = (leader + 1) % NODES;

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();
    ASSERT_TRUE(send_echoes(session, 10)) << node_dump();
    session.close();

    stop_node_gracefully(follower);

    // The reference only stops the follower and waits for service termination;
    // the cluster keeps serving with the leader and the other follower.
    ClusterClientSession after;
    ASSERT_TRUE(connect_udp_retry(after)) << node_dump() << after.last_error();
    EXPECT_TRUE(send_echoes(after, 10)) << node_dump();
    after.close();

    stop_round();
}

TEST_F(Xc2RestartPathTest, shouldCatchupFromEmptyLog)
{
    start_cluster(27);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    const int follower = (leader + 2) % NODES;
    ASSERT_TRUE(wait_for_role(follower, 0, std::chrono::seconds(30))) << node_dump();
    const std::int64_t term = node_counter(leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    ClusterClientSession first;
    ASSERT_TRUE(connect_udp_retry(first)) << node_dump() << first.last_error();
    ASSERT_TRUE(send_echoes(first, 10)) << node_dump();

    stop_node_gracefully(follower);
    ASSERT_TRUE(send_echoes(first, 10, 10)) << node_dump();
    const std::int64_t commit = node_counter(leader, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID);
    first.close();

    restart_node(round_, follower, true);
    ASSERT_TRUE(wait_for_node_active(follower, std::chrono::seconds(30))) << node_dump();

    std::string trace;
    ASSERT_TRUE(wait_for_settled(std::chrono::seconds(30), term - 1, trace)) << "the empty follower never caught up: \n"
                                                                             << trace << node_dump();
    EXPECT_GE(node_counter(follower, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID), commit) << node_dump();

    ClusterClientSession second;
    ASSERT_TRUE(connect_udp_retry(second)) << node_dump() << second.last_error();
    EXPECT_TRUE(send_echoes(second, 5)) << node_dump();
    second.close();

    stop_round();
}

TEST_F(Xc2RestartPathTest, shouldRecoverWhenFollowerArrivesPartWayThroughTerm)
{
    start_cluster(28);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    const int follower = (leader + 2) % NODES;
    ASSERT_TRUE(wait_for_role(follower, 0, std::chrono::seconds(30))) << node_dump();
    const std::int64_t term = node_counter(leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    ClusterClientSession first;
    ASSERT_TRUE(connect_udp_retry(first)) << node_dump() << first.last_error();
    ASSERT_TRUE(send_echoes(first, 5)) << node_dump();
    stop_node_gracefully(follower);
    ASSERT_TRUE(send_echoes(first, 5, 5)) << node_dump();
    const std::int64_t commit = node_counter(leader, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID);
    first.close();

    restart_node(round_, follower, false);
    ASSERT_TRUE(wait_for_node_active(follower, std::chrono::seconds(30))) << node_dump();

    std::string trace;
    ASSERT_TRUE(wait_for_settled(std::chrono::seconds(30), term - 1, trace))
        << "the part-way follower never caught up: \n"
        << trace << node_dump();
    EXPECT_GE(node_counter(follower, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID), commit) << node_dump();

    ClusterClientSession second;
    ASSERT_TRUE(connect_udp_retry(second)) << node_dump() << second.last_error();
    EXPECT_TRUE(send_echoes(second, 5)) << node_dump();
    second.close();

    stop_round();
}

TEST_F(Xc2RestartPathTest, shouldCatchUpTwoFreshNodesAfterRestart)
{
    start_cluster(29);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    const int follower_a = (leader + 1) % NODES;
    const int follower_b = (leader + 2) % NODES;
    const std::int64_t term = node_counter(leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    ClusterClientSession first;
    ASSERT_TRUE(connect_udp_retry(first)) << node_dump() << first.last_error();
    ASSERT_TRUE(send_echoes(first, 20)) << node_dump();
    const std::int64_t commit = node_counter(leader, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID);
    first.close();

    stop_node_gracefully(leader);
    stop_node_gracefully(follower_a);
    stop_node_gracefully(follower_b);

    restart_node(round_, leader, false);
    restart_node(round_, follower_a, true);
    restart_node(round_, follower_b, true);

    for (int i = 0; i < NODES; ++i)
    {
        ASSERT_TRUE(wait_for_node_active(i, std::chrono::seconds(30))) << node_dump();
    }

    std::string trace;
    ASSERT_TRUE(wait_for_settled(std::chrono::seconds(30), term - 1, trace))
        << "the two fresh nodes never caught up: \n"
        << trace << node_dump();
    for (int i = 0; i < NODES; ++i)
    {
        EXPECT_GE(node_counter(i, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID), commit) << node_dump();
    }

    ClusterClientSession second;
    ASSERT_TRUE(connect_udp_retry(second)) << node_dump() << second.last_error();
    EXPECT_TRUE(send_echoes(second, 5)) << node_dump();
    second.close();

    stop_round();
}

TEST_F(Xc2RestartPathTest, shouldRecoverWhileMessagesContinue)
{
    start_cluster(30);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    const int follower = (leader + 2) % NODES;
    const std::int64_t term = node_counter(leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    ClusterClientSession first;
    ASSERT_TRUE(connect_udp_retry(first)) << node_dump() << first.last_error();
    ASSERT_TRUE(send_echoes(first, 5)) << node_dump();
    stop_node_gracefully(follower);

    restart_node(round_, follower, true);
    ASSERT_TRUE(wait_for_node_active(follower, std::chrono::seconds(30))) << node_dump();
    // Keep sending while the restarted follower is catching up.
    ASSERT_TRUE(send_echoes(first, 10, 5)) << node_dump();
    const std::int64_t commit = node_counter(leader, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID);
    first.close();

    std::string trace;
    ASSERT_TRUE(wait_for_settled(std::chrono::seconds(30), term - 1, trace))
        << "members never settled while messages continued: \n"
        << trace << node_dump();
    EXPECT_GE(node_counter(follower, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID), commit) << node_dump();

    stop_round();
}

// ClusterTest.shouldRecoverWhenFollowerIsMultipleTermsBehindFromEmptyLog: the
// reference starts four nodes, so after the original leader stops two
// leadership terms can pass and the two remaining nodes keep the quorum. With
// three nodes the clean restart of the second leader is also what restores the
// quorum; the point stays the same: the original leader comes back from an
// empty log two terms behind and must catch up through both.
TEST_F(Xc2RestartPathTest, shouldRecoverWhenFollowerIsMultipleTermsBehindFromEmptyLog)
{
    start_cluster(31);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int original_leader = leader_index();
    ASSERT_GE(original_leader, 0) << node_dump();
    const int member_a = (original_leader + 1) % NODES;
    const int member_b = (original_leader + 2) % NODES;
    const std::int64_t first_term =
        node_counter(original_leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    ClusterClientSession first;
    ASSERT_TRUE(connect_udp_retry(first)) << node_dump() << first.last_error();
    ASSERT_TRUE(send_echoes(first, 10)) << node_dump();
    first.close();

    // Term two: the original leader goes away and one survivor takes over.
    stop_node_gracefully(original_leader);
    const int second_leader = await_new_leader(first_term, {member_a, member_b}, std::chrono::seconds(20));
    ASSERT_GE(second_leader, 0) << node_dump();
    const std::int64_t second_term =
        node_counter(second_leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    ClusterClientSession second;
    ASSERT_TRUE(connect_udp_retry(second)) << node_dump() << second.last_error();
    ASSERT_TRUE(send_echoes(second, 10)) << node_dump();
    second.close();

    // Term three: the second leader goes away and comes back clean. The only
    // survivor has no quorum left, so this restart is what lets the remaining
    // two members elect.
    stop_node_gracefully(second_leader);
    restart_node(round_, second_leader, true);
    const int third_leader = await_new_leader(second_term, {member_a, member_b}, std::chrono::seconds(30));
    ASSERT_GE(third_leader, 0) << node_dump();
    const std::int64_t third_term =
        node_counter(third_leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    ClusterClientSession third;
    ASSERT_TRUE(connect_udp_retry(third)) << node_dump() << third.last_error();
    ASSERT_TRUE(send_echoes(third, 10)) << node_dump();
    third.close();

    // The original leader rejoins from an empty log, two terms behind.
    restart_node(round_, original_leader, true);
    ASSERT_TRUE(wait_for_node_active(original_leader, std::chrono::seconds(30))) << node_dump();
    ASSERT_TRUE(wait_for_role(original_leader, 0, std::chrono::seconds(30)))
        << "the late joiner did not settle as a follower: " << node_dump();

    ClusterClientSession fourth;
    ASSERT_TRUE(connect_udp_retry(fourth)) << node_dump() << fourth.last_error();
    ASSERT_TRUE(send_echoes(fourth, 10)) << node_dump();
    const std::int64_t commit = node_counter(third_leader, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID);
    fourth.close();

    ASSERT_TRUE(await_commit(original_leader, commit, std::chrono::seconds(30)))
        << "the late joiner never reached commit " << commit << ": " << node_dump();

    std::string trace;
    EXPECT_TRUE(wait_for_settled(std::chrono::seconds(30), third_term - 1, trace))
        << "members never settled after the late join: \n"
        << trace << node_dump();
    EXPECT_GE(node_counter(original_leader, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID), commit)
        << node_dump();

    stop_round();
}

TEST_F(Xc2RestartPathTest, shouldStopLeaderAndRestartAsFollowerWithSendingAfter)
{
    start_cluster(21);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int original_leader = leader_index();
    ASSERT_GE(original_leader, 0) << node_dump();
    const std::int64_t term =
        node_counter(original_leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    stop_node_gracefully(original_leader);
    const std::vector<int> survivors = {(original_leader + 1) % NODES, (original_leader + 2) % NODES};
    ASSERT_GE(await_new_leader(term, survivors, std::chrono::seconds(20)), 0) << node_dump();

    restart_node(round_, original_leader, false);
    ASSERT_TRUE(wait_for_node_active(original_leader, std::chrono::seconds(30))) << node_dump();
    ASSERT_TRUE(wait_for_role(original_leader, 0, std::chrono::seconds(30))) << node_dump();

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();
    EXPECT_TRUE(send_echoes(session, 10)) << node_dump();
    session.close();

    stop_round();
}

TEST_F(Xc2RestartPathTest, shouldStopLeaderAndRestartAsFollowerWithSendingAfterThenStopLeader)
{
    start_cluster(22);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int original_leader = leader_index();
    ASSERT_GE(original_leader, 0) << node_dump();
    const std::int64_t first_term =
        node_counter(original_leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    stop_node_gracefully(original_leader);
    ASSERT_GE(await_new_leader(first_term, {(original_leader + 1) % NODES, (original_leader + 2) % NODES},
                               std::chrono::seconds(20)),
              0)
        << node_dump();

    restart_node(round_, original_leader, false);
    ASSERT_TRUE(wait_for_node_active(original_leader, std::chrono::seconds(30))) << node_dump();
    ASSERT_TRUE(wait_for_role(original_leader, 0, std::chrono::seconds(30))) << node_dump();

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();
    ASSERT_TRUE(send_echoes(session, 10)) << node_dump();
    session.close();

    const int leader_two = leader_index();
    ASSERT_GE(leader_two, 0) << node_dump();
    const std::int64_t second_term =
        node_counter(leader_two, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);
    stop_node_gracefully(leader_two);
    EXPECT_EQ(nullptr, nodes_[leader_two].get()) << "stop_node_gracefully did not retire node " << leader_two;

    std::string trace;
    EXPECT_TRUE(wait_for_settled(std::chrono::seconds(20), second_term, trace))
        << "members never settled above term " << second_term << ":\n"
        << trace << node_dump();

    stop_round();
}

TEST_F(Xc2RestartPathTest, shouldAcceptMessagesAfterTwoNodeCleanRestart)
{
    start_cluster(23);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    const int follower_a = (leader + 1) % NODES;
    const int follower_b = (leader + 2) % NODES;
    const std::int64_t term = node_counter(leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    ClusterClientSession first;
    ASSERT_TRUE(connect_udp_retry(first)) << node_dump() << first.last_error();
    ASSERT_TRUE(send_echoes(first, 10)) << node_dump();
    first.close();

    stop_node_gracefully(follower_a);
    stop_node_gracefully(follower_b);
    restart_node(round_, follower_a, true);
    restart_node(round_, follower_b, true);

    ASSERT_TRUE(wait_for_node_active(follower_a, std::chrono::seconds(30))) << node_dump();
    ASSERT_TRUE(wait_for_node_active(follower_b, std::chrono::seconds(30))) << node_dump();

    std::string trace;
    ASSERT_TRUE(wait_for_settled(std::chrono::seconds(30), term, trace)) << "clean-restarted members never settled: \n"
                                                                         << trace << node_dump();

    ClusterClientSession second;
    ASSERT_TRUE(connect_udp_retry(second)) << node_dump() << second.last_error();
    EXPECT_TRUE(send_echoes(second, 10)) << node_dump();
    second.close();

    stop_round();
}

TEST_F(Xc2RestartPathTest, shouldTolerateMultipleLeaderFailures)
{
    start_cluster(24);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int first_leader = leader_index();
    ASSERT_GE(first_leader, 0) << node_dump();
    const std::int64_t first_term =
        node_counter(first_leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    stop_node_gracefully(first_leader);
    const int second_leader = await_new_leader(first_term, {(first_leader + 1) % NODES, (first_leader + 2) % NODES},
                                               std::chrono::seconds(20));
    ASSERT_GE(second_leader, 0) << node_dump();
    const std::int64_t second_term =
        node_counter(second_leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);
    const std::int64_t commit = node_counter(second_leader, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID);

    restart_node(round_, first_leader, false);
    ASSERT_TRUE(wait_for_node_active(first_leader, std::chrono::seconds(30))) << node_dump();
    ASSERT_TRUE(await_commit(first_leader, commit, std::chrono::seconds(30))) << node_dump();

    stop_node_gracefully(second_leader);
    std::string trace;
    ASSERT_TRUE(wait_for_settled(std::chrono::seconds(20), second_term, trace))
        << "members never settled after the second leader stopped: \n"
        << trace << node_dump();

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();
    EXPECT_TRUE(send_echoes(session, 10)) << node_dump();
    session.close();

    stop_round();
}

TEST_F(Xc2RestartPathTest, shouldRecoverAfterTwoLeaderNodesFailAndComeBackUpAtSameTime)
{
    start_cluster(25);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int first_leader = leader_index();
    ASSERT_GE(first_leader, 0) << node_dump();
    const std::int64_t first_term =
        node_counter(first_leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    ClusterClientSession first;
    ASSERT_TRUE(connect_udp_retry(first)) << node_dump() << first.last_error();
    ASSERT_TRUE(send_echoes(first, 100)) << node_dump();
    first.close();

    stop_node_gracefully(first_leader);
    const int second_leader = await_new_leader(first_term, {(first_leader + 1) % NODES, (first_leader + 2) % NODES},
                                               std::chrono::seconds(20));
    ASSERT_GE(second_leader, 0) << node_dump();
    const std::int64_t second_term =
        node_counter(second_leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    stop_node_gracefully(second_leader);
    restart_nodes_together(round_, {{first_leader, false}, {second_leader, false}});

    ASSERT_TRUE(wait_for_node_active(first_leader, std::chrono::seconds(30))) << node_dump();
    ASSERT_TRUE(wait_for_node_active(second_leader, std::chrono::seconds(30))) << node_dump();
    ASSERT_GE(await_new_leader(second_term, {0, 1, 2}, std::chrono::seconds(30)), 0) << node_dump();

    ClusterClientSession second;
    ASSERT_TRUE(connect_udp_retry(second)) << node_dump() << second.last_error();
    EXPECT_TRUE(send_echoes(second, 10)) << node_dump();
    second.close();

    stop_round();
}

TEST_F(Xc2RestartPathTest, shouldRecoverQuicklyAfterKillingFollowersThenRestartingOne)
{
    start_cluster(26);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    const int follower_one = (leader + 1) % NODES;
    const int follower_two = (leader + 2) % NODES;
    const std::int64_t term = node_counter(leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    ClusterClientSession first;
    ASSERT_TRUE(connect_udp_retry(first)) << node_dump() << first.last_error();
    ASSERT_TRUE(send_echoes(first, 10)) << node_dump();
    first.close();

    stop_node_gracefully(follower_one);
    stop_node_gracefully(follower_two);
    ASSERT_TRUE(wait_for_not_leader(leader, std::chrono::seconds(15)))
        << "the leader did not step down after both followers stopped: " << node_dump();

    restart_node(round_, follower_two, true);
    ASSERT_TRUE(wait_for_node_active(follower_two, std::chrono::seconds(30))) << node_dump();

    const int new_leader = await_new_leader(term, {0, 1, 2}, std::chrono::seconds(30));
    EXPECT_GE(new_leader, 0) << node_dump();

    stop_round();
}

}  // namespace
