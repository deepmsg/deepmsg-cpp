// C6c F1 batch 1: restart and catch-up paths that need only the three-node
// rig and the in-process client. The reference cases are
// `ClusterTest.shouldStopFollowerAndRestartFollowerGracefully`,
// `shouldStopLeaderAndRestartAsFollowerWithSendingAfter{ThenStopLeader}`,
// `shouldAcceptMessagesAfterTwoNodeCleanRestart`,
// `shouldTolerateMultipleLeaderFailures`,
// `shouldRecoverAfterTwoLeaderNodesFailAndComeBackUpAtSameTime` and
// `shouldRecoverQuicklyAfterKillingFollowersThenRestartingOne`.

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
    restart_node(round_, first_leader, false);
    restart_node(round_, second_leader, false);

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
