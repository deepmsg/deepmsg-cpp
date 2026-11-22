// C7: the recovery plan's uncommitted tail and the snapshot a member missed
// while it was down. `Xc7RecoveryPlanTest` runs the reference's five-node
// shape for the uncommitted-tail cases (three followers go, the leader keeps
// appending without a quorum, the survivors elect at the committed position)
// and the three-node shape for the snapshot-taken-while-down case.

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "fixture/cluster_client_session.h"
#include "fixture/three_node_rig.h"

namespace
{

using namespace axc::interop;

constexpr int SERVICE_MESSAGES_PER_INGRESS = 3;
constexpr int TIMER_MESSAGES_PER_INGRESS = 2;

[[nodiscard]] std::string int_payload(std::int32_t value)
{
    std::string bytes(sizeof(value), '\0');
    std::memcpy(bytes.data(), &value, sizeof(value));
    return bytes;
}

class Xc7RecoveryPlanTest : public ThreeNodeRig
{
protected:
    void SetUp() override
    {
        if (!set_up_environment())
        {
            return;
        }
    }

    void use_tracking_service()
    {
        service_library_ = AXC_TRACKING_SERVICE_LIBRARY;
        service_name_ = "tracking";
        tracking_dir_ = root_dir_ + "/tracking";
    }

    [[nodiscard]] std::string ingress_endpoints_for(int round)
    {
        const Ports ports = ports_for(round);
        std::string endpoints;
        for (int i = 0; i < node_count_; ++i)
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
            for (int i = 0; i < node_count_; ++i)
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

    [[nodiscard]] bool send_ints(ClusterClientSession& session, int count, int first)
    {
        for (int i = 0; i < count; ++i)
        {
            const std::int32_t value = first + i;
            if (!session.offer_and_await_echo(int_payload(value), std::chrono::seconds(20)))
            {
                return false;
            }
        }
        return true;
    }

    struct Tracking
    {
        std::vector<std::int32_t> client_messages;
        std::vector<std::int32_t> service_messages;
        std::vector<std::int64_t> timers;
    };

    [[nodiscard]] Tracking tracking_for(int index) const
    {
        Tracking tracking;
        for (const std::string& line : tracking_lines(index))
        {
            const char type = line[0];
            if ('M' != type && 'S' != type && 'T' != type)
            {
                continue;
            }
            const std::int64_t value = std::stoll(line.substr(2));
            if ('M' == type)
            {
                tracking.client_messages.push_back(static_cast<std::int32_t>(value));
            }
            else if ('S' == type)
            {
                tracking.service_messages.push_back(static_cast<std::int32_t>(value));
            }
            else
            {
                tracking.timers.push_back(value);
            }
        }
        return tracking;
    }

    [[nodiscard]] bool await_tracking(int client_messages, std::string& trace)
    {
        const std::size_t expected_service = static_cast<std::size_t>(client_messages * SERVICE_MESSAGES_PER_INGRESS);
        const std::size_t expected_timers = static_cast<std::size_t>(client_messages * TIMER_MESSAGES_PER_INGRESS);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(90);
        while (std::chrono::steady_clock::now() < deadline)
        {
            drive_node_clients();
            bool all_agree = true;
            bool any_live = false;
            std::vector<std::int32_t> reference;
            bool have_reference = false;
            std::string line;
            for (int i = 0; i < node_count_; ++i)
            {
                if (!nodes_[i])
                {
                    continue;
                }
                any_live = true;
                const Tracking tracking = tracking_for(i);
                line.append(" n")
                    .append(std::to_string(i))
                    .append("=M")
                    .append(std::to_string(tracking.client_messages.size()))
                    .append("/S")
                    .append(std::to_string(tracking.service_messages.size()));
                if (tracking.client_messages.size() != static_cast<std::size_t>(client_messages) ||
                    tracking.service_messages.size() != expected_service || tracking.timers.size() != expected_timers)
                {
                    all_agree = false;
                    continue;
                }
                if (!have_reference)
                {
                    reference = tracking.client_messages;
                    have_reference = true;
                }
                else if (reference != tracking.client_messages)
                {
                    all_agree = false;
                }
            }
            if (line != trace)
            {
                trace.append(line).append("\n");
            }
            if (all_agree && any_live)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return false;
    }
};

TEST_F(Xc7RecoveryPlanTest,
       shouldRecoverWithUncommittedMessagesAfterRestartWhenNewCommitPosIsLessThanPreviousAppendedPos)
{
    // `ClusterTest.shouldRecoverWithUncommittedMessagesAfterRestartWhenNew
    // CommitPosIsLessThanPreviousAppendedPos`, five nodes as the reference
    // starts it: ten committed messages, three followers gone, ten appends
    // the leader can never commit, the leader and the fourth follower gone,
    // the three followers elect at the committed position, then the old
    // leader and the fourth follower rejoin — the appended-only tail must
    // never reach a service.
    use_tracking_service();
    node_count_ = 5;
    for (int i = 0; i < node_count_; ++i)
    {
        node_property_overrides_[i]["aeron.cluster.leader.heartbeat.timeout"] = "10s";
    }
    start_cluster(85);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(40))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    const std::int64_t term = node_counter(leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();
    ASSERT_TRUE(send_ints(session, 10, 1)) << node_dump();
    std::string trace;
    ASSERT_TRUE(await_tracking(10, trace)) << "the committed batch never settled:\n" << trace << node_dump();

    std::vector<int> followers;
    for (int i = 0; i < node_count_; ++i)
    {
        if (i != leader)
        {
            followers.push_back(i);
        }
    }
    ASSERT_EQ(4U, followers.size());

    // Three followers go: two of five are alive, no quorum, so the ten
    // appends below stay uncommitted.
    for (int i = 0; i < 3; ++i)
    {
        stop_node_gracefully(followers[static_cast<std::size_t>(i)]);
    }
    for (int i = 0; i < 10; ++i)
    {
        ASSERT_TRUE(session.offer_only(int_payload(9001 + i)))
            << "the leader refused an append with no quorum: " << node_dump();
    }
    pause_for(std::chrono::milliseconds(500));
    const std::int64_t appended_before =
        node_counter(leader, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID);
    ASSERT_GT(appended_before, 0) << node_dump();

    // The fourth follower and the leader go as well; the first three come
    // back and elect at what they had committed.
    stop_node_gracefully(followers[3]);
    stop_node_gracefully(leader);
    session.close();
    node_clients_[leader].reset();

    restart_nodes_together(round_, {{followers[0], false}, {followers[1], false}, {followers[2], false}});
    const std::vector<int> live{followers[0], followers[1], followers[2]};
    ASSERT_GE(await_new_leader(term, live, std::chrono::seconds(40)), 0) << node_dump();

    trace.clear();
    ASSERT_TRUE(await_tracking(10, trace)) << "the survivors replayed the uncommitted tail:\n" << trace << node_dump();

    // The old leader and the fourth follower rejoin; the old leader has ten
    // appended entries the new leadership never committed.
    restart_nodes_together(round_, {{followers[3], false}, {leader, false}});
    ASSERT_TRUE(wait_for_cluster()) << "the five-node cluster never settled: " << node_dump();

    trace.clear();
    ASSERT_TRUE(await_tracking(10, trace)) << "the rejoined leader's replay diverged:\n" << trace << node_dump();

    ClusterClientSession second;
    ASSERT_TRUE(connect_udp_retry(second)) << node_dump() << second.last_error();
    ASSERT_TRUE(send_ints(second, 5, 11)) << node_dump();
    trace.clear();
    ASSERT_TRUE(await_tracking(15, trace)) << "the cluster stopped after the rollback:\n" << trace << node_dump();

    stop_round();
}

TEST_F(Xc7RecoveryPlanTest, shouldReplaySnapshotTakenWhileDown)
{
    // `ClusterTest.shouldReplaySnapshotTakenWhileDown`: a follower is down
    // while the leader takes a snapshot and ten messages follow; the follower
    // comes back, picks the snapshot up and replays the tail.
    use_tracking_service();
    start_cluster(86);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    const int down = (leader + 1) % node_count_;

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();
    stop_node_gracefully(down);

    set_control_toggle(leader, 4);
    const auto snapshot_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < snapshot_deadline &&
           node_counter(leader, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID) < 1)
    {
        drive_node_clients();
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_GE(node_counter(leader, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID), 1) << node_dump();

    ASSERT_TRUE(send_ints(session, 10, 1)) << node_dump();
    std::string trace;
    ASSERT_TRUE(await_tracking(10, trace)) << "the batch never settled:\n" << trace << node_dump();

    restart_node(round_, down, false);
    ASSERT_TRUE(wait_for_node_active(down, std::chrono::seconds(30))) << node_dump();

    // The returning member loads the snapshot it missed: its own snapshot
    // counter shows one.
    const auto catchup_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (std::chrono::steady_clock::now() < catchup_deadline &&
           node_counter(down, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID) < 1)
    {
        drive_node_clients();
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_GE(node_counter(down, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID), 1)
        << "the returning member never took the snapshot: " << node_dump();

    trace.clear();
    ASSERT_TRUE(await_tracking(10, trace)) << "the returning member diverged:\n" << trace << node_dump();
    session.close();

    stop_round();
}

}  // namespace
