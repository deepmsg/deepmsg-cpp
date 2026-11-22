// C7: the crash windows. The card's own DoD (the reference has no scenario
// for it) is `createRecoveryPlan` under a `kill -9` in three places:
//
//   - commit: the leader dies right after a message the client saw echoed;
//   - append: the leader dies holding messages it appended without a quorum;
//   - snapshot: the leader dies inside a service snapshot.
//
// Every case restarts the crashed member and checks that the service state is
// the committed prefix — once, in order — and that the cluster serves again.
// The tracking service is the witness: `M <id>` per applied client message.

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

class Xc7CrashTest : public ThreeNodeRig
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

    /// Waits until every live member's service has applied exactly the first
    /// `client_messages` ids (`first`..`first + client_messages - 1`), with
    /// their service messages and timers, and they all agree.
    [[nodiscard]] bool await_tracking(int first, int client_messages, std::string& trace)
    {
        const std::size_t expected_service = static_cast<std::size_t>(client_messages * SERVICE_MESSAGES_PER_INGRESS);
        const std::size_t expected_timers = static_cast<std::size_t>(client_messages * TIMER_MESSAGES_PER_INGRESS);
        std::vector<std::int32_t> expected(static_cast<std::size_t>(client_messages));
        for (int i = 0; i < client_messages; ++i)
        {
            expected[static_cast<std::size_t>(i)] = first + i;
        }
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline)
        {
            drive_node_clients();
            bool all_agree = true;
            bool any_live = false;
            std::string line;
            for (int i = 0; i < NODES; ++i)
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
                    .append(std::to_string(tracking.service_messages.size()))
                    .append("/T")
                    .append(std::to_string(tracking.timers.size()));
                if (tracking.client_messages != expected || tracking.service_messages.size() != expected_service ||
                    tracking.timers.size() != expected_timers)
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

    [[nodiscard]] std::vector<int> survivors_of(int killed) const
    {
        std::vector<int> survivors;
        for (int i = 0; i < NODES; ++i)
        {
            if (i != killed && nodes_[i])
            {
                survivors.push_back(i);
            }
        }
        return survivors;
    }

    void crash_member(int index, ClusterClientSession& session)
    {
        session.close();
        kill_hard(containers_[index]);
        kill_hard(nodes_[index]);
        node_clients_[index].reset();
    }

    /// A SIGKILL leaves the archive mark file freshly active; the archive
    /// refuses to start until its 10 s liveness window passes (the reference
    /// does the same), so the restart is retried until the member is active.
    void restart_crashed_member(int index, std::chrono::milliseconds timeout = std::chrono::seconds(60))
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            restart_node(round_, index, false);
            if (wait_for_node_active(index, std::chrono::seconds(8)))
            {
                return;
            }
            stop_node_gracefully(index);
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
        FAIL() << "node " << index << " never came back after the crash: " << node_dump();
    }

    [[nodiscard]] std::int64_t leader_term(int leader)
    {
        return node_counter(leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);
    }
};

TEST_F(Xc7CrashTest, aCommittedMessageSurvivesTheLeaderKilledRightAfterTheCommit)
{
    // Commit window: the client has seen every echo, so every message is
    // committed; killing the leader right afterwards must not lose one.
    use_tracking_service();
    start_cluster(70);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    const std::int64_t term = leader_term(leader);

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();
    ASSERT_TRUE(send_ints(session, 10, 1)) << node_dump();

    std::string trace;
    ASSERT_TRUE(await_tracking(1, 10, trace)) << "the committed batch never settled:\n" << trace << node_dump();
    const std::int64_t commit_before = node_counter(leader, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID);
    ASSERT_GT(commit_before, 0) << node_dump();

    crash_member(leader, session);
    ASSERT_GE(await_new_leader(term, survivors_of(leader), std::chrono::seconds(30)), 0) << node_dump();

    restart_crashed_member(leader);

    trace.clear();
    ASSERT_TRUE(await_tracking(1, 10, trace)) << "the restarted member's replay diverged:\n" << trace << node_dump();

    // The crash must not have rewound the commit position: the new leadership
    // starts at or above what the old one had committed.
    const int new_leader = leader_index();
    ASSERT_GE(new_leader, 0) << node_dump();
    EXPECT_GE(node_counter(new_leader, deepmsg::cluster::common::CLUSTER_COMMIT_POSITION_TYPE_ID), commit_before)
        << "the commit position went backwards across the crash: " << node_dump();

    ClusterClientSession second;
    ASSERT_TRUE(connect_udp_retry(second)) << node_dump() << second.last_error();
    ASSERT_TRUE(send_ints(second, 5, 11)) << node_dump();
    trace.clear();
    ASSERT_TRUE(await_tracking(1, 15, trace)) << "the cluster stopped after the crash:\n" << trace << node_dump();

    stop_round();
}

TEST_F(Xc7CrashTest, anUncommittedAppendIsRolledBackWhenTheLeaderDies)
{
    // Append window: with no quorum the leader appends without committing.
    // The survivors elect at the committed position; the crashed leader comes
    // back as a follower and must drop the tail it never committed.
    use_tracking_service();
    for (int i = 0; i < NODES; ++i)
    {
        node_property_overrides_[i]["aeron.cluster.leader.heartbeat.timeout"] = "10s";
    }
    start_cluster(71);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();
    ASSERT_TRUE(send_ints(session, 5, 1)) << node_dump();

    std::string trace;
    ASSERT_TRUE(await_tracking(1, 5, trace)) << "the committed batch never settled:\n" << trace << node_dump();

    // No quorum left: both followers go, the leader keeps appending.
    const std::vector<int> survivors = survivors_of(leader);
    ASSERT_EQ(2U, survivors.size());
    for (const int index : survivors)
    {
        stop_node_gracefully(index);
    }
    for (int i = 0; i < 5; ++i)
    {
        ASSERT_TRUE(session.offer_only(int_payload(9001 + i)))
            << "the leader refused an append with no quorum: " << node_dump();
    }
    // Give the consensus module the cycle that appends them to the log before
    // the crash; they stay uncommitted without a quorum.
    pause_for(std::chrono::milliseconds(500));
    crash_member(leader, session);

    restart_nodes_together(round_, {{survivors[0], false}, {survivors[1], false}});
    ASSERT_TRUE(wait_for_cluster()) << "the survivors never elected: " << node_dump();

    restart_crashed_member(leader);

    trace.clear();
    ASSERT_TRUE(await_tracking(1, 5, trace)) << "the uncommitted tail was applied:\n" << trace << node_dump();

    ClusterClientSession second;
    ASSERT_TRUE(connect_udp_retry(second)) << node_dump() << second.last_error();
    ASSERT_TRUE(send_ints(second, 5, 6)) << node_dump();
    trace.clear();
    ASSERT_TRUE(await_tracking(1, 10, trace)) << "the cluster stopped after the rollback:\n" << trace << node_dump();

    stop_round();
}

TEST_F(Xc7CrashTest, aSnapshotKilledMidwayLeavesTheServiceConsistent)
{
    // Snapshot window: the service sleeps inside `on_take_snapshot` and the
    // leader is killed while the snapshot is in flight, so the recording log
    // may hold a snapshot entry whose data never completed. Recovery must
    // fall back to the log, replay the committed messages once, and still be
    // able to take a fresh snapshot afterwards.
    use_tracking_service();
    for (int i = 0; i < NODES; ++i)
    {
        snapshot_sleep_micros_[i] = 2'000'000;
    }
    start_cluster(72);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    const std::int64_t term = leader_term(leader);

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();
    ASSERT_TRUE(send_ints(session, 10, 1)) << node_dump();
    std::string trace;
    ASSERT_TRUE(await_tracking(1, 10, trace)) << "the batch never settled:\n" << trace << node_dump();

    set_control_toggle(leader, 4);
    // The service is inside its 2 s snapshot; kill the member now. The
    // snapshot counter must still be 0 — the snapshot never completed, which
    // is the partial state recovery has to cope with.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_EQ(0, node_counter(leader, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID))
        << "the snapshot completed before the kill: " << node_dump();
    crash_member(leader, session);
    ASSERT_GE(await_new_leader(term, survivors_of(leader), std::chrono::seconds(30)), 0) << node_dump();

    restart_crashed_member(leader);

    trace.clear();
    ASSERT_TRUE(await_tracking(1, 10, trace)) << "the recovery after the killed snapshot diverged:\n"
                                              << trace << node_dump();

    // A fresh snapshot still completes on the current leader.
    const int new_leader = leader_index();
    ASSERT_GE(new_leader, 0) << node_dump();
    const auto before = node_counter(new_leader, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);
    set_control_toggle(new_leader, 4);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < deadline &&
           node_counter(new_leader, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID) <= before)
    {
        drive_node_clients();
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_GT(node_counter(new_leader, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID), before)
        << "no snapshot completed after the crash: " << node_dump();

    stop_round();
}

}  // namespace
