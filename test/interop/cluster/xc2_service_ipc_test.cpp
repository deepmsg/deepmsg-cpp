// C6c-2 F4b: service IPC ingress. The reference's
// `ServiceIpcIngressMessageTest` drives a `MessageTrackingService`: every
// client message records its id, sends three service messages through
// `Cluster.offer`, schedules two timers and echoes the client payload; the
// service messages come back to the service with `session == nullptr` and are
// recorded too. The scenarios send client messages, move leadership, restart
// members and check that every service saw every id exactly once.

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <map>
#include <string>
#include <vector>

#include "fixture/cluster_client_session.h"
#include "fixture/three_node_rig.h"

namespace
{

using namespace axc::interop;

constexpr std::string_view ECHO_SERVICE_IPC_INGRESS_MSG = "Echo as Service IPC ingress";
constexpr int SERVICE_MESSAGES_PER_INGRESS = 3;
constexpr int TIMER_MESSAGES_PER_INGRESS = 2;

[[nodiscard]] std::string int_payload(std::int32_t value)
{
    std::string bytes(sizeof(value), '\0');
    std::memcpy(bytes.data(), &value, sizeof(value));
    return bytes;
}

class Xc2ServiceIpcTest : public ThreeNodeRig
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
            if (!session.offer_and_await(int_payload(value), static_cast<std::size_t>(i + 1), std::chrono::seconds(20)))
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
            else if ('T' == type)
            {
                tracking.timers.push_back(value);
            }
        }
        return tracking;
    }

    /// Waits until every live member's service has recorded `client_messages`
    /// client ids (each with its 3 service messages and 2 timers) and they all
    /// agree, with no duplicates.
    [[nodiscard]] bool await_tracking(int client_messages, std::string& trace)
    {
        const std::size_t expected_service = static_cast<std::size_t>(client_messages * SERVICE_MESSAGES_PER_INGRESS);
        const std::size_t expected_timers = static_cast<std::size_t>(client_messages * TIMER_MESSAGES_PER_INGRESS);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(45);
        while (std::chrono::steady_clock::now() < deadline)
        {
            drive_node_clients();
            bool all_agree = true;
            std::vector<std::int32_t> reference_clients;
            bool have_reference = false;
            std::string line;
            for (int i = 0; i < NODES; ++i)
            {
                if (!nodes_[i])
                {
                    continue;
                }
                const Tracking tracking = tracking_for(i);
                line.append(" n")
                    .append(std::to_string(i))
                    .append("=M")
                    .append(std::to_string(tracking.client_messages.size()))
                    .append("/S")
                    .append(std::to_string(tracking.service_messages.size()))
                    .append("/T")
                    .append(std::to_string(tracking.timers.size()));
                if (tracking.client_messages.size() != static_cast<std::size_t>(client_messages) ||
                    tracking.service_messages.size() != expected_service || tracking.timers.size() != expected_timers)
                {
                    all_agree = false;
                    continue;
                }
                if (!have_reference)
                {
                    reference_clients = tracking.client_messages;
                    have_reference = true;
                }
                else if (reference_clients != tracking.client_messages)
                {
                    all_agree = false;
                }
            }
            if (line != trace)
            {
                trace.append(line).append("\n");
            }
            if (all_agree && have_reference)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return false;
    }
};

TEST_F(Xc2ServiceIpcTest, shouldEchoServiceIpcMessages)
{
    // `ServiceIpcIngressMessageTest.shouldEchoServiceIpcMessages`: the echo
    // service turns this payload into a service message; the service only
    // answers from the service-message branch, so each client response proves
    // the ingress round trip.
    start_cluster(50);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();

    constexpr int message_count = 10;
    for (int i = 0; i < message_count; ++i)
    {
        ASSERT_TRUE(session.offer_and_await(ECHO_SERVICE_IPC_INGRESS_MSG, static_cast<std::size_t>(i + 1),
                                            std::chrono::seconds(20)))
            << node_dump();
    }

    session.close();
    stop_round();
}

TEST_F(Xc2ServiceIpcTest, serviceMessagesAreDeliveredOnceAcrossAFailover)
{
    // `shouldProcessServiceMessagesWithoutDuplicates` adapted to one service
    // per container: 10 client messages, the leader stops, 10 more on a new
    // session, then the old leader comes back; every service sees all 20 ids
    // once, with three service messages and two timers per client message.
    use_tracking_service();
    start_cluster(51);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    const std::int64_t term = node_counter(leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    ClusterClientSession first;
    ASSERT_TRUE(connect_udp_retry(first)) << node_dump() << first.last_error();
    ASSERT_TRUE(send_ints(first, 10, 1)) << node_dump();
    first.close();

    stop_node_gracefully(leader);
    ASSERT_GE(await_new_leader(term, {(leader + 1) % NODES, (leader + 2) % NODES}, std::chrono::seconds(30)), 0)
        << node_dump();

    ClusterClientSession second;
    ASSERT_TRUE(connect_udp_retry(second)) << node_dump() << second.last_error();
    ASSERT_TRUE(send_ints(second, 10, 11)) << node_dump();
    second.close();

    restart_node(round_, leader, false);
    ASSERT_TRUE(wait_for_node_active(leader, std::chrono::seconds(30))) << node_dump();

    std::string trace;
    ASSERT_TRUE(await_tracking(20, trace)) << "the tracking services never converged:\n" << trace << node_dump();

    // No duplicate service message ids on any live member.
    for (int i = 0; i < NODES; ++i)
    {
        std::vector<std::int32_t> ids = tracking_for(i).service_messages;
        std::sort(ids.begin(), ids.end());
        EXPECT_EQ(ids.end(), std::adjacent_find(ids.begin(), ids.end())) << "node " << i << ": " << trace;
    }

    stop_round();
}

TEST_F(Xc2ServiceIpcTest, serviceMessagesSurviveAFullClusterRestart)
{
    // `shouldProcessServiceMessagesWithoutDuplicatesAfterAFullClusterRestart`:
    // every node stops and comes back non-clean; the replay must deliver the
    // same client and service messages to every service, once.
    use_tracking_service();
    start_cluster(52);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    ClusterClientSession first;
    ASSERT_TRUE(connect_udp_retry(first)) << node_dump() << first.last_error();
    ASSERT_TRUE(send_ints(first, 10, 1)) << node_dump();
    first.close();

    for (int i = 0; i < NODES; ++i)
    {
        stop_node_gracefully(i);
    }
    for (int i = 0; i < NODES; ++i)
    {
        restart_node(round_, i, false);
    }
    ASSERT_TRUE(wait_for_cluster()) << "the restarted cluster never elected: " << node_dump();

    ClusterClientSession second;
    ASSERT_TRUE(connect_udp_retry(second)) << node_dump() << second.last_error();
    ASSERT_TRUE(send_ints(second, 10, 11)) << node_dump();
    second.close();

    std::string trace;
    ASSERT_TRUE(await_tracking(20, trace)) << "the tracking services never converged:\n" << trace << node_dump();

    stop_round();
}

TEST_F(Xc2ServiceIpcTest, serviceMessagesSurviveARestartAfterASnapshot)
{
    // `shouldProcessServiceMessagesWithoutDuplicatesWhenClusterIsRestarted
    // AfterTakingASnapshot`: the snapshot carries the tracking state; the log
    // tail restores the rest; the ids must not repeat.
    use_tracking_service();
    start_cluster(53);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();

    ClusterClientSession first;
    ASSERT_TRUE(connect_udp_retry(first)) << node_dump() << first.last_error();
    ASSERT_TRUE(send_ints(first, 10, 1)) << node_dump();
    first.close();

    std::string trace;
    ASSERT_TRUE(await_tracking(10, trace)) << "the first batch never settled:\n" << trace << node_dump();

    set_control_toggle(leader, 4);
    const auto snapshot_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < snapshot_deadline &&
           node_counter(leader, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID) < 1)
    {
        drive_node_clients();
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_GE(node_counter(leader, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID), 1) << node_dump();

    ClusterClientSession second;
    ASSERT_TRUE(connect_udp_retry(second)) << node_dump() << second.last_error();
    ASSERT_TRUE(send_ints(second, 10, 11)) << node_dump();
    second.close();

    for (int i = 0; i < NODES; ++i)
    {
        stop_node_gracefully(i);
    }
    for (int i = 0; i < NODES; ++i)
    {
        restart_node(round_, i, false);
    }
    ASSERT_TRUE(wait_for_cluster()) << "the restarted cluster never elected: " << node_dump();

    trace.clear();
    ASSERT_TRUE(await_tracking(20, trace)) << "the restored tracking services never converged:\n"
                                           << trace << node_dump();

    ClusterClientSession third;
    ASSERT_TRUE(connect_udp_retry(third)) << node_dump() << third.last_error();
    ASSERT_TRUE(send_ints(third, 5, 21)) << node_dump();
    third.close();

    trace.clear();
    ASSERT_TRUE(await_tracking(25, trace)) << "the tracking services stopped after the restart:\n"
                                           << trace << node_dump();

    stop_round();
}

TEST_F(Xc2ServiceIpcTest, serviceMessagesSurviveTheLeaderServiceStopping)
{
    // `shouldProcessServiceMessagesAndTimersWithoutDuplicatesWhenLeader
    // ServicesAreStopped`: closing the leader's container terminates that
    // member (C6c-2 S3); the survivors take over, the old member rejoins and
    // every service still sees every id once.
    use_tracking_service();
    start_cluster(54);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    const std::int64_t term = node_counter(leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    ClusterClientSession first;
    ASSERT_TRUE(connect_udp_retry(first)) << node_dump() << first.last_error();
    ASSERT_TRUE(send_ints(first, 10, 1)) << node_dump();
    first.close();

    ASSERT_TRUE(containers_[leader]);
    containers_[leader]->terminate(std::chrono::seconds(5));
    containers_[leader].reset();

    ASSERT_GE(await_new_leader(term, {(leader + 1) % NODES, (leader + 2) % NODES}, std::chrono::seconds(30)), 0)
        << node_dump();

    ClusterClientSession second;
    ASSERT_TRUE(connect_udp_retry(second)) << node_dump() << second.last_error();
    ASSERT_TRUE(send_ints(second, 10, 11)) << node_dump();
    second.close();

    restart_node(round_, leader, false);
    ASSERT_TRUE(wait_for_node_active(leader, std::chrono::seconds(30))) << node_dump();

    std::string trace;
    ASSERT_TRUE(await_tracking(20, trace)) << "the tracking services never converged:\n" << trace << node_dump();

    stop_round();
}

}  // namespace
