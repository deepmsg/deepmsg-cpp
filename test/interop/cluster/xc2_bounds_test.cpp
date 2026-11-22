// C6c-2 F5: the session-message edge cases. The reference's
// `ClusterTest.shouldAssembleFragmentedSessionMessages` sends one unfragmented
// and one multi-fragment message and checks the reassembled log frames;
// `shouldCatchupFollowerWithSlowService` keeps a service deliberately slow
// while a follower leaves and rejoins; `shouldRemainStableWhenThereIsASlow
// Follower` parks one member's service while the cluster keeps committing.
// The judgements here are the C++ equivalents: byte-for-byte echo, exactly
// once on every service, and no leader change under a slow service.

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

class Xc2BoundsTest : public ThreeNodeRig
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

    /// Waits until every live member's service has recorded `client_messages`
    /// ids (each with its service messages and timers) and they all agree.
    [[nodiscard]] bool await_tracking(int client_messages, std::string& trace)
    {
        const std::size_t expected_service = static_cast<std::size_t>(client_messages * SERVICE_MESSAGES_PER_INGRESS);
        const std::size_t expected_timers = static_cast<std::size_t>(client_messages * TIMER_MESSAGES_PER_INGRESS);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
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

    [[nodiscard]] std::string container_dump() const
    {
        std::string dump;
        for (int i = 0; i < NODES; ++i)
        {
            dump.append(" container")
                .append(std::to_string(i))
                .append(" alive=")
                .append(containers_[i] && containers_[i]->running() ? "1" : "0");
        }
        return dump;
    }

    /// No service message id is recorded twice on any live member.
    void expect_no_duplicate_service_messages(const std::string& trace)
    {
        for (int i = 0; i < NODES; ++i)
        {
            if (!nodes_[i])
            {
                continue;
            }
            std::vector<std::int32_t> ids = tracking_for(i).service_messages;
            std::sort(ids.begin(), ids.end());
            EXPECT_EQ(ids.end(), std::adjacent_find(ids.begin(), ids.end())) << "node " << i << ": " << trace;
        }
    }
};

TEST_F(Xc2BoundsTest, theClusterAssemblesFragmentedSessionMessages)
{
    // `ClusterTest.shouldAssembleFragmentedSessionMessages`: a 63-byte message
    // rides one frame, a 5979-byte message rides several, and an 8 KiB message
    // (the plan's size) rides more still. The echo service returns the
    // assembled payload; a broken reassembly changes the bytes or the length,
    // which the comparison catches.
    start_cluster(60);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();

    const std::string unfragmented(63, '\xEA');
    const std::string fragmented(5979, '\xBC');
    const std::string eight_k(8192, '\xBD');

    ASSERT_TRUE(session.offer_and_await_echo(unfragmented, std::chrono::seconds(20))) << node_dump();
    ASSERT_TRUE(session.offer_and_await_echo(fragmented, std::chrono::seconds(20))) << node_dump();
    ASSERT_TRUE(session.offer_and_await_echo(eight_k, std::chrono::seconds(30))) << node_dump();

    ASSERT_EQ(3U, session.messages().size()) << node_dump();
    EXPECT_EQ(unfragmented, session.messages()[0]);
    EXPECT_EQ(fragmented, session.messages()[1]);
    EXPECT_EQ(eight_k, session.messages()[2]);

    session.close();
    stop_round();
}

TEST_F(Xc2BoundsTest, aFollowerCatchesUpWithASlowService)
{
    // `ClusterTest.shouldCatchupFollowerWithSlowService`: a service sleeps
    // 100 ms per client message; a follower leaves, the cluster keeps going,
    // the follower rejoins and must replay the messages it missed without
    // recording any of them twice. The reference's 5 + 7 + 3 messages are
    // 500/100, 700/100 and 300/100; it slows every member's service, this case
    // slows the follower under test (records/c6c-2.md S5-2).
    use_tracking_service();
    start_cluster(61);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    const int follower = (leader + 1) % NODES;

    // The slow service runs on the follower whose departure and rejoin the
    // case is about: it is restarted once with the switch so the container
    // picks it up, and the leadership side stays fast.
    slow_service_micros_[follower] = 100'000;
    restart_node(round_, follower, false);
    ASSERT_TRUE(wait_for_node_active(follower, std::chrono::seconds(30))) << node_dump();

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();
    ASSERT_TRUE(send_ints(session, 5, 1)) << node_dump();

    std::string trace;
    ASSERT_TRUE(await_tracking(5, trace)) << "the first batch never settled:\n" << trace << node_dump();

    stop_node_gracefully(follower);
    ASSERT_TRUE(send_ints(session, 7, 6)) << "the cluster stopped serving without the follower: " << node_dump();

    restart_node(round_, follower, false);
    ASSERT_TRUE(wait_for_node_active(follower, std::chrono::seconds(30))) << node_dump();
    ASSERT_TRUE(send_ints(session, 3, 13)) << node_dump();
    session.close();

    trace.clear();
    ASSERT_TRUE(await_tracking(15, trace)) << "the tracking services never converged:\n"
                                           << trace << node_dump() << container_dump();
    expect_no_duplicate_service_messages(trace);

    stop_round();
}

TEST_F(Xc2BoundsTest, theClusterRemainsStableWhenALiveFollowersServiceIsSlow)
{
    // `ClusterTest.shouldRemainStableWhenThereIsASlowFollower`: the reference
    // parks one live follower's service for 1.5 s by name
    // (`sendMessageToSlowDownService`), stops the other follower, sends a
    // hundred messages, restarts the stopped follower and sends a hundred
    // more; every service must process every message and the leadership must
    // not move. The slow service is the reference's mechanism — a whole-node
    // SIGSTOP would also stall that member's driver, which is a harsher fault
    // than the case names.
    use_tracking_service();
    start_cluster(62);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    const std::int64_t term = node_counter(leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    std::vector<int> followers;
    for (int i = 0; i < NODES; ++i)
    {
        if (i != leader)
        {
            followers.push_back(i);
        }
    }
    const int stopped = followers[0];
    const int slow = followers[1];
    stop_node_gracefully(stopped);

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();

    // `PAUSE + "|" + index + "|" + durationNs`.
    const std::string pause = "Please pause when processing message|" + std::to_string(slow) + "|1500000000";
    ASSERT_TRUE(session.offer_only(pause, std::chrono::seconds(20)))
        << "the pause command was not accepted: " << node_dump();
    ASSERT_TRUE(send_ints(session, 10, 1)) << node_dump();

    restart_node(round_, stopped, false);
    ASSERT_TRUE(wait_for_node_active(stopped, std::chrono::seconds(30))) << node_dump();
    ASSERT_TRUE(send_ints(session, 10, 11)) << node_dump();
    session.close();

    std::string trace;
    ASSERT_TRUE(await_tracking(20, trace)) << "the tracking services never converged:\n"
                                           << trace << node_dump() << container_dump();

    EXPECT_EQ(leader, leader_index()) << "the slow service moved the leadership: " << node_dump();
    EXPECT_EQ(term, node_counter(leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID))
        << "the slow service forced a new term: " << node_dump();

    stop_round();
}

}  // namespace
