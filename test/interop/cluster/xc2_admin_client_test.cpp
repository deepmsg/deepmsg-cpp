// C6c-2 F4: admin requests, session lifecycle and the per-session counters,
// all through the in-process client. The reference cases live in
// `ClusterTest` (`shouldTakeASnapshotAfterReceivingAdminRequestOfTypeSnapshot
// AndNotifyViaControlledPoll`, `shouldRejectTakeSnapshotRequestWithAn
// AuthorisationError`, `shouldRejectAnInvalidAdminRequest`,
// `shouldCloseClientOnTimeout`, `shouldCloseClientAfterClusterBecomes
// Unavailable`, `clusterShouldCreateSessionCounterForEachConnectedClient`).

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "fixture/cluster_client_session.h"
#include "fixture/three_node_rig.h"
#include "io_aeron_cluster_codecs/AdminRequest.h"
#include "io_aeron_cluster_codecs/AdminRequestType.h"
#include "io_aeron_cluster_codecs/AdminResponseCode.h"

namespace
{

using namespace axc::interop;

namespace codecs = deepmsg::cluster::common::codecs;

class Xc2AdminClientTest : public ThreeNodeRig
{
protected:
    void SetUp() override
    {
        if (!set_up_environment())
        {
            return;
        }
    }

    void set_authorisation(const std::string& supplier)
    {
        for (int i = 0; i < NODES; ++i)
        {
            node_property_overrides_[i]["aeron.cluster.authorisation.service.supplier"] = supplier;
        }
    }

    void set_node_property(const std::string& name, const std::string& value)
    {
        for (int i = 0; i < NODES; ++i)
        {
            node_property_overrides_[i][name] = value;
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

    [[nodiscard]] bool connect_udp_retry(
        ClusterClientSession& session, std::chrono::milliseconds new_leader_timeout = std::chrono::milliseconds::zero())
    {
        for (int attempt = 0; attempt < 3; ++attempt)
        {
            if (session.connect_udp(client_aeron_dir_, ingress_endpoints_for(round_), egress_channel_for(round_),
                                    std::chrono::seconds(30), new_leader_timeout))
            {
                return true;
            }
            session.close();
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        return false;
    }

    /// A controlled egress listener that records what the reference's
    /// `injectAdminRequestControlledEgressListener` checks.
    struct ControlledRecorder final : deepmsg::cluster::client::ControlledEgressListener
    {
        std::vector<RecordingEgressListener::AdminResponse> admin_responses{};

        aeron_controlled_fragment_handler_action_t on_message(std::int64_t, std::int64_t,
                                                              std::span<const std::uint8_t>) override
        {
            return AERON_ACTION_CONTINUE;
        }

        void on_session_event(std::int64_t, std::int64_t, std::int64_t, std::int32_t, codecs::EventCode::Value,
                              std::string_view) override
        {
        }

        void on_new_leader(std::int64_t, std::int64_t, std::int32_t, std::string_view) override {}

        void on_admin_response(std::int64_t session_id, std::int64_t correlation_id,
                               codecs::AdminRequestType::Value request_type, codecs::AdminResponseCode::Value code,
                               std::string_view message, std::span<const std::uint8_t> payload) override
        {
            admin_responses.push_back(RecordingEgressListener::AdminResponse{
                session_id, correlation_id, request_type, code, std::string{message}, payload.size()});
        }
    };
};

TEST_F(Xc2AdminClientTest, theLeaderTakesASnapshotOnAnAdminRequest)
{
    // The reference's default authorisation service only allows backup and
    // standby actions, so this scenario configures ALLOW_ALL like
    // `ClusterTest` does with its authorisation supplier.
    set_authorisation("ALLOW_ALL");
    start_cluster(40);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();

    constexpr std::int64_t correlation_id = 771;
    ASSERT_TRUE(session.send_admin_request_to_take_a_snapshot(correlation_id));
    ASSERT_TRUE(session.pump_until([&] { return !session.admin_responses().empty(); }, std::chrono::seconds(10)))
        << node_dump();

    const auto& response = session.admin_responses().front();
    EXPECT_EQ(correlation_id, response.correlation_id);
    EXPECT_EQ(codecs::AdminRequestType::SNAPSHOT, response.request_type);
    EXPECT_EQ(codecs::AdminResponseCode::OK, response.code);
    EXPECT_EQ(0U, response.payload_length);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline &&
           node_counter(leader, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID) < 1)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_GE(node_counter(leader, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID), 1) << node_dump();

    session.close();
    stop_round();
}

TEST_F(Xc2AdminClientTest, theControlledPollReceivesTheAdminResponse)
{
    set_authorisation("ALLOW_ALL");
    start_cluster(41);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();
    ASSERT_GE(leader_index(), 0) << node_dump();

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();

    ControlledRecorder controlled;
    session.set_controlled_listener(&controlled);

    constexpr std::int64_t correlation_id = 772;
    ASSERT_TRUE(session.send_admin_request_to_take_a_snapshot(correlation_id));

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline && controlled.admin_responses.empty())
    {
        (void)session.controlled_pump_once();
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_FALSE(controlled.admin_responses.empty()) << node_dump();
    EXPECT_EQ(correlation_id, controlled.admin_responses.front().correlation_id);
    EXPECT_EQ(codecs::AdminResponseCode::OK, controlled.admin_responses.front().code);

    session.close();
    stop_round();
}

TEST_F(Xc2AdminClientTest, aSnapshotRequestIsRejectedWhenNotAuthorised)
{
    // `ClusterTest.shouldRejectTakeSnapshotRequestWithAnAuthorisationError`
    // runs with the default (backup/standby) service; DENY_ALL makes the
    // rejection explicit instead of relying on the default's template list.
    set_authorisation("DENY_ALL");
    start_cluster(42);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();

    constexpr std::int64_t correlation_id = 773;
    ASSERT_TRUE(session.send_admin_request_to_take_a_snapshot(correlation_id));
    ASSERT_TRUE(session.pump_until([&] { return !session.admin_responses().empty(); }, std::chrono::seconds(10)))
        << node_dump();

    const auto& response = session.admin_responses().front();
    EXPECT_EQ(correlation_id, response.correlation_id);
    EXPECT_EQ(codecs::AdminRequestType::SNAPSHOT, response.request_type);
    EXPECT_EQ(codecs::AdminResponseCode::UNAUTHORISED_ACCESS, response.code);
    EXPECT_NE(std::string::npos, response.message.find("was not authorised")) << response.message;

    // The reference samples the snapshot count for one heartbeat timeout and
    // requires it to stay at zero everywhere.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline)
    {
        drive_node_clients();
        for (int i = 0; i < NODES; ++i)
        {
            EXPECT_EQ(0, node_counter(i, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID)) << node_dump();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    session.close();
    stop_round();
}

TEST_F(Xc2AdminClientTest, anInvalidAdminRequestIsRejected)
{
    // `ClusterTest.shouldRejectAnInvalidAdminRequest`: the reference's
    // authorisation supplier accepts everything and the request still has to
    // come back as ERROR, not take a snapshot.
    set_authorisation("ALLOW_ALL");
    start_cluster(43);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();

    constexpr std::int64_t correlation_id = 774;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    bool offered = false;
    while (std::chrono::steady_clock::now() < deadline && !offered)
    {
        offered = session.offer_admin_request(codecs::AdminRequestType::NULL_VALUE, correlation_id);
        if (!offered)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }
    ASSERT_TRUE(offered) << node_dump();

    ASSERT_TRUE(session.pump_until([&] { return !session.admin_responses().empty(); }, std::chrono::seconds(10)))
        << node_dump();

    const auto& response = session.admin_responses().front();
    EXPECT_EQ(correlation_id, response.correlation_id);
    EXPECT_EQ(codecs::AdminResponseCode::ERROR, response.code);
    EXPECT_NE(std::string::npos, response.message.find("Unknown request type")) << response.message;
    for (int i = 0; i < NODES; ++i)
    {
        EXPECT_EQ(0, node_counter(i, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID)) << node_dump();
    }

    session.close();
    stop_round();
}

TEST_F(Xc2AdminClientTest, anIdleClientIsClosedOnTheSessionTimeout)
{
    // `ClusterTest.shouldCloseClientOnTimeout`: the client connects and sends
    // nothing; the CM times the session out and the client sees the CLOSED
    // session event. The reference's session timeout is 10 s; 2 s keeps the
    // scenario short.
    set_node_property("aeron.cluster.session.timeout", "2s");
    start_cluster(44);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();
    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();

    ASSERT_TRUE(session.pump_until(
        [&]
        {
            const auto& events = session.events();
            return std::find(events.begin(), events.end(), codecs::EventCode::CLOSED) != events.end();
        },
        std::chrono::seconds(15)))
        << node_dump();

    EXPECT_FALSE(session.is_open()) << node_dump();
    EXPECT_GE(node_counter(leader, deepmsg::cluster::common::CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID), 1) << node_dump();

    session.close();
    stop_round();
}

TEST_F(Xc2AdminClientTest, theClientClosesAfterTheClusterBecomesUnavailable)
{
    // `ClusterTest.shouldCloseClientAfterClusterBecomesUnavailable`: a 1 s
    // new-leader timeout turns "all nodes gone" into a closed client instead
    // of an indefinite await.
    start_cluster(45);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session, std::chrono::seconds(1))) << node_dump() << session.last_error();
    EXPECT_TRUE(session.is_open());

    for (int i = 0; i < NODES; ++i)
    {
        stop_node_gracefully(i);
    }

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (std::chrono::steady_clock::now() < deadline && session.is_open())
    {
        (void)session.send_keep_alive();
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    EXPECT_FALSE(session.is_open()) << "the client did not close after the cluster went away: " << node_dump();

    session.close();
    stop_round();
}

TEST_F(Xc2AdminClientTest, eachConnectedClientGetsASessionCounter)
{
    // `ClusterTest.clusterShouldCreateSessionCounterForEachConnectedClient`:
    // one CLUSTER_SESSION_TYPE_ID counter per client, keyed by cluster id and
    // cluster session id, and reclaimed when the session closes.
    start_cluster(46);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();

    ClusterClientSession first;
    ClusterClientSession second;
    ASSERT_TRUE(connect_udp_retry(first)) << node_dump() << first.last_error();
    ASSERT_TRUE(connect_udp_retry(second)) << node_dump() << second.last_error();
    const std::int64_t first_session_id = first.cluster_session_id();
    const std::int64_t second_session_id = second.cluster_session_id();

    const auto find_sessions = [&]
    {
        std::vector<CounterInfo> sessions;
        for (const CounterInfo& counter : counters_of_type(leader, deepmsg::cluster::common::CLUSTER_SESSION_TYPE_ID))
        {
            if (counter.key.size() < sizeof(std::int32_t) + sizeof(std::int64_t))
            {
                continue;
            }
            std::int32_t cluster_id = 0;
            std::int64_t session_id = 0;
            std::memcpy(&cluster_id, counter.key.data(), sizeof(cluster_id));
            std::memcpy(&session_id, counter.key.data() + sizeof(cluster_id), sizeof(session_id));
            if (CLUSTER_ID == cluster_id && counter.value >= 0)
            {
                sessions.push_back(counter);
            }
        }
        return sessions;
    };

    std::vector<CounterInfo> sessions;
    const auto open_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < open_deadline)
    {
        (void)first.send_keep_alive();
        (void)second.send_keep_alive();
        sessions = find_sessions();
        if (2 == sessions.size())
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_EQ(2U, sessions.size()) << node_dump();

    const auto has_session = [&sessions](std::int64_t session_id)
    {
        return std::any_of(sessions.begin(), sessions.end(),
                           [&sessions, session_id](const CounterInfo& counter)
                           {
                               std::int32_t cluster_id = 0;
                               std::int64_t key_session_id = 0;
                               std::memcpy(&cluster_id, counter.key.data(), sizeof(cluster_id));
                               std::memcpy(&key_session_id, counter.key.data() + sizeof(cluster_id),
                                           sizeof(key_session_id));
                               return key_session_id == session_id;
                           });
    };
    EXPECT_TRUE(has_session(first_session_id)) << node_dump();
    EXPECT_TRUE(has_session(second_session_id)) << node_dump();
    for (const CounterInfo& counter : sessions)
    {
        EXPECT_EQ(std::string::npos, counter.label.find("name=null")) << counter.label;
        EXPECT_NE(std::string::npos, counter.label.find("sourceIdentity=")) << counter.label;
        EXPECT_NE(std::string::npos, counter.label.find("sessionId=")) << counter.label;
        EXPECT_NE(std::string::npos, counter.label.find(" - clusterId=7")) << counter.label;
    }

    first.close();
    second.close();

    const auto close_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < close_deadline && !find_sessions().empty())
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_TRUE(find_sessions().empty()) << "the session counters were not reclaimed: " << node_dump();

    stop_round();
}

}  // namespace
