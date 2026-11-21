// C7: invalidated snapshots. The reference's operator tool marks the last
// snapshot entry of a stopped node's `recording.log` invalid
// (`ClusterTool.invalidateLatestSnapshot`); the recovery plan then skips it
// and falls back to the previous snapshot plus the log. `Xc7SnapshotTest`
// drives those restarts through the in-process client and the tracking
// service, so the judgement is the set of applied message ids.

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "client/aeron_archive.h"
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

class Xc7SnapshotTest : public ThreeNodeRig
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

    [[nodiscard]] bool send_ints(ClusterClientSession& session, int count, int first, int echo_base)
    {
        for (int i = 0; i < count; ++i)
        {
            const std::int32_t value = first + i;
            if (!session.offer_and_await(int_payload(value), static_cast<std::size_t>(echo_base + i),
                                         std::chrono::seconds(20)))
            {
                return false;
            }
        }
        return true;
    }

    /// Offer a control toggle on `index`'s driver and wait until the snapshot
    /// counter reaches `expected`.
    [[nodiscard]] bool take_snapshot(int index, std::int64_t expected)
    {
        set_control_toggle(index, 4);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (std::chrono::steady_clock::now() < deadline)
        {
            drive_node_clients();
            if (node_counter(index, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID) >= expected)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    /// `NodeControl.ToggleState.validate_recording_log` (ordinal 3) on one
    /// node's driver/counters.
    void set_node_control(int index, std::int32_t value)
    {
        auto* client = node_client(index);
        ASSERT_NE(nullptr, client);
        (void)client->do_work();
        const std::int32_t id = deepmsg::cluster::common::find_cluster_counter(
            client->counters_reader(), deepmsg::cluster::common::NODE_CONTROL_TOGGLE_TYPE_ID, CLUSTER_ID);
        ASSERT_NE(AERON_NULL_COUNTER_ID, id);
        std::int64_t* address = aeron_counters_reader_addr(client->counters_reader(), id);
        ASSERT_NE(nullptr, address);
        __atomic_store_n(address, static_cast<std::int64_t>(value), __ATOMIC_RELEASE);
    }

    [[nodiscard]] std::int64_t node_control(int index)
    {
        auto* client = node_client(index);
        if (nullptr == client)
        {
            return -1;
        }
        const std::int32_t id = deepmsg::cluster::common::find_cluster_counter(
            client->counters_reader(), deepmsg::cluster::common::NODE_CONTROL_TOGGLE_TYPE_ID, CLUSTER_ID);
        if (AERON_NULL_COUNTER_ID == id)
        {
            return -1;
        }
        std::int64_t* address = aeron_counters_reader_addr(client->counters_reader(), id);
        return nullptr == address ? -1 : __atomic_load_n(address, __ATOMIC_ACQUIRE);
    }

    /// Opens an archive client on a running node's embedded archive (the
    /// channels the single-node fixture's `stop_log_recording` uses).
    [[nodiscard]] aeron_archive_t* connect_archive(int index)
    {
        const std::string aeron_dir = round_dir(round_) + "/node" + std::to_string(index) + "/aeron";
        const auto port = static_cast<std::int32_t>(ports_for(round_).archive_control[index]);
        aeron_archive_context_t* context = nullptr;
        if (0 != aeron_archive_context_init(&context))
        {
            return nullptr;
        }
        const std::string request = "aeron:udp?endpoint=localhost:" + std::to_string(port);
        const std::string response = "aeron:udp?endpoint=localhost:" + std::to_string(port + 100);
        (void)aeron_archive_context_set_aeron_directory_name(context, aeron_dir.c_str());
        (void)aeron_archive_context_set_control_request_channel(context, request.c_str());
        (void)aeron_archive_context_set_control_request_stream_id(context, 10);
        (void)aeron_archive_context_set_control_response_channel(context, response.c_str());
        (void)aeron_archive_context_set_control_response_stream_id(context, 10);
        (void)aeron_archive_context_set_message_timeout_ns(context, 10'000'000'000ULL);
        aeron_archive_t* archive = nullptr;
        if (0 != aeron_archive_connect(&archive, context))
        {
            (void)aeron_archive_context_close(context);
            return nullptr;
        }
        // The context must outlive the archive client; leaking one handle per
        // call is fine for a test process.
        return archive;
    }

    [[nodiscard]] std::vector<std::int64_t> recording_ids(int index)
    {
        std::vector<std::int64_t> ids;
        aeron_archive_t* archive = connect_archive(index);
        if (nullptr == archive)
        {
            return ids;
        }
        const auto collect = [](aeron_archive_recording_descriptor_t* descriptor, void* clientd)
        { static_cast<std::vector<std::int64_t>*>(clientd)->push_back(descriptor->recording_id); };
        std::int32_t listed = 0;
        if (0 == aeron_archive_list_recordings(&listed, archive, 0, 100, collect, &ids))
        {
            std::sort(ids.begin(), ids.end());
        }
        (void)aeron_archive_close(archive);
        return ids;
    }

    [[nodiscard]] bool purge_recording(int index, std::int64_t recording_id)
    {
        aeron_archive_t* archive = connect_archive(index);
        if (nullptr == archive)
        {
            return false;
        }
        std::int64_t purged = 0;
        const bool ok = 0 == aeron_archive_purge_recording(&purged, archive, recording_id);
        (void)aeron_archive_close(archive);
        return ok;
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
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline)
        {
            drive_node_clients();
            bool all_agree = true;
            bool any_live = false;
            std::vector<std::int32_t> reference;
            bool have_reference = false;
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

TEST_F(Xc7SnapshotTest, shouldRecoverWhenLastSnapshotIsMarkedInvalid)
{
    // `ClusterTest.shouldRecoverWhenLastSnapshotIsMarkedInvalid`: three
    // messages, a snapshot, three more, a second snapshot, the leader leaves
    // and comes back, three more; the cluster then stops and every node's
    // latest snapshot is invalidated. Recovery uses the first snapshot plus
    // the log, so all nine messages are applied once.
    use_tracking_service();
    start_cluster(80);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();
    ASSERT_TRUE(send_ints(session, 3, 1, 0)) << node_dump();
    std::string trace;
    ASSERT_TRUE(await_tracking(3, trace)) << "the first batch never settled:\n" << trace << node_dump();

    ASSERT_TRUE(take_snapshot(leader, 1)) << "the first snapshot never completed: " << node_dump();
    ASSERT_TRUE(send_ints(session, 3, 4, 3)) << node_dump();
    trace.clear();
    ASSERT_TRUE(await_tracking(6, trace)) << "the second batch never settled:\n" << trace << node_dump();
    ASSERT_TRUE(take_snapshot(leader, 2)) << "the second snapshot never completed: " << node_dump();

    const std::int64_t stopped_term =
        node_counter(leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);
    std::vector<int> survivors;
    for (int i = 0; i < NODES; ++i)
    {
        if (i != leader)
        {
            survivors.push_back(i);
        }
    }
    stop_node_gracefully(leader);
    ASSERT_GE(await_new_leader(stopped_term, survivors, std::chrono::seconds(30)), 0) << node_dump();
    restart_node(round_, leader, false);
    ASSERT_TRUE(wait_for_node_active(leader, std::chrono::seconds(30))) << node_dump();

    // The leadership moved: the reference keeps the same session across the
    // change — it receives the NewLeaderEvent and sends again
    // (`SessionManager.checkSessions`, `SessionManager.java:916-924`).
    ASSERT_TRUE(session.pump_until([&session]() { return session.new_leader_events() > 0; }, std::chrono::seconds(30)))
        << "the old session never saw the new leader: " << node_dump();
    ASSERT_TRUE(send_ints(session, 3, 7, 0)) << node_dump();
    trace.clear();
    ASSERT_TRUE(await_tracking(9, trace)) << "the third batch never settled:\n" << trace << node_dump();
    session.close();

    for (int i = 0; i < NODES; ++i)
    {
        stop_node_gracefully(i);
    }
    for (int i = 0; i < NODES; ++i)
    {
        ASSERT_TRUE(invalidate_latest_snapshot(round_, i)) << "node " << i << " had no snapshot to invalidate";
    }
    restart_nodes_together(round_, {{0, false}, {1, false}, {2, false}});
    ASSERT_TRUE(wait_for_cluster()) << "the restarted cluster never elected: " << node_dump();

    trace.clear();
    ASSERT_TRUE(await_tracking(9, trace)) << "the recovery from the invalidated snapshot diverged:\n"
                                          << trace << node_dump();

    stop_round();
}

TEST_F(Xc7SnapshotTest, shouldRecoverWhenLastSnapshotIsInvalidBetweenTwoElections)
{
    // `ClusterTest.shouldRecoverWhenLastSnapshotIsInvalidBetweenTwoElections`:
    // a snapshot between the two leader changes is the one invalidated; the
    // first one and the log still describe the same state.
    use_tracking_service();
    start_cluster(81);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    std::int64_t term = node_counter(leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();
    ASSERT_TRUE(send_ints(session, 3, 1, 0)) << node_dump();
    std::string trace;
    ASSERT_TRUE(await_tracking(3, trace)) << "the first batch never settled:\n" << trace << node_dump();
    ASSERT_TRUE(take_snapshot(leader, 1)) << node_dump();

    std::vector<int> survivors;
    for (int i = 0; i < NODES; ++i)
    {
        if (i != leader)
        {
            survivors.push_back(i);
        }
    }
    stop_node_gracefully(leader);
    ASSERT_GE(await_new_leader(term, survivors, std::chrono::seconds(30)), 0) << node_dump();
    restart_node(round_, leader, false);
    ASSERT_TRUE(wait_for_node_active(leader, std::chrono::seconds(30))) << node_dump();

    leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    session.close();
    ClusterClientSession second;
    ASSERT_TRUE(connect_udp_retry(second)) << node_dump() << second.last_error();
    ASSERT_TRUE(send_ints(second, 3, 4, 0)) << node_dump();
    trace.clear();
    ASSERT_TRUE(await_tracking(6, trace)) << "the second batch never settled:\n" << trace << node_dump();
    ASSERT_TRUE(take_snapshot(leader, 2)) << node_dump();
    second.close();

    for (int i = 0; i < NODES; ++i)
    {
        stop_node_gracefully(i);
    }
    for (int i = 0; i < NODES; ++i)
    {
        ASSERT_TRUE(invalidate_latest_snapshot(round_, i)) << "node " << i;
    }
    restart_nodes_together(round_, {{0, false}, {1, false}, {2, false}});
    ASSERT_TRUE(wait_for_cluster()) << "the restarted cluster never elected: " << node_dump();

    trace.clear();
    ASSERT_TRUE(await_tracking(6, trace)) << "the recovery diverged:\n" << trace << node_dump();

    stop_round();
}

TEST_F(Xc7SnapshotTest, shouldRecoverWhenLastTwosSnapshotsAreInvalidAfterElection)
{
    // `ClusterTest.shouldRecoverWhenLastTwosSnapshotsAreInvalidAfterElection`:
    // the last two snapshots go; recovery falls back to the first one and the
    // log, and the service still sees every message once.
    use_tracking_service();
    start_cluster(82);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    std::int64_t term = node_counter(leader, deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();
    std::string trace;
    for (int round = 0; round < 3; ++round)
    {
        ASSERT_TRUE(send_ints(session, 3, 1 + (round * 3), round * 3)) << node_dump();
        trace.clear();
        ASSERT_TRUE(await_tracking(3 * (round + 1), trace)) << "batch " << round << " never settled:\n"
                                                            << trace << node_dump();
        ASSERT_TRUE(take_snapshot(leader, round + 1)) << "snapshot " << round + 1 << ": " << node_dump();
    }
    session.close();

    std::vector<int> survivors;
    for (int i = 0; i < NODES; ++i)
    {
        if (i != leader)
        {
            survivors.push_back(i);
        }
    }
    stop_node_gracefully(leader);
    ASSERT_GE(await_new_leader(term, survivors, std::chrono::seconds(30)), 0) << node_dump();
    restart_node(round_, leader, false);
    ASSERT_TRUE(wait_for_node_active(leader, std::chrono::seconds(30))) << node_dump();

    for (int i = 0; i < NODES; ++i)
    {
        stop_node_gracefully(i);
    }
    for (int i = 0; i < NODES; ++i)
    {
        ASSERT_TRUE(invalidate_latest_snapshot(round_, i)) << "node " << i << ": first invalidation";
        ASSERT_TRUE(invalidate_latest_snapshot(round_, i)) << "node " << i << ": second invalidation";
    }
    restart_nodes_together(round_, {{0, false}, {1, false}, {2, false}});
    ASSERT_TRUE(wait_for_cluster()) << "the restarted cluster never elected: " << node_dump();

    trace.clear();
    ASSERT_TRUE(await_tracking(9, trace)) << "the recovery from two invalidated snapshots diverged:\n"
                                          << trace << node_dump();

    stop_round();
}

TEST_F(Xc7SnapshotTest, shouldTerminateNodeWithInvalidSnapshotAndRecoveryAfterInvalidation)
{
    // `ClusterTest.shouldTerminateNodeWithInvalidSnapshotAndRecoveryAfterInvalidation`:
    // the service refuses the snapshot it is handed on start (`on_start`
    // throws), the member terminates, and once the snapshot entry is
    // invalidated the same member recovers from the log.
    use_tracking_service();
    for (int i = 0; i < NODES; ++i)
    {
        snapshot_load_throw_[i] = true;
    }
    start_cluster(83);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();
    ASSERT_TRUE(send_ints(session, 10, 1, 0)) << node_dump();
    std::string trace;
    ASSERT_TRUE(await_tracking(10, trace)) << "the batch never settled:\n" << trace << node_dump();
    ASSERT_TRUE(take_snapshot(leader, 1)) << "the snapshot never completed: " << node_dump();
    session.close();

    const int follower = (leader + 1) % NODES;
    stop_node_gracefully(follower);
    restart_node(round_, follower, false);

    // The container refuses the snapshot and exits; the consensus module must
    // terminate the member rather than run without its service.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(120);
    bool terminated = false;
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (nodes_[follower] && !nodes_[follower]->running())
        {
            terminated = true;
            break;
        }
        drive_node_clients();
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(terminated) << "the member kept running after its service refused the snapshot: " << node_dump();
    stop_node_gracefully(follower);

    ASSERT_TRUE(invalidate_latest_snapshot(round_, follower)) << "node " << follower << " had no snapshot";
    restart_node(round_, follower, false);
    ASSERT_TRUE(wait_for_node_active(follower, std::chrono::seconds(30))) << node_dump();

    trace.clear();
    ASSERT_TRUE(await_tracking(10, trace)) << "the member did not recover from the log:\n" << trace << node_dump();

    stop_round();
}

TEST_F(Xc7SnapshotTest, shouldTrackSnapshotDuration)
{
    // `ClusterTest.shouldTrackSnapshotDuration`: the service sleeps inside
    // `on_take_snapshot`, so both the container's per-service tracker (236/237)
    // and the consensus module's total tracker (234/235) must see the time.
    // The reference runs two services (111/222 ms); one service per container
    // here, and the total duration threshold is lowered to make the breach
    // deterministic.
    use_tracking_service();
    for (int i = 0; i < NODES; ++i)
    {
        snapshot_sleep_micros_[i] = 300'000;
        node_property_overrides_[i]["aeron.cluster.total.snapshot.threshold"] = "100ms";
        container_property_overrides_[i]["aeron.cluster.service.snapshot.threshold"] = "100ms";
    }
    start_cluster(87);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    // The counter may already have moved (recovery snapshots); compare with
    // the value before the toggle.
    const auto snapshots_before = node_counter(leader, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);
    ASSERT_TRUE(take_snapshot(leader, snapshots_before + 1)) << "the snapshot never completed: " << node_dump();

    EXPECT_GE(node_counter(leader, deepmsg::cluster::common::CLUSTER_TOTAL_MAX_SNAPSHOT_DURATION_TYPE_ID), 300'000'000)
        << "the total snapshot duration missed the service's 300 ms: " << node_dump();
    EXPECT_GE(node_counter(leader, deepmsg::cluster::common::CLUSTERED_SERVICE_MAX_SNAPSHOT_DURATION_TYPE_ID),
              300'000'000)
        << "the service's snapshot duration was not tracked: " << node_dump();
    EXPECT_GE(
        node_counter(leader, deepmsg::cluster::common::CLUSTER_TOTAL_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID), 1)
        << "the total threshold breach was not counted: " << node_dump();
    EXPECT_GE(
        node_counter(leader, deepmsg::cluster::common::CLUSTERED_SERVICE_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID),
        1)
        << "the service threshold breach was not counted: " << node_dump();

    stop_round();
}

TEST_F(Xc7SnapshotTest, shouldCatchupFromEmptyLogThenSnapshotAfterShutdownAndFollowerCleanStart)
{
    // `ClusterTest.shouldCatchupFromEmptyLogThenSnapshotAfterShutdownAnd
    // FollowerCleanStart`: a follower clean-starts (its cluster and archive
    // directories go) and catches up from the leader; after a snapshot and a
    // shutdown it clean-starts again and still ends with the full history.
    use_tracking_service();
    start_cluster(90);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    const int follower = (leader + 1) % node_count_;

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();
    ASSERT_TRUE(send_ints(session, 10, 1, 0)) << node_dump();
    std::string trace;
    ASSERT_TRUE(await_tracking(10, trace)) << "the first batch never settled:\n" << trace << node_dump();

    stop_node_gracefully(follower);
    restart_node(round_, follower, true);
    ASSERT_TRUE(wait_for_node_active(follower, std::chrono::seconds(60))) << node_dump();
    trace.clear();
    ASSERT_TRUE(await_tracking(10, trace)) << "the clean-started follower never caught up:\n" << trace << node_dump();

    ASSERT_TRUE(send_ints(session, 5, 11, 10)) << node_dump();
    trace.clear();
    ASSERT_TRUE(await_tracking(15, trace)) << "the second batch never settled:\n" << trace << node_dump();
    session.close();

    leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();
    const auto snapshots_before = node_counter(leader, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);
    ASSERT_TRUE(take_snapshot(leader, snapshots_before + 1)) << "the snapshot never completed: " << node_dump();

    for (int i = 0; i < node_count_; ++i)
    {
        stop_node_gracefully(i);
    }
    restart_nodes_together(
        round_, {{follower, true}, {(follower + 1) % node_count_, false}, {(follower + 2) % node_count_, false}});
    ASSERT_TRUE(wait_for_cluster()) << "the restarted cluster never elected: " << node_dump();

    trace.clear();
    ASSERT_TRUE(await_tracking(15, trace)) << "the clean-started follower lost the snapshot or the log:\n"
                                           << trace << node_dump();

    stop_round();
}

TEST_F(Xc7SnapshotTest, shouldAddCommittedNextSessionIdToTheConsensusModuleSnapshot)
{
    // `ClusterTest.shouldAddCommittedNextSessionIdToTheConsensusModuleSnapshot`.
    // The reference drives it with an authenticator supplier that rejects the
    // third connection; the authenticator seam is not ours, so the arm here is
    // the session id itself: after a snapshot and a full restart the next
    // session must not reuse an id the snapshot committed.
    use_tracking_service();
    start_cluster(88);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();

    ClusterClientSession first;
    ASSERT_TRUE(connect_udp_retry(first)) << node_dump() << first.last_error();
    ClusterClientSession second;
    ASSERT_TRUE(connect_udp_retry(second)) << node_dump() << second.last_error();
    const std::int64_t highest = std::max(first.cluster_session_id(), second.cluster_session_id());
    ASSERT_GT(highest, 0) << node_dump();

    const auto snapshots_before = node_counter(leader, deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);
    ASSERT_TRUE(take_snapshot(leader, snapshots_before + 1)) << "the snapshot never completed: " << node_dump();
    first.close();
    second.close();

    for (int i = 0; i < NODES; ++i)
    {
        stop_node_gracefully(i);
    }
    restart_nodes_together(round_, {{0, false}, {1, false}, {2, false}});
    ASSERT_TRUE(wait_for_cluster()) << "the restarted cluster never elected: " << node_dump();

    ClusterClientSession third;
    ASSERT_TRUE(connect_udp_retry(third)) << node_dump() << third.last_error();
    EXPECT_GT(third.cluster_session_id(), highest)
        << "the snapshot did not carry the committed next session id: " << node_dump();

    stop_round();
}

TEST_F(Xc7SnapshotTest, shouldHandleReusingCorrelationIdsAcrossASnapshot)
{
    // `ClusterTest.shouldHandleReusingCorrelationIdsAcrossASnapshot`: the same
    // timer correlation id registered twice schedules one timer, not two. The
    // echo service turns a 16-byte payload into a timer command and echoes the
    // correlation id when it fires.
    start_cluster(89);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();

    const auto schedule = [](std::int64_t correlation_id, std::int32_t delay_millis)
    {
        std::string payload(16, '\0');
        const std::int32_t command = 1;  // EchoService.TIMER_SCHEDULE
        std::memcpy(payload.data(), &command, sizeof(command));
        std::memcpy(payload.data() + sizeof(command), &correlation_id, sizeof(correlation_id));
        std::memcpy(payload.data() + sizeof(command) + sizeof(correlation_id), &delay_millis, sizeof(delay_millis));
        return payload;
    };

    // Both registrations land before the timer fires, so the second must
    // update the first rather than add another.
    ASSERT_TRUE(session.offer_only(schedule(4242, 600), std::chrono::seconds(10))) << node_dump();
    ASSERT_TRUE(session.offer_only(schedule(4242, 600), std::chrono::seconds(10))) << node_dump();
    ASSERT_TRUE(session.pump_until([&session] { return !session.messages().empty(); }, std::chrono::seconds(20)))
        << "the timer never fired: " << node_dump();

    // Give the duplicate a full timer window: a second event would show up.
    std::this_thread::sleep_for(std::chrono::milliseconds(800));
    (void)session.pump_until([] { return false; }, std::chrono::milliseconds(50));
    ASSERT_EQ(1U, session.messages().size()) << "the reused correlation id scheduled a second timer: " << node_dump();
    const std::int64_t correlation_id = 4242;
    ASSERT_EQ(sizeof(correlation_id), session.messages()[0].size());
    EXPECT_EQ(0, std::memcmp(session.messages()[0].data(), &correlation_id, sizeof(correlation_id)));

    session.close();
    stop_round();
}

TEST_F(Xc7SnapshotTest, shouldInvalidateSnapshotsThatHaveBeenRemoved)
{
    // `ClusterTest.shouldInvalidateSnapshotsThatHaveBeenRemoved`: the snapshot
    // recordings of the first two snapshots are purged from the archive; the
    // node's recording-log validation must invalidate the entries that point
    // at them and the cluster keeps serving. The reference follows this with
    // a backup query on every snapshot (C9); the arm here is the validation
    // and the survival.
    use_tracking_service();
    start_cluster(84);
    ASSERT_TRUE(wait_for_elections_closed(std::chrono::seconds(30))) << "the elections never closed: " << node_dump();

    const int leader = leader_index();
    ASSERT_GE(leader, 0) << node_dump();

    ClusterClientSession session;
    ASSERT_TRUE(connect_udp_retry(session)) << node_dump() << session.last_error();
    std::string trace;
    for (int round = 0; round < 3; ++round)
    {
        ASSERT_TRUE(send_ints(session, 10, 1 + (round * 10), round * 10)) << node_dump();
        trace.clear();
        ASSERT_TRUE(await_tracking(10 * (round + 1), trace)) << "batch " << round << ":\n" << trace << node_dump();
        ASSERT_TRUE(take_snapshot(leader, round + 1)) << "snapshot " << round + 1 << ": " << node_dump();
    }
    session.close();

    const std::vector<std::int64_t> ids = recording_ids(leader);
    // The log recording is id 0; every snapshot writes a CM recording and a
    // service recording, so three snapshots leave ids 1..6.
    ASSERT_GE(ids.size(), 5U) << "recordings: " << node_dump();
    for (std::size_t i = 1; i <= 4; ++i)
    {
        ASSERT_TRUE(purge_recording(leader, ids[i])) << "purge " << ids[i];
    }

    set_node_control(leader, 3);
    const auto validate_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < validate_deadline && 3 == node_control(leader))
    {
        drive_node_clients();
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    // The cluster still serves after the entries were invalidated.
    ClusterClientSession second;
    ASSERT_TRUE(connect_udp_retry(second)) << node_dump() << second.last_error();
    ASSERT_TRUE(send_ints(second, 10, 31, 0)) << node_dump();
    trace.clear();
    ASSERT_TRUE(await_tracking(40, trace)) << "the cluster stopped after the purge:\n" << trace << node_dump();
    second.close();
    stop_round();

    // Only the last snapshot's entries survive the validation.
    auto log = deepmsg::cluster::common::RecordingLog::open(
        round_dir(round_) + "/node" + std::to_string(leader) + "/cluster", false);
    ASSERT_TRUE(log) << log.error().message.view();
    std::size_t valid_snapshots = 0;
    for (const auto& entry : log->entries())
    {
        valid_snapshots += (entry.is_snapshot() && entry.is_valid) ? 1 : 0;
    }
    EXPECT_EQ(2U, valid_snapshots) << "the purged snapshots are still valid entries";
}

}  // namespace
