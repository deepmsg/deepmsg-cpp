// C3c / XC-1: the client scenarios that need real bytes, against the
// reference's own `ClusteredMediaDriver` (Java ConsensusModule + archive) and
// our echo service container.
//
// The reference's client unit tests (`AeronClusterTest`, `EgressAdapterTest`)
// mock the driver; `xc1_client_unit_tests` ports them in
// `test/unit/cluster/client/`. What only an interop run can prove is that the
// same calls -- offer, offerv, tryClaim, controlled poll, keep-alive, close --
// travel the real protocol with the reference CM on the other side, and that
// two sessions on one cluster stay apart.
//
// Missing jar or container binary = SKIP with the reason printed.

#include <gtest/gtest.h>

#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "cluster/client/async_connect.h"
#include "cluster/client/client_config.h"
#include "cluster/client/cluster_client.h"
#include "cluster/common/cluster_counters.h"
#include "core/deepmsg/deepmsg_client.h"
#include "fixture/process.h"

#ifndef AXC_AERON_JAR
#define AXC_AERON_JAR ""
#endif
#ifndef AXC_CONTAINER_BINARY
#define AXC_CONTAINER_BINARY ""
#endif

namespace
{

namespace codecs = deepmsg::cluster::common::codecs;

using axc::core::DeepmsgClient;
using deepmsg::cluster::client::AsyncConnect;
using deepmsg::cluster::client::ClientConfig;
using deepmsg::cluster::client::ClusterClient;
using deepmsg::cluster::client::ControlledEgressListener;
using deepmsg::cluster::client::EgressListener;

constexpr std::int32_t CLUSTER_ID = 11;

[[nodiscard]] std::string aeron_jar()
{
    if (const char* from_env = std::getenv("AXC_AERON_JAR"); nullptr != from_env && '\0' != *from_env)
    {
        return from_env;
    }
    return AXC_AERON_JAR;
}

[[nodiscard]] std::span<const std::uint8_t> bytes(std::string_view text)
{
    return std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(text.data()), text.size());
}

struct RecordingListener final : EgressListener
{
    struct Message
    {
        std::int64_t session_id{};
        std::string payload{};
    };

    std::vector<Message> messages{};

    void on_message(std::int64_t cluster_session_id, std::int64_t, std::span<const std::uint8_t> payload) override
    {
        messages.push_back(Message{cluster_session_id, std::string(payload.begin(), payload.end())});
    }

    void on_session_event(std::int64_t, std::int64_t, std::int64_t, std::int32_t, codecs::EventCode::Value,
                          std::string_view) override
    {
    }

    void on_new_leader(std::int64_t, std::int64_t, std::int32_t, std::string_view) override {}

    void on_admin_response(std::int64_t, std::int64_t, codecs::AdminRequestType::Value,
                           codecs::AdminResponseCode::Value, std::string_view, std::span<const std::uint8_t>) override
    {
    }
};

struct ControlledRecordingListener final : ControlledEgressListener
{
    std::vector<std::string> messages{};

    aeron_controlled_fragment_handler_action_t on_message(std::int64_t, std::int64_t,
                                                          std::span<const std::uint8_t> payload) override
    {
        messages.emplace_back(payload.begin(), payload.end());
        return AERON_ACTION_CONTINUE;
    }

    void on_session_event(std::int64_t, std::int64_t, std::int64_t, std::int32_t, codecs::EventCode::Value,
                          std::string_view) override
    {
    }

    void on_new_leader(std::int64_t, std::int64_t, std::int32_t, std::string_view) override {}

    void on_admin_response(std::int64_t, std::int64_t, codecs::AdminRequestType::Value,
                           codecs::AdminResponseCode::Value, std::string_view, std::span<const std::uint8_t>) override
    {
    }
};

/// A connected session: the `DeepmsgClient` must outlive the `ClusterClient`
/// and must not move after the connect (see `cluster_client.h`).
struct Session
{
    std::unique_ptr<DeepmsgClient> client{};
    std::optional<ClusterClient> cluster{};
    RecordingListener listener{};
};

class Xc1ClientInteropTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        jar_ = aeron_jar();
        if (jar_.empty() || !std::filesystem::exists(jar_))
        {
            GTEST_SKIP() << "reference jar not found (set AXC_AERON_JAR): " << jar_;
        }
        if (!std::filesystem::exists(AXC_CONTAINER_BINARY))
        {
            GTEST_SKIP() << "container binary not found: " << AXC_CONTAINER_BINARY;
        }

        directory_ = std::filesystem::temp_directory_path() / ("axc-c3c-" + std::to_string(::getpid()));
        std::filesystem::remove_all(directory_);
        std::filesystem::create_directories(directory_);
        aeron_dir_ = (directory_ / "aeron").string();
        cluster_dir_ = (directory_ / "cluster").string();
        archive_dir_ = (directory_ / "archive").string();
    }

    void TearDown() override
    {
        stop_container();
        if (java_running_)
        {
            java_.terminate(std::chrono::seconds(5));
        }
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
    }

    void start_node_and_container()
    {
        start_java_node();
        start_container();
    }

    [[nodiscard]] std::vector<std::string> java_node_arguments() const
    {
        return {"--add-opens",
                "java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens",
                "java.base/sun.nio.ch=ALL-UNNAMED",
                "-Daeron.dir=" + aeron_dir_,
                "-Daeron.cluster.dir=" + cluster_dir_,
                "-Daeron.archive.dir=" + archive_dir_,
                "-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:0",
                "-Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0",
                "-Daeron.cluster.id=" + std::to_string(CLUSTER_ID),
                "-Daeron.cluster.member.id=0",
                "-Daeron.cluster.members=0,localhost:0,localhost:0,localhost:0,localhost:0,localhost:0",
                "-Daeron.cluster.ingress.channel=aeron:ipc",
                "-Daeron.cluster.consensus.channel=aeron:ipc",
                "-Daeron.cluster.log.channel=aeron:ipc",
                "-Daeron.cluster.control.channel=aeron:ipc?term-length=128k",
                "-Daeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0",
                "-Daeron.cluster.service.count=1",
                "-Daeron.threading.mode=SHARED",
                "-Daeron.print.configuration=false",
                "-cp",
                jar_,
                "io.aeron.cluster.ClusteredMediaDriver"};
    }

    void start_java_node()
    {
        auto process = axc::interop::ChildProcess::spawn("java", java_node_arguments());
        ASSERT_TRUE(process) << process.error().message.view();
        java_ = std::move(*process);
        java_running_ = true;

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline)
        {
            ASSERT_TRUE(java_.running()) << "the Java cluster node exited while starting";
            if (cm_state() >= 0)
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        FAIL() << "the Java consensus module never published its state counter";
    }

    [[nodiscard]] std::int64_t counter_value(std::int32_t type_id)
    {
        auto client = axc::core::DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir_});
        if (!client)
        {
            return -1;
        }
        (void)client->do_work();
        const std::int32_t counter_id =
            deepmsg::cluster::common::find_cluster_counter(client->counters_reader(), type_id, CLUSTER_ID);
        if (AERON_NULL_COUNTER_ID == counter_id)
        {
            return -1;
        }
        std::int64_t* address = aeron_counters_reader_addr(client->counters_reader(), counter_id);
        return nullptr == address ? -1 : __atomic_load_n(address, __ATOMIC_ACQUIRE);
    }

    [[nodiscard]] std::int64_t cm_state()
    {
        return counter_value(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID);
    }

    void start_container()
    {
        const std::vector<std::string> arguments = {
            "-Daeron.dir=" + aeron_dir_,
            "-Daeron.cluster.dir=" + cluster_dir_,
            "-Daeron.cluster.id=" + std::to_string(CLUSTER_ID),
            "-Daeron.cluster.service.id=0",
            "-Daeron.cluster.service.name=clustered-service-" + std::to_string(CLUSTER_ID) + "-0",
            "-Daeron.cluster.control.channel=aeron:ipc?term-length=128k",
            "-Daeron.cluster.service.stream.id=104",
            "-Daeron.cluster.consensus.module.stream.id=105",
            "-Daeron.cluster.log.fragment.limit=50",
            "-Daeron.cluster.service.responder=true",
            "-Daeron.cluster.idle.strategy=org.agrona.concurrent.BackoffIdleStrategy",
            "-Daeron.cluster.replay.channel=aeron:ipc",
            "-Daeron.cluster.replay.stream.id=103",
            "-Daeron.cluster.snapshot.channel=aeron:ipc?alias=snapshot",
            "-Daeron.cluster.snapshot.stream.id=106",
            "-Daeron.archive.local.control.channel=aeron:ipc?term-length=64k",
            "-Daeron.archive.local.control.stream.id=10",
            "-Daeron.archive.control.response.stream.id=20",
            "-Ddeepmsg.cluster.service=echo"};

        auto process = axc::interop::ChildProcess::spawn(AXC_CONTAINER_BINARY, arguments);
        ASSERT_TRUE(process) << process.error().message.view();
        container_ = std::move(*process);
        container_running_ = true;

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline)
        {
            ASSERT_TRUE(container_.running()) << "the container binary exited while starting";
            if (cm_state() == 1)
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        FAIL() << "the consensus module never became active";
    }

    void stop_container()
    {
        if (container_running_)
        {
            container_.terminate(std::chrono::seconds(10));
            container_running_ = false;
        }
    }

    /// `AsyncConnect` to DONE, with the reference node on the other side.
    bool connect_session(Session& session)
    {
        auto connected = DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir_});
        if (!connected)
        {
            return false;
        }
        session.client = std::make_unique<DeepmsgClient>(std::move(*connected));

        ClientConfig config;
        config.ingress_channel = "aeron:ipc";
        config.egress_channel = "aeron:ipc";
        config.client_name = "c3c-probe";
        if (!config.conclude().has_value())
        {
            return false;
        }

        auto connect = AsyncConnect::create(config);
        if (!connect.has_value())
        {
            return false;
        }

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (std::chrono::steady_clock::now() < deadline)
        {
            auto polled = connect->poll(*session.client);
            if (!polled || polled->has_value())
            {
                if (polled && polled->has_value())
                {
                    session.cluster = std::move(**polled);
                    session.cluster->set_egress_listener(&session.listener);
                    return true;
                }
                return false;
            }
            (void)session.client->do_work();
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
        return false;
    }

    /// Drain the client and poll egress until `predicate` holds.
    template <class Predicate>
    [[nodiscard]] bool pump(Session& session, Predicate predicate,
                            std::chrono::milliseconds timeout = std::chrono::seconds(10))
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            (void)session.client->do_work();
            auto polled = session.cluster->poll_egress();
            if (!polled)
            {
                ADD_FAILURE() << "poll_egress: " << polled.error().message.view()
                              << " errcode=" << polled.error().errcode;
                return false;
            }
            if (predicate())
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
        return false;
    }

    std::string jar_{};
    std::filesystem::path directory_{};
    std::string aeron_dir_{};
    std::string cluster_dir_{};
    std::string archive_dir_{};
    axc::interop::ChildProcess java_{};
    axc::interop::ChildProcess container_{};
    bool java_running_{false};
    bool container_running_{false};
};

TEST_F(Xc1ClientInteropTest, twoSessionsEachGetTheirOwnEcho)
{
    start_node_and_container();

    Session first;
    Session second;
    ASSERT_TRUE(connect_session(first)) << "the first client never connected";
    ASSERT_TRUE(connect_session(second)) << "the second client never connected";
    ASSERT_NE(first.cluster->cluster_session_id(), second.cluster->cluster_session_id());

    EXPECT_GT(first.cluster->offer(bytes("from-first")), 0);
    EXPECT_GT(second.cluster->offer(bytes("from-second")), 0);

    ASSERT_TRUE(pump(first, [&] { return !first.listener.messages.empty(); }));
    ASSERT_TRUE(pump(second, [&] { return !second.listener.messages.empty(); }));

    ASSERT_EQ(1U, first.listener.messages.size());
    EXPECT_EQ(first.cluster->cluster_session_id(), first.listener.messages[0].session_id);
    EXPECT_EQ("from-first", first.listener.messages[0].payload);

    ASSERT_EQ(1U, second.listener.messages.size());
    EXPECT_EQ(second.cluster->cluster_session_id(), second.listener.messages[0].session_id);
    EXPECT_EQ("from-second", second.listener.messages[0].payload);
}

TEST_F(Xc1ClientInteropTest, offervCarriesBothRegionsAcrossTheWire)
{
    start_node_and_container();

    Session session;
    ASSERT_TRUE(connect_session(session));

    const std::span<const std::uint8_t> parts[] = {bytes("hello-"), bytes("vectored")};
    EXPECT_GT(session.cluster->offer_vectored(parts), 0);

    ASSERT_TRUE(pump(session, [&] { return !session.listener.messages.empty(); }));
    ASSERT_EQ(1U, session.listener.messages.size());
    EXPECT_EQ("hello-vectored", session.listener.messages[0].payload);
}

TEST_F(Xc1ClientInteropTest, tryClaimPublishesTheClaimedBytes)
{
    start_node_and_container();

    Session session;
    ASSERT_TRUE(connect_session(session));

    auto claim = session.cluster->try_claim(4);
    ASSERT_TRUE(claim.has_value());
    claim->payload()[0] = 'w';
    claim->payload()[1] = 'x';
    claim->payload()[2] = 'y';
    claim->payload()[3] = 'z';
    EXPECT_GT(claim->commit(), 0);

    ASSERT_TRUE(pump(session, [&] { return !session.listener.messages.empty(); }));
    ASSERT_EQ(1U, session.listener.messages.size());
    EXPECT_EQ("wxyz", session.listener.messages[0].payload);
}

TEST_F(Xc1ClientInteropTest, controlledPollEgressDeliversTheEcho)
{
    start_node_and_container();

    Session session;
    ASSERT_TRUE(connect_session(session));

    ControlledRecordingListener listener;
    session.cluster->set_egress_listener(nullptr);
    session.cluster->set_controlled_egress_listener(&listener);

    EXPECT_GT(session.cluster->offer(bytes("controlled")), 0);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline && listener.messages.empty())
    {
        (void)session.client->do_work();
        auto polled = session.cluster->controlled_poll_egress();
        ASSERT_TRUE(polled) << polled.error().message.view();
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }

    ASSERT_EQ(1U, listener.messages.size());
    EXPECT_EQ("controlled", listener.messages[0]);
}

TEST_F(Xc1ClientInteropTest, keepAliveKeepsTheSessionAndTheRoundTripStillWorks)
{
    start_node_and_container();

    Session session;
    ASSERT_TRUE(connect_session(session));

    auto sent = session.cluster->send_keep_alive();
    ASSERT_TRUE(sent.has_value());
    EXPECT_TRUE(*sent);
    EXPECT_EQ(ClusterClient::State::connected, session.cluster->state());

    // 16 bytes would be the echo service's timer protocol, so the payload is
    // deliberately not 16 (`echo_service.cpp`).
    EXPECT_GT(session.cluster->offer(bytes("after-keep-alive-msg")), 0);
    ASSERT_TRUE(pump(session, [&] { return !session.listener.messages.empty(); }));
    ASSERT_EQ(1U, session.listener.messages.size());
    EXPECT_EQ("after-keep-alive-msg", session.listener.messages[0].payload);
    EXPECT_FALSE(session.cluster->is_closed());
}

TEST_F(Xc1ClientInteropTest, closingOneSessionLeavesTheOtherAndTheClusterWorking)
{
    start_node_and_container();

    Session closed;
    Session survivor;
    ASSERT_TRUE(connect_session(closed));
    ASSERT_TRUE(connect_session(survivor));
    const std::int64_t closed_session_id = closed.cluster->cluster_session_id();

    closed.cluster->close();
    EXPECT_TRUE(closed.cluster->is_closed());
    EXPECT_EQ(AERON_PUBLICATION_NOT_CONNECTED, closed.cluster->offer(bytes("too-late")));

    // The surviving session still round-trips, and a new client gets a new
    // session id -- the cluster is not confused by the close.
    EXPECT_GT(survivor.cluster->offer(bytes("still-here")), 0);
    ASSERT_TRUE(pump(survivor, [&] { return !survivor.listener.messages.empty(); }));
    EXPECT_EQ("still-here", survivor.listener.messages[0].payload);

    Session fresh;
    ASSERT_TRUE(connect_session(fresh));
    EXPECT_NE(closed_session_id, fresh.cluster->cluster_session_id());
    EXPECT_GT(fresh.cluster->offer(bytes("fresh")), 0);
    ASSERT_TRUE(pump(fresh, [&] { return !fresh.listener.messages.empty(); }));
    EXPECT_EQ("fresh", fresh.listener.messages[0].payload);
}

}  // namespace
