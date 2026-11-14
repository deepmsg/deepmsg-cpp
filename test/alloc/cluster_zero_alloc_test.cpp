// C4f: R1 for the cluster single-member steady state -- after a 10^4 message
// warm-up the CM and container threads must not call the allocator for the next
// 10^5 messages.
//
// Topology: embedded C driver + DEDICATED C++ archive + CM agent and C++
// container agent on their own labelled threads, a C++ ClusterClient on a
// third. The driver/archive threads are deliberate `other` labels: R1 is about
// our agent paths, not the infrastructure.

#include <gtest/gtest.h>

#include <aeronc.h>
#include <aeronmd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <thread>

#include <unistd.h>

#include "alloc_probe.h"
#include "apps/services/echo-service/echo_service.h"
#include "archive/server/archive_launch.h"
#include "cluster/client/async_connect.h"
#include "cluster/client/cluster_client.h"
#include "cluster/consensus/consensus_module_agent.h"
#include "cluster/consensus/consensus_module_context.h"
#include "cluster/service/clustered_service_agent.h"
#include "cluster/service/container_config.h"
#include "core/deepmsg/deepmsg_client.h"

namespace
{

using axc::alloc_test::count;
using axc::alloc_test::Label;
using axc::core::DeepmsgClient;
using deepmsg::apps::EchoService;
using deepmsg::archive::server::ArchiveLaunchConfig;
using deepmsg::archive::server::ArchiveServer;
using deepmsg::cluster::client::AsyncConnect;
using deepmsg::cluster::client::ClusterClient;
using deepmsg::cluster::client::EgressListener;
using deepmsg::cluster::consensus::ConsensusModuleAgent;
using deepmsg::cluster::consensus::ConsensusModuleConfig;
using deepmsg::cluster::service::ClusteredServiceAgent;
using deepmsg::cluster::service::ServiceContainerConfig;

constexpr std::int32_t CLUSTER_ID = 13;
constexpr std::int32_t MEMBER_ID = 0;
constexpr std::int32_t SERVICE_ID = 0;
constexpr const char* MEMBERS = "0,localhost:0,localhost:0,localhost:0,localhost:0,localhost:0";
constexpr std::int64_t WARMUP_MESSAGES = 10'000;
constexpr std::int64_t MEASURED_MESSAGES = 100'000;

class EmbeddedDriver
{
public:
    EmbeddedDriver()
    {
        char pattern[] = "/tmp/axc-c4f-alloc-XXXXXX";
        if (nullptr == ::mkdtemp(pattern))
        {
            return;
        }
        dir_ = pattern;
        if (0 != aeron_driver_context_init(&context_))
        {
            return;
        }
        (void)aeron_driver_context_set_dir(context_, dir_.c_str());
        aeron_driver_context_set_dir_delete_on_start(context_, true);
        aeron_driver_context_set_dir_delete_on_shutdown(context_, true);
        if (0 != aeron_driver_init(&driver_, context_))
        {
            return;
        }
        started_ = 0 == aeron_driver_start(driver_, false);
    }

    ~EmbeddedDriver()
    {
        if (nullptr != driver_)
        {
            (void)aeron_driver_close(driver_);
        }
        if (nullptr != context_)
        {
            (void)aeron_driver_context_close(context_);
        }
        std::error_code error;
        std::filesystem::remove_all(dir_, error);
    }

    [[nodiscard]] bool started() const noexcept
    {
        return started_;
    }

    [[nodiscard]] const std::string& dir() const noexcept
    {
        return dir_;
    }

private:
    std::string dir_{};
    aeron_driver_context_t* context_{nullptr};
    aeron_driver_t* driver_{nullptr};
    bool started_{false};
};

class EchoListener final : public EgressListener
{
public:
    void on_message(std::int64_t /*cluster_session_id*/, std::int64_t /*timestamp*/,
                    std::span<const std::uint8_t> /*payload*/) override
    {
        received.fetch_add(1, std::memory_order_release);
    }

    void on_session_event(std::int64_t, std::int64_t, std::int64_t, std::int32_t,
                          deepmsg::cluster::common::codecs::EventCode::Value, std::string_view) override
    {
    }

    void on_new_leader(std::int64_t, std::int64_t, std::int32_t, std::string_view) override {}

    void on_admin_response(std::int64_t, std::int64_t, deepmsg::cluster::common::codecs::AdminRequestType::Value,
                           deepmsg::cluster::common::codecs::AdminResponseCode::Value, std::string_view,
                           std::span<const std::uint8_t>) override
    {
    }

    std::atomic<std::int64_t> received{0};
};

class ClusterZeroAllocTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        ASSERT_TRUE(driver_.started());
        const std::string dir = driver_.dir();

        ArchiveLaunchConfig archive;
        archive.aeron_dir = dir;
        archive.archive_dir = dir + "/archive";
        archive.mark_file_dir = archive.archive_dir;
        archive.control_channel = "aeron:udp?endpoint=localhost:0";
        archive.replication_channel = "aeron:udp?endpoint=localhost:0";
        archive.local_control_channel = "aeron:ipc?term-length=64k";
        archive.local_control_stream_id = 10;
        auto prepared = deepmsg::archive::server::prepare_archive_directories(archive);
        ASSERT_TRUE(prepared) << prepared.error().message.view();
        auto created = ArchiveServer::create(archive);
        ASSERT_TRUE(created) << created.error().message.view();
        archive_ = std::move(*created);
        auto started = archive_->start();
        ASSERT_TRUE(started) << started.error().message.view();

        cm_config_.cluster_id = CLUSTER_ID;
        cm_config_.member_id = MEMBER_ID;
        cm_config_.members = MEMBERS;
        cm_config_.aeron_dir = dir;
        cm_config_.cluster_dir = dir + "/cluster";
        cm_config_.ingress_channel = "aeron:ipc";
        cm_config_.consensus_channel = "aeron:ipc";
        cm_config_.log_channel =
            "aeron:udp?endpoint=localhost:" + std::to_string(24600 + (static_cast<int>(::getpid()) % 500));
        cm_config_.replication_channel = "aeron:udp";
        cm_config_.control_channel = "aeron:ipc?term-length=128k";
        cm_config_.service_count = 1;
        auto concluded = cm_config_.conclude();
        ASSERT_TRUE(concluded) << concluded.error().message.view();

        container_config_.cluster_id = CLUSTER_ID;
        container_config_.service_id = SERVICE_ID;
        container_config_.service_name =
            "clustered-service-" + std::to_string(CLUSTER_ID) + "-" + std::to_string(SERVICE_ID);
        container_config_.aeron_dir = dir;
        container_config_.cluster_dir = cm_config_.cluster_dir;
        container_config_.control_channel = "aeron:ipc?term-length=128k";
    }

    void TearDown() override
    {
        stop_.store(true, std::memory_order_release);
        if (client_thread_.joinable())
        {
            client_thread_.join();
        }
        if (container_thread_.joinable())
        {
            container_thread_.join();
        }
        if (cm_thread_.joinable())
        {
            cm_thread_.join();
        }
        if (archive_)
        {
            archive_->close();
            archive_.reset();
        }
    }

    void start_agents()
    {
        cm_thread_ = std::thread(
            [this]()
            {
                axc::alloc_test::label_thread("cm");
                cm_ = std::make_unique<ConsensusModuleAgent>(cm_config_);
                auto started = cm_->on_start();
                cm_started_.store(started.has_value(), std::memory_order_release);
                if (!started)
                {
                    return;
                }
                while (!stop_.load(std::memory_order_acquire))
                {
                    (void)cm_->do_work();
                    std::this_thread::yield();
                }
                cm_->on_close();
            });

        container_thread_ = std::thread(
            [this]()
            {
                axc::alloc_test::label_thread("container");
                // The CM creates its control publication inside `on_start`;
                // give it a moment, then retry a container that races it.
                std::this_thread::sleep_for(std::chrono::milliseconds(300));
                while (!stop_.load(std::memory_order_acquire))
                {
                    container_ = std::make_unique<ClusteredServiceAgent<EchoService>>(container_config_, EchoService{});
                    auto started = container_->on_start();
                    container_started_.store(started.has_value(), std::memory_order_release);
                    if (started)
                    {
                        break;
                    }
                    container_.reset();
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }
                while (!stop_.load(std::memory_order_acquire) && container_)
                {
                    (void)container_->do_work();
                    std::this_thread::yield();
                }
                if (container_)
                {
                    container_->on_close();
                }
            });

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (cm_started_.load(std::memory_order_acquire) && container_started_.load(std::memory_order_acquire))
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        FAIL() << "the CM/container never reached ACTIVE";
    }

    void start_client()
    {
        client_thread_ = std::thread(
            [this]()
            {
                axc::alloc_test::label_thread("client");
                auto client = DeepmsgClient::connect(axc::core::ClientConfig{driver_.dir()});
                if (!client)
                {
                    client_failed_.store(true, std::memory_order_release);
                    return;
                }
                client_ = std::make_unique<DeepmsgClient>(std::move(*client));

                deepmsg::cluster::client::ClientConfig config;
                config.ingress_channel = "aeron:ipc";
                config.egress_channel = "aeron:ipc";
                config.client_name = "zero-alloc";
                if (!config.conclude().has_value())
                {
                    client_failed_.store(true, std::memory_order_release);
                    return;
                }
                auto connect = AsyncConnect::create(config);
                if (!connect.has_value())
                {
                    client_failed_.store(true, std::memory_order_release);
                    return;
                }

                const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
                while (std::chrono::steady_clock::now() < deadline && !stop_.load(std::memory_order_acquire))
                {
                    auto polled = connect->poll(*client_);
                    if (polled && polled->has_value())
                    {
                        cluster_ = std::make_unique<ClusterClient>(std::move(**polled));
                        cluster_->set_egress_listener(&listener_);
                        break;
                    }
                    (void)client_->do_work();
                    std::this_thread::yield();
                }
                if (!cluster_)
                {
                    client_failed_.store(true, std::memory_order_release);
                    return;
                }

                send_round(WARMUP_MESSAGES);
                warmup_done_.store(true, std::memory_order_release);
                while (!go_.load(std::memory_order_acquire) && !stop_.load(std::memory_order_acquire))
                {
                    // Keep the conductor running so its one-time heartbeat-label
                    // extension (third-party C client) lands in the warm-up.
                    (void)client_->do_work();
                    (void)cluster_->poll_egress();
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                send_round(MEASURED_MESSAGES);
                measured_done_.store(true, std::memory_order_release);

                cluster_->close();
                client_->close();
            });
    }

    void send_round(std::int64_t messages)
    {
        std::uint8_t payload[4] = {};
        for (std::int64_t i = 0; i < messages && !stop_.load(std::memory_order_acquire); ++i)
        {
            const std::int32_t value = static_cast<std::int32_t>(i);
            std::memcpy(payload, &value, sizeof(value));
            std::int64_t result = cluster_->offer(std::span<const std::uint8_t>(payload, sizeof(payload)));
            while (result < 0 && !stop_.load(std::memory_order_acquire))
            {
                (void)cluster_->poll_egress();
                (void)client_->do_work();
                result = cluster_->offer(std::span<const std::uint8_t>(payload, sizeof(payload)));
            }
            const std::int64_t target = listener_.received.load(std::memory_order_acquire) + 1;
            while (listener_.received.load(std::memory_order_acquire) < target &&
                   !stop_.load(std::memory_order_acquire))
            {
                (void)cluster_->poll_egress();
                (void)client_->do_work();
            }
        }
    }

    EmbeddedDriver driver_{};
    std::unique_ptr<ArchiveServer> archive_{};
    ConsensusModuleConfig cm_config_{};
    ServiceContainerConfig container_config_{};
    std::unique_ptr<ConsensusModuleAgent> cm_{};
    std::unique_ptr<ClusteredServiceAgent<EchoService>> container_{};
    std::unique_ptr<DeepmsgClient> client_{};
    std::unique_ptr<ClusterClient> cluster_{};
    EchoListener listener_{};
    std::thread cm_thread_{};
    std::thread container_thread_{};
    std::thread client_thread_{};
    std::atomic<bool> stop_{false};
    std::atomic<bool> cm_started_{false};
    std::atomic<bool> container_started_{false};
    std::atomic<bool> warmup_done_{false};
    std::atomic<bool> go_{false};
    std::atomic<bool> measured_done_{false};
    std::atomic<bool> client_failed_{false};
};

TEST_F(ClusterZeroAllocTest, steadyStateEchoDoesNotAllocate)
{
    start_agents();
    start_client();

    const auto warmup_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(120);
    while (!warmup_done_.load(std::memory_order_acquire) && !client_failed_.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < warmup_deadline)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_FALSE(client_failed_.load(std::memory_order_acquire)) << "the client never connected";
    ASSERT_TRUE(warmup_done_.load(std::memory_order_acquire)) << "the warm-up traffic did not finish";

    // Let the agents drain the last warm-up bookkeeping (the client is parked
    // on `go_`, so no new traffic arrives) before the counter is zeroed.
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    axc::alloc_test::reset();
    go_.store(true, std::memory_order_release);

    const auto measured_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(300);
    while (!measured_done_.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < measured_deadline)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(measured_done_.load(std::memory_order_acquire)) << "the measured traffic did not finish";

    EXPECT_EQ(WARMUP_MESSAGES + MEASURED_MESSAGES, listener_.received.load(std::memory_order_acquire));
    EXPECT_EQ(0, count(Label::cm)) << "the consensus module allocated in steady state";
    EXPECT_EQ(0, count(Label::container)) << "the container allocated in steady state";
}

}  // namespace
