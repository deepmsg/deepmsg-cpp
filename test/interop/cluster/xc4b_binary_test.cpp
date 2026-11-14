// C4f: XC-4b across the process boundary -- `deepmsg-consensus-module`
// against the reference Java media driver, archive and service container,
// with the reference Java clients. The in-process behaviour is C4d's
// `snapshot_recovery_interop_test`; this checks the binary form.

#include <gtest/gtest.h>

#include <aeronc.h>
#include <client/aeron_archive.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <unistd.h>

#include "cluster/common/cluster_counters.h"
#include "cluster/common/recording_log.h"
#include "core/deepmsg/deepmsg_client.h"
#include "fixture/process.h"

#ifndef AXC_CONSENSUS_MODULE_BIN
#define AXC_CONSENSUS_MODULE_BIN ""
#endif
#ifndef AXC_CONTAINER_BINARY
#define AXC_CONTAINER_BINARY ""
#endif
#ifndef AXC_AERON_JAR
#define AXC_AERON_JAR ""
#endif

namespace
{

using axc::core::DeepmsgClient;
using axc::interop::ChildProcess;
using axc::interop::ProcessResult;

constexpr std::int32_t CLUSTER_ID = 11;
constexpr std::int32_t MEMBER_ID = 0;
constexpr const char* MEMBERS = "0,localhost:0,localhost:0,localhost:0,localhost:0,localhost:0";
constexpr const char* CONTROL_CHANNEL = "aeron:ipc?term-length=128k";

[[nodiscard]] std::string aeron_jar()
{
    if (const char* from_env = std::getenv("AXC_AERON_JAR"); nullptr != from_env && '\0' != *from_env)
    {
        return from_env;
    }
    return AXC_AERON_JAR;
}

[[nodiscard]] std::string tests_root()
{
    const char* from_env = std::getenv("AXC_AERON_TESTS");
    return nullptr == from_env ? std::string{} : std::string{from_env};
}

[[nodiscard]] std::string find_probe(const std::string& root, const std::string& name)
{
    for (const std::string& relative : {"/deepmsg-tests/java/" + name, "/java/" + name, "/" + name})
    {
        const std::string candidate = root + relative;
        if (std::filesystem::exists(candidate))
        {
            return candidate;
        }
    }
    return {};
}

[[nodiscard]] std::vector<std::string> java_opens()
{
    return {"--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED", "--add-opens",
            "java.base/sun.nio.ch=ALL-UNNAMED"};
}

class Xc4bBinaryTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        const std::string jar = aeron_jar();
        if (jar.empty() || !std::filesystem::exists(jar))
        {
            GTEST_SKIP() << "reference jar not found (set AXC_AERON_JAR): " << jar;
        }
        jar_ = jar;
        if (!std::filesystem::exists("/usr/bin/javac") && !std::filesystem::exists("/usr/local/bin/javac"))
        {
            GTEST_SKIP() << "javac not found: the probes cannot be compiled";
        }
        const std::string root = tests_root();
        if (root.empty())
        {
            GTEST_SKIP() << "AXC_AERON_TESTS is not set; the fork's probe sources are required";
        }
        for (const char* const name : {"ClusterServiceContainerProbe.java", "ClusterEchoTimerService.java",
                                       "ClusterEchoClientProbe.java", "ClusterTimerClientProbe.java"})
        {
            const std::string path = find_probe(root, name);
            if (path.empty())
            {
                GTEST_SKIP() << "probe source not found under " << root << ": " << name;
            }
            probe_sources_.push_back(path);
        }
        if (!std::filesystem::exists(AXC_CONSENSUS_MODULE_BIN) || !std::filesystem::exists(AXC_CONTAINER_BINARY))
        {
            GTEST_SKIP() << "cluster binaries not found";
        }

        std::string pattern = "/tmp/axc-c4f-xc4b-XXXXXX";
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        directory_ = created;
        aeron_dir_ = directory_ + "/aeron";
        cluster_dir_ = directory_ + "/cluster";
        archive_dir_ = directory_ + "/archive";
        classes_dir_ = directory_ + "/classes";
        std::filesystem::create_directories(classes_dir_);
        log_port_ = 25000 + (static_cast<int>(::getpid()) % 500);

        compile_probes();
    }

    void TearDown() override
    {
        stop_all();
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
    }

    void compile_probes()
    {
        axc::interop::ProcessOptions compile;
        compile.arguments = {"-proc:none", "-cp", jar_, "-d", classes_dir_};
        for (const std::string& source : probe_sources_)
        {
            compile.arguments.push_back(source);
        }
        compile.timeout = std::chrono::seconds(120);
        auto compiled = axc::interop::run_process("javac", compile);
        ASSERT_TRUE(compiled) << compiled.error().message.view();
        ASSERT_EQ(0, compiled->exit_code) << compiled->output;
    }

    [[nodiscard]] std::expected<ChildProcess, axc::core::Error> spawn_logged(const std::string& executable,
                                                                             const std::vector<std::string>& arguments,
                                                                             const std::string& log)
    {
        std::string command = "exec " + executable;
        for (const std::string& argument : arguments)
        {
            command.push_back(' ');
            command.append(argument);
        }
        command.append(" > ");
        command.append(log);
        command.append(" 2>&1");
        return ChildProcess::spawn("/bin/sh", {"-c", command});
    }

    void start_java_driver()
    {
        std::vector<std::string> arguments = java_opens();
        arguments.insert(arguments.end(),
                         {"-Daeron.dir=" + aeron_dir_, "-Daeron.print.configuration=false",
                          "-Daeron.threading.mode=SHARED", "-cp", jar_, "io.aeron.driver.MediaDriver"});
        auto driver = spawn_logged("java", arguments, directory_ + "/driver.log");
        ASSERT_TRUE(driver) << driver.error().message.view();
        driver_ = std::make_unique<ChildProcess>(std::move(*driver));

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline)
        {
            ASSERT_TRUE(driver_->running()) << "the Java driver exited" << log_text("driver");
            auto client = DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir_});
            if (client)
            {
                client->close();
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        FAIL() << "the Java driver never published its CnC file";
    }

    void start_java_archive()
    {
        std::vector<std::string> arguments = java_opens();
        arguments.insert(arguments.end(),
                         {"-Daeron.dir=" + aeron_dir_, "-Daeron.archive.dir=" + archive_dir_,
                          "-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:0",
                          "-Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0",
                          "-Daeron.print.configuration=false", "-cp", jar_, "io.aeron.archive.Archive"});
        auto archive = spawn_logged("java", arguments, directory_ + "/archive.log");
        ASSERT_TRUE(archive) << archive.error().message.view();
        archive_ = std::make_unique<ChildProcess>(std::move(*archive));

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (std::chrono::steady_clock::now() < deadline)
        {
            ASSERT_TRUE(archive_->running()) << "the Java archive exited" << log_text("archive");
            if (archive_is_reachable())
            {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        FAIL() << "the Java archive never answered on its local control channel";
    }

    [[nodiscard]] bool archive_is_reachable()
    {
        auto client = DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir_});
        if (!client)
        {
            return false;
        }

        aeron_archive_context_t* context = nullptr;
        if (0 != aeron_archive_context_init(&context))
        {
            return false;
        }
        (void)aeron_archive_context_set_aeron(context, client->handle());
        (void)aeron_archive_context_set_owns_aeron_client(context, false);
        (void)aeron_archive_context_set_control_request_channel(context, "aeron:ipc?term-length=64k");
        (void)aeron_archive_context_set_control_request_stream_id(context, 10);
        (void)aeron_archive_context_set_control_response_channel(context, "aeron:ipc?term-length=64k");
        (void)aeron_archive_context_set_control_response_stream_id(context, CLUSTER_ID * 100 + 100 + 20);
        (void)aeron_archive_context_set_message_timeout_ns(context, 100'000'000ULL);

        aeron_archive_t* archive = nullptr;
        const int result = aeron_archive_connect(&archive, context);
        if (nullptr != archive)
        {
            (void)aeron_archive_close(archive);
        }
        (void)aeron_archive_context_close(context);
        return 0 == result;
    }

    void write_properties()
    {
        const std::string cm = directory_ + "/cm.properties";
        std::ofstream out{cm};
        ASSERT_TRUE(out.good());
        out << "aeron.dir=" << aeron_dir_ << "\n"
            << "aeron.cluster.dir=" << cluster_dir_ << "\n"
            << "aeron.cluster.id=" << CLUSTER_ID << "\n"
            << "aeron.cluster.member.id=" << MEMBER_ID << "\n"
            << "aeron.cluster.members=" << MEMBERS << "\n"
            << "aeron.cluster.ingress.channel=aeron:ipc\n"
            << "aeron.cluster.consensus.channel=aeron:ipc\n"
            << "aeron.cluster.log.channel=aeron:udp?endpoint=localhost:" << log_port_ << "\n"
            << "aeron.cluster.replication.channel=aeron:udp\n"
            << "aeron.cluster.control.channel=" << CONTROL_CHANNEL << "\n"
            << "aeron.cluster.service.count=1\n";
    }

    void start_cm()
    {
        auto cm = spawn_logged(AXC_CONSENSUS_MODULE_BIN, {directory_ + "/cm.properties"}, directory_ + "/cm.log");
        ASSERT_TRUE(cm) << cm.error().message.view();
        cm_ = std::make_unique<ChildProcess>(std::move(*cm));
    }

    void start_container()
    {
        std::vector<std::string> arguments = java_opens();
        arguments.insert(arguments.end(),
                         {"-cp", jar_ + ":" + classes_dir_, "ClusterServiceContainerProbe", aeron_dir_, cluster_dir_,
                          std::to_string(CLUSTER_ID), "0", "ClusterEchoTimerService", "true", CONTROL_CHANNEL});
        auto container = spawn_logged("java", arguments, directory_ + "/container.log");
        ASSERT_TRUE(container) << container.error().message.view();
        container_ = std::make_unique<ChildProcess>(std::move(*container));
    }

    void start()
    {
        write_properties();
        start_java_driver();
        start_java_archive();

        start_cm();
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        for (int attempt = 0; attempt < 20; ++attempt)
        {
            start_container();
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            if (container_->running())
            {
                break;
            }
            container_.reset();
        }
        ASSERT_TRUE(container_ && container_->running()) << log_text("container");

        auto client = DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir_});
        ASSERT_TRUE(client) << client.error().message.view();
        client_ = std::make_unique<DeepmsgClient>(std::move(*client));
        ASSERT_TRUE(wait_for_active()) << log_text("cm") << log_text("container");
    }

    void stop()
    {
        if (container_)
        {
            container_->terminate(std::chrono::seconds(10));
            container_.reset();
        }
        if (cm_)
        {
            cm_->terminate(std::chrono::seconds(10));
            cm_.reset();
        }
        client_.reset();
    }

    void stop_all()
    {
        stop();
        if (archive_)
        {
            archive_->terminate(std::chrono::seconds(10));
            archive_.reset();
        }
        if (driver_)
        {
            driver_->terminate(std::chrono::seconds(10));
            driver_.reset();
        }
    }

    [[nodiscard]] bool wait_for_active()
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(45);
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (nullptr == cm_ || !cm_->running())
            {
                return false;
            }
            if (cluster_counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID) == 1 &&
                cluster_counter(deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID) >= 0)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return false;
    }

    [[nodiscard]] std::int64_t cluster_counter(std::int32_t type_id)
    {
        if (!client_)
        {
            return -1;
        }
        (void)client_->do_work();
        auto* reader = client_->counters_reader();
        const std::int32_t id = deepmsg::cluster::common::find_cluster_counter(reader, type_id, CLUSTER_ID);
        if (AERON_NULL_COUNTER_ID == id)
        {
            return -1;
        }
        std::int64_t* address = aeron_counters_reader_addr(reader, id);
        return nullptr == address ? -1 : __atomic_load_n(address, __ATOMIC_ACQUIRE);
    }

    void set_control_toggle(std::int32_t value)
    {
        ASSERT_TRUE(client_);
        (void)client_->do_work();
        auto* reader = client_->counters_reader();
        const std::int32_t id = deepmsg::cluster::common::find_cluster_counter(
            reader, deepmsg::cluster::common::CLUSTER_CONTROL_TOGGLE_TYPE_ID, CLUSTER_ID);
        ASSERT_NE(AERON_NULL_COUNTER_ID, id);
        std::int64_t* address = aeron_counters_reader_addr(reader, id);
        ASSERT_NE(nullptr, address);
        __atomic_store_n(address, static_cast<std::int64_t>(value), __ATOMIC_RELEASE);
    }

    [[nodiscard]] ProcessResult run_probe(const std::string& main_class, const std::vector<std::string>& arguments,
                                          std::chrono::milliseconds timeout = std::chrono::seconds(90))
    {
        axc::interop::ProcessOptions options;
        options.arguments = java_opens();
        options.arguments.insert(options.arguments.end(), {"-cp", jar_ + ":" + classes_dir_, main_class});
        options.arguments.insert(options.arguments.end(), arguments.begin(), arguments.end());
        options.timeout = timeout;
        auto result = axc::interop::run_process("java", options);
        EXPECT_TRUE(result) << result.error().message.view();
        return result ? *result : ProcessResult{};
    }

    [[nodiscard]] std::string log_text(const std::string& name) const
    {
        std::ifstream in{directory_ + "/" + name + ".log"};
        if (!in)
        {
            return "";
        }
        return "\n--- " + name + ".log ---\n" +
               std::string{std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
    }

    std::string jar_{};
    std::string directory_{};
    std::string aeron_dir_{};
    std::string cluster_dir_{};
    std::string archive_dir_{};
    std::string classes_dir_{};
    std::vector<std::string> probe_sources_{};
    std::int32_t log_port_{0};
    std::unique_ptr<ChildProcess> driver_{};
    std::unique_ptr<ChildProcess> archive_{};
    std::unique_ptr<ChildProcess> cm_{};
    std::unique_ptr<ChildProcess> container_{};
    std::unique_ptr<DeepmsgClient> client_{};
};

TEST_F(Xc4bBinaryTest, anEchoRoundTripSurvivesTheBinaryBoundary)
{
    start();

    const auto one = run_probe("ClusterEchoClientProbe", {aeron_dir_, "1", "30"});
    EXPECT_EQ(0, one.exit_code) << one.output;
    EXPECT_NE(std::string::npos, one.output.find("RECEIVED 1")) << one.output;

    const auto many = run_probe("ClusterEchoClientProbe", {aeron_dir_, "10000", "90"});
    EXPECT_EQ(0, many.exit_code) << many.output;
    EXPECT_NE(std::string::npos, many.output.find("RECEIVED 10000")) << many.output;
}

TEST_F(Xc4bBinaryTest, timersFireThreeRoundsAcrossTheBinary)
{
    start();

    for (int round = 0; round < 3; ++round)
    {
        const auto timers = run_probe("ClusterTimerClientProbe", {aeron_dir_, "30"});
        EXPECT_EQ(0, timers.exit_code) << timers.output;
        EXPECT_NE(std::string::npos, timers.output.find("TIMER 1")) << "round " << round << ": " << timers.output;
        EXPECT_NE(std::string::npos, timers.output.find("CANCELLED 2")) << "round " << round << ": " << timers.output;
        EXPECT_NE(std::string::npos, timers.output.find("TIMER 3")) << "round " << round << ": " << timers.output;
    }
}

TEST_F(Xc4bBinaryTest, aSnapshotSurvivesACmProcessRestart)
{
    start();

    const auto snapshots_before = cluster_counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);
    set_control_toggle(4);

    bool taken = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (cluster_counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID) > snapshots_before &&
            cluster_counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID) == 1)
        {
            taken = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(taken) << "the snapshot was not taken" << log_text("cm");

    stop();
    start();

    const auto echo = run_probe("ClusterEchoClientProbe", {aeron_dir_, "1", "30"});
    EXPECT_EQ(0, echo.exit_code) << echo.output;
    EXPECT_NE(std::string::npos, echo.output.find("RECEIVED 1")) << echo.output;
}

}  // namespace
