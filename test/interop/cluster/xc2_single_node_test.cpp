// C4f: XC-2 on a single node -- a Java client against the all-C++ cluster
// (`deepmsg-cluster-node --embedded-driver` + the C++ service container).
//
// The reference's 179-case system table is C6c; this is the single-node smoke
// table agreed in `plan-c4f.md` §3.2: echo, timers, admin snapshot,
// snapshot/restart, suspend/resume and orderly shutdown.

#include <gtest/gtest.h>

#include <aeronc.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <unistd.h>

#include "client/aeron_archive.h"
#include "cluster/common/cluster_counters.h"
#include "cluster/common/recording_log.h"
#include "core/deepmsg/deepmsg_client.h"
#include "fixture/cluster_client_session.h"
#include "fixture/process.h"

#ifndef AXC_CLUSTER_NODE_BIN
#define AXC_CLUSTER_NODE_BIN ""
#endif
#ifndef AXC_CONTAINER_BINARY
#define AXC_CONTAINER_BINARY ""
#endif
#ifndef AXC_AERON_JAR
#define AXC_AERON_JAR ""
#endif
#ifndef AXC_ROLE_CHANGE_SERVICE_LIBRARY
#define AXC_ROLE_CHANGE_SERVICE_LIBRARY ""
#endif
#ifndef AXC_TRACKING_SERVICE_LIBRARY
#define AXC_TRACKING_SERVICE_LIBRARY ""
#endif
#ifndef AXC_ARCHIVE_SERVER_BIN
#define AXC_ARCHIVE_SERVER_BIN ""
#endif

namespace
{

using axc::core::DeepmsgClient;
using axc::interop::ChildProcess;
using axc::interop::ClusterClientSession;
using axc::interop::ProcessResult;

constexpr std::int32_t CLUSTER_ID = 7;
constexpr std::int32_t MEMBER_ID = 0;
constexpr std::int32_t SERVICE_COUNT = 1;
constexpr const char* MEMBERS = "0,localhost:0,localhost:0,localhost:0,localhost:0,localhost:0";

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

/// Run a child with its stdout/stderr in `log`; the child is exec'd by a shell
/// so `ChildProcess::terminate` still signals the real process.
[[nodiscard]] std::expected<ChildProcess, axc::core::Error> spawn_logged(const std::string& executable,
                                                                         const std::vector<std::string>& arguments,
                                                                         const std::string& log)
{
    // The child is spawned directly: a shell would split on a `|` in an
    // argument (a channel URI), and `spawn` already redirects to `log`.
    return ChildProcess::spawn(executable, arguments, {}, log);
}

class Xc2SingleNodeTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        if (!set_up_environment())
        {
            return;
        }
        compile_probes();
    }

    [[nodiscard]] bool set_up_environment()
    {
        const auto skip = [](const std::string& message) { GTEST_SKIP() << message; };

        const std::string jar = aeron_jar();
        if (jar.empty() || !std::filesystem::exists(jar))
        {
            skip("reference jar not found (set AXC_AERON_JAR): " + jar);
            return false;
        }
        jar_ = jar;
        if (!std::filesystem::exists(AXC_CLUSTER_NODE_BIN) || !std::filesystem::exists(AXC_CONTAINER_BINARY))
        {
            skip("cluster binaries not found");
            return false;
        }

        std::string pattern = "/tmp/axc-c4f-xc2-XXXXXX";
        const char* created = ::mkdtemp(pattern.data());
        EXPECT_NE(nullptr, created);
        if (nullptr == created)
        {
            return false;
        }
        directory_ = created;
        aeron_dir_ = directory_ + "/aeron";
        cluster_dir_ = directory_ + "/cluster";
        archive_dir_ = directory_ + "/archive";
        classes_dir_ = directory_ + "/classes";
        std::filesystem::create_directories(classes_dir_);
        // Each test in this binary reuses the same process id; vary the port
        // block so a previous test's bound UDP sockets do not collide with the
        // next one.
        static std::atomic<int> port_sequence{0};
        log_port_ = 24500 + ((static_cast<int>(::getpid()) % 100) * 100) + (port_sequence.fetch_add(1) * 2);
        archive_control_port_ = log_port_ + 100;
        return true;
    }

    void compile_probes()
    {
        if (!std::filesystem::exists("/usr/bin/javac") && !std::filesystem::exists("/usr/local/bin/javac"))
        {
            GTEST_SKIP() << "javac not found: the probes cannot be compiled";
            return;
        }
        const std::string root = tests_root();
        if (root.empty())
        {
            GTEST_SKIP() << "AXC_AERON_TESTS is not set; the fork's probe sources are required";
            return;
        }
        for (const char* const name : {"ClusterEchoClientProbe.java", "ClusterTimerClientProbe.java",
                                       "ClusterAdminRequestProbe.java", "ClusterSessionEventProbe.java"})
        {
            const std::string path = find_probe(root, name);
            if (path.empty())
            {
                GTEST_SKIP() << "probe source not found under " << root << ": " << name;
                return;
            }
            probe_sources_.push_back(path);
        }

        axc::interop::ProcessOptions compile;
        compile.arguments = {"-proc:none", "-cp", jar_, "-d", classes_dir_};
        for (const std::string& source : probe_sources_)
        {
            compile.arguments.push_back(source);
        }
        compile.timeout = std::chrono::seconds(120);
        auto compiled = axc::interop::run_process("javac", compile);
        EXPECT_TRUE(compiled) << compiled.error().message.view();
        if (compiled)
        {
            EXPECT_EQ(0, compiled->exit_code) << compiled->output;
        }
    }

    void TearDown() override
    {
        stop();
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
    }

    void write_properties(const std::string& supplier = {})
    {
        // The reference's `LOG_CHANNEL` is `aeron:udp?term-length=512k`; the
        // padding case pins it so the log recording's term length is known.
        std::string log_channel = "aeron:udp?endpoint=localhost:" + std::to_string(log_port_);
        if (log_term_length_ > 0)
        {
            log_channel = "aeron:udp?term-length=" + std::to_string(log_term_length_) +
                          "|endpoint=localhost:" + std::to_string(log_port_);
        }

        const std::string node = directory_ + "/node.properties";
        std::ofstream out{node};
        ASSERT_TRUE(out.good());
        out << "aeron.dir=" << aeron_dir_ << "\n"
            << "aeron.cluster.dir=" << cluster_dir_ << "\n"
            << "aeron.archive.dir=" << archive_dir_ << "\n"
            << "aeron.cluster.id=" << CLUSTER_ID << "\n"
            << "aeron.cluster.member.id=" << MEMBER_ID << "\n"
            << "aeron.cluster.members=" << MEMBERS << "\n"
            << "aeron.cluster.ingress.channel=aeron:ipc\n"
            << "aeron.cluster.consensus.channel=aeron:ipc\n"
            << "aeron.cluster.log.channel=" << log_channel << "\n"
            << "aeron.cluster.replication.channel=aeron:udp\n"
            << "aeron.cluster.control.channel=aeron:ipc?term-length=128k\n"
            << "aeron.archive.control.channel=aeron:udp?endpoint=localhost:" << archive_control_port_ << "\n"
            << "aeron.cluster.service.count=" << SERVICE_COUNT << "\n"
            << "aeron.archive.replication.channel=aeron:udp?endpoint=localhost:0\n";
        if (!supplier.empty())
        {
            out << "aeron.cluster.authorisation.service.supplier=" << supplier << "\n";
        }
        out.close();

        const std::string container = directory_ + "/container.properties";
        std::ofstream cout{container};
        ASSERT_TRUE(cout.good());
        cout << "aeron.dir=" << aeron_dir_ << "\n"
             << "aeron.cluster.dir=" << cluster_dir_ << "\n"
             << "aeron.cluster.id=" << CLUSTER_ID << "\n"
             << "aeron.cluster.service.id=0\n"
             << "aeron.cluster.service.name=clustered-service-" << CLUSTER_ID << "-0\n"
             << "deepmsg.cluster.service=echo\n";
        if (!service_library_.empty())
        {
            cout << "deepmsg.cluster.service.library=" << service_library_ << "\n"
                 << "deepmsg.cluster.service=" << service_name_ << "\n";
        }
    }

    void start(const std::string& supplier = {})
    {
        write_properties(supplier);
        if (!role_dir_.empty())
        {
            std::error_code error;
            std::filesystem::create_directories(role_dir_, error);
        }
        if (!tracking_dir_.empty())
        {
            std::error_code error;
            std::filesystem::create_directories(tracking_dir_, error);
            const std::string file = tracking_dir_ + "/tracking-0.txt";
            (void)::setenv("AXC_TEST_TRACKING_FILE", file.c_str(), 1);
            (void)::setenv("AXC_TEST_TRACKING_SERVICE_ID", "1", 1);
            (void)::setenv("AXC_TEST_TRACKING_NODE_INDEX", "0", 1);
            (void)::setenv("AXC_TEST_TRACKING_MODE", tracking_mode_.c_str(), 1);
        }
        else
        {
            (void)::unsetenv("AXC_TEST_TRACKING_FILE");
            (void)::unsetenv("AXC_TEST_TRACKING_SERVICE_ID");
            (void)::unsetenv("AXC_TEST_TRACKING_NODE_INDEX");
            (void)::unsetenv("AXC_TEST_TRACKING_MODE");
        }
        // A driver left active by a control-toggle shutdown (its CnC mark is
        // still fresh) makes the next start refuse the directory until the
        // 10 s liveness window passes; respawn instead of failing.
        for (int attempt = 0; attempt < 15; ++attempt)
        {
            auto node = spawn_logged(AXC_CLUSTER_NODE_BIN, {"--embedded-driver", directory_ + "/node.properties"},
                                     directory_ + "/node.log");
            ASSERT_TRUE(node) << node.error().message.view();
            node_ = std::make_unique<ChildProcess>(std::move(*node));
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            if (node_->running())
            {
                break;
            }
            node_.reset();
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        ASSERT_TRUE(node_ && node_->running()) << log_text("node");

        // The container connects to the CnC file, so wait for the embedded
        // driver before spawning it. It also needs the CM's control publication
        // (105) to exist: the reference container does not retry that add, so
        // give the CM a moment and respawn a container that exits early.
        ASSERT_TRUE(wait_for_driver()) << log_text("node");
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        if (!role_dir_.empty())
        {
            const std::string file = role_dir_ + "/role-0.txt";
            (void)::setenv("AXC_TEST_ROLE_FILE", file.c_str(), 1);
        }
        else
        {
            (void)::unsetenv("AXC_TEST_ROLE_FILE");
        }
        // The container needs the consensus module's control publication (105)
        // and a live driver; a loaded runner can take several seconds to get
        // there, so the retry window is 20 s rather than 6.
        for (int attempt = 0; attempt < 40; ++attempt)
        {
            auto container = spawn_logged(AXC_CONTAINER_BINARY, {directory_ + "/container.properties"},
                                          directory_ + "/container.log");
            ASSERT_TRUE(container) << container.error().message.view();
            container_ = std::make_unique<ChildProcess>(std::move(*container));
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            if (container_->running())
            {
                break;
            }
            container_.reset();
        }
        ASSERT_TRUE(container_ && container_->running()) << log_text("container");

        ASSERT_TRUE(wait_for_active()) << log_text("node") << log_text("container");
    }

    void stop()
    {
        if (container_)
        {
            container_->terminate(std::chrono::seconds(10));
            container_.reset();
        }
        if (node_)
        {
            node_->terminate(std::chrono::seconds(10));
            node_.reset();
        }
        client_.reset();
    }

    [[nodiscard]] bool wait_for_driver()
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (std::chrono::steady_clock::now() < deadline)
        {
            auto client = DeepmsgClient::connect(axc::core::ClientConfig{aeron_dir_});
            if (client)
            {
                client_ = std::make_unique<DeepmsgClient>(std::move(*client));
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    [[nodiscard]] bool wait_for_active()
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(45);
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (nullptr == node_ || !node_->running())
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

    [[nodiscard]] std::vector<deepmsg::cluster::common::RecordingLog::Entry> recording_log_entries(std::int32_t type)
    {
        auto log = deepmsg::cluster::common::RecordingLog::open(cluster_dir_, false);
        EXPECT_TRUE(log) << log.error().message.view();
        std::vector<deepmsg::cluster::common::RecordingLog::Entry> entries;
        if (!log)
        {
            return entries;
        }
        for (const auto& entry : log->entries())
        {
            if (entry.type == type && entry.is_valid)
            {
                entries.push_back(entry);
            }
        }
        return entries;
    }

    [[nodiscard]] std::vector<std::int32_t> role_records() const
    {
        std::ifstream in{role_dir_ + "/role-0.txt"};
        std::vector<std::int32_t> roles;
        std::int32_t role = 0;
        while (in >> role)
        {
            roles.push_back(role);
        }
        return roles;
    }

    [[nodiscard]] std::string role_files() const
    {
        std::string names;
        std::error_code error;
        for (const auto& entry : std::filesystem::directory_iterator(role_dir_, error))
        {
            names.append(entry.path().filename().string()).append(" ");
        }
        return names;
    }

    /// `ClusterTest.shouldEnterElectionWhenRecordingStopsUnexpectedlyOnLeader`
    /// on one node: stop the cluster log recording (id 0 on a fresh directory).
    [[nodiscard]] bool stop_log_recording()
    {
        aeron_archive_context_t* context = nullptr;
        if (0 != aeron_archive_context_init(&context))
        {
            return false;
        }
        const std::string request = "aeron:udp?endpoint=localhost:" + std::to_string(archive_control_port_);
        const std::string response = "aeron:udp?endpoint=localhost:" + std::to_string(archive_control_port_ + 100);
        (void)aeron_archive_context_set_aeron_directory_name(context, aeron_dir_.c_str());
        (void)aeron_archive_context_set_control_request_channel(context, request.c_str());
        (void)aeron_archive_context_set_control_request_stream_id(context, 10);
        (void)aeron_archive_context_set_control_response_channel(context, response.c_str());
        (void)aeron_archive_context_set_control_response_stream_id(context, 10);
        (void)aeron_archive_context_set_message_timeout_ns(context, 10'000'000'000ULL);

        aeron_archive_t* archive = nullptr;
        bool stopped = false;
        const bool connected = 0 == aeron_archive_connect(&archive, context);
        if (connected)
        {
            (void)aeron_archive_try_stop_recording_by_identity(&stopped, archive, 0);
        }
        if (nullptr != archive)
        {
            (void)aeron_archive_close(archive);
        }
        (void)aeron_archive_context_close(context);
        return connected && stopped;
    }

    /// The log recording's position and term length through the node's own
    /// archive control channel (the connection `stop_log_recording` uses). A
    /// live recording's catalog stop position is `NULL_POSITION`, so the
    /// position comes from the recorder (`getRecordingPosition`). The padding
    /// case reads it while the cluster runs, so the filler message that does
    /// not fit is chosen from the real position, not an assumed frame prefix.
    [[nodiscard]] bool log_recording_extent(std::int64_t& stop_position, std::int64_t& term_length)
    {
        aeron_archive_context_t* context = nullptr;
        if (0 != aeron_archive_context_init(&context))
        {
            return false;
        }
        const std::string request = "aeron:udp?endpoint=localhost:" + std::to_string(archive_control_port_);
        const std::string response = "aeron:udp?endpoint=localhost:" + std::to_string(archive_control_port_ + 100);
        (void)aeron_archive_context_set_aeron_directory_name(context, aeron_dir_.c_str());
        (void)aeron_archive_context_set_control_request_channel(context, request.c_str());
        (void)aeron_archive_context_set_control_request_stream_id(context, 10);
        (void)aeron_archive_context_set_control_response_channel(context, response.c_str());
        (void)aeron_archive_context_set_control_response_stream_id(context, 10);
        (void)aeron_archive_context_set_message_timeout_ns(context, 10'000'000'000ULL);

        aeron_archive_t* archive = nullptr;
        bool ok = false;
        if (0 == aeron_archive_connect(&archive, context))
        {
            std::vector<aeron_archive_recording_descriptor_t> descriptors;
            const auto collect = [](aeron_archive_recording_descriptor_t* descriptor, void* clientd)
            { static_cast<std::vector<aeron_archive_recording_descriptor_t>*>(clientd)->push_back(*descriptor); };
            std::int32_t listed = 0;
            ok = 0 == aeron_archive_list_recordings(&listed, archive, 0, 100, collect, &descriptors) &&
                 1 == descriptors.size();
            if (ok)
            {
                term_length = descriptors[0].term_buffer_length;
                stop_position = descriptors[0].stop_position;
                if (AERON_NULL_POSITION == stop_position)
                {
                    ok =
                        0 == aeron_archive_get_recording_position(&stop_position, archive, descriptors[0].recording_id);
                }
            }
        }
        if (nullptr != archive)
        {
            (void)aeron_archive_close(archive);
        }
        (void)aeron_archive_context_close(context);
        return ok;
    }

    /// The tracking test service, in `record` mode: it records and echoes the
    /// client messages but offers no service messages and schedules no timers,
    /// so the log carries one frame per client message (the reference's
    /// `TestService` shape, which the padding case counts).
    void use_tracking_service()
    {
        service_library_ = AXC_TRACKING_SERVICE_LIBRARY;
        service_name_ = "tracking";
        tracking_dir_ = directory_ + "/tracking";
        tracking_mode_ = "record";
    }

    /// Lines appended by the tracking service (`M <id>` etc.).
    [[nodiscard]] std::vector<std::string> tracking_lines() const
    {
        std::ifstream in{tracking_dir_ + "/tracking-0.txt"};
        std::vector<std::string> lines;
        std::string line;
        while (std::getline(in, line))
        {
            if (!line.empty())
            {
                lines.push_back(line);
            }
        }
        return lines;
    }

    /// `SingleNodeTest.truncateRecordingToTermLength`: with the node stopped,
    /// open its archive directory through our archive server, find the one
    /// cluster log recording and cut it at its term length. The recording
    /// then ends on the padding frame the log publication wrote when the term
    /// filled.
    [[nodiscard]] bool truncate_log_recording_to_term_length(std::int64_t& term_length, std::string& trace)
    {
        if (std::string{AXC_ARCHIVE_SERVER_BIN}.empty() || !std::filesystem::exists(AXC_ARCHIVE_SERVER_BIN))
        {
            trace = "archive server not built: " + std::string{AXC_ARCHIVE_SERVER_BIN};
            return false;
        }

        const std::string driver_dir = directory_ + "/truncate-aeron";
        std::error_code error;
        std::filesystem::remove_all(driver_dir, error);
        const std::string request = "aeron:udp?endpoint=localhost:" + std::to_string(archive_control_port_);
        const std::string response = "aeron:udp?endpoint=localhost:" + std::to_string(archive_control_port_ + 100);

        auto spawned = spawn_logged(AXC_ARCHIVE_SERVER_BIN,
                                    {"-Daeron.dir=" + driver_dir, "-Daeron.archive.dir=" + archive_dir_,
                                     "-Daeron.archive.control.channel=" + request,
                                     "-Daeron.archive.threading.mode=SHARED", "-Daeron.spies.simulate.connection=true"},
                                    directory_ + "/truncate-archive.log");
        if (!spawned)
        {
            trace = spawned.error().message.view();
            return false;
        }
        ChildProcess server = std::move(*spawned);

        aeron_archive_t* archive = nullptr;
        aeron_archive_context_t* context = nullptr;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (nullptr == archive && std::chrono::steady_clock::now() < deadline)
        {
            // A failed connect leaves its context unusable, so every attempt
            // builds a fresh one (`ContentParity.wait_for_control`).
            context = nullptr;
            if (0 != aeron_archive_context_init(&context))
            {
                break;
            }
            (void)aeron_archive_context_set_aeron_directory_name(context, driver_dir.c_str());
            (void)aeron_archive_context_set_control_request_channel(context, request.c_str());
            (void)aeron_archive_context_set_control_request_stream_id(context, 10);
            (void)aeron_archive_context_set_control_response_channel(context, response.c_str());
            (void)aeron_archive_context_set_control_response_stream_id(context, 10);
            (void)aeron_archive_context_set_message_timeout_ns(context, 5'000'000'000ULL);
            if (0 != aeron_archive_connect(&archive, context))
            {
                archive = nullptr;
                (void)aeron_archive_context_close(context);
                context = nullptr;
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
        if (nullptr == archive)
        {
            trace = "the archive never answered on " + request + "\n" + log_text("truncate-archive");
            server.terminate(std::chrono::seconds(5));
            return false;
        }

        std::vector<aeron_archive_recording_descriptor_t> descriptors;
        const auto collect = [](aeron_archive_recording_descriptor_t* descriptor, void* clientd)
        { static_cast<std::vector<aeron_archive_recording_descriptor_t>*>(clientd)->push_back(*descriptor); };
        std::int32_t listed = 0;
        const bool ok = 0 == aeron_archive_list_recordings(&listed, archive, 0, 100, collect, &descriptors);
        bool truncated = false;
        if (!ok)
        {
            trace = "list_recordings failed: " + std::string{aeron_errmsg()};
        }
        else if (1 != descriptors.size())
        {
            trace = "expected one recording, found " + std::to_string(descriptors.size());
        }
        else
        {
            const aeron_archive_recording_descriptor_t& descriptor = descriptors[0];
            term_length = descriptor.term_buffer_length;
            trace = "id=" + std::to_string(descriptor.recording_id) +
                    " term=" + std::to_string(descriptor.term_buffer_length) +
                    " stop=" + std::to_string(descriptor.stop_position);
            if (descriptor.stop_position <= descriptor.term_buffer_length)
            {
                trace.append(" (the recording never passed one term)");
            }
            else
            {
                std::int64_t deleted = 0;
                truncated = 0 == aeron_archive_truncate_recording(&deleted, archive, descriptor.recording_id,
                                                                  descriptor.term_buffer_length);
                if (!truncated)
                {
                    trace.append(" truncate failed: ").append(aeron_errmsg());
                }
            }
        }

        (void)aeron_archive_close(archive);
        if (nullptr != context)
        {
            (void)aeron_archive_context_close(context);
        }
        server.terminate(std::chrono::seconds(5));
        return truncated;
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

    /// Runs a probe while `tick` runs every millisecond (a control toggle must
    /// land while the client is connected).
    [[nodiscard]] ProcessResult run_probe_with(const std::string& main_class, const std::vector<std::string>& arguments,
                                               std::chrono::milliseconds timeout, const std::function<void()>& tick)
    {
        axc::interop::ProcessOptions options;
        options.arguments = java_opens();
        options.arguments.insert(options.arguments.end(), {"-cp", jar_ + ":" + classes_dir_, main_class});
        options.arguments.insert(options.arguments.end(), arguments.begin(), arguments.end());
        options.timeout = timeout;

        ProcessResult result;
        std::atomic<bool> done{false};
        std::thread runner(
            [&result, &done, &options]
            {
                auto ran = axc::interop::run_process("java", options);
                if (ran)
                {
                    result = std::move(*ran);
                }
                done.store(true, std::memory_order_release);
            });

        const auto deadline = std::chrono::steady_clock::now() + timeout + std::chrono::seconds(10);
        while (!done.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline)
        {
            tick();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        runner.join();
        return result;
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
    std::int32_t archive_control_port_{0};
    std::string service_library_{};
    std::string service_name_{"role-change"};
    std::string tracking_dir_{};
    std::string tracking_mode_{"full"};
    std::int32_t log_term_length_{0};
    std::string role_dir_{};
    std::unique_ptr<ChildProcess> node_{};
    std::unique_ptr<ChildProcess> container_{};
    std::unique_ptr<DeepmsgClient> client_{};
};

/// The replay cases drive the in-process client and the tracking service; they
/// run without the fork's Java probes (whose checkout is optional here).
class Xc2SingleNodeReplayTest : public Xc2SingleNodeTest
{
protected:
    void SetUp() override
    {
        if (!set_up_environment())
        {
            return;
        }
    }
};

/// The C7 snapshot cases drive the in-process client and the tracking service
/// like the replay fixture, and read the node's own `recording.log`.
class Xc7SingleNodeSnapshotTest : public Xc2SingleNodeTest
{
protected:
    void SetUp() override
    {
        if (!set_up_environment())
        {
            return;
        }
    }
};

/// The election-trigger cases only read counters; they run without the fork's
/// Java probes.
class Xc2SingleNodeElectionTest : public Xc2SingleNodeTest
{
protected:
    void SetUp() override
    {
        if (!set_up_environment())
        {
            return;
        }
    }
};

TEST_F(Xc2SingleNodeTest, theNodeStartsAndEchoes)
{
    start();

    const auto one = run_probe("ClusterEchoClientProbe", {aeron_dir_, "1", "30"});
    EXPECT_EQ(0, one.exit_code) << one.output;
    EXPECT_NE(std::string::npos, one.output.find("RECEIVED 1")) << one.output;

    const auto many = run_probe("ClusterEchoClientProbe", {aeron_dir_, "10000", "90"});
    EXPECT_EQ(0, many.exit_code) << many.output;
    EXPECT_NE(std::string::npos, many.output.find("RECEIVED 10000")) << many.output;
}

TEST_F(Xc2SingleNodeTest, theTimersFireAndCancel)
{
    start();

    for (int round = 0; round < 2; ++round)
    {
        const auto timers = run_probe("ClusterTimerClientProbe", {aeron_dir_, "30"});
        EXPECT_EQ(0, timers.exit_code) << timers.output;
        EXPECT_NE(std::string::npos, timers.output.find("TIMER 1")) << "round " << round << ": " << timers.output;
        EXPECT_NE(std::string::npos, timers.output.find("CANCELLED 2")) << "round " << round << ": " << timers.output;
        EXPECT_NE(std::string::npos, timers.output.find("TIMER 3")) << "round " << round << ": " << timers.output;
    }
}

TEST_F(Xc2SingleNodeTest, anAdminSnapshotRequestTakesASnapshot)
{
    start("ALLOW_ALL");

    const auto snapshots_before = cluster_counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);
    const auto probe =
        run_probe("ClusterAdminRequestProbe", {aeron_dir_, "5000", "SNAPSHOT"}, std::chrono::seconds(30));
    EXPECT_EQ(0, probe.exit_code) << probe.output;
    EXPECT_NE(std::string::npos, probe.output.find("ADMIN_RESPONSE OK")) << probe.output;

    bool taken = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
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
    EXPECT_TRUE(taken) << "the admin snapshot was not taken";
}

TEST_F(Xc2SingleNodeTest, aSnapshotSurvivesANodeRestart)
{
    start();
    const auto snapshots_before = cluster_counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID);
    set_control_toggle(4);

    bool taken = false;
    const auto snapshot_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (std::chrono::steady_clock::now() < snapshot_deadline)
    {
        if (cluster_counter(deepmsg::cluster::common::CLUSTER_SNAPSHOT_COUNTER_TYPE_ID) > snapshots_before &&
            cluster_counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID) == 1)
        {
            taken = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(taken) << "the snapshot was not taken";
    const auto snapshot_entries = recording_log_entries(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_SNAPSHOT);
    ASSERT_FALSE(snapshot_entries.empty());

    const std::size_t terms_before =
        recording_log_entries(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_TERM).size();
    stop();
    start();

    const auto echo = run_probe("ClusterEchoClientProbe", {aeron_dir_, "1", "30"});
    EXPECT_EQ(0, echo.exit_code) << echo.output;
    EXPECT_NE(std::string::npos, echo.output.find("RECEIVED 1")) << echo.output;

    EXPECT_EQ(snapshot_entries.size(),
              recording_log_entries(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_SNAPSHOT).size());
    EXPECT_EQ(terms_before + 1, recording_log_entries(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_TERM).size());
}

TEST_F(Xc2SingleNodeTest, suspendStopsAndResumeRestarts)
{
    start();

    const auto start_time = std::chrono::steady_clock::now();
    bool suspended = false;
    bool resumed = false;
    std::int64_t resumed_at_ms = 0;

    const auto probe = run_probe_with(
        "ClusterSessionEventProbe", {aeron_dir_, "12000", "3", "1500"}, std::chrono::seconds(45),
        [&]()
        {
            const auto elapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time);
            if (!suspended && elapsed > std::chrono::milliseconds(300))
            {
                suspended = true;
                set_control_toggle(2);
            }
            else if (suspended && !resumed && elapsed > std::chrono::milliseconds(2800))
            {
                resumed = true;
                resumed_at_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::system_clock::now().time_since_epoch())
                                    .count();
                set_control_toggle(3);
            }
        });

    EXPECT_EQ(0, probe.exit_code) << probe.output;
    ASSERT_TRUE(resumed) << probe.output;
    EXPECT_NE(std::string::npos, probe.output.find("RECEIVED 3")) << probe.output;

    // Every echo belongs after the RESUME (the probe delays its offers so they
    // wait in the ingress publication while the module is suspended).
    std::size_t echoes_after_resume = 0;
    std::istringstream lines{probe.output};
    std::string line;
    while (std::getline(lines, line))
    {
        if (0 == line.rfind("ECHO ", 0))
        {
            std::istringstream fields{line};
            std::string tag;
            std::int64_t at = 0;
            fields >> tag >> at;
            if (at >= resumed_at_ms - 100)
            {
                ++echoes_after_resume;
            }
        }
    }
    EXPECT_EQ(3U, echoes_after_resume) << probe.output;
}

// Reference `ClusterTest.shouldSetClientName`: the CM and the container set
// `aeron.client.name`, which the heartbeat counter's label carries.
TEST_F(Xc2SingleNodeTest, theClientNamesAppearOnTheHeartbeatCounters)
{
    start();

    const std::string cm_name = "consensus-module-" + std::to_string(CLUSTER_ID) + "-0";
    const std::string container_name = "clustered-service-" + std::to_string(CLUSTER_ID) + "-0";
    bool cm_seen = false;
    bool container_seen = false;

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    while ((!cm_seen || !container_seen) && std::chrono::steady_clock::now() < deadline)
    {
        ASSERT_TRUE(client_);
        (void)client_->do_work();
        auto* reader = client_->counters_reader();
        const std::int32_t max_id = aeron_counters_reader_max_counter_id(reader);
        for (std::int32_t id = 0; id < max_id; ++id)
        {
            std::int32_t state = 0;
            if (0 != aeron_counters_reader_counter_state(reader, id, &state) || AERON_COUNTER_RECORD_ALLOCATED != state)
            {
                continue;
            }
            char label[1024] = {};
            const int length = aeron_counters_reader_counter_label(reader, id, label, sizeof(label));
            if (length <= 0)
            {
                continue;
            }
            const std::string text{label, static_cast<std::size_t>(length)};
            cm_seen = cm_seen || std::string::npos != text.find("name=" + cm_name);
            container_seen = container_seen || std::string::npos != text.find("name=" + container_name);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    EXPECT_TRUE(cm_seen) << "no heartbeat counter names " << cm_name;
    EXPECT_TRUE(container_seen) << "no heartbeat counter names " << container_name;
}

// Reference `ClusterTest.shouldHandleReplayAfterShutdown`: an orderly shutdown
// snapshots and terminates; the restarted node replays the log and serves the
// client again.
TEST_F(Xc2SingleNodeTest, aShutdownRestartsAndReplays)
{
    start();

    const auto echo_before = run_probe("ClusterEchoClientProbe", {aeron_dir_, "1", "30"});
    EXPECT_EQ(0, echo_before.exit_code) << echo_before.output;
    EXPECT_NE(std::string::npos, echo_before.output.find("RECEIVED 1")) << echo_before.output;

    const std::size_t terms_before =
        recording_log_entries(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_TERM).size();
    const std::size_t snapshots_before =
        recording_log_entries(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_SNAPSHOT).size();

    set_control_toggle(5);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (node_ && !node_->running() && container_ && !container_->running())
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(node_ && !node_->running()) << "the node did not stop" << log_text("node");
    ASSERT_TRUE(container_ && !container_->running()) << "the container did not stop" << log_text("container");

    stop();
    start();

    const auto echo_after = run_probe("ClusterEchoClientProbe", {aeron_dir_, "1", "30"});
    EXPECT_EQ(0, echo_after.exit_code) << echo_after.output;
    EXPECT_NE(std::string::npos, echo_after.output.find("RECEIVED 1")) << echo_after.output;

    EXPECT_EQ(terms_before + 1, recording_log_entries(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_TERM).size());
    EXPECT_GE(recording_log_entries(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_SNAPSHOT).size(),
              snapshots_before);
    EXPECT_EQ(1, cluster_counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID));
}

// Reference `SingleNodeTest.shouldReplayLogWithPaddingAtEndOfRecording`: 481
// one KiB messages nearly fill the 512 KiB log term; the message that cannot
// fit makes the log publication pad the rest of the term, and the recording is
// then cut at exactly the term length so it ends on that padding frame. The
// restarted node must replay everything before the padding, skip the padding
// frame itself, and not read past the truncated stop position (the filler that
// went into the next term is cut away).
TEST_F(Xc2SingleNodeReplayTest, shouldReplayLogWithPaddingAtEndOfRecording)
{
    use_tracking_service();
    log_term_length_ = 512 * 1024;
    start();

    ClusterClientSession session;
    ASSERT_TRUE(session.connect_ipc(aeron_dir_, std::chrono::seconds(30))) << log_text("node") << log_text("container");

    constexpr int large_message_count = 481;
    const std::string large_tail(1024 - sizeof(std::int32_t), 'a');
    std::vector<std::int32_t> expected_ids;
    for (int i = 1; i <= large_message_count; ++i)
    {
        std::int32_t id = i;
        std::string payload(sizeof(id), '\0');
        std::memcpy(payload.data(), &id, sizeof(id));
        payload.append(large_tail);
        ASSERT_TRUE(session.offer_and_await(payload, expected_ids.size() + 1, std::chrono::seconds(20)))
            << "message " << i << log_text("node") << log_text("container");
        expected_ids.push_back(id);
    }

    // Offer one message that cannot fit: the log publication fills the rest of
    // the term with a padding frame and the message lands in the next term.
    // The archive's catalog trails the recorder, so wait until the stop
    // position is within one log frame of the term's end; a stale (smaller)
    // stop position only makes the filler larger than the true remainder, and
    // a frame of `remaining + 64` never fits in `remaining` bytes either way.
    std::int64_t stop_position = 0;
    std::int64_t term_length_probe = 0;
    std::int64_t remaining = 0;
    const auto extent_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (std::chrono::steady_clock::now() < extent_deadline)
    {
        ASSERT_TRUE(log_recording_extent(stop_position, term_length_probe))
            << "the log recording was not listed" << log_text("node") << log_text("container");
        ASSERT_EQ(512 * 1024, term_length_probe);
        remaining = term_length_probe - (stop_position % term_length_probe);
        // The log publication's maximum message length is termLength / 8.
        if (remaining > 0 && remaining <= 60'000)
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_GT(remaining, 0) << "the catalog never came near the end of the term";
    ASSERT_LE(remaining, 60'000) << "the catalog never came near the end of the term";

    const std::int32_t filler_id = 9001;
    std::string filler(static_cast<std::size_t>(remaining), 'p');
    if (static_cast<std::size_t>(remaining) < sizeof(filler_id))
    {
        filler.assign(sizeof(filler_id), 'p');
    }
    std::memcpy(filler.data(), &filler_id, sizeof(filler_id));
    ASSERT_TRUE(session.offer_and_await(filler, expected_ids.size() + 1, std::chrono::seconds(20)))
        << "filler " << filler_id << " remaining=" << remaining << log_text("node") << log_text("container");
    // The filler itself is in the next term: the truncation cuts it away, so it
    // is not part of the expected replay.
    session.close();

    stop();

    // The case is only about padding if the recording really ends on a padding
    // frame: the frame at `termLength - remaining` is PAD (`HDR_TYPE_PAD`) and
    // fills the rest of the term.
    {
        std::string recording_file;
        for (const auto& item : std::filesystem::directory_iterator(archive_dir_))
        {
            const std::string name = item.path().filename().string();
            if (name.size() > 4 && 0 == name.compare(name.size() - 4, 4, ".rec"))
            {
                recording_file = item.path().string();
            }
        }
        ASSERT_FALSE(recording_file.empty()) << log_text("node");
        std::ifstream in{recording_file, std::ios::binary};
        ASSERT_TRUE(in.good()) << recording_file;
        const std::int64_t pad_offset = 512 * 1024 - remaining;
        char header[8]{};
        in.seekg(pad_offset);
        in.read(header, sizeof(header));
        ASSERT_TRUE(in.good()) << recording_file;
        std::int32_t frame_length = 0;
        std::memcpy(&frame_length, header, sizeof(frame_length));
        // `HDR_TYPE_PAD` is 0x00 in the data header; the frame length is what
        // separates a written padding frame from the unwritten zeros after it.
        EXPECT_EQ(0x00, static_cast<unsigned char>(header[6])) << "no padding frame at " << pad_offset;
        EXPECT_EQ(remaining, frame_length) << "the padding frame is " << pad_offset;

        // The filler that forced the padding is the first frame of the next
        // term (`HDR_TYPE_DATA` is 0x01).
        char next_term[8]{};
        in.seekg(512 * 1024);
        in.read(next_term, sizeof(next_term));
        ASSERT_TRUE(in.good()) << recording_file;
        EXPECT_EQ(0x01, static_cast<unsigned char>(next_term[6])) << "the filler is not in the next term";
        std::int32_t filler_frame_length = 0;
        std::memcpy(&filler_frame_length, next_term, sizeof(filler_frame_length));
        EXPECT_EQ(remaining + 64, filler_frame_length) << "header + session header + the filler payload";
    }

    std::int64_t term_length = 0;
    std::string trace;
    ASSERT_TRUE(truncate_log_recording_to_term_length(term_length, trace)) << trace;
    EXPECT_EQ(512 * 1024, term_length) << trace;

    start();

    // The restarted service truncates its tracking file in `on_start` and the
    // replay appends the messages it re-applies; settle when the count has
    // stopped moving.
    const auto count_replayed_messages = [this]()
    {
        std::size_t count = 0;
        for (const std::string& line : tracking_lines())
        {
            count += (!line.empty() && 'M' == line[0]) ? 1 : 0;
        }
        return count;
    };
    std::size_t replayed = 0;
    std::size_t previous = std::string::npos;
    const auto settle_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (std::chrono::steady_clock::now() < settle_deadline)
    {
        const std::size_t count = count_replayed_messages();
        if (count == previous && count > 0)
        {
            replayed = count;
            break;
        }
        previous = count;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    ASSERT_GT(replayed, 0U) << "the replay never reached the service" << log_text("node") << log_text("container");

    // Every message before the padding frame is replayed, in order and once;
    // the padding frame itself is skipped, and the filler that went into the
    // next term was cut away with the truncation.
    std::vector<std::int32_t> ids;
    for (const std::string& line : tracking_lines())
    {
        if (!line.empty() && 'M' == line[0])
        {
            ids.push_back(static_cast<std::int32_t>(std::stoll(line.substr(2))));
        }
    }
    ASSERT_EQ(replayed, ids.size()) << trace;
    EXPECT_EQ(expected_ids.size(), ids.size()) << trace;
    for (std::size_t i = 0; i < std::min(ids.size(), expected_ids.size()); ++i)
    {
        ASSERT_EQ(expected_ids[i], ids[i]) << "the replay diverged at index " << i << ": " << trace;
    }

    EXPECT_EQ(1, cluster_counter(deepmsg::cluster::common::CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID)) << log_text("node");
}

// Reference `ClusterTest.shouldRecoverWhenLastSnapshotForShutdownIsMarkedInvalid`:
// an orderly shutdown takes a snapshot; with that snapshot entry invalidated
// the restarted node recovers from the log instead, and every message is still
// applied once. The tracking service, in `record` mode, is the witness.
TEST_F(Xc7SingleNodeSnapshotTest, shouldRecoverWhenLastSnapshotForShutdownIsMarkedInvalid)
{
    use_tracking_service();
    start();

    const auto payload_of = [](std::int32_t value)
    {
        std::string bytes(sizeof(value), '\0');
        std::memcpy(bytes.data(), &value, sizeof(value));
        return bytes;
    };

    ClusterClientSession session;
    ASSERT_TRUE(session.connect_ipc(aeron_dir_, std::chrono::seconds(30))) << log_text("node") << log_text("container");
    for (int i = 1; i <= 3; ++i)
    {
        ASSERT_TRUE(session.offer_and_await(payload_of(i), static_cast<std::size_t>(i), std::chrono::seconds(20)))
            << "message " << i << log_text("node") << log_text("container");
    }
    session.close();

    // `ClusterControl.ToggleState.SHUTDOWN`: the cluster snapshots and stops.
    set_control_toggle(5);
    const auto stop_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < stop_deadline)
    {
        if (node_ && !node_->running() && container_ && !container_->running())
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(node_ && !node_->running()) << "the node did not stop" << log_text("node");
    ASSERT_TRUE(container_ && !container_->running()) << "the container did not stop" << log_text("container");
    ASSERT_FALSE(recording_log_entries(deepmsg::cluster::common::RecordingLog::ENTRY_TYPE_SNAPSHOT).empty())
        << "the shutdown took no snapshot" << log_text("node");

    {
        auto log = deepmsg::cluster::common::RecordingLog::open(cluster_dir_, false);
        ASSERT_TRUE(log) << log.error().message.view();
        auto invalidated = log->invalidate_latest_snapshot();
        ASSERT_TRUE(invalidated) << invalidated.error().message.view();
        ASSERT_TRUE(*invalidated) << "no valid snapshot left to invalidate";
    }

    start();

    ClusterClientSession second;
    ASSERT_TRUE(second.connect_ipc(aeron_dir_, std::chrono::seconds(30))) << log_text("node") << log_text("container");
    for (int i = 4; i <= 6; ++i)
    {
        ASSERT_TRUE(second.offer_and_await(payload_of(i), static_cast<std::size_t>(i - 3), std::chrono::seconds(20)))
            << "message " << i << log_text("node") << log_text("container");
    }
    second.close();

    // The restarted service truncates its file in `on_start`, replays the log
    // (1..3) and then applies the new messages (4..6).
    const auto settle_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    std::size_t replayed = 0;
    std::size_t previous = std::string::npos;
    while (std::chrono::steady_clock::now() < settle_deadline)
    {
        std::size_t count = 0;
        for (const std::string& line : tracking_lines())
        {
            count += (!line.empty() && 'M' == line[0]) ? 1 : 0;
        }
        if (count == previous && count > 0)
        {
            replayed = count;
            break;
        }
        previous = count;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    EXPECT_EQ(6U, replayed) << "the recovery from the invalidated shutdown snapshot diverged" << log_text("node")
                            << log_text("container");
}

TEST_F(Xc2SingleNodeTest, shutdownTerminatesBothProcesses)
{
    start();

    set_control_toggle(5);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (node_ && !node_->running() && container_ && !container_->running())
        {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ASSERT_TRUE(node_ && !node_->running()) << "the node did not stop" << log_text("node");
    ASSERT_TRUE(container_ && !container_->running()) << "the container did not stop" << log_text("container");
}

TEST_F(Xc2SingleNodeElectionTest, aSingleNodeDoesNotEnterAnElectionOnItsOwn)
{
    start();

    const std::int64_t term_before = cluster_counter(deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);
    ASSERT_EQ(2, cluster_counter(deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID))
        << "node=" << (node_ && node_->running() ? "running" : "stopped")
        << " container=" << (container_ && container_->running() ? "running" : "stopped")
        << " client=" << (client_ ? "yes" : "no") << log_text("node") << log_text("container");

    // The default leader heartbeat timeout is 10s; wait past it. A single
    // member is its own quorum, so `slowTickWork:2438` must not fire.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(12);
    while (std::chrono::steady_clock::now() < deadline)
    {
        // Keep the counter client's conductor serviced while we wait.
        (void)cluster_counter(deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    EXPECT_EQ(2, cluster_counter(deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID)) << log_text("node");
    EXPECT_EQ(term_before, cluster_counter(deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID))
        << log_text("node");
}

TEST_F(Xc2SingleNodeElectionTest, shouldEnterElectionWhenRecordingStopsUnexpectedlyOnLeaderOfSingleNodeCluster)
{
    start();

    const std::int64_t term_before = cluster_counter(deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);
    ASSERT_EQ(2, cluster_counter(deepmsg::cluster::common::CLUSTER_NODE_ROLE_TYPE_ID)) << log_text("node");

    ASSERT_TRUE(stop_log_recording()) << log_text("node");

    bool advanced = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (std::chrono::steady_clock::now() < deadline)
    {
        // Drive the counter client's conductor while the CM elects itself.
        const std::int64_t term = cluster_counter(deepmsg::cluster::common::CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);
        if (term > term_before)
        {
            advanced = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    EXPECT_TRUE(advanced) << "the single node did not start a new term after the recording stopped" << log_text("node");
}

TEST_F(Xc2SingleNodeElectionTest, shouldCallOnRoleChangeOnBecomingLeaderSingleNodeCluster)
{
    service_library_ = AXC_ROLE_CHANGE_SERVICE_LIBRARY;
    role_dir_ = directory_ + "/roles";

    start();

    const std::vector<std::int32_t> roles = role_records();
    EXPECT_NE(roles.end(), std::find(roles.begin(), roles.end(), 2))
        << "the single node's service never saw onRoleChange(LEADER); files: " << role_files() << log_text("node")
        << log_text("container");
}

}  // namespace
