// A5b / XA-2: the reference's Java `AeronArchive` client against our archive on
// the reference's Java media driver.
//
// The instrument is `ReferenceArchiveClientProbe.java` from the fork's
// `deepmsg-tests` branch (a Java file belongs there, not in this repository):
// it records through `startRecording`, finds the recording-position counter the
// way the reference's own benchmark does, replays through `startReplay`, and —
// with `--segment-ops` — truncates and purges. The probe prints one reading per
// line and holds the recording open at `LIVE` so this side can read the same
// counter slot with our own reader while it is live.
//
// Skips when the reference build (aeron-all jar), a JDK or the probe directory
// is absent; the directory is `AXC_DEEPMSG_PROBE_DIR` or the sibling fork
// checkout.

#include <gtest/gtest.h>

#include <aeronc.h>
#include <poll.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

#include "archive/server/recording_pos.h"
#include "core/deepmsg/deepmsg_client.h"
#include "fixture/c_driver.h"
#include "fixture/process.h"

extern "C"
{
#include "client/aeron_archive.h"
#include "client/aeron_archive_async_connect.h"
}

#ifndef AXC_AERON_JAR
#define AXC_AERON_JAR ""
#endif

namespace
{

using deepmsg::archive::server::RecordingPos;

constexpr std::int64_t ARCHIVE_ID = 0x0423'6483'BEEF;
constexpr std::int32_t RECORD_STREAM_ID = 1001;
constexpr std::int32_t REPLAY_STREAM_ID = 1002;
constexpr std::int32_t CONTROL_STREAM_ID = 10;
constexpr std::int32_t RESPONSE_STREAM_ID = 11;
constexpr const char* RECORD_CHANNEL = "aeron:ipc?alias=axc-xa2-record";
constexpr const char* REPLAY_CHANNEL = "aeron:ipc?alias=axc-xa2-replay";
constexpr auto DEADLINE = std::chrono::seconds(30);

[[nodiscard]] std::string aeron_jar()
{
    if (const char* from_env = std::getenv("AXC_AERON_JAR"); nullptr != from_env && '\0' != *from_env)
    {
        return from_env;
    }
    return AXC_AERON_JAR;
}

/// Where the fork's Java probes live. `AXC_DEEPMSG_PROBE_DIR` wins; otherwise the
/// two checkouts that can carry the `deepmsg-tests` branch (the Rust track's
/// worktree is the one that normally does).
[[nodiscard]] std::string probe_directory()
{
    if (const char* from_env = std::getenv("AXC_DEEPMSG_PROBE_DIR"); nullptr != from_env && '\0' != *from_env)
    {
        return from_env;
    }

    for (const char* candidate :
         {"/tmp/aeron-deepmsg-tests/deepmsg-tests/java", "/home/gavin/trader/aeron-deepmsg/deepmsg-tests/java"})
    {
        if (std::filesystem::exists(std::string{candidate} + "/ReferenceArchiveClientProbe.java"))
        {
            return candidate;
        }
    }
    return "/tmp/aeron-deepmsg-tests/deepmsg-tests/java";
}

/// A child whose stdout is read line by line and whose stdin the test writes.
class Probe
{
public:
    ~Probe()
    {
        if (pid_ > 0)
        {
            int status = 0;
            if (0 == ::waitpid(pid_, &status, WNOHANG))
            {
                ::kill(pid_, SIGKILL);
                (void)::waitpid(pid_, &status, 0);
            }
        }
        if (stdout_fd_ >= 0)
        {
            ::close(stdout_fd_);
        }
        if (stdin_fd_ >= 0)
        {
            ::close(stdin_fd_);
        }
    }

    /// `arguments` excludes argv[0]; `java` is spawned with them.
    [[nodiscard]] bool spawn(const std::string& java, const std::vector<std::string>& arguments)
    {
        int stdin_pipe[2] = {-1, -1};
        int stdout_pipe[2] = {-1, -1};
        if (0 != ::pipe(stdin_pipe) || 0 != ::pipe(stdout_pipe))
        {
            return false;
        }

        posix_spawn_file_actions_t actions;
        (void)posix_spawn_file_actions_init(&actions);
        (void)posix_spawn_file_actions_adddup2(&actions, stdin_pipe[0], STDIN_FILENO);
        (void)posix_spawn_file_actions_adddup2(&actions, stdout_pipe[1], STDOUT_FILENO);
        (void)posix_spawn_file_actions_addclose(&actions, stdin_pipe[1]);
        (void)posix_spawn_file_actions_addclose(&actions, stdout_pipe[0]);

        std::vector<std::string> storage;
        storage.reserve(arguments.size() + 1);
        storage.push_back(java);
        for (const std::string& argument : arguments)
        {
            storage.push_back(argument);
        }
        std::vector<char*> argv;
        argv.reserve(storage.size() + 1);
        for (std::string& argument : storage)
        {
            argv.push_back(argument.data());
        }
        argv.push_back(nullptr);

        pid_ = -1;
        const int spawned = posix_spawn(&pid_, java.c_str(), &actions, nullptr, argv.data(), ::environ);
        (void)posix_spawn_file_actions_destroy(&actions);
        ::close(stdin_pipe[0]);
        ::close(stdout_pipe[1]);

        if (0 != spawned)
        {
            ::close(stdin_pipe[1]);
            ::close(stdout_pipe[0]);
            return false;
        }

        stdin_fd_ = stdin_pipe[1];
        stdout_fd_ = stdout_pipe[0];
        return true;
    }

    /// Blocks until one line is available or the deadline passes.
    [[nodiscard]] bool read_line(std::string& line, std::chrono::steady_clock::time_point deadline)
    {
        line.clear();
        while (true)
        {
            const auto remaining =
                std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now())
                    .count();
            if (remaining <= 0)
            {
                return false;
            }

            pollfd polled{stdout_fd_, POLLIN, 0};
            const int ready = ::poll(&polled, 1, static_cast<int>(remaining));
            if (ready <= 0)
            {
                return false;
            }

            char character = '\0';
            const ssize_t read_count = ::read(stdout_fd_, &character, 1);
            if (read_count <= 0)
            {
                return false;
            }
            if ('\n' == character)
            {
                return true;
            }
            line.push_back(character);
        }
    }

    void write_go()
    {
        const char go[] = "go\n";
        (void)::write(stdin_fd_, go, sizeof(go) - 1);
    }

    [[nodiscard]] int wait_exit(std::chrono::steady_clock::time_point deadline)
    {
        while (std::chrono::steady_clock::now() < deadline)
        {
            int status = 0;
            const pid_t reaped = ::waitpid(pid_, &status, WNOHANG);
            if (reaped == pid_)
            {
                pid_ = -1;
                return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return -1;
    }

private:
    pid_t pid_{-1};
    int stdin_fd_{-1};
    int stdout_fd_{-1};
};

[[nodiscard]] std::int64_t field_of(const std::string& line, std::size_t index)
{
    std::size_t start = line.find(' ');
    for (std::size_t field = 0; field < index && start != std::string::npos; ++field)
    {
        start = line.find(' ', start + 1);
    }
    if (std::string::npos == start)
    {
        return -1;
    }
    return std::strtoll(line.c_str() + start + 1, nullptr, 10);
}

class ReferenceJavaClientTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        if (aeron_jar().empty() || !std::filesystem::exists(aeron_jar()))
        {
            GTEST_SKIP() << "reference aeron-all jar not built (set AXC_AERON_JAR)";
        }
        const std::string probe = probe_directory() + "/ReferenceArchiveClientProbe.java";
        if (!std::filesystem::exists(probe))
        {
            GTEST_SKIP() << "probe source not found at " << probe << " (set AXC_DEEPMSG_PROBE_DIR)";
        }

        std::array<char, 64> pattern{};
        std::snprintf(pattern.data(), pattern.size(), "/tmp/axc-xa2-XXXXXX");
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        root_ = created;
        aeron_dir_ = root_ + "/aeron";
        archive_dir_ = root_ + "/archive";
        classes_dir_ = root_ + "/classes";
        std::filesystem::create_directories(archive_dir_);
        std::filesystem::create_directories(classes_dir_);

        ASSERT_TRUE(std::filesystem::exists(AXC_ARCHIVE_SERVER_BIN)) << AXC_ARCHIVE_SERVER_BIN;

        compile_probe();
        start_java_driver();
        start_archive();
    }

    void TearDown() override
    {
        archive_.terminate();
        driver_.terminate();
        std::error_code error;
        std::filesystem::remove_all(root_, error);
    }

    void compile_probe()
    {
        axc::interop::ProcessOptions options;
        options.arguments = {"-proc:none", "-cp",        aeron_jar(),
                             "-d",         classes_dir_, probe_directory() + "/ReferenceArchiveClientProbe.java"};
        options.timeout = std::chrono::seconds(120);
        auto compiled = axc::interop::run_process("/usr/bin/javac", options);
        ASSERT_TRUE(compiled) << compiled.error().message.view();
        ASSERT_EQ(0, compiled->exit_code) << compiled->output;
    }

    void start_java_driver()
    {
        std::vector<std::string> arguments = {"--add-opens",
                                              "java.base/jdk.internal.misc=ALL-UNNAMED",
                                              "--add-opens",
                                              "java.base/java.util.zip=ALL-UNNAMED",
                                              "-Daeron.dir=" + aeron_dir_,
                                              "-Daeron.threading.mode=SHARED",
                                              "-Daeron.idle.strategy=yield",
                                              "-cp",
                                              aeron_jar(),
                                              "io.aeron.driver.MediaDriver"};

        auto spawned = axc::interop::ChildProcess::spawn("/usr/bin/java", arguments);
        ASSERT_TRUE(spawned) << spawned.error().message.view();
        driver_ = std::move(*spawned);

        // The archive's client refuses a driver without a live heartbeat, so wait
        // for the driver the same way our client does.
        const auto deadline = std::chrono::steady_clock::now() + DEADLINE;
        bool ready = false;
        while (std::chrono::steady_clock::now() < deadline && !ready)
        {
            axc::core::ClientConfig config;
            config.dir = aeron_dir_;
            auto client = axc::core::DeepmsgClient::connect(config);
            if (client)
            {
                ready = true;
            }
            else
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
        ASSERT_TRUE(ready) << "the Java media driver never came up";
    }

    void start_archive()
    {
        control_channel_ = "aeron:udp?endpoint=localhost:" + std::to_string(control_port());
        const std::string response_channel_value = "aeron:udp?endpoint=localhost:" + std::to_string(control_port() + 1);

        std::vector<std::string> arguments = {"-Daeron.dir=" + aeron_dir_,
                                              "-Daeron.archive.dir=" + archive_dir_,
                                              "-Daeron.archive.control.channel=" + control_channel_,
                                              "-Daeron.archive.control.stream.id=" + std::to_string(CONTROL_STREAM_ID),
                                              "-Daeron.archive.local.control.stream.id=12",
                                              "-Daeron.archive.control.response.channel=" + response_channel_value,
                                              "-Daeron.archive.id=" + std::to_string(ARCHIVE_ID),
                                              "-Daeron.archive.threading.mode=SHARED",
                                              "-Daeron.archive.idle.strategy=yield",
                                              "-Daeron.archive.dir.delete.on.start=true",
                                              "-Daeron.spies.simulate.connection=true",
                                              "io.aeron.archive.Archive"};

        auto spawned = axc::interop::ChildProcess::spawn(AXC_ARCHIVE_SERVER_BIN, arguments);
        ASSERT_TRUE(spawned) << spawned.error().message.view();
        archive_ = std::move(*spawned);

        // Wait for the control plane: an async connect that completes is the
        // archive's own "ready".
        aeron_archive_context_t* context = nullptr;
        ASSERT_EQ(0, aeron_archive_context_init(&context));
        aeron_archive_context_set_aeron_directory_name(context, aeron_dir_.c_str());
        aeron_archive_context_set_control_request_channel(context, control_channel_.c_str());
        aeron_archive_context_set_control_request_stream_id(context, CONTROL_STREAM_ID);
        aeron_archive_context_set_control_response_channel(context, response_channel().c_str());
        aeron_archive_context_set_control_response_stream_id(context, RESPONSE_STREAM_ID);

        aeron_archive_async_connect_t* async = nullptr;
        ASSERT_EQ(0, aeron_archive_async_connect(&async, context));
        aeron_archive_t* archive = nullptr;
        const auto deadline = std::chrono::steady_clock::now() + DEADLINE;
        while (std::chrono::steady_clock::now() < deadline && nullptr == archive)
        {
            const int poll = aeron_archive_async_connect_poll(&archive, async);
            ASSERT_GE(poll, 0) << aeron_errmsg();
            if (0 != poll)
            {
                async = nullptr;
            }
            if (nullptr == archive)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
        ASSERT_NE(nullptr, archive) << "the archive never answered on " << control_channel_;
        (void)aeron_archive_close(archive);
        EXPECT_EQ(0, aeron_archive_context_close(context));
    }

    [[nodiscard]] std::string response_channel() const
    {
        return "aeron:udp?endpoint=localhost:" + std::to_string(control_port() + 1);
    }

    [[nodiscard]] static std::uint16_t control_port()
    {
        return static_cast<std::uint16_t>(34010 + (::getpid() % 400) * 2);
    }

    void run_probe(bool segment_ops)
    {
        std::vector<std::string> arguments = {
            "--add-opens",
            "java.base/jdk.internal.misc=ALL-UNNAMED",
            "--add-opens",
            "java.base/java.util.zip=ALL-UNNAMED",
            "-cp",
            aeron_jar() + ":" + classes_dir_,
            "ReferenceArchiveClientProbe",
            aeron_dir_,
            control_channel_,
            response_channel(),
            RECORD_CHANNEL,
            std::to_string(RECORD_STREAM_ID),
            REPLAY_CHANNEL,
            std::to_string(REPLAY_STREAM_ID),
            std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(DEADLINE * 2).count())};
        if (segment_ops)
        {
            arguments.push_back("--segment-ops");
        }

        Probe probe;
        ASSERT_TRUE(probe.spawn("/usr/bin/java", arguments));

        const auto deadline = std::chrono::steady_clock::now() + DEADLINE * 2;
        std::vector<std::string> lines;
        std::string line;
        while (probe.read_line(line, deadline))
        {
            lines.push_back(line);
            if (0 == line.compare(0, 5, "LIVE "))
            {
                // The recording is live: read the counter with our own reader,
                // the same slot the probe found.
                const auto session_id = static_cast<std::int32_t>(field_of(line, 0));
                std::size_t counter_id = static_cast<std::size_t>(-1);
                for (int attempt = 0; attempt < 1000 && static_cast<std::size_t>(-1) == counter_id; ++attempt)
                {
                    axc::core::ClientConfig config;
                    config.dir = aeron_dir_;
                    auto client = axc::core::DeepmsgClient::connect(config);
                    if (client)
                    {
                        (void)client->do_work();
                        auto* counters = aeron_counters_reader(client->handle());
                        const std::int32_t found = RecordingPos::find_by_session(counters, ARCHIVE_ID, session_id);
                        if (AERON_NULL_COUNTER_ID != found)
                        {
                            counter_id = static_cast<std::size_t>(found);
                        }
                    }
                    if (static_cast<std::size_t>(-1) == counter_id)
                    {
                        std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    }
                }
                EXPECT_NE(static_cast<std::size_t>(-1), counter_id)
                    << "our reader did not find the recording position counter for session " << session_id;
                probe.write_go();
            }
            if (0 == line.compare(0, 4, "DONE"))
            {
                break;
            }
        }

        EXPECT_EQ(0, probe.wait_exit(deadline));

        const auto find = [&lines](std::string_view prefix) -> const std::string*
        {
            for (const std::string& candidate : lines)
            {
                if (0 == candidate.compare(0, prefix.size(), prefix))
                {
                    return &candidate;
                }
            }
            return nullptr;
        };

        const std::string* archive_id = find("ARCHIVE_ID ");
        ASSERT_NE(nullptr, archive_id);
        EXPECT_EQ(ARCHIVE_ID, field_of(*archive_id, 0));

        const std::string* listed_count = find("LISTED_COUNT ");
        ASSERT_NE(nullptr, listed_count);
        EXPECT_GE(field_of(*listed_count, 0), 0);

        const std::string* published = find("PUBLISHED ");
        ASSERT_NE(nullptr, published);
        const std::int64_t published_position = field_of(*published, 1);

        const std::string* stop_position = find("STOP_POSITION ");
        ASSERT_NE(nullptr, stop_position) << "the catalog never wrote a stop position";
        EXPECT_EQ(published_position, field_of(*stop_position, 0));

        const std::string* replayed = find("REPLAYED ");
        ASSERT_NE(nullptr, replayed);
        EXPECT_EQ(field_of(*published, 0), field_of(*replayed, 0));

        if (segment_ops)
        {
            EXPECT_NE(nullptr, find("TRUNCATED "));
            EXPECT_NE(nullptr, find("PURGED "));
            const std::string* listed_after = find("LISTED_AFTER_PURGE ");
            ASSERT_NE(nullptr, listed_after);
            EXPECT_EQ(0, field_of(*listed_after, 0));
        }

        if (::testing::Test::HasFailure())
        {
            for (const std::string& printed : lines)
            {
                std::fprintf(stderr, "[probe] %s\n", printed.c_str());
            }
        }
    }

    std::string root_;
    std::string aeron_dir_;
    std::string archive_dir_;
    std::string classes_dir_;
    std::string control_channel_;
    axc::interop::ChildProcess driver_;
    axc::interop::ChildProcess archive_;
};

TEST_F(ReferenceJavaClientTest, theReferenceJavaClientRecordsQueriesAndReplaysAgainstOurArchive)
{
    run_probe(false);
}

TEST_F(ReferenceJavaClientTest, theReferenceJavaClientTruncatesAndPurgesOurCatalog)
{
    run_probe(true);
}

}  // namespace
