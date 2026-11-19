// T2: the offline half of `ClusterToolTest` (`ClusterToolTest.java`), run
// in-process against `deepmsg::tools::run_cluster_tool`. The running-cluster
// half lives in `test/interop/cluster/t2_cluster_tool_interop_test.cpp`.

#include <gtest/gtest.h>

#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <optional>
#include <string>
#include <vector>

#include "cluster/common/cluster_mark_file.h"
#include "cluster/common/recording_log.h"
#include "tools/cluster-tool/tool.h"

namespace
{

using deepmsg::cluster::common::ClusterComponentType;
using deepmsg::cluster::common::ClusterMarkFile;
using deepmsg::cluster::common::RecordingLog;
using deepmsg::tools::run_cluster_tool;
using deepmsg::tools::TOOL_FAILURE;
using deepmsg::tools::TOOL_SUCCESS;

std::string read_file(const std::string& path)
{
    std::ifstream input{path, std::ios::binary};
    return std::string{std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{}};
}

struct ToolResult
{
    int status{0};
    std::string out{};
    std::string err{};
};

ToolResult run(const std::vector<std::string>& args)
{
    ToolResult result;
    result.status = run_cluster_tool(args, result.out, result.err);
    return result;
}

class ClusterToolTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::string pattern = "/tmp/axc-t2-tool-XXXXXX";
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        directory_ = created;
        cluster_dir_ = directory_ + "/cluster";
        std::filesystem::create_directories(cluster_dir_);
    }

    void TearDown() override
    {
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
    }

    [[nodiscard]] std::string path(std::string_view name) const
    {
        return cluster_dir_ + "/" + std::string{name};
    }

    std::optional<ClusterMarkFile> create_mark_file(
        ClusterComponentType component_type = ClusterComponentType::consensus_module, std::string_view directory = {},
        std::string_view services_directory = {}, std::int64_t now = 1'700'000'000'000)
    {
        const std::string target = directory.empty() ? cluster_dir_ : std::string{directory};
        const std::string file_name = ClusterComponentType::container == component_type
                                          ? ClusterMarkFile::mark_filename_for_service(0)
                                          : std::string{ClusterMarkFile::FILE_NAME};
        ClusterMarkFile::Options options;
        options.path = target + "/" + file_name;
        options.component_type = component_type;
        options.error_buffer_length = ClusterMarkFile::ERROR_BUFFER_MIN_LENGTH;
        options.timeout_ms = 10'000;
        options.file_page_size = 4 * 1024;
        options.now_ms = now;

        auto mark_file = ClusterMarkFile::create_or_open(options);
        EXPECT_TRUE(mark_file) << mark_file.error().message.view();
        if (!mark_file)
        {
            return std::nullopt;
        }
        mark_file->set_cluster_id(7);
        mark_file->set_member_id(1);
        mark_file->set_service_id(0);
        mark_file->set_archive_stream_id(1);
        mark_file->set_service_stream_id(2);
        mark_file->set_consensus_module_stream_id(3);
        mark_file->set_ingress_stream_id(4);
        mark_file->set_channels(directory_ + "/aeron", "aeron:ipc", "aeron:ipc", "", "", services_directory);
        auto ready = mark_file->signal_ready(now);
        EXPECT_TRUE(ready) << ready.error().message.view();
        return std::optional<ClusterMarkFile>{std::move(*mark_file)};
    }

    /// One Agrona distinct-error-log entry, as `CommonContext.printErrorLog`
    /// reads it (24-byte header, text, 8-byte aligned).
    static void write_error(ClusterMarkFile& mark_file, std::string_view text)
    {
        auto buffer = mark_file.error_buffer();
        const std::size_t length = 24 + text.size();
        std::int32_t value = static_cast<std::int32_t>(length);
        std::memcpy(buffer.data(), &value, sizeof(value));
        value = 1;
        std::memcpy(buffer.data() + 4, &value, sizeof(value));
        std::int64_t timestamp = 1'700'000'000'000;
        std::memcpy(buffer.data() + 8, &timestamp, sizeof(timestamp));
        std::memcpy(buffer.data() + 16, &timestamp, sizeof(timestamp));
        std::memcpy(buffer.data() + 24, text.data(), text.size());
    }

    std::string directory_{};
    std::string cluster_dir_{};
};

TEST_F(ClusterToolTest, helpListsTheEighteenCommands)
{
    const auto result = run({});
    EXPECT_EQ(TOOL_FAILURE, result.status);
    for (const char* command :
         {"describe", "pid", "recovery-plan", "recording-log", "sort-recording-log", "seed-recording-log-from-snapshot",
          "errors", "list-members", "backup-query", "invalidate-latest-snapshot", "is-leader", "snapshot", "suspend",
          "resume", "shutdown", "abort", "describe-latest-cm-snapshot", "validate-recording-log"})
    {
        EXPECT_NE(std::string::npos, result.out.find(command)) << command;
    }
}

TEST_F(ClusterToolTest, shouldFailIfMarkFileUnavailable)
{
    const auto result = run({cluster_dir_, "snapshot"});
    EXPECT_EQ(TOOL_FAILURE, result.status);
    EXPECT_NE(std::string::npos, result.out.find("cluster-mark.dat does not exist.")) << result.out;
}

TEST_F(ClusterToolTest, pidPrintsTheMarkFilesPid)
{
    auto mark_file = create_mark_file();
    ASSERT_TRUE(mark_file);
    const auto result = run({cluster_dir_, "pid"});
    EXPECT_EQ(TOOL_SUCCESS, result.status);
    EXPECT_EQ(std::to_string(mark_file->pid()) + "\n", result.out);
}

TEST_F(ClusterToolTest, describePrintsTheHeaderAndTheServiceMarkFiles)
{
    auto mark_file = create_mark_file(ClusterComponentType::consensus_module, {}, directory_ + "/services");
    ASSERT_TRUE(mark_file);
    write_error(*mark_file, "the consensus module's error");
    ASSERT_TRUE(mark_file->force());

    const std::string service_dir = directory_ + "/services";
    std::filesystem::create_directories(service_dir);
    auto service = create_mark_file(ClusterComponentType::container, service_dir);
    ASSERT_TRUE(service);
    write_error(*service, "the container's error");
    ASSERT_TRUE(service->force());

    const auto described = run({cluster_dir_, "describe"});
    EXPECT_EQ(TOOL_SUCCESS, described.status);
    EXPECT_NE(std::string::npos, described.out.find("Type: CONSENSUS_MODULE")) << described.out;
    EXPECT_NE(std::string::npos, described.out.find("[MarkFileHeader](sbeTemplateId=200|sbeSchemaId=110"))
        << described.out;
    EXPECT_NE(std::string::npos, described.out.find("Type: CONTAINER")) << described.out;

    const auto errors = run({cluster_dir_, "errors"});
    // Without a media driver the reference throws out of `printDriverErrors`;
    // the tool reports FAILURE after the mark-file half has been printed.
    EXPECT_EQ(TOOL_FAILURE, errors.status);
    EXPECT_NE(std::string::npos, errors.out.find("the consensus module's error")) << errors.out;
}

TEST_F(ClusterToolTest, shouldBeAbleToAccessClusterMarkFilesInANonDefaultLocation)
{
    const std::string mark_dir = directory_ + "/mark";
    std::filesystem::create_directories(mark_dir);

    auto mark_file = create_mark_file(ClusterComponentType::consensus_module, mark_dir);
    ASSERT_TRUE(mark_file);
    write_error(*mark_file, "the mark file's error");
    ASSERT_TRUE(mark_file->force());

    {
        std::ofstream link{cluster_dir_ + "/" + std::string{ClusterMarkFile::LINK_FILE_NAME}};
        link << mark_dir;
    }

    const auto result = run({cluster_dir_, "errors"});
    EXPECT_NE(std::string::npos, result.out.find("the mark file's error")) << result.out;
}

TEST_F(ClusterToolTest, recordingLogPrintsTheEntryList)
{
    {
        auto log = RecordingLog::open(cluster_dir_, true);
        ASSERT_TRUE(log) << log.error().message.view();
        ASSERT_TRUE(log->append_term(21, 0, 100, 100));
        ASSERT_TRUE(log->append_snapshot(0, 0, 100, 0, 200, 0));
    }

    const auto result = run({cluster_dir_, "recording-log"});
    EXPECT_EQ(TOOL_SUCCESS, result.status);
    EXPECT_NE(std::string::npos, result.out.find("Entry{recordingId=21, leadershipTermId=0"));
    EXPECT_NE(std::string::npos, result.out.find("RecordingLog{entries=[Entry{")) << result.out;
}

TEST_F(ClusterToolTest, sortRecordingLogIsANoOpIfRecordLogIsEmpty)
{
    {
        auto log = RecordingLog::open(cluster_dir_, true);
        ASSERT_TRUE(log);
    }
    const std::string log_path = path(RecordingLog::FILE_NAME);
    const auto original = std::filesystem::file_size(log_path);

    const auto result = run({cluster_dir_, "sort-recording-log"});
    EXPECT_EQ(TOOL_FAILURE, result.status);
    EXPECT_EQ(original, std::filesystem::file_size(log_path));
}

TEST_F(ClusterToolTest, sortRecordingLogIsANoOpIfRecordDoesNotExist)
{
    const auto result = run({cluster_dir_, "sort-recording-log"});
    EXPECT_EQ(TOOL_FAILURE, result.status);
    EXPECT_FALSE(std::filesystem::exists(path(RecordingLog::FILE_NAME)));
}

TEST_F(ClusterToolTest, sortRecordingLogIsANoOpIfRecordLogIsAlreadySorted)
{
    {
        auto log = RecordingLog::open(cluster_dir_, true);
        ASSERT_TRUE(log);
        ASSERT_TRUE(log->append_term(21, 0, 100, 100));
        ASSERT_TRUE(log->append_snapshot(0, 0, 100, 0, 200, 0));
        ASSERT_TRUE(log->append_term(21, 1, 1024, 200));
    }

    const std::string original = read_file(path(RecordingLog::FILE_NAME));
    const auto result = run({cluster_dir_, "sort-recording-log"});
    EXPECT_EQ(TOOL_FAILURE, result.status);
    EXPECT_EQ(original, read_file(path(RecordingLog::FILE_NAME)));
}

TEST_F(ClusterToolTest, sortRecordingLogShouldRearrangeDataOnDisc)
{
    std::vector<RecordingLog::Entry> expected;
    {
        auto log = RecordingLog::open(cluster_dir_, true);
        ASSERT_TRUE(log);
        ASSERT_TRUE(log->append_term(21, 2, 100, 100));
        ASSERT_TRUE(log->append_snapshot(1, 2, 50, 60, 42, 89));
        ASSERT_TRUE(log->append_term(21, 1, 1024, 200));
        ASSERT_TRUE(log->append_snapshot(0, 0, 0, 0, 200, 0));
        expected = log->entries();
    }

    std::string before = read_file(path(RecordingLog::FILE_NAME));
    const auto result = run({cluster_dir_, "sort-recording-log"});
    EXPECT_EQ(TOOL_SUCCESS, result.status) << result.out;
    EXPECT_NE(before, read_file(path(RecordingLog::FILE_NAME)));

    auto reloaded = RecordingLog::open(cluster_dir_, false);
    ASSERT_TRUE(reloaded) << reloaded.error().message.view();
    const auto& entries = reloaded->entries();
    ASSERT_EQ(expected.size(), entries.size());
    for (std::size_t i = 0; i < entries.size(); ++i)
    {
        EXPECT_EQ(static_cast<std::int32_t>(i), entries[i].entry_index);
    }
}

TEST_F(ClusterToolTest, seedRecordingLogFromSnapshotShouldDeleteOriginalRecordingLogFileIfThereAreNoValidSnapshots)
{
    {
        auto log = RecordingLog::open(cluster_dir_, true);
        ASSERT_TRUE(log);
        ASSERT_TRUE(log->append_term(1, 1, 0, 100));
        ASSERT_TRUE(log->append_snapshot(1, 1, 1000, 256, 300, 0));
        ASSERT_TRUE(log->append_snapshot(1, 1, 1000, 256, 300, RecordingLog::SERVICE_ID));
        ASSERT_TRUE(log->append_term(1, 2, 2000, 400));
        ASSERT_TRUE(log->append_snapshot(2, 5, 56, 111, 500, 5));
        ASSERT_TRUE(log->invalidate_latest_snapshot());
    }

    const std::string log_path = path(RecordingLog::FILE_NAME);
    const std::string backup_path = log_path + ".bak";
    const auto log_size = std::filesystem::file_size(log_path);
    ASSERT_FALSE(std::filesystem::exists(backup_path));

    const auto result = run({cluster_dir_, "seed-recording-log-from-snapshot"});
    EXPECT_EQ(TOOL_SUCCESS, result.status) << result.out;
    EXPECT_FALSE(std::filesystem::exists(log_path));
    ASSERT_TRUE(std::filesystem::exists(backup_path));
    EXPECT_EQ(log_size, std::filesystem::file_size(backup_path));
}

TEST_F(ClusterToolTest, seedRecordingLogFromSnapshotShouldCreateANewRecordingLogFromALatestValidSnapshot)
{
    const std::string log_path = path(RecordingLog::FILE_NAME);
    const std::string backup_path = log_path + ".bak";
    {
        std::ofstream backup{backup_path, std::ios::binary};
        const char bytes[] = {0x1, static_cast<char>(0x80), 0, 1, static_cast<char>(0xff), 0x7f};
        backup.write(bytes, sizeof(bytes));
    }

    {
        auto log = RecordingLog::open(cluster_dir_, true);
        ASSERT_TRUE(log);
        ASSERT_TRUE(log->append_term(1, 0, 0, 0));
        ASSERT_TRUE(log->append_snapshot(1, 3, 4000, 4000, 600, 2));
        ASSERT_TRUE(log->append_term(1, 3, 3000, 500));
        ASSERT_TRUE(log->append_snapshot(1, 2, 2900, 2200, 400, 2));
        ASSERT_TRUE(log->append_snapshot(1, 2, 2900, 2200, 400, 1));
        ASSERT_TRUE(log->append_snapshot(1, 2, 2900, 2200, 400, 0));
        ASSERT_TRUE(log->append_snapshot(1, 2, 2900, 2200, 400, RecordingLog::SERVICE_ID));
        ASSERT_TRUE(log->append_term(1, 2, 2000, 300));
        ASSERT_TRUE(log->append_snapshot(1, 1, 1800, 1000, 200, RecordingLog::SERVICE_ID));
        ASSERT_TRUE(log->append_snapshot(1, 1, 1800, 1000, 200, 0));
        ASSERT_TRUE(log->append_snapshot(1, 1, 1800, 1000, 200, 1));
        ASSERT_TRUE(log->append_term(1, 1, 1000, 100));
        ASSERT_TRUE(log->invalidate_latest_snapshot());
    }

    // The three entries the reference's test keeps (`entries.get(2..4)`).
    std::vector<std::int32_t> expected_service_ids;
    std::vector<std::int64_t> expected_recording_ids;
    {
        auto original = RecordingLog::open(cluster_dir_, false);
        ASSERT_TRUE(original);
        const auto& entries = original->entries();
        ASSERT_EQ(12U, entries.size());
        for (std::size_t i = 2; i < 5; ++i)
        {
            expected_service_ids.push_back(entries[i].service_id);
            expected_recording_ids.push_back(entries[i].recording_id);
        }
    }

    const auto result = run({cluster_dir_, "seed-recording-log-from-snapshot"});
    EXPECT_EQ(TOOL_SUCCESS, result.status) << result.out;

    auto reloaded = RecordingLog::open(cluster_dir_, false);
    ASSERT_TRUE(reloaded) << reloaded.error().message.view();
    const auto& entries = reloaded->entries();
    ASSERT_EQ(3U, entries.size());
    for (std::size_t i = 0; i < entries.size(); ++i)
    {
        EXPECT_EQ(0, entries[i].term_base_log_position);
        EXPECT_EQ(0, entries[i].log_position);
        EXPECT_EQ(static_cast<std::int32_t>(i), entries[i].entry_index);
        EXPECT_EQ(expected_service_ids[i], entries[i].service_id);
        EXPECT_EQ(expected_recording_ids[i], entries[i].recording_id);
    }
}

TEST_F(ClusterToolTest, isLeaderFailsWithoutAMarkFile)
{
    const auto result = run({cluster_dir_, "is-leader"});
    EXPECT_EQ(TOOL_FAILURE, result.status);
    EXPECT_NE(std::string::npos, result.out.find("cluster-mark.dat does not exist.")) << result.out;
}

TEST_F(ClusterToolTest, backupQueryRejectsANonBackupMarkFile)
{
    ASSERT_TRUE(create_mark_file());
    const auto result = run({cluster_dir_, "backup-query"});
    EXPECT_EQ(TOOL_FAILURE, result.status);
    EXPECT_NE(std::string::npos, result.out.find("not a cluster backup node")) << result.out;
}

TEST_F(ClusterToolTest, invalidateLatestSnapshotPrintsTheReferenceLine)
{
    {
        auto log = RecordingLog::open(cluster_dir_, true);
        ASSERT_TRUE(log);
        ASSERT_TRUE(log->append_term(1, 1, 0, 100));
        ASSERT_TRUE(log->append_snapshot(1, 1, 1000, 256, 300, RecordingLog::SERVICE_ID));
    }

    const auto result = run({cluster_dir_, "invalidate-latest-snapshot"});
    EXPECT_EQ(TOOL_SUCCESS, result.status) << result.out;
    EXPECT_EQ(" invalidate latest snapshot: true\n", result.out);
}

}  // namespace
