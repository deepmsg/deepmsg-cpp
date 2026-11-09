// Interop criterion for C1a: the reference `ClusterTool` must read a
// `recording.log` written by our code and print exactly the same entries as it
// prints for the reference-written golden file.
//
// `validate-recording-log` is deliberately not here: in the reference it
// toggles a *running* node (`ClusterToolOperator.java:956-967`), so it belongs
// to a node-level card (C4/C7), not to the offline file.

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

#include "cluster/common/recording_log.h"
#include "fixture/java_process.h"

#ifndef AXC_AERON_JAR
#define AXC_AERON_JAR ""
#endif
#ifndef AXC_GOLDEN_DIR
#error "AXC_GOLDEN_DIR must point at test/golden"
#endif

namespace
{

using deepmsg::cluster::common::RecordingLog;

[[nodiscard]] std::string make_temp_dir()
{
    std::array<char, 64> pattern{};
    std::snprintf(pattern.data(), pattern.size(), "/tmp/axc-recording-log-interop-XXXXXX");
    const char* created = ::mkdtemp(pattern.data());
    return nullptr == created ? std::string{} : std::string{created};
}

[[nodiscard]] std::string aeron_jar()
{
    if (const char* from_env = std::getenv("AXC_AERON_JAR"); nullptr != from_env && '\0' != *from_env)
    {
        return from_env;
    }
    return AXC_AERON_JAR;
}

/// The portion of `ClusterTool recording-log` output that is a byte contract:
/// the sorted entries. `cacheIndex={...}` is a Java hash-map dump and is not.
[[nodiscard]] std::string entries_text(const std::string& output)
{
    const std::string marker = "entries=[";
    const std::size_t begin = output.find(marker);
    if (std::string::npos == begin)
    {
        return {};
    }

    const std::string end_marker = "], cacheIndex=";
    const std::size_t end = output.find(end_marker, begin);
    if (std::string::npos == end)
    {
        return {};
    }

    return output.substr(begin + marker.size(), end - (begin + marker.size()));
}

class RecordingLogInteropTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        jar_ = aeron_jar();
        if (jar_.empty() || !std::filesystem::exists(jar_))
        {
            GTEST_SKIP() << "reference jar not found (set AXC_AERON_JAR): " << jar_;
        }
        directory_ = make_temp_dir();
        ASSERT_FALSE(directory_.empty());
    }

    void TearDown() override
    {
        if (!directory_.empty())
        {
            std::error_code error;
            std::filesystem::remove_all(directory_, error);
        }
    }

    /// The same sequence `RecordingLogGoldenGen.generateMixed` writes.
    void write_reference_sequence()
    {
        auto log = RecordingLog::open(directory_, true);
        ASSERT_TRUE(log) << log.error().message.view();

        auto ok = [](const auto& result) { EXPECT_TRUE(result) << result.error().message.view(); };

        ok(log->append_term(100, 1, 0, 1000));
        ok(log->append_snapshot(101, 1, 0, 500, 1100, RecordingLog::SERVICE_ID));
        ok(log->append_snapshot(102, 1, 0, 500, 1100, 0));
        ok(log->append_term(100, 2, 700, 1200));
        ok(log->append_standby_snapshot(103, 2, 700, 900, 1300, RecordingLog::SERVICE_ID, "remotehost.aeron.io:20002"));
        ok(log->append_standby_snapshot(104, 2, 700, 900, 1300, 0, "remotehost.aeron.io:20002"));
        ok(log->append_snapshot(105, 2, 700, 1300, 1400, RecordingLog::SERVICE_ID));
        ok(log->append_snapshot(106, 2, 700, 1300, 1400, 0));

        std::int32_t snapshot_index = -1;
        for (std::int32_t i = 0; i < static_cast<std::int32_t>(log->entries().size()); ++i)
        {
            if (101 == log->entries()[static_cast<std::size_t>(i)].recording_id)
            {
                snapshot_index = i;
                break;
            }
        }
        EXPECT_NE(-1, snapshot_index);
        if (-1 == snapshot_index)
        {
            return;
        }
        ok(log->invalidate_entry(snapshot_index));
        ok(log->commit_log_position(2, 1500));
    }

    [[nodiscard]] std::string run_cluster_tool(const std::string& cluster_dir)
    {
        const std::vector<std::string> arguments = {"--add-opens",
                                                    "java.base/jdk.internal.misc=ALL-UNNAMED",
                                                    "--add-opens",
                                                    "java.base/sun.nio.ch=ALL-UNNAMED",
                                                    "-cp",
                                                    jar_,
                                                    "io.aeron.cluster.ClusterTool",
                                                    cluster_dir,
                                                    "recording-log"};

        auto result = java_.run(arguments, std::chrono::seconds(60));
        EXPECT_TRUE(result) << result.error().message.view();
        EXPECT_EQ(0, result->exit_code) << result->output;
        return result->output;
    }

    std::string jar_{};
    std::string directory_{};
    axc::interop::JavaProcess java_{};
};

TEST_F(RecordingLogInteropTest, clusterToolPrintsTheSameEntriesForOurLogAsForTheReferenceLog)
{
    write_reference_sequence();

    const std::string ours = entries_text(run_cluster_tool(directory_));
    const std::string reference =
        entries_text(run_cluster_tool(std::string(AXC_GOLDEN_DIR) + "/cluster/recording-log/mixed"));

    EXPECT_FALSE(ours.empty());
    EXPECT_EQ(reference, ours);
}

}  // namespace
