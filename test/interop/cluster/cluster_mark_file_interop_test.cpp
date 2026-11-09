// Interop criterion for C1b: the reference `ClusterTool pid` reads a
// `cluster-mark.dat` written by our code.

#include <gtest/gtest.h>

#include <unistd.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

#include "cluster/common/cluster_mark_file.h"
#include "fixture/java_process.h"

#ifndef AXC_AERON_JAR
#define AXC_AERON_JAR ""
#endif

namespace
{

using deepmsg::cluster::common::ClusterComponentType;
using deepmsg::cluster::common::ClusterMarkFile;

[[nodiscard]] std::string make_temp_dir()
{
    std::array<char, 64> pattern{};
    std::snprintf(pattern.data(), pattern.size(), "/tmp/axc-cluster-mark-interop-XXXXXX");
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

[[nodiscard]] std::string trimmed(const std::string& value)
{
    const std::size_t begin = value.find_first_not_of(" \t\r\n");
    if (std::string::npos == begin)
    {
        return {};
    }
    const std::size_t end = value.find_last_not_of(" \t\r\n");
    return value.substr(begin, end - begin + 1);
}

class ClusterMarkFileInteropTest : public ::testing::Test
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

    std::string jar_{};
    std::string directory_{};
    axc::interop::JavaProcess java_{};
};

TEST_F(ClusterMarkFileInteropTest, clusterToolPidReadsOurMarkFile)
{
    ClusterMarkFile::Options options;
    options.path = directory_ + "/cluster-mark.dat";
    options.component_type = ClusterComponentType::consensus_module;
    options.error_buffer_length = ClusterMarkFile::ERROR_BUFFER_MIN_LENGTH;
    options.timeout_ms = 1000;
    options.file_page_size = 4096;
    options.now_ms = 123456;

    {
        auto mark_file = ClusterMarkFile::create_or_open(options);
        ASSERT_TRUE(mark_file) << mark_file.error().message.view();
        auto ready = mark_file->signal_ready(123456);
        ASSERT_TRUE(ready) << ready.error().message.view();
    }

    const std::vector<std::string> arguments = {"--add-opens",
                                                "java.base/jdk.internal.misc=ALL-UNNAMED",
                                                "--add-opens",
                                                "java.base/sun.nio.ch=ALL-UNNAMED",
                                                "-cp",
                                                jar_,
                                                "io.aeron.cluster.ClusterTool",
                                                directory_,
                                                "pid"};

    auto result = java_.run(arguments, std::chrono::seconds(60));
    ASSERT_TRUE(result) << result.error().message.view();
    EXPECT_EQ(0, result->exit_code) << result->output;
    EXPECT_EQ(std::to_string(static_cast<std::int64_t>(::getpid())), trimmed(result->output));
}

}  // namespace
