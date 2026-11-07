#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include "fixture/c_driver.h"
#include "fixture/java_process.h"
#include "fixture/process.h"

#ifndef AXC_INTEROP_DIR
#error "AXC_INTEROP_DIR must be defined by the build"
#endif

#ifndef AXC_AERONMD_DEFAULT
#error "AXC_AERONMD_DEFAULT must be defined by the build"
#endif

namespace
{

using axc::interop::CDriver;
using axc::interop::JavaProcess;
using axc::interop::ProcessOptions;
using axc::interop::run_process;

class TempDir
{
public:
    TempDir()
    {
        char pattern[] = "/tmp/axc-interop-XXXXXX";
        char* made = ::mkdtemp(pattern);
        if (nullptr != made)
        {
            path_ = made;
        }
    }

    ~TempDir()
    {
        std::error_code error;
        if (!path_.empty())
        {
            std::filesystem::remove_all(path_, error);
        }
    }

    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;

    [[nodiscard]] const std::string& path() const noexcept
    {
        return path_;
    }

private:
    std::string path_;
};

bool wait_for_file(const std::string& path, std::chrono::milliseconds timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (std::filesystem::exists(path))
        {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return std::filesystem::exists(path);
}

}  // namespace

TEST(Process, CapturesOutputAndExitCode)
{
    ProcessOptions options;
    options.arguments = {"-c", "echo out; echo err >&2; exit 7"};
    options.timeout = std::chrono::seconds(5);

    const auto result = run_process("/bin/sh", options);
    ASSERT_TRUE(result.has_value()) << result.error().message.c_str();
    EXPECT_EQ(7, result->exit_code);
    EXPECT_FALSE(result->timed_out);
    EXPECT_NE(std::string::npos, result->output.find("out"));
    EXPECT_NE(std::string::npos, result->output.find("err"));
}

TEST(Process, TimesOutAndKills)
{
    ProcessOptions options;
    options.arguments = {"-c", "sleep 5"};
    options.timeout = std::chrono::milliseconds(100);

    const auto result = run_process("/bin/sh", options);
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(result->timed_out);
}

TEST(Process, AppliesChildEnvironment)
{
    ProcessOptions options;
    options.arguments = {"-c", "printf '%s' \"$AXC_TEST_VALUE\""};
    options.environment = {"AXC_TEST_VALUE=42"};
    options.timeout = std::chrono::seconds(5);

    const auto result = run_process("/bin/sh", options);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(0, result->exit_code);
    EXPECT_EQ("42", result->output);
}

TEST(JavaProcess, RunsVersion)
{
    const JavaProcess java;
    const auto result = java.run({"-version"}, std::chrono::seconds(20));
    ASSERT_TRUE(result.has_value());
    if (127 == result->exit_code)
    {
        GTEST_SKIP() << "java not found on PATH (set AXC_REAL_JAVA)";
    }
    EXPECT_EQ(0, result->exit_code);
    EXPECT_NE(std::string::npos, result->output.find("version"));
}

TEST(Shim, SelfTestPasses)
{
    ProcessOptions options;
    options.arguments = {std::string(AXC_INTEROP_DIR) + "/shim/shim_test.sh"};
    options.timeout = std::chrono::seconds(30);

    const auto result = run_process("/bin/bash", options);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(0, result->exit_code) << result->output;
    EXPECT_NE(std::string::npos, result->output.find("all checks passed"));
}

TEST(CDriver, StartsAndStopsWithoutDeletingTheDirectory)
{
    const std::string aeronmd = AXC_AERONMD_DEFAULT;
    if (!std::filesystem::exists(aeronmd))
    {
        GTEST_SKIP() << "aeronmd not built at " << aeronmd;
    }

    TempDir temp;
    ASSERT_FALSE(temp.path().empty());

    auto driver = CDriver::start(aeronmd, temp.path());
    ASSERT_TRUE(driver.has_value()) << driver.error().message.c_str();
    EXPECT_TRUE(driver->running());

    // The driver creates its CnC file in the shared directory.
    EXPECT_TRUE(wait_for_file(temp.path() + "/cnc.dat", std::chrono::seconds(5)));

    driver->stop();
    EXPECT_FALSE(driver->running());

    // The fixture must not delete a directory it does not own.
    EXPECT_TRUE(std::filesystem::is_directory(temp.path()));
}
