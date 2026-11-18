// `ClusterBackupContextTest` (aeron-cluster `:95-260`) by name where our
// context has the same seam: the directory conclusion from properties and the
// active-mark-file refusal. The reference's link-file and page-alignment cases
// live in the agent's `write_mark_file_link` (exercised by the interop
// directory tests) and in `ClusterMarkFile`; they are not duplicated here.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>

#include "cluster/backup/cluster_backup_context.h"
#include "cluster/common/cluster_mark_file.h"
#include "core/config/property_source.h"

namespace
{

using deepmsg::cluster::backup::cluster_backup_config_from_properties;
using deepmsg::cluster::common::ClusterMarkFile;

class ClusterBackupContextTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::string pattern = "/tmp/axc-c9-context-XXXXXX";
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        root_ = created;
    }

    void TearDown() override
    {
        std::error_code error;
        std::filesystem::remove_all(root_, error);
    }

    [[nodiscard]] std::expected<deepmsg::cluster::backup::ClusterBackupConfig, axc::core::Error> parse()
    {
        return cluster_backup_config_from_properties(properties);
    }

    axc::core::PropertySource properties{};
    std::string root_{};
};

TEST_F(ClusterBackupContextTest, clusterDirectoryNameShouldMatchClusterDirWhenClusterDirSet)
{
    properties.set("aeron.cluster.dir", root_ + "/cluster");
    const auto config = parse();
    ASSERT_TRUE(config) << config.error().message.view();
    EXPECT_EQ(root_ + "/cluster", config->cluster_dir);
    EXPECT_EQ(root_ + "/cluster/cluster-mark.dat", config->mark_file_path());
}

TEST_F(ClusterBackupContextTest, concludeShouldCreateMarkFileDirSetDirectly)
{
    properties.set("aeron.cluster.dir", root_ + "/cluster");
    properties.set("aeron.cluster.mark.file.dir", root_ + "/mark");
    const auto config = parse();
    ASSERT_TRUE(config) << config.error().message.view();
    ASSERT_TRUE(config->mark_file_dir.has_value());
    EXPECT_EQ(root_ + "/mark", *config->mark_file_dir);
    EXPECT_EQ(root_ + "/mark/cluster-mark.dat", config->mark_file_path());
}

TEST_F(ClusterBackupContextTest, throwsIllegalStateExceptionIfThereIsAnActiveMarkFile)
{
    const std::string cluster_dir = root_ + "/cluster";
    std::filesystem::create_directories(cluster_dir);
    const std::string path = cluster_dir + "/cluster-mark.dat";

    ClusterMarkFile::Options options;
    options.path = path;
    options.component_type = deepmsg::cluster::common::ClusterComponentType::backup;
    options.now_ms = 1'000;
    options.timeout_ms = 10'000;
    auto created = ClusterMarkFile::create_or_open(options);
    ASSERT_TRUE(created) << created.error().message.view();

    // A second open inside the timeout sees an active mark file.
    ClusterMarkFile::Options second_options = options;
    second_options.now_ms = 1'100;
    auto refused = ClusterMarkFile::create_or_open(second_options);
    ASSERT_FALSE(refused);
    EXPECT_NE(std::string::npos, std::string{refused.error().message.view()}.find("active mark file"))
        << refused.error().message.view();
}

}  // namespace
