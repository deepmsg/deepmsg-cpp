// A2b-2 criterion: the SHARED binary's `Archive.Context` names
// (`Archive.java:1140-1400`, 1.54.0 = 454da1e4e6) and its archive-directory
// preparation.

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>

#include "archive/server/archive_launch.h"

namespace
{

using axc::core::PropertySource;
using deepmsg::archive::server::parse_launch_config;
using deepmsg::archive::server::prepare_archive_directories;

TEST(ArchiveLaunchTest, archiveDirectoryIsRequired)
{
    PropertySource source;
    const auto config = parse_launch_config(source);
    ASSERT_FALSE(config.has_value());
    EXPECT_EQ("Archive.Context.archiveDir must be set", config.error().message.view());
}

TEST(ArchiveLaunchTest, defaultsMatchTheReference)
{
    PropertySource source;
    source.set("aeron.archive.dir", "/tmp/archive");

    const auto config = parse_launch_config(source);
    ASSERT_TRUE(config.has_value()) << config.error().message.view();

    EXPECT_EQ("/tmp/archive", config->archive_dir);
    EXPECT_EQ("/tmp/archive", config->mark_file_dir);
    EXPECT_EQ("aeron:udp?endpoint=localhost:8010", config->control_channel);
    EXPECT_EQ(10, config->control_stream_id);
    EXPECT_EQ("aeron:ipc?term-length=64k", config->local_control_channel);
    EXPECT_EQ(10, config->local_control_stream_id);
    EXPECT_EQ(-1, config->archive_id);
    EXPECT_FALSE(config->delete_archive_on_start);
    EXPECT_EQ(5000, config->connect_timeout_ms);
    EXPECT_EQ(1000, config->session_liveness_check_interval_ms);
    EXPECT_EQ(64 * 1024, config->control_term_buffer_length);
    EXPECT_EQ(1408, config->control_mtu_length);
    EXPECT_TRUE(config->control_term_buffer_sparse);
    EXPECT_EQ(1024 * 1024, config->error_buffer_length);
    EXPECT_EQ(1024 * 1024, config->catalog_capacity);
    EXPECT_EQ(0, config->catalog_file_sync_level);
    EXPECT_TRUE(config->authenticator_supplier.empty());
}

TEST(ArchiveLaunchTest, configuredValuesAreRead)
{
    PropertySource source;
    source.set("aeron.dir", "/tmp/aeron");
    source.set("aeron.archive.dir", "/tmp/archive");
    source.set("aeron.archive.mark.file.dir", "/tmp/mark");
    source.set("aeron.archive.control.channel", "aeron:ipc");
    source.set("aeron.archive.control.stream.id", "20");
    source.set("aeron.archive.local.control.stream.id", "21");
    source.set("aeron.archive.id", "42");
    source.set("aeron.archive.dir.delete.on.start", "true");
    source.set("aeron.archive.connect.timeout", "5s");
    source.set("aeron.archive.session.liveness.check.interval", "1500ms");
    source.set("aeron.archive.control.term.buffer.length", "64k");
    source.set("aeron.archive.control.mtu.length", "1024");
    source.set("aeron.archive.control.term.buffer.sparse", "false");
    source.set("aeron.archive.error.buffer.length", "2m");
    source.set("aeron.archive.catalog.capacity", "4m");
    source.set("aeron.archive.file.sync.level", "2");
    source.set("aeron.archive.catalog.file.sync.level", "2");
    source.set("aeron.archive.authenticator.supplier", "io.aeron.samples.archive.SampleAuthenticatorSupplier");

    const auto config = parse_launch_config(source);
    ASSERT_TRUE(config.has_value()) << config.error().message.view();

    EXPECT_EQ("/tmp/aeron", config->aeron_dir);
    EXPECT_EQ("/tmp/mark", config->mark_file_dir);
    EXPECT_EQ("aeron:ipc", config->control_channel);
    EXPECT_EQ(20, config->control_stream_id);
    EXPECT_EQ(21, config->local_control_stream_id);
    EXPECT_EQ(42, config->archive_id);
    EXPECT_TRUE(config->delete_archive_on_start);
    EXPECT_EQ(5000, config->connect_timeout_ms);
    EXPECT_EQ(1500, config->session_liveness_check_interval_ms);
    EXPECT_EQ(65536, config->control_term_buffer_length);
    EXPECT_EQ(1024, config->control_mtu_length);
    EXPECT_FALSE(config->control_term_buffer_sparse);
    EXPECT_EQ(2 * 1024 * 1024, config->error_buffer_length);
    EXPECT_EQ(4 * 1024 * 1024, config->catalog_capacity);
    EXPECT_EQ(2, config->recording.file_sync_level);
    EXPECT_EQ(2, config->catalog_file_sync_level);
    EXPECT_EQ("io.aeron.samples.archive.SampleAuthenticatorSupplier", config->authenticator_supplier);
}

TEST(ArchiveLaunchTest, catalogSyncLevelMustNotBeBelowTheFileSyncLevel)
{
    PropertySource source;
    source.set("aeron.archive.dir", "/tmp/archive");
    source.set("aeron.archive.file.sync.level", "2");

    const auto config = parse_launch_config(source);
    ASSERT_FALSE(config.has_value());
    EXPECT_EQ("catalogFileSyncLevel 0 < fileSyncLevel 2", config.error().message.view());
}

TEST(ArchiveLaunchTest, anUnknownAuthenticatorSupplierIsRefusedLoudly)
{
    PropertySource source;
    source.set("aeron.archive.dir", "/tmp/archive");
    source.set("aeron.archive.authenticator.supplier", "com.example.MyAuthenticatorSupplier");

    const auto config = parse_launch_config(source);
    ASSERT_FALSE(config.has_value());
    EXPECT_EQ("unsupported aeron.archive.authenticator.supplier: com.example.MyAuthenticatorSupplier",
              config.error().message.view());
}

TEST(ArchiveLaunchTest, prepareDeletesOnStartAndCreatesBothDirectories)
{
    std::error_code error;
    const auto root = std::filesystem::temp_directory_path(error) / "axc-launch-test";
    ASSERT_FALSE(error);
    std::filesystem::remove_all(root, error);

    const auto archive_dir = root / "archive";
    const auto mark_dir = root / "mark";
    std::filesystem::create_directories(archive_dir, error);
    ASSERT_FALSE(error);
    {
        std::ofstream file(archive_dir / "stale.bin");
        file << "old";
    }

    PropertySource source;
    source.set("aeron.archive.dir", archive_dir.string());
    source.set("aeron.archive.mark.file.dir", mark_dir.string());
    source.set("aeron.archive.dir.delete.on.start", "true");

    const auto config = parse_launch_config(source);
    ASSERT_TRUE(config.has_value()) << config.error().message.view();

    auto prepared = prepare_archive_directories(*config);
    ASSERT_TRUE(prepared.has_value()) << prepared.error().message.view();

    EXPECT_TRUE(std::filesystem::is_directory(archive_dir));
    EXPECT_TRUE(std::filesystem::is_directory(mark_dir));
    EXPECT_FALSE(std::filesystem::exists(archive_dir / "stale.bin"));

    std::filesystem::remove_all(root, error);
}

}  // namespace
