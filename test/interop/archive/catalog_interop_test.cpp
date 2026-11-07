// Interop criterion for card A1a: a catalog written by our C++ code has to be
// readable by the reference tool (`ArchiveTool`), which is the oracle the
// archive's own operators use.
//
// `count-entries` and `describe <id>` are the two subcommands that work on a
// bare catalog: `describe-all` needs an `archive-mark.dat` (A1b) and `verify`
// needs segment files (A1b). Recorded as a deviation in records/a1a.md.

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "archive/server/catalog.h"
#include "fixture/java_process.h"

#ifndef AXC_AERON_JAR
#define AXC_AERON_JAR ""
#endif

namespace
{

using deepmsg::archive::server::Catalog;
using deepmsg::archive::server::CatalogOpenOptions;
using deepmsg::archive::server::CatalogOptions;
using io::aeron::archive::codecs::RecordingState;

constexpr std::int32_t TERM_LENGTH = 8192;
constexpr std::int32_t SEGMENT_LENGTH = 2 * TERM_LENGTH;
constexpr std::int32_t MTU_LENGTH = 1024;

[[nodiscard]] std::string make_temp_dir()
{
    std::array<char, 64> pattern{};
    std::snprintf(pattern.data(), pattern.size(), "/tmp/axc-catalog-interop-XXXXXX");
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

/// `-D` and `--add-opens` options the reference needs on a modern JDK
/// (`org.agrona.UnsafeApi` and `org.agrona.checksum.Crc32`).
[[nodiscard]] std::vector<std::string> archive_tool_arguments(const std::string& jar, const std::string& archive_dir)
{
    return {"--add-opens",
            "java.base/jdk.internal.misc=ALL-UNNAMED",
            "--add-opens",
            "java.base/sun.nio.ch=ALL-UNNAMED",
            "--add-opens",
            "java.base/java.util.zip=ALL-UNNAMED",
            "-cp",
            jar,
            "io.aeron.archive.ArchiveTool",
            archive_dir};
}

class CatalogInteropTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        jar_ = aeron_jar();
        if (jar_.empty() || !std::filesystem::exists(jar_))
        {
            GTEST_SKIP() << "reference jar not found (set AXC_AERON_JAR): " << jar_;
        }
        archive_dir_ = make_temp_dir();
        ASSERT_FALSE(archive_dir_.empty());
    }

    void TearDown() override
    {
        if (!archive_dir_.empty())
        {
            std::error_code error;
            std::filesystem::remove_all(archive_dir_, error);
        }
    }

    [[nodiscard]] std::expected<axc::interop::ProcessResult, axc::core::Error> run_archive_tool(
        const std::string& subcommand, const std::vector<std::string>& extra = {})
    {
        std::vector<std::string> arguments = archive_tool_arguments(jar_, archive_dir_);
        arguments.push_back(subcommand);
        for (const std::string& argument : extra)
        {
            arguments.push_back(argument);
        }

        axc::interop::ProcessOptions options;
        options.arguments = std::move(arguments);
        options.timeout = std::chrono::seconds(60);
        return java_.run(options.arguments, options.timeout);
    }

    [[nodiscard]] std::expected<Catalog, axc::core::Error> create_catalog(
        std::int64_t capacity = Catalog::DEFAULT_CAPACITY)
    {
        CatalogOptions options;
        options.archive_dir = archive_dir_;
        options.catalog_capacity = capacity;
        return Catalog::create(options);
    }

    [[nodiscard]] std::expected<Catalog, axc::core::Error> open_catalog()
    {
        CatalogOpenOptions options;
        options.archive_dir = archive_dir_;
        options.catalog_capacity = Catalog::MIN_CAPACITY;
        return Catalog::open(options);
    }

    struct Recording
    {
        std::int64_t id{0};
        std::string stripped_channel;
        std::string original_channel;
        std::string source_identity;
        std::int32_t session_id{0};
        std::int32_t stream_id{0};
        std::int64_t start_position{0};
        std::int64_t stop_position{Catalog::NULL_POSITION};
        std::int64_t start_timestamp{0};
        std::int64_t stop_timestamp{Catalog::NULL_TIMESTAMP};
        std::int32_t initial_term_id{0};
        std::int32_t segment_file_length{SEGMENT_LENGTH};
        std::int32_t term_buffer_length{TERM_LENGTH};
        std::int32_t mtu_length{MTU_LENGTH};
    };

    std::string jar_;
    std::string archive_dir_;
    axc::interop::JavaProcess java_{};
};

TEST_F(CatalogInteropTest, archiveToolReadsEveryFieldOfOurCatalog)
{
    const Recording recordings[] = {
        {0, "channelG", "channelG?tag=f", "sourceA", 6, 1},
        {0, "channelH", "channelH?tag=f", "sourceV", 7, 2},
        {0, "channelThatIsVeryLongAndShouldNotBeTruncated", "channelThatIsVeryLongAndShouldNotBeTruncated?tag=f",
         "source can also be a very very very long String and it will not be truncated even if gets very very long", 8,
         3},
    };

    std::vector<Recording> written;
    {
        auto catalog = create_catalog(1024);
        ASSERT_TRUE(catalog) << catalog.error().message.view();

        for (const Recording& recording : recordings)
        {
            auto id = catalog->add_new_recording(
                recording.start_position, recording.start_timestamp, recording.initial_term_id,
                recording.segment_file_length, recording.term_buffer_length, recording.mtu_length, recording.session_id,
                recording.stream_id, recording.stripped_channel, recording.original_channel, recording.source_identity);
            ASSERT_TRUE(id) << id.error().message.view();
            Recording created = recording;
            created.id = *id;
            written.push_back(created);
        }

        // One completed recording and one invalidated one, so the tool sees both
        // descriptor states.
        ASSERT_TRUE(catalog->recording_stopped(written[0].id, 140, 231723682323LL));
        written[0].stop_position = 140;
        written[0].stop_timestamp = 231723682323LL;

        auto changed = catalog->change_state(written[1].id, RecordingState::INVALID);
        ASSERT_TRUE(changed) << changed.error().message.view();
        ASSERT_TRUE(*changed);

        auto closed = catalog->close();
        ASSERT_TRUE(closed) << closed.error().message.view();
    }

    auto count = run_archive_tool("count-entries");
    ASSERT_TRUE(count) << count.error().message.view();
    EXPECT_EQ(0, count->exit_code) << count->output;
    EXPECT_EQ("2", std::string{count->output}.substr(0, count->output.find('\n')))
        << "one of the three recordings is INVALID";

    for (const Recording& recording : written)
    {
        if (recording.id == written[1].id)
        {
            continue;  // invalidated: not listed by `describe`
        }

        auto described = run_archive_tool("describe", {std::to_string(recording.id)});
        ASSERT_TRUE(described) << described.error().message.view();
        ASSERT_EQ(0, described->exit_code) << described->output;

        const std::string& output = described->output;
        const auto expect = [&](const std::string& key, const std::string& value)
        { EXPECT_NE(std::string::npos, output.find(key + "=" + value)) << key << " in: " << output; };
        expect("recordingId", std::to_string(recording.id));
        expect("startPosition", std::to_string(recording.start_position));
        expect("stopPosition", std::to_string(recording.stop_position));
        expect("startTimestamp", std::to_string(recording.start_timestamp));
        expect("stopTimestamp", std::to_string(recording.stop_timestamp));
        expect("initialTermId", std::to_string(recording.initial_term_id));
        expect("segmentFileLength", std::to_string(recording.segment_file_length));
        expect("termBufferLength", std::to_string(recording.term_buffer_length));
        expect("mtuLength", std::to_string(recording.mtu_length));
        expect("sessionId", std::to_string(recording.session_id));
        expect("streamId", std::to_string(recording.stream_id));
        expect("strippedChannel", "'" + recording.stripped_channel + "'");
        expect("originalChannel", "'" + recording.original_channel + "'");
        expect("sourceIdentity", "'" + recording.source_identity + "'");
    }
}

TEST_F(CatalogInteropTest, archiveToolReadsACatalogThatGrewWhileBeingWritten)
{
    // `MIN_CAPACITY` is the bare header, so every recording forces a growth.
    {
        auto catalog = create_catalog(Catalog::MIN_CAPACITY);
        ASSERT_TRUE(catalog) << catalog.error().message.view();
        for (int index = 0; index < 8; index++)
        {
            auto id = catalog->add_new_recording(0, 0, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 6, 1, "channelG",
                                                 "channelG?tag=f", "sourceA");
            ASSERT_TRUE(id) << id.error().message.view();
        }
        ASSERT_GT(catalog->capacity(), Catalog::MIN_CAPACITY);
        auto closed = catalog->close();
        ASSERT_TRUE(closed) << closed.error().message.view();
    }

    auto catalog = open_catalog();
    ASSERT_TRUE(catalog) << catalog.error().message.view();
    EXPECT_EQ(8, catalog->entry_count());

    auto count = run_archive_tool("count-entries");
    ASSERT_TRUE(count) << count.error().message.view();
    EXPECT_EQ(0, count->exit_code) << count->output;
    EXPECT_EQ("8", std::string{count->output}.substr(0, count->output.find('\n')));

    auto described = run_archive_tool("describe", {std::to_string(catalog->next_recording_id() - 1)});
    ASSERT_TRUE(described) << described.error().message.view();
    EXPECT_EQ(0, described->exit_code) << described->output;
    EXPECT_NE(std::string::npos,
              described->output.find("recordingId=" + std::to_string(catalog->next_recording_id() - 1)))
        << described->output;
}

}  // namespace
