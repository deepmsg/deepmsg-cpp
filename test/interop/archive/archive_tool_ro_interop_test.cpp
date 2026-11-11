// T1: the read-only `ArchiveTool` subset against the reference tool itself.
//
// Every case runs the reference `io.aeron.archive.ArchiveTool` and
// `archive-tool-ro` on the same archive directory and compares the output
// (after normalising the "now" line, the tool's own timestamps and any
// directory that differs between the two copies) and the exit code. The
// mutating commands (`checksum`, `verify`) run on separate copies and also
// compare the bytes they leave behind.
//
// The fixture is a hand-built archive: a catalog with one recording, one
// 128-byte segment with two DATA frames, a mark file and a synthetic CnC file
// with an empty error log. That is enough for all eight commands and keeps the
// test independent of a running driver.

#include <gtest/gtest.h>

#include <unistd.h>
#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <regex>
#include <string>
#include <vector>

#include "archive/server/archive_mark_file.h"
#include "archive/server/catalog.h"
#include "archive/server/checksum.h"
#include "fixture/java_process.h"
#include "fixture/process.h"

namespace
{

using axc::interop::ProcessOptions;
using axc::interop::ProcessResult;
using axc::interop::run_process;
using deepmsg::archive::server::ArchiveMarkFile;
using deepmsg::archive::server::Catalog;
using deepmsg::archive::server::CatalogOptions;

constexpr std::int32_t TERM_LENGTH = 64 * 1024;
constexpr std::int32_t SEGMENT_LENGTH = 64 * 1024;
constexpr std::int32_t MTU_LENGTH = 1408;
constexpr std::int32_t STREAM_ID = 7;
constexpr std::int32_t SESSION_ID = 42;
constexpr std::int32_t INITIAL_TERM_ID = 0;
constexpr std::int64_t STOP_POSITION = 128;  // two 64-byte frames
constexpr std::int64_t FRAME_LENGTH = 64;
constexpr std::int32_t DATA_HEADER_LENGTH = 32;
constexpr std::int32_t CNC_VERSION = (0 << 16) | (2 << 8);  // `SemanticVersion.compose(0, 2, 0)`

constexpr std::size_t TO_DRIVER = 256;
constexpr std::size_t TO_CLIENTS = 256;
constexpr std::size_t COUNTERS_METADATA = 512;
constexpr std::size_t COUNTERS_VALUES = 128;
constexpr std::size_t ERROR_LOG = 256;

std::string aeron_jar()
{
#ifdef AXC_AERON_JAR
    if (const char* from_env = std::getenv("AXC_AERON_JAR"); nullptr != from_env && '\0' != *from_env)
    {
        return from_env;
    }
    return AXC_AERON_JAR;
#else
    return {};
#endif
}

std::string tool_binary()
{
#ifdef AXC_ARCHIVE_TOOL_RO_BIN
    return AXC_ARCHIVE_TOOL_RO_BIN;
#else
    return {};
#endif
}

void put_i32(std::vector<std::uint8_t>& bytes, std::size_t offset, std::int32_t value)
{
    for (int i = 0; i < 4; ++i)
    {
        bytes[offset + i] = static_cast<std::uint8_t>((value >> (8 * i)) & 0xFF);
    }
}

std::string escape_regex(const std::string& value)
{
    std::string escaped;
    for (const char character : value)
    {
        if (std::string_view{"\\.^$|()[]{}*+?"}.find(character) != std::string_view::npos)
        {
            escaped += '\\';
        }
        escaped += character;
    }
    return escaped;
}

std::string normalize(std::string text, const std::string& directory)
{
    text = std::regex_replace(text, std::regex(R"(\d{2}:\d{2}:\d{2} \(start:)"), "<NOW> (start:");
    if (!directory.empty())
    {
        text = std::regex_replace(text, std::regex(escape_regex(directory)), "<DIR>");
    }
    return text;
}

std::string read_file(const std::filesystem::path& path)
{
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

class ArchiveToolRoInteropTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        jar_ = aeron_jar();
        binary_ = tool_binary();
        if (jar_.empty() || !std::filesystem::exists(jar_))
        {
            GTEST_SKIP() << "reference jar not found (set AXC_AERON_JAR): " << jar_;
        }
        if (binary_.empty() || !std::filesystem::exists(binary_))
        {
            GTEST_SKIP() << "archive-tool-ro not built: " << binary_;
        }

        root_ = std::filesystem::temp_directory_path() /
                ("axc-t1-" + std::to_string(::getpid()) + "-" + std::to_string(reinterpret_cast<std::uintptr_t>(this)));
        std::error_code error;
        std::filesystem::remove_all(root_, error);
        std::filesystem::create_directories(root_);
        archive_dir_ = root_ / "archive";
        aeron_dir_ = root_ / "aeron";
        std::filesystem::create_directories(archive_dir_);
        std::filesystem::create_directories(aeron_dir_);
        build_archive();
    }

    void TearDown() override
    {
        std::error_code error;
        std::filesystem::remove_all(root_, error);
    }

    void build_archive()
    {
        CatalogOptions options;
        options.archive_dir = archive_dir_.string();
        options.catalog_capacity = 1024;
        auto catalog = Catalog::create(options);
        ASSERT_TRUE(catalog) << catalog.error().message.view();
        auto id =
            catalog->add_new_recording(0, STOP_POSITION, 1000, 2000, INITIAL_TERM_ID, SEGMENT_LENGTH, TERM_LENGTH,
                                       MTU_LENGTH, SESSION_ID, STREAM_ID, "aeron:ipc", "aeron:ipc?tag=o", "sourceA");
        ASSERT_TRUE(id) << id.error().message.view();
        ASSERT_TRUE(catalog->close());

        std::vector<std::uint8_t> segment(SEGMENT_LENGTH, 0);
        for (int frame = 0; frame < 2; ++frame)
        {
            const std::size_t offset = static_cast<std::size_t>(frame) * FRAME_LENGTH;
            put_i32(segment, offset + 0, FRAME_LENGTH);
            segment[offset + 5] = 0xC0;                                       // UNFRAGMENTED
            segment[offset + 6] = 1;                                          // DATA
            put_i32(segment, offset + 8, static_cast<std::int32_t>(offset));  // term offset
            put_i32(segment, offset + 12, SESSION_ID);
            put_i32(segment, offset + 16, STREAM_ID);
            put_i32(segment, offset + 20, INITIAL_TERM_ID);
            for (std::int32_t i = DATA_HEADER_LENGTH; i < FRAME_LENGTH; ++i)
            {
                segment[offset + static_cast<std::size_t>(i)] =
                    static_cast<std::uint8_t>('a' + (frame * 16) + (i % 16));
            }
        }
        std::ofstream segment_file(archive_dir_ / "0-0.rec", std::ios::binary | std::ios::trunc);
        segment_file.write(reinterpret_cast<const char*>(segment.data()), static_cast<std::streamsize>(segment.size()));
        segment_file.close();

        ArchiveMarkFile::Options mark_options;
        mark_options.directory = archive_dir_.string();
        mark_options.total_file_length = ArchiveMarkFile::HEADER_LENGTH + 4096;
        mark_options.error_buffer_length = 4096;
        ArchiveMarkFile::HeaderValues values;
        values.start_timestamp = 1000;
        values.control_stream_id = 10;
        values.local_control_stream_id = 11;
        values.events_stream_id = 12;
        values.archive_id = 7;
        values.control_channel = "aeron:udp?endpoint=localhost:8010";
        values.local_control_channel = "aeron:ipc";
        values.events_channel = "aeron:ipc";
        values.aeron_directory = aeron_dir_.string();
        auto mark_file = ArchiveMarkFile::create_or_open(mark_options, values);
        ASSERT_TRUE(mark_file) << mark_file.error().message.view();
        mark_file->signal_ready(1000);
        ASSERT_TRUE(mark_file->force());
        mark_file->close();

        std::vector<std::uint8_t> cnc(128 + TO_DRIVER + TO_CLIENTS + COUNTERS_METADATA + COUNTERS_VALUES + ERROR_LOG,
                                      0);
        put_i32(cnc, 0, CNC_VERSION);
        put_i32(cnc, 4, static_cast<std::int32_t>(TO_DRIVER));
        put_i32(cnc, 8, static_cast<std::int32_t>(TO_CLIENTS));
        put_i32(cnc, 12, static_cast<std::int32_t>(COUNTERS_METADATA));
        put_i32(cnc, 16, static_cast<std::int32_t>(COUNTERS_VALUES));
        put_i32(cnc, 20, static_cast<std::int32_t>(ERROR_LOG));
        std::ofstream cnc_file(aeron_dir_ / "cnc.dat", std::ios::binary | std::ios::trunc);
        cnc_file.write(reinterpret_cast<const char*>(cnc.data()), static_cast<std::streamsize>(cnc.size()));
    }

    std::expected<ProcessResult, axc::core::Error> run(bool ours, const std::filesystem::path& directory,
                                                       const std::vector<std::string>& arguments,
                                                       const std::string& input = {}) const
    {
        std::vector<std::string> argv;
        std::string executable;
        if (ours)
        {
            executable = binary_;
        }
        else
        {
            executable = axc::interop::JavaProcess::default_executable();
            argv = {"--add-opens",
                    "java.base/jdk.internal.misc=ALL-UNNAMED",
                    "--add-opens",
                    "java.base/sun.nio.ch=ALL-UNNAMED",
                    "--add-opens",
                    "java.base/java.util.zip=ALL-UNNAMED",
                    "-cp",
                    jar_,
                    "io.aeron.archive.ArchiveTool"};
        }
        argv.push_back(directory.string());
        argv.insert(argv.end(), arguments.begin(), arguments.end());

        ProcessOptions options;
        options.arguments = argv;
        options.timeout = std::chrono::seconds(60);
        options.input = input;
        return run_process(executable, options);
    }

    void expect_match(const std::filesystem::path& directory, const std::vector<std::string>& arguments,
                      const std::string& input = {})
    {
        auto java = run(false, directory, arguments, input);
        ASSERT_TRUE(java) << java.error().message.view();
        auto ours = run(true, directory, arguments, input);
        ASSERT_TRUE(ours) << ours.error().message.view();

        EXPECT_EQ(java->exit_code, ours->exit_code) << "java:\n" << java->output << "\nours:\n" << ours->output;
        EXPECT_EQ(normalize(java->output, directory.string()), normalize(ours->output, directory.string()))
            << "java:\n"
            << java->output << "\nours:\n"
            << ours->output;
    }

    std::filesystem::path copy_archive(const std::string& name)
    {
        const std::filesystem::path directory = root_ / name;
        std::filesystem::create_directories(directory);
        std::filesystem::copy_file(archive_dir_ / "archive.catalog", directory / "archive.catalog");
        std::filesystem::copy_file(archive_dir_ / "0-0.rec", directory / "0-0.rec");
        return directory;
    }

    void patch_byte(const std::filesystem::path& path, std::streamoff offset, char value)
    {
        std::fstream file(path, std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(file);
        file.seekp(offset);
        file.put(value);
    }

    std::string jar_;
    std::string binary_;
    std::filesystem::path root_;
    std::filesystem::path archive_dir_;
    std::filesystem::path aeron_dir_;
};

TEST_F(ArchiveToolRoInteropTest, countEntriesMatches)
{
    expect_match(archive_dir_, {"count-entries"});
}

TEST_F(ArchiveToolRoInteropTest, maxEntriesMatches)
{
    expect_match(archive_dir_, {"max-entries"});
    expect_match(archive_dir_, {"max-entries", "16"});
}

TEST_F(ArchiveToolRoInteropTest, pidMatches)
{
    expect_match(archive_dir_, {"pid"});

    const std::filesystem::path link_dir = root_ / "linked";
    std::filesystem::create_directories(link_dir);
    std::ofstream link(link_dir / "archive-mark.lnk", std::ios::binary | std::ios::trunc);
    link << archive_dir_.string();
    link.close();
    expect_match(link_dir, {"pid"});
}

TEST_F(ArchiveToolRoInteropTest, describeMatches)
{
    expect_match(archive_dir_, {"describe"});
    expect_match(archive_dir_, {"describe", "0"});
    expect_match(archive_dir_, {"describe-all"});
}

TEST_F(ArchiveToolRoInteropTest, dumpMatches)
{
    expect_match(archive_dir_, {"dump", "1"}, "n\n");
}

TEST_F(ArchiveToolRoInteropTest, errorsMatches)
{
    expect_match(archive_dir_, {"errors"});
}

TEST_F(ArchiveToolRoInteropTest, checksumLeavesTheSameBytes)
{
    const std::filesystem::path java_dir = copy_archive("checksum-java");
    const std::filesystem::path ours_dir = copy_archive("checksum-ours");

    auto java = run(false, java_dir, {"checksum", "CRC-32"});
    ASSERT_TRUE(java) << java.error().message.view();
    auto ours = run(true, ours_dir, {"checksum", "CRC-32"});
    ASSERT_TRUE(ours) << ours.error().message.view();

    EXPECT_EQ(java->exit_code, ours->exit_code);
    EXPECT_EQ(normalize(java->output, java_dir.string()), normalize(ours->output, ours_dir.string()));
    EXPECT_EQ(read_file(java_dir / "archive.catalog"), read_file(ours_dir / "archive.catalog"));
    EXPECT_EQ(read_file(java_dir / "0-0.rec"), read_file(ours_dir / "0-0.rec"));
}

TEST_F(ArchiveToolRoInteropTest, verifyRejectsACorruptedFrame)
{
    const std::filesystem::path java_dir = copy_archive("corrupt-java");
    const std::filesystem::path ours_dir = copy_archive("corrupt-ours");
    // The first frame's term offset (header offset 8): both tools must see it.
    patch_byte(java_dir / "0-0.rec", 8, '\x11');
    patch_byte(ours_dir / "0-0.rec", 8, '\x11');

    auto java = run(false, java_dir, {"verify", "0"});
    ASSERT_TRUE(java) << java.error().message.view();
    auto ours = run(true, ours_dir, {"verify", "0"});
    ASSERT_TRUE(ours) << ours.error().message.view();

    EXPECT_EQ(java->exit_code, ours->exit_code) << java->output << "\n" << ours->output;
    EXPECT_EQ(normalize(java->output, java_dir.string()), normalize(ours->output, ours_dir.string()));
    EXPECT_EQ(read_file(java_dir / "archive.catalog"), read_file(ours_dir / "archive.catalog"));
}

TEST_F(ArchiveToolRoInteropTest, verifyMatches)
{
    const std::filesystem::path java_dir = copy_archive("verify-java");
    const std::filesystem::path ours_dir = copy_archive("verify-ours");

    auto java = run(false, java_dir, {"verify", "-a"});
    ASSERT_TRUE(java) << java.error().message.view();
    auto ours = run(true, ours_dir, {"verify", "-a"});
    ASSERT_TRUE(ours) << ours.error().message.view();

    EXPECT_EQ(java->exit_code, ours->exit_code) << java->output << "\n" << ours->output;
    EXPECT_EQ(normalize(java->output, java_dir.string()), normalize(ours->output, ours_dir.string()));
    EXPECT_EQ(read_file(java_dir / "archive.catalog"), read_file(ours_dir / "archive.catalog"));
}

}  // namespace
