// Golden criterion for card A1b: the mark file written by the reference Java
// implementation has to be read back field by field.
//
// `test/golden/archive/mark-file/archive-mark.dat` was produced by
// `MarkFileGoldenGen` (reference Aeron 1.54.0); `expected.txt` is its own dump of
// the same file. Regenerate both, never edit them.

#include <gtest/gtest.h>

#include <cstdint>
#include <fstream>
#include <map>
#include <string>

#include "archive/server/archive_mark_file.h"

#ifndef AXC_GOLDEN_DIR
#error "AXC_GOLDEN_DIR must point at test/golden"
#endif

namespace
{

using deepmsg::archive::server::ArchiveMarkFile;

[[nodiscard]] std::string golden_dir()
{
    return std::string(AXC_GOLDEN_DIR) + "/archive/mark-file";
}

[[nodiscard]] std::map<std::string, std::string> read_expected()
{
    std::map<std::string, std::string> values;
    std::ifstream input{golden_dir() + "/expected.txt"};
    std::string line;
    while (std::getline(input, line))
    {
        const std::size_t equals = line.find('=');
        if (std::string::npos != equals)
        {
            values[line.substr(0, equals)] = line.substr(equals + 1);
        }
    }
    return values;
}

TEST(MarkFileGoldenTest, readsEveryHeaderFieldOfTheReferenceMarkFile)
{
    const std::string directory = golden_dir();
    const auto expected = read_expected();
    ASSERT_FALSE(expected.empty()) << "missing " << directory << "/expected.txt";

    ArchiveMarkFile::Options options;
    options.directory = directory;
    options.error_buffer_length = 4096;  // the reference run used `errorBufferLength` 4096

    auto mark_file = ArchiveMarkFile::open_existing(options);
    ASSERT_TRUE(mark_file) << mark_file.error().message.view();

    const auto expect = [&](const std::string& key, std::int64_t actual)
    {
        const auto found = expected.find(key);
        ASSERT_NE(expected.end(), found) << "expected.txt has no " << key;
        EXPECT_EQ(found->second, std::to_string(actual)) << key;
    };
    const auto expect_text = [&](const std::string& key, const std::string& actual)
    {
        const auto found = expected.find(key);
        ASSERT_NE(expected.end(), found) << "expected.txt has no " << key;
        EXPECT_EQ(found->second, actual) << key;
    };

    EXPECT_EQ(ArchiveMarkFile::HEADER_OFFSET, mark_file->header_offset());

    expect("version", mark_file->version());
    expect("activityTimestamp", mark_file->activity_timestamp_volatile());
    expect("startTimestamp", mark_file->start_timestamp());
    expect("controlStreamId", mark_file->control_stream_id());
    expect("localControlStreamId", mark_file->local_control_stream_id());
    expect("eventsStreamId", mark_file->events_stream_id());
    expect("headerLength", mark_file->header_length());
    expect("errorBufferLength", mark_file->error_buffer_length());
    expect("archiveId", mark_file->archive_id());
    expect_text("controlChannel", mark_file->control_channel());
    expect_text("localControlChannel", mark_file->local_control_channel());
    expect_text("eventsChannel", mark_file->events_channel());
    expect_text("aeronDirectory", mark_file->aeron_directory());

    expect("fileLength", static_cast<std::int64_t>(mark_file->buffer().size()));
    EXPECT_EQ(static_cast<std::size_t>(4096), mark_file->error_buffer().size());
}

}  // namespace
