// Golden criterion for C1a: `recording.log` written by the reference Java
// implementation is read back field by field.
//
// `test/golden/cluster/recording-log/<case>/recording.log` was produced by
// `RecordingLogGoldenGen` (reference Aeron 1.54.0); `expected.txt` is its own
// dump of the same file. Regenerate both, never edit them.

#include <gtest/gtest.h>

#include <cstdint>
#include <fstream>
#include <map>
#include <string>

#include "cluster/common/recording_log.h"

#ifndef AXC_GOLDEN_DIR
#error "AXC_GOLDEN_DIR must point at test/golden"
#endif

namespace
{

using deepmsg::cluster::common::RecordingLog;

[[nodiscard]] std::string golden_dir()
{
    return std::string(AXC_GOLDEN_DIR) + "/cluster/recording-log";
}

[[nodiscard]] std::map<std::string, std::string> read_expected(const std::string& directory)
{
    std::map<std::string, std::string> values;
    std::ifstream input{directory + "/expected.txt"};
    std::string line;
    while (std::getline(input, line))
    {
        const std::size_t equals = line.find('=');
        if (std::string::npos == equals)
        {
            continue;
        }
        values[line.substr(0, equals)] = line.substr(equals + 1);
    }
    return values;
}

[[nodiscard]] std::string value_of(const std::map<std::string, std::string>& values, const std::string& key)
{
    const auto found = values.find(key);
    return values.end() == found ? std::string{"<missing>"} : found->second;
}

void check_case(const std::string& name)
{
    const std::string directory = golden_dir() + "/" + name;
    const auto expected = read_expected(directory);
    ASSERT_FALSE(expected.empty()) << "missing " << directory << "/expected.txt";

    auto log = RecordingLog::open(directory, false);
    ASSERT_TRUE(log) << log.error().message.view();

    const int entry_count = std::stoi(value_of(expected, "entryCount"));
    ASSERT_EQ(static_cast<std::size_t>(entry_count), log->entries().size());
    EXPECT_EQ(std::stoi(value_of(expected, "nextEntryIndex")), log->next_entry_index());

    for (int i = 0; i < entry_count; ++i)
    {
        const std::string prefix = "entry." + std::to_string(i) + ".";
        const RecordingLog::Entry& entry = log->entries()[static_cast<std::size_t>(i)];

        EXPECT_EQ(std::stoll(value_of(expected, prefix + "recordingId")), entry.recording_id) << prefix;
        EXPECT_EQ(std::stoll(value_of(expected, prefix + "leadershipTermId")), entry.leadership_term_id) << prefix;
        EXPECT_EQ(std::stoll(value_of(expected, prefix + "termBaseLogPosition")), entry.term_base_log_position)
            << prefix;
        EXPECT_EQ(std::stoll(value_of(expected, prefix + "logPosition")), entry.log_position) << prefix;
        EXPECT_EQ(std::stoll(value_of(expected, prefix + "timestamp")), entry.timestamp) << prefix;
        EXPECT_EQ(std::stoi(value_of(expected, prefix + "serviceId")), entry.service_id) << prefix;
        EXPECT_EQ(std::stoi(value_of(expected, prefix + "type")), entry.type) << prefix;
        EXPECT_EQ(value_of(expected, prefix + "isValid") == "true", entry.is_valid) << prefix;
        EXPECT_EQ(std::stoi(value_of(expected, prefix + "entryIndex")), entry.entry_index) << prefix;
        EXPECT_EQ(value_of(expected, prefix + "archiveEndpoint"), entry.archive_endpoint) << prefix;
    }
}

TEST(RecordingLogGoldenTest, readsEveryFieldOfTheReferenceMixedLog)
{
    check_case("mixed");
}

TEST(RecordingLogGoldenTest, readsEveryFieldOfTheReferenceBackfilledLog)
{
    check_case("backfill");
}

}  // namespace
