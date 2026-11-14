// C4e: `RecordingLogValidatorTest` (`RecordingLogValidatorTest.java:48`):
// a SNAPSHOT entry whose recording the archive does not know is invalidated;
// a live recording and a STANDBY_SNAPSHOT entry are left alone.

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include <unistd.h>

#include "cluster/common/recording_log.h"
#include "cluster/consensus/recording_log_validator.h"

namespace
{

using deepmsg::cluster::common::RecordingLog;
using deepmsg::cluster::consensus::RecordingLogValidator;

constexpr std::int64_t TERM_RECORDING_ID = 100;
constexpr std::int64_t KNOWN_RECORDING_ID = 101;
constexpr std::int64_t UNKNOWN_RECORDING_ID = 202;
constexpr std::int64_t STANDBY_RECORDING_ID = 303;
constexpr std::int32_t SERVICE_ID = 0;

class RecordingLogValidatorTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::string pattern = "/tmp/axc-recording-log-validator-XXXXXX";
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        directory_ = created;

        auto opened = RecordingLog::open(directory_, true);
        ASSERT_TRUE(opened) << opened.error().message.view();
        log_ = std::move(*opened);

        auto term = log_.append_term(TERM_RECORDING_ID, 1, 0, 1'000);
        ASSERT_TRUE(term) << term.error().message.view();
        auto known = log_.append_snapshot(KNOWN_RECORDING_ID, 1, 0, 0, 2'000, SERVICE_ID);
        ASSERT_TRUE(known) << known.error().message.view();
        auto unknown = log_.append_snapshot(UNKNOWN_RECORDING_ID, 1, 0, 0, 3'000, SERVICE_ID);
        ASSERT_TRUE(unknown) << unknown.error().message.view();
        auto standby = log_.append_standby_snapshot(STANDBY_RECORDING_ID, 1, 0, 0, 4'000, SERVICE_ID, "localhost:1234");
        ASSERT_TRUE(standby) << standby.error().message.view();
    }

    void TearDown() override
    {
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
    }

    [[nodiscard]] const RecordingLog::Entry* entry_for(std::int64_t recording_id)
    {
        for (const RecordingLog::Entry& entry : log_.entries())
        {
            if (entry.recording_id == recording_id)
            {
                return &entry;
            }
        }
        return nullptr;
    }

    std::string directory_{};
    RecordingLog log_{};
};

TEST_F(RecordingLogValidatorTest, shouldInvalidateSnapshotsWithUnknownRecordings)
{
    RecordingLogValidator validator(
        0, log_,
        [](std::int64_t recording_id, auto&) -> RecordingLogValidator::Lookup
        {
            return UNKNOWN_RECORDING_ID == recording_id ? RecordingLogValidator::Lookup::unknown
                                                        : RecordingLogValidator::Lookup::known;
        },
        [](const auto&) {});

    int polls = 0;
    while (!validator.is_complete() && polls < 10)
    {
        validator.poll();
        polls++;
    }
    EXPECT_TRUE(validator.is_complete());

    const RecordingLog::Entry* known = entry_for(KNOWN_RECORDING_ID);
    const RecordingLog::Entry* unknown = entry_for(UNKNOWN_RECORDING_ID);
    const RecordingLog::Entry* standby = entry_for(STANDBY_RECORDING_ID);
    ASSERT_NE(nullptr, known);
    ASSERT_NE(nullptr, unknown);
    ASSERT_NE(nullptr, standby);
    EXPECT_TRUE(known->is_valid);
    EXPECT_FALSE(unknown->is_valid);
    EXPECT_TRUE(standby->is_valid);
}

TEST_F(RecordingLogValidatorTest, anArchiveErrorLeavesTheEntryValidAndIsReported)
{
    std::vector<std::string> reported;
    RecordingLogValidator validator(
        0, log_,
        [](std::int64_t, axc::core::Error& error) -> RecordingLogValidator::Lookup
        {
            error = axc::core::make_error(axc::core::ErrorCode::io_error, "archive timed out");
            return RecordingLogValidator::Lookup::error;
        },
        [&reported](const axc::core::Error& error) { reported.emplace_back(error.message.view()); });

    int polls = 0;
    while (!validator.is_complete() && polls < 10)
    {
        validator.poll();
        polls++;
    }

    EXPECT_TRUE(validator.is_complete());
    ASSERT_EQ(2U, reported.size());
    EXPECT_EQ("archive timed out", reported[0]);
    EXPECT_TRUE(entry_for(KNOWN_RECORDING_ID)->is_valid);
    EXPECT_TRUE(entry_for(UNKNOWN_RECORDING_ID)->is_valid);
}

}  // namespace
