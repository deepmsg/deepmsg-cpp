// Port of `RecordingLogTest` (Aeron 1.54.0, `454da1e4e6`). Case names are the
// reference method names (work-packages §0 rule 10).

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "cluster/common/recording_log.h"

namespace
{

using deepmsg::cluster::common::RecordingLog;

constexpr std::int64_t NULL_VALUE = -1;
constexpr std::int64_t NULL_POSITION = -1;
constexpr std::int32_t SERVICE_ID = -1;
constexpr std::int32_t ENTRY_TYPE_TERM = RecordingLog::ENTRY_TYPE_TERM;
constexpr std::int32_t ENTRY_TYPE_SNAPSHOT = RecordingLog::ENTRY_TYPE_SNAPSHOT;
constexpr std::int32_t ENTRY_TYPE_STANDBY_SNAPSHOT = RecordingLog::ENTRY_TYPE_STANDBY_SNAPSHOT;
constexpr std::int64_t RECORDING_ID = 9234236;

RecordingLog::Entry make_entry(std::int64_t recording_id, std::int64_t leadership_term_id,
                               std::int64_t term_base_log_position, std::int64_t log_position, std::int64_t timestamp,
                               std::int32_t service_id, std::int32_t type, bool is_valid, std::int32_t entry_index,
                               std::string archive_endpoint = {})
{
    RecordingLog::Entry entry;
    entry.recording_id = recording_id;
    entry.leadership_term_id = leadership_term_id;
    entry.term_base_log_position = term_base_log_position;
    entry.log_position = log_position;
    entry.timestamp = timestamp;
    entry.service_id = service_id;
    entry.type = type;
    entry.is_valid = is_valid;
    entry.entry_index = entry_index;
    entry.archive_endpoint = std::move(archive_endpoint);
    return entry;
}

class RecordingLogTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        std::string pattern = "/tmp/axc-recording-log-XXXXXX";
        const char* created = ::mkdtemp(pattern.data());
        ASSERT_NE(nullptr, created);
        directory_ = created;
    }

    void TearDown() override
    {
        std::error_code error;
        std::filesystem::remove_all(directory_, error);
    }

    [[nodiscard]] RecordingLog open_log(bool create_new = true)
    {
        auto log = RecordingLog::open(directory_, create_new);
        EXPECT_TRUE(log) << log.error().message.view();
        return std::move(*log);
    }

    void append_term(RecordingLog& log, std::int64_t recording_id, std::int64_t leadership_term_id,
                     std::int64_t term_base_log_position, std::int64_t timestamp)
    {
        auto result = log.append_term(recording_id, leadership_term_id, term_base_log_position, timestamp);
        ASSERT_TRUE(result) << result.error().message.view();
    }

    void append_snapshot(RecordingLog& log, std::int64_t recording_id, std::int64_t leadership_term_id,
                         std::int64_t term_base_log_position, std::int64_t log_position, std::int64_t timestamp,
                         std::int32_t service_id)
    {
        auto result = log.append_snapshot(recording_id, leadership_term_id, term_base_log_position, log_position,
                                          timestamp, service_id);
        ASSERT_TRUE(result) << result.error().message.view();
    }

    void append_standby_snapshot(RecordingLog& log, std::int64_t recording_id, std::int64_t leadership_term_id,
                                 std::int64_t term_base_log_position, std::int64_t log_position, std::int64_t timestamp,
                                 std::int32_t service_id, std::string_view endpoint)
    {
        auto result = log.append_standby_snapshot(recording_id, leadership_term_id, term_base_log_position,
                                                  log_position, timestamp, service_id, endpoint);
        ASSERT_TRUE(result) << result.error().message.view();
    }

    std::string directory_{};
};

TEST_F(RecordingLogTest, shouldCreateNewIndex)
{
    auto log = open_log();
    EXPECT_TRUE(log.entries().empty());
}

TEST_F(RecordingLogTest, shouldAppendAndThenReloadLatestSnapshot)
{
    const auto entry = make_entry(1, 3, 2, 777, 4, SERVICE_ID, ENTRY_TYPE_SNAPSHOT, true, 0);

    {
        auto log = open_log(true);
        append_snapshot(log, entry.recording_id, entry.leadership_term_id, entry.term_base_log_position,
                        entry.log_position, entry.timestamp, SERVICE_ID);
    }

    auto log = open_log(true);
    ASSERT_EQ(1U, log.entries().size());

    const RecordingLog::Entry* snapshot = log.get_latest_snapshot(SERVICE_ID);
    ASSERT_NE(nullptr, snapshot);
    EXPECT_EQ(entry, *snapshot);
}

TEST_F(RecordingLogTest, shouldIgnoreIncompleteSnapshotInRecoveryPlan)
{
    constexpr std::int32_t service_count = 1;

    {
        auto log = open_log(true);
        append_snapshot(log, 1, 1, 0, 777, 0, 0);
        append_snapshot(log, 2, 1, 0, 777, 0, SERVICE_ID);
        append_snapshot(log, 3, 1, 0, 888, 0, 0);
    }

    auto log = open_log(true);
    ASSERT_EQ(3U, log.entries().size());

    RecordingLog::RecordingExtentSource source = [](std::int64_t)
    { return std::optional<RecordingLog::RecordingExtent>{}; };
    auto plan = log.create_recovery_plan(source, service_count, NULL_VALUE);
    ASSERT_TRUE(plan) << plan.error().message.view();
    ASSERT_EQ(2U, plan->snapshots.size());
    EXPECT_EQ(SERVICE_ID, plan->snapshots[0].service_id);
    EXPECT_EQ(2, plan->snapshots[0].recording_id);
    EXPECT_EQ(0, plan->snapshots[1].service_id);
    EXPECT_EQ(1, plan->snapshots[1].recording_id);
}

TEST_F(RecordingLogTest, shouldIgnoreInvalidMidSnapshotInRecoveryPlan)
{
    constexpr std::int32_t service_count = 1;

    {
        auto log = open_log(true);
        append_snapshot(log, 1, 1, 0, 777, 0, 0);
        append_snapshot(log, 2, 1, 0, 777, 0, SERVICE_ID);
        append_snapshot(log, 3, 1, 0, 888, 0, 0);
        append_snapshot(log, 4, 1, 0, 888, 0, SERVICE_ID);
        append_snapshot(log, 5, 1, 0, 999, 0, 0);
        append_snapshot(log, 6, 1, 0, 999, 0, SERVICE_ID);

        EXPECT_TRUE(log.invalidate_entry(2));
        EXPECT_TRUE(log.invalidate_entry(3));
    }

    auto log = open_log(true);
    RecordingLog::RecordingExtentSource source = [](std::int64_t)
    { return std::optional<RecordingLog::RecordingExtent>{}; };
    auto plan = log.create_recovery_plan(source, service_count, NULL_VALUE);
    ASSERT_TRUE(plan) << plan.error().message.view();
    ASSERT_EQ(2U, plan->snapshots.size());
    EXPECT_EQ(SERVICE_ID, plan->snapshots[0].service_id);
    EXPECT_EQ(6, plan->snapshots[0].recording_id);
    EXPECT_EQ(0, plan->snapshots[1].service_id);
    EXPECT_EQ(5, plan->snapshots[1].recording_id);
}

TEST_F(RecordingLogTest, shouldIgnoreInvalidTermInRecoveryPlan)
{
    constexpr std::int32_t service_count = 1;
    constexpr std::int64_t removed_leadership_term = 11;

    {
        auto log = open_log(true);
        append_term(log, 0, 9, 444, 0);
        append_term(log, 0, 10, 666, 0);
        append_snapshot(log, 1, 10, 666, 777, 0, 0);
        append_snapshot(log, 2, 10, 666, 777, 0, SERVICE_ID);
        append_snapshot(log, 3, 10, 666, 888, 0, 0);
        append_snapshot(log, 4, 10, 666, 888, 0, SERVICE_ID);
        append_term(log, 0, removed_leadership_term, 999, 0);

        const RecordingLog::Entry* last_term = log.find_last_term();
        ASSERT_NE(nullptr, last_term);
        EXPECT_EQ(999, last_term->term_base_log_position);

        EXPECT_TRUE(log.invalidate_entry(6));
    }

    auto log = open_log(true);
    RecordingLog::RecordingExtentSource source = [](std::int64_t)
    { return std::optional<RecordingLog::RecordingExtent>{RecordingLog::RecordingExtent{}}; };
    auto plan = log.create_recovery_plan(source, service_count, NULL_VALUE);
    ASSERT_TRUE(plan) << plan.error().message.view();
    ASSERT_TRUE(plan->log.has_value());
    EXPECT_EQ(0, plan->log->recording_id);
    EXPECT_EQ(10, plan->log->leadership_term_id);
    EXPECT_EQ(666, plan->log->term_base_log_position);

    const RecordingLog::Entry* last_term = log.find_last_term();
    ASSERT_NE(nullptr, last_term);
    EXPECT_EQ(0, last_term->recording_id);
    EXPECT_EQ(0, log.find_last_term_recording_id());
    EXPECT_TRUE(log.is_unknown(removed_leadership_term));
    EXPECT_EQ(NULL_VALUE, log.get_term_timestamp(removed_leadership_term));

    EXPECT_FALSE(log.get_term_entry(removed_leadership_term));
    EXPECT_FALSE(log.commit_log_position(removed_leadership_term, 99));
}

TEST_F(RecordingLogTest, shouldAppendAndThenCommitTermPosition)
{
    constexpr std::int64_t new_position = 9999;

    {
        auto log = open_log(true);
        append_term(log, 1, 1111, 2222, 3333);
        auto committed = log.commit_log_position(1111, new_position);
        ASSERT_TRUE(committed) << committed.error().message.view();
    }

    auto log = open_log(true);
    ASSERT_EQ(1U, log.entries().size());
    EXPECT_EQ(new_position, log.entries()[0].log_position);
}

TEST_F(RecordingLogTest, shouldRemoveEntry)
{
    {
        auto log = open_log(true);
        const auto entry_one = make_entry(1, 3, 2, NULL_POSITION, 4, 0, ENTRY_TYPE_TERM, true, 0);
        append_term(log, entry_one.recording_id, entry_one.leadership_term_id, entry_one.term_base_log_position,
                    entry_one.timestamp);

        const auto entry_two = make_entry(1, 4, 3, NULL_POSITION, 5, 0, ENTRY_TYPE_TERM, true, 0);
        append_term(log, entry_two.recording_id, entry_two.leadership_term_id, entry_two.term_base_log_position,
                    entry_two.timestamp);

        auto removed = log.remove_entry(entry_two.leadership_term_id, log.next_entry_index() - 1);
        ASSERT_TRUE(removed) << removed.error().message.view();
        EXPECT_EQ(1U, log.entries().size());
    }

    auto log = open_log(true);
    EXPECT_EQ(1U, log.entries().size());
    EXPECT_EQ(2, log.next_entry_index());
}

TEST_F(RecordingLogTest, shouldCorrectlyOrderSnapshots)
{
    std::vector<RecordingLog::Snapshot> snapshots;
    std::vector<RecordingLog::Entry> entries;

    const auto add_recording_log_entry =
        [&entries](std::int32_t service_id, std::int32_t recording_id, std::int32_t entry_type)
    {
        entries.push_back(make_entry(recording_id, 1, 1440, 2880, 0, service_id, entry_type, true,
                                     static_cast<std::int32_t>(entries.size())));
    };

    add_recording_log_entry(SERVICE_ID, 0, ENTRY_TYPE_TERM);
    add_recording_log_entry(2, 4, ENTRY_TYPE_SNAPSHOT);
    add_recording_log_entry(1, 5, ENTRY_TYPE_SNAPSHOT);
    add_recording_log_entry(0, 6, ENTRY_TYPE_SNAPSHOT);
    add_recording_log_entry(SERVICE_ID, 7, ENTRY_TYPE_SNAPSHOT);

    auto added = RecordingLog::add_snapshots(snapshots, entries, 3, static_cast<std::int32_t>(entries.size()) - 1);
    ASSERT_TRUE(added) << added.error().message.view();

    ASSERT_EQ(4U, snapshots.size());
    EXPECT_EQ(SERVICE_ID, snapshots[0].service_id);
    EXPECT_EQ(0, snapshots[1].service_id);
    EXPECT_EQ(1, snapshots[2].service_id);
    EXPECT_EQ(2, snapshots[3].service_id);
}

TEST_F(RecordingLogTest, shouldInvalidateLatestSnapshot)
{
    constexpr std::int64_t term_base_log_position = 0;
    constexpr std::int64_t log_increment = 640;
    std::int64_t leadership_term_id = 7;
    std::int64_t log_position = 0;
    std::int64_t timestamp = 1000;

    {
        auto log = open_log(true);
        append_term(log, 1, leadership_term_id, term_base_log_position, timestamp);

        timestamp += 1;
        log_position += log_increment;

        append_snapshot(log, 2, leadership_term_id, term_base_log_position, log_position, timestamp, 0);
        append_snapshot(log, 3, leadership_term_id, term_base_log_position, log_position, timestamp, SERVICE_ID);

        timestamp += 1;
        log_position += log_increment;

        append_snapshot(log, 4, leadership_term_id, term_base_log_position, log_position, timestamp, 0);
        append_snapshot(log, 5, leadership_term_id, term_base_log_position, log_position, timestamp, SERVICE_ID);

        leadership_term_id++;
        append_term(log, 1, leadership_term_id, log_position, timestamp);

        auto invalidated = log.invalidate_latest_snapshot();
        ASSERT_TRUE(invalidated) << invalidated.error().message.view();
        EXPECT_TRUE(*invalidated);
        EXPECT_EQ(6U, log.entries().size());
    }

    {
        auto log = open_log(true);
        ASSERT_EQ(6U, log.entries().size());

        EXPECT_TRUE(log.entries()[0].is_valid);
        EXPECT_TRUE(log.entries()[1].is_valid);
        EXPECT_TRUE(log.entries()[2].is_valid);
        EXPECT_FALSE(log.entries()[3].is_valid);
        EXPECT_FALSE(log.entries()[4].is_valid);
        EXPECT_TRUE(log.entries()[5].is_valid);

        EXPECT_EQ(&log.entries()[1], log.get_latest_snapshot(0));
        EXPECT_EQ(&log.entries()[2], log.get_latest_snapshot(SERVICE_ID));

        auto invalidated = log.invalidate_latest_snapshot();
        ASSERT_TRUE(invalidated) << invalidated.error().message.view();
        EXPECT_TRUE(*invalidated);
        EXPECT_EQ(6U, log.entries().size());
    }

    auto log = open_log(true);
    ASSERT_EQ(6U, log.entries().size());

    EXPECT_TRUE(log.entries()[0].is_valid);
    EXPECT_FALSE(log.entries()[1].is_valid);
    EXPECT_FALSE(log.entries()[2].is_valid);
    EXPECT_FALSE(log.entries()[3].is_valid);
    EXPECT_FALSE(log.entries()[4].is_valid);
    EXPECT_TRUE(log.entries()[5].is_valid);

    auto invalidated = log.invalidate_latest_snapshot();
    ASSERT_TRUE(invalidated) << invalidated.error().message.view();
    EXPECT_FALSE(*invalidated);
    EXPECT_EQ(&log.entries()[5], log.get_term_entry(leadership_term_id).value());
}

TEST_F(RecordingLogTest, shouldRecoverSnapshotsMidLogMarkedInvalid)
{
    {
        auto log = open_log(true);
        append_snapshot(log, 1, 1, 10, 555, 0, 0);
        append_snapshot(log, 2, 1, 10, 555, 0, SERVICE_ID);
        append_snapshot(log, 3, 1, 10, 777, 0, 0);
        append_snapshot(log, 4, 1, 10, 777, 0, SERVICE_ID);
        append_snapshot(log, 5, 1, 10, 888, 0, 0);
        append_snapshot(log, 6, 1, 10, 888, 0, SERVICE_ID);

        EXPECT_TRUE(log.invalidate_entry(2));
        EXPECT_TRUE(log.invalidate_entry(3));
    }

    {
        auto log = open_log(true);
        append_snapshot(log, 7, 1, 10, 777, 555, 0);
        append_snapshot(log, 8, 1, 10, 777, -999, SERVICE_ID);

        ASSERT_EQ(6U, log.entries().size());
        EXPECT_TRUE(log.entries()[2].is_valid);
        EXPECT_EQ(7, log.entries()[2].recording_id);
        EXPECT_TRUE(log.entries()[3].is_valid);
        EXPECT_EQ(8, log.entries()[3].recording_id);
    }

    auto log = open_log(true);
    ASSERT_EQ(6U, log.entries().size());
    EXPECT_TRUE(log.entries()[2].is_valid);
    EXPECT_EQ(7, log.entries()[2].recording_id);
    EXPECT_TRUE(log.entries()[3].is_valid);
    EXPECT_EQ(8, log.entries()[3].recording_id);
}

TEST_F(RecordingLogTest, shouldRecoverSnapshotsLastInLogMarkedWithInvalid)
{
    {
        auto log = open_log(true);
        append_snapshot(log, -10, 1, 0, 777, 0, 0);
        append_snapshot(log, -11, 1, 0, 777, 0, SERVICE_ID);

        append_term(log, 1, 2, 10, 0);
        append_snapshot(log, -12, 2, 10, 888, 0, 0);
        append_snapshot(log, -13, 2, 10, 888, 0, SERVICE_ID);

        append_term(log, 1, 3, 20, 0);
        append_snapshot(log, -14, 3, 20, 999, 0, 0);
        append_snapshot(log, -15, 3, 20, 999, 0, SERVICE_ID);

        auto invalidated_one = log.invalidate_latest_snapshot();
        ASSERT_TRUE(invalidated_one) << invalidated_one.error().message.view();
        auto invalidated_two = log.invalidate_latest_snapshot();
        ASSERT_TRUE(invalidated_two) << invalidated_two.error().message.view();
    }

    auto log = open_log(true);
    append_snapshot(log, 1, 2, 10, 888, 0, 0);
    append_snapshot(log, 2, 2, 10, 888, 0, SERVICE_ID);

    append_snapshot(log, 3, 3, 20, 999, 0, 0);
    append_snapshot(log, 4, 3, 20, 999, 0, SERVICE_ID);

    ASSERT_EQ(8U, log.entries().size());
    EXPECT_TRUE(log.entries()[2].is_valid);
    EXPECT_EQ(1, log.entries()[3].recording_id);
    EXPECT_TRUE(log.entries()[3].is_valid);
    EXPECT_EQ(2, log.entries()[4].recording_id);
    EXPECT_TRUE(log.entries()[4].is_valid);
    EXPECT_EQ(3, log.entries()[6].recording_id);
    EXPECT_TRUE(log.entries()[5].is_valid);
    EXPECT_EQ(4, log.entries()[7].recording_id);
}

TEST_F(RecordingLogTest, shouldNotAllowInvalidateOfSnapshotWithoutParentTerm)
{
    auto log = open_log(true);
    append_snapshot(log, -10, 1, 0, 777, 0, 0);
    append_snapshot(log, -11, 1, 0, 777, 0, SERVICE_ID);

    auto invalidated = log.invalidate_latest_snapshot();
    ASSERT_FALSE(invalidated);
    EXPECT_EQ("no matching term for snapshot: leadershipTermId=1", invalidated.error().message.view());
}

TEST_F(RecordingLogTest, shouldFailToRecoverSnapshotsMarkedInvalidIfFieldsDoNotMatchCorrectly)
{
    {
        auto log = open_log(true);
        append_term(log, 10, 0, 0, 0);
        append_term(log, 10, 1, 500, 0);
        append_snapshot(log, 0, 1, 500, 777, 0, 0);
        append_snapshot(log, 1, 1, 500, 777, 0, SERVICE_ID);
        append_snapshot(log, 2, 1, 500, 888, 0, 0);
        append_snapshot(log, 3, 1, 500, 888, 0, SERVICE_ID);
        append_snapshot(log, 4, 1, 500, 999, 0, 0);
        append_snapshot(log, 5, 1, 500, 999, 0, SERVICE_ID);
        append_term(log, 10, 2, 1000, 5);

        auto invalidated = log.invalidate_latest_snapshot();
        ASSERT_TRUE(invalidated) << invalidated.error().message.view();
        EXPECT_TRUE(*invalidated);
    }

    auto log = open_log(true);
    append_snapshot(log, 6, 2, 500, 999, 0, SERVICE_ID);
    append_snapshot(log, 7, 1, 501, 999, 0, SERVICE_ID);
    append_snapshot(log, 8, 1, 500, 998, 0, SERVICE_ID);
    append_snapshot(log, 9, 1, 500, 999, 0, 42);

    const auto& entries = log.entries();
    EXPECT_EQ(make_entry(8, 1, 500, 998, 0, SERVICE_ID, ENTRY_TYPE_SNAPSHOT, true, 11), entries[6]);
    EXPECT_EQ(make_entry(9, 1, 500, 999, 0, 42, ENTRY_TYPE_SNAPSHOT, true, 12), entries[7]);
    EXPECT_EQ(make_entry(4, 1, 500, 999, 0, 0, ENTRY_TYPE_SNAPSHOT, false, 6), entries[8]);
    EXPECT_EQ(make_entry(5, 1, 500, 999, 0, SERVICE_ID, ENTRY_TYPE_SNAPSHOT, false, 7), entries[9]);
    EXPECT_EQ(make_entry(7, 1, 501, 999, 0, SERVICE_ID, ENTRY_TYPE_SNAPSHOT, true, 10), entries[10]);
    EXPECT_EQ(make_entry(6, 2, 500, 999, 0, SERVICE_ID, ENTRY_TYPE_SNAPSHOT, true, 9), entries[11]);
    EXPECT_EQ(make_entry(10, 2, 1000, NULL_POSITION, 5, NULL_VALUE, ENTRY_TYPE_TERM, true, 8), entries[12]);
    const RecordingLog::Entry* latest_snapshot = log.get_latest_snapshot(SERVICE_ID);
    ASSERT_NE(nullptr, latest_snapshot);
    EXPECT_EQ(6, latest_snapshot->recording_id);
}

TEST_F(RecordingLogTest, shouldFindSnapshotAtOrBeforeOrLowest)
{
    auto log = open_log(true);
    std::int64_t recording_id = 0;
    append_snapshot(log, recording_id++, 1, 500, 777, 0, 0);
    append_snapshot(log, recording_id++, 1, 500, 777, 0, 1);
    append_snapshot(log, recording_id++, 1, 500, 777, 0, SERVICE_ID);
    append_standby_snapshot(log, recording_id++, 1, 500, 888, 0, 0, "localhost:8080");
    append_standby_snapshot(log, recording_id++, 1, 500, 888, 0, 1, "localhost:8080");
    append_standby_snapshot(log, recording_id++, 1, 500, 888, 0, SERVICE_ID, "localhost:8080");
    append_snapshot(log, recording_id++, 1, 500, 888, 0, 0);
    append_snapshot(log, recording_id++, 1, 500, 888, 0, 1);
    append_snapshot(log, recording_id++, 1, 500, 888, 0, SERVICE_ID);
    append_snapshot(log, recording_id++, 1, 500, 890, 0, 0);
    append_snapshot(log, recording_id++, 1, 500, 890, 0, SERVICE_ID);  // Missing one service snapshot
    append_standby_snapshot(log, recording_id++, 1, 500, 990, 0, 0, "localhost:8080");
    append_standby_snapshot(log, recording_id++, 1, 500, 990, 0, 1, "localhost:8080");
    append_standby_snapshot(log, recording_id++, 1, 500, 990, 0, SERVICE_ID, "localhost:8080");
    append_snapshot(log, recording_id++, 1, 500, 999, 0, 0);
    append_snapshot(log, recording_id++, 1, 500, 999, 0, 1);
    append_snapshot(log, recording_id++, 1, 500, 999, 0, SERVICE_ID);

    constexpr std::int32_t service_count = 2;

    const auto assert_snapshot =
        [service_count](const std::optional<std::vector<RecordingLog::Snapshot>>& snapshot, std::int64_t log_position)
    {
        ASSERT_TRUE(snapshot.has_value());
        ASSERT_EQ(static_cast<std::size_t>(service_count + 1), snapshot->size());
        for (const auto& s : *snapshot)
        {
            EXPECT_EQ(log_position, s.log_position);
        }
    };

    assert_snapshot(log.find_snapshot_at_or_before_or_lowest(777, service_count), 777);
    assert_snapshot(log.find_snapshot_at_or_before_or_lowest(888, service_count), 888);
    assert_snapshot(log.find_snapshot_at_or_before_or_lowest(999, service_count), 999);
    assert_snapshot(log.find_snapshot_at_or_before_or_lowest(890, service_count), 888);
    assert_snapshot(log.find_snapshot_at_or_before_or_lowest(990, service_count), 888);
    assert_snapshot(log.find_snapshot_at_or_before_or_lowest(750, service_count), 777);
    assert_snapshot(log.find_snapshot_at_or_before_or_lowest(1000, service_count), 999);
}

TEST_F(RecordingLogTest, shouldAppendTermWithLeadershipTermIdOutOfOrder)
{
    const std::vector<RecordingLog::Entry> sorted_entries{
        make_entry(0, 0, 0, 700, 0, NULL_VALUE, ENTRY_TYPE_TERM, true, 0),
        make_entry(0, 1, 700, 2048, 0, NULL_VALUE, ENTRY_TYPE_TERM, true, 3),
        make_entry(0, 2, 2048, 5000, 0, NULL_VALUE, ENTRY_TYPE_TERM, true, 1),
        make_entry(0, 3, 5000, NULL_POSITION, 100, NULL_VALUE, ENTRY_TYPE_TERM, true, 2)};

    {
        auto log = open_log(true);
        append_term(log, 0, 0, 0, 0);
        append_term(log, 0, 2, 2048, 0);
        append_term(log, 0, 3, 5000, 100);
        append_term(log, 0, 1, 700, 0);

        ASSERT_EQ(4, log.next_entry_index());
        EXPECT_EQ(sorted_entries, log.entries());

        EXPECT_EQ(&log.entries()[0], log.get_term_entry(0).value());
        EXPECT_EQ(&log.entries()[1], log.get_term_entry(1).value());
        EXPECT_EQ(&log.entries()[2], log.get_term_entry(2).value());
        EXPECT_EQ(&log.entries()[3], log.get_term_entry(3).value());
    }

    auto log = open_log(true);
    EXPECT_EQ(sorted_entries, log.entries());
}

TEST_F(RecordingLogTest, shouldAppendSnapshotWithLeadershipTermIdOutOfOrder)
{
    const std::vector<RecordingLog::Entry> sorted_entries{
        make_entry(3, 1, 0, 200, 0, NULL_VALUE, ENTRY_TYPE_TERM, true, 0),
        make_entry(10, 1, 0, 56, 42, SERVICE_ID, ENTRY_TYPE_SNAPSHOT, true, 1),
        make_entry(3, 2, 200, 2048, 555, NULL_VALUE, ENTRY_TYPE_TERM, true, 2),
        make_entry(11, 2, 200, 250, 100, 1, ENTRY_TYPE_SNAPSHOT, true, 4),
        make_entry(100, 2, 200, 250, 100, 0, ENTRY_TYPE_SNAPSHOT, true, 5),
        make_entry(3, 3, 2048, NULL_POSITION, 0, NULL_VALUE, ENTRY_TYPE_TERM, true, 3)};

    {
        auto log = open_log(true);
        append_term(log, 3, 1, 0, 0);
        append_snapshot(log, 10, 1, 0, 56, 42, SERVICE_ID);
        EXPECT_TRUE(log.invalidate_entry(1));

        auto committed_one = log.commit_log_position(1, 200);
        ASSERT_TRUE(committed_one) << committed_one.error().message.view();
        append_term(log, 3, 2, 200, 555);

        auto committed_two = log.commit_log_position(2, 2048);
        ASSERT_TRUE(committed_two) << committed_two.error().message.view();
        append_term(log, 3, 3, 2048, 0);
        append_snapshot(log, 11, 2, 200, 250, 100, 1);
        append_snapshot(log, 10, 1, 0, 56, 42, SERVICE_ID);
        append_snapshot(log, 100, 2, 200, 250, 100, 0);

        EXPECT_EQ(sorted_entries, log.entries());
        EXPECT_EQ(6, log.next_entry_index());

        EXPECT_EQ(&log.entries()[0], log.get_term_entry(1).value());
        EXPECT_EQ(nullptr, log.find_term_entry(0));
        EXPECT_EQ(&log.entries()[2], log.get_term_entry(2).value());
        EXPECT_EQ(&log.entries()[5], log.get_term_entry(3).value());
        EXPECT_EQ(&log.entries()[1], log.get_latest_snapshot(SERVICE_ID));
    }

    auto log = open_log(true);
    EXPECT_EQ(sorted_entries, log.entries());
}

TEST_F(RecordingLogTest, appendTermShouldRejectNullValueAsRecordingId)
{
    auto log = open_log(true);
    auto result = log.append_term(NULL_VALUE, 0, 0, 0);
    ASSERT_FALSE(result);
    EXPECT_EQ("invalid recordingId=-1", result.error().message.view());
    EXPECT_TRUE(log.entries().empty());
}

TEST_F(RecordingLogTest, appendSnapshotShouldRejectNullValueAsRecordingId)
{
    auto log = open_log(true);
    auto result = log.append_snapshot(NULL_VALUE, 0, 0, 0, 0, 0);
    ASSERT_FALSE(result);
    EXPECT_EQ("invalid recordingId=-1", result.error().message.view());
    EXPECT_TRUE(log.entries().empty());
}

TEST_F(RecordingLogTest, appendTermShouldNotAcceptDifferentRecordingIds)
{
    {
        auto log = open_log(true);
        append_term(log, 42, 0, 0, 0);

        auto result = log.append_term(21, 1, 0, 0);
        ASSERT_FALSE(result);
        EXPECT_EQ("invalid TERM recordingId=21, expected recordingId=42", result.error().message.view());
        EXPECT_EQ(1U, log.entries().size());
    }

    auto log = open_log(true);
    auto result = log.append_term(-5, -5, -5, -5);
    ASSERT_FALSE(result);
    EXPECT_EQ("invalid TERM recordingId=-5, expected recordingId=42", result.error().message.view());
    EXPECT_EQ(1U, log.entries().size());
}

TEST_F(RecordingLogTest, appendTermShouldOnlyAllowASingleValidTermForTheSameLeadershipTermId)
{
    auto log = open_log(true);
    append_term(log, 8, 0, 0, 0);
    append_term(log, 8, 1, 1, 1);

    EXPECT_TRUE(log.invalidate_entry(0));
    append_term(log, 8, 0, 100, 100);

    auto result = log.append_term(8, 1, 5, 5);
    ASSERT_FALSE(result);
    EXPECT_EQ("duplicate TERM entry for leadershipTermId=1", result.error().message.view());
    EXPECT_EQ(3U, log.entries().size());
}

TEST_F(RecordingLogTest, entriesInTheRecordingLogShouldBeSorted)
{
    const std::string archive_endpoint = "aeron:udp?endpoint=localhost:8080";
    std::vector<RecordingLog::Entry> sorted_list{
        make_entry(0, 0, 0, 90, 0, NULL_VALUE, ENTRY_TYPE_TERM, true, 0),
        make_entry(0, 1, 0, 777, 42, 2, ENTRY_TYPE_SNAPSHOT, true, 10),
        make_entry(0, 1, 90, 400, 9, NULL_VALUE, ENTRY_TYPE_TERM, true, 7),
        make_entry(0, 1, 111, 222, 12, 1, ENTRY_TYPE_SNAPSHOT, false, 1),
        make_entry(0, 1, 111, 222, 12, 0, ENTRY_TYPE_SNAPSHOT, false, 3),
        make_entry(0, 1, 111, 222, 12, SERVICE_ID, ENTRY_TYPE_SNAPSHOT, false, 2),
        make_entry(0, 2, 400, 2500, 1'000'000, NULL_VALUE, ENTRY_TYPE_TERM, false, 5),
        make_entry(0, 2, 400, 2500, 20, NULL_VALUE, ENTRY_TYPE_TERM, true, 6),
        make_entry(15, 2, 400, 800, 200, 1, ENTRY_TYPE_STANDBY_SNAPSHOT, true, 18, archive_endpoint),
        make_entry(16, 2, 400, 800, 200, 0, ENTRY_TYPE_STANDBY_SNAPSHOT, true, 19, archive_endpoint),
        make_entry(17, 2, 400, 800, 200, SERVICE_ID, ENTRY_TYPE_STANDBY_SNAPSHOT, true, 20, archive_endpoint),
        make_entry(400, 2, 400, 800, 200, 1, ENTRY_TYPE_SNAPSHOT, true, 15),
        make_entry(401, 2, 400, 800, 200, 0, ENTRY_TYPE_SNAPSHOT, true, 16),
        make_entry(402, 2, 400, 800, 200, SERVICE_ID, ENTRY_TYPE_SNAPSHOT, true, 17),
        make_entry(50, 2, 400, 1400, 300, 1, ENTRY_TYPE_STANDBY_SNAPSHOT, true, 13, archive_endpoint),
        make_entry(51, 2, 400, 1400, 300, 0, ENTRY_TYPE_STANDBY_SNAPSHOT, true, 14, archive_endpoint),
        make_entry(52, 2, 400, 1400, 300, SERVICE_ID, ENTRY_TYPE_STANDBY_SNAPSHOT, true, 12, archive_endpoint),
        make_entry(652, 2, 400, 1400, 300, 1, ENTRY_TYPE_SNAPSHOT, false, 9),
        make_entry(650, 2, 400, 1400, 300, 0, ENTRY_TYPE_SNAPSHOT, true, 11),
        make_entry(651, 2, 400, 1400, 300, SERVICE_ID, ENTRY_TYPE_SNAPSHOT, true, 8),
        make_entry(0, 3, 2500, NULL_POSITION, 30, NULL_VALUE, ENTRY_TYPE_TERM, true, 4)};

    auto log = open_log(true);
    append_term(log, 0, 0, 0, 0);
    append_snapshot(log, 0, 1, 111, 222, 12, 1);
    append_snapshot(log, 0, 1, 111, 222, 12, SERVICE_ID);
    append_snapshot(log, 0, 1, 111, 222, 12, 0);
    append_term(log, 0, 3, 2500, 30);
    append_term(log, 0, 2, 400, 1'000'000);

    EXPECT_TRUE(log.invalidate_entry(1));
    EXPECT_TRUE(log.invalidate_entry(4));

    append_term(log, 0, 2, 400, 20);
    append_term(log, 0, 1, 90, 9);

    auto invalidated = log.invalidate_latest_snapshot();
    ASSERT_TRUE(invalidated) << invalidated.error().message.view();
    EXPECT_TRUE(*invalidated);

    append_snapshot(log, 651, 2, 400, 1400, 300, SERVICE_ID);
    append_snapshot(log, 652, 2, 400, 1400, 300, 1);
    append_snapshot(log, 0, 1, 0, 777, 42, 2);
    append_snapshot(log, 650, 2, 400, 1400, 300, 0);

    EXPECT_TRUE(log.invalidate_entry(8));

    append_standby_snapshot(log, 52, 2, 400, 1400, 300, SERVICE_ID, archive_endpoint);
    append_standby_snapshot(log, 50, 2, 400, 1400, 300, 1, archive_endpoint);
    append_standby_snapshot(log, 51, 2, 400, 1400, 300, 0, archive_endpoint);

    append_snapshot(log, 400, 2, 400, 800, 200, 1);
    append_snapshot(log, 401, 2, 400, 800, 200, 0);
    append_snapshot(log, 402, 2, 400, 800, 200, SERVICE_ID);
    append_standby_snapshot(log, 15, 2, 400, 800, 200, 1, archive_endpoint);
    append_standby_snapshot(log, 16, 2, 400, 800, 200, 0, archive_endpoint);
    append_standby_snapshot(log, 17, 2, 400, 800, 200, SERVICE_ID, archive_endpoint);

    EXPECT_EQ(sorted_list, log.entries());  // in memory view

    auto reloaded = log.reload();
    ASSERT_TRUE(reloaded) << reloaded.error().message.view();

    EXPECT_EQ(sorted_list, log.entries());  // reload from disc and re-sort
}

TEST_F(RecordingLogTest, shouldCreateInitialEmptyTermsWithAnEmptyRecordingLog)
{
    const std::vector<std::pair<std::pair<std::int64_t, std::int64_t>, std::int64_t>> cases{
        {{0, 0}, 4}, {{2, 500}, 4}, {{100, 1'000'000}, 200}};

    for (const auto& [initial, leadership_term_id] : cases)
    {
        const std::int64_t initial_log_leadership_term_id = initial.first;
        const std::int64_t initial_term_base_log_position = initial.second;
        const std::int64_t log_position = initial_term_base_log_position + 500;
        constexpr std::int64_t now_ns = 10'000'000'000;
        const std::int64_t timestamp_ms = now_ns / 1'000'000;

        auto log = open_log(true);
        auto coherent = log.ensure_coherent(RECORDING_ID, initial_log_leadership_term_id,
                                            initial_term_base_log_position, leadership_term_id,
                                            initial_term_base_log_position, log_position, now_ns, timestamp_ms, 1);
        ASSERT_TRUE(coherent) << coherent.error().message.view();

        for (std::int64_t term_id = initial_log_leadership_term_id + 1; term_id < leadership_term_id; ++term_id)
        {
            const RecordingLog::Entry* term_entry = log.find_term_entry(term_id);
            ASSERT_NE(nullptr, term_entry);
            EXPECT_EQ(initial_term_base_log_position, term_entry->term_base_log_position);
            EXPECT_EQ(initial_term_base_log_position, term_entry->log_position);
            EXPECT_EQ(timestamp_ms, term_entry->timestamp);
        }

        const RecordingLog::Entry* term_entry = log.find_term_entry(leadership_term_id);
        ASSERT_NE(nullptr, term_entry);
        EXPECT_EQ(initial_term_base_log_position, term_entry->term_base_log_position);
        EXPECT_EQ(log_position, term_entry->log_position);
        EXPECT_EQ(timestamp_ms, term_entry->timestamp);

        std::error_code error;
        std::filesystem::remove(directory_ + "/recording.log", error);
    }
}

TEST_F(RecordingLogTest, shouldNotCreateInitialTermWithMinusOneTermId)
{
    constexpr std::int64_t initial_log_leadership_term_id = -1;
    constexpr std::int64_t initial_term_base_log_position = 0;
    constexpr std::int64_t leadership_term_id = 4;
    constexpr std::int64_t log_position = initial_term_base_log_position + 500;
    constexpr std::int64_t now_ns = 1'000'000;

    auto log = open_log(true);
    auto coherent =
        log.ensure_coherent(RECORDING_ID, initial_log_leadership_term_id, initial_term_base_log_position,
                            leadership_term_id, initial_term_base_log_position, log_position, now_ns, now_ns, 1);
    ASSERT_TRUE(coherent) << coherent.error().message.view();

    for (const auto& entry : log.entries())
    {
        EXPECT_NE(-1, entry.leadership_term_id);
    }
}

TEST_F(RecordingLogTest, shouldBackFillEmptyLeadershipTermsInANonemptyRecordingLog)
{
    const std::vector<std::pair<std::pair<std::int64_t, std::int64_t>, std::int64_t>> cases{
        {{0, 0}, 4}, {{2, 500}, 4}, {{100, 1'000'000}, 200}};

    for (const auto& [initial, leadership_term_id] : cases)
    {
        const std::int64_t initial_log_leadership_term_id = initial.first;
        const std::int64_t initial_term_base_log_position = initial.second;
        const std::int64_t term_base_log_position = initial_term_base_log_position + 100;
        const std::int64_t log_position = initial_term_base_log_position + 500;
        constexpr std::int64_t now_ns = 1'000'000;

        auto log = open_log(true);
        append_term(log, RECORDING_ID, initial_log_leadership_term_id, initial_term_base_log_position, now_ns);

        auto coherent =
            log.ensure_coherent(RECORDING_ID, initial_log_leadership_term_id, initial_term_base_log_position,
                                leadership_term_id, term_base_log_position, log_position, now_ns, now_ns, 1);
        ASSERT_TRUE(coherent) << coherent.error().message.view();

        for (std::int64_t term_id = initial_log_leadership_term_id + 1; term_id < leadership_term_id; ++term_id)
        {
            const RecordingLog::Entry* term_entry = log.find_term_entry(term_id);
            ASSERT_NE(nullptr, term_entry);
            EXPECT_EQ(term_base_log_position, term_entry->term_base_log_position);
            EXPECT_EQ(term_base_log_position, term_entry->log_position);
        }

        const RecordingLog::Entry* term_entry = log.find_term_entry(leadership_term_id);
        ASSERT_NE(nullptr, term_entry);
        EXPECT_EQ(term_base_log_position, term_entry->term_base_log_position);
        EXPECT_EQ(log_position, term_entry->log_position);

        std::error_code error;
        std::filesystem::remove(directory_ + "/recording.log", error);
    }
}

TEST_F(RecordingLogTest, shouldCompleteExistingTerm)
{
    const std::vector<std::pair<std::pair<std::int64_t, std::int64_t>, std::int64_t>> cases{
        {{0, 0}, 4}, {{2, 500}, 4}, {{100, 1'000'000}, 200}};

    for (const auto& [initial, leadership_term_id] : cases)
    {
        const std::int64_t initial_log_leadership_term_id = initial.first;
        const std::int64_t initial_term_base_log_position = initial.second;
        const std::int64_t term_base_log_position = initial_term_base_log_position + 100;
        const std::int64_t log_position = initial_term_base_log_position + 500;
        constexpr std::int64_t now_ns = 1'000'000;

        auto log = open_log(true);
        append_term(log, RECORDING_ID, leadership_term_id, term_base_log_position, now_ns);

        auto coherent =
            log.ensure_coherent(RECORDING_ID, initial_log_leadership_term_id, initial_term_base_log_position,
                                leadership_term_id, term_base_log_position, log_position, now_ns, now_ns, 1);
        ASSERT_TRUE(coherent) << coherent.error().message.view();

        const RecordingLog::Entry* term_entry = log.find_term_entry(leadership_term_id);
        ASSERT_NE(nullptr, term_entry);
        EXPECT_EQ(term_base_log_position, term_entry->term_base_log_position);
        EXPECT_EQ(log_position, term_entry->log_position);
        EXPECT_EQ(1U, log.entries().size());

        std::error_code error;
        std::filesystem::remove(directory_ + "/recording.log", error);
    }
}

TEST_F(RecordingLogTest, shouldBackFillPriorTerm)
{
    constexpr std::int64_t initial_log_leadership_term_id = 0;
    constexpr std::int64_t initial_term_base_log_position = 0;
    constexpr std::int64_t leadership_term_id = 1;
    constexpr std::int64_t term_base_log_position = 10'000;
    constexpr std::int64_t log_position = -1;
    constexpr std::int64_t now_ns = 1'000'000;

    auto log = open_log(true);
    auto coherent = log.ensure_coherent(RECORDING_ID, initial_log_leadership_term_id, initial_term_base_log_position,
                                        leadership_term_id, term_base_log_position, log_position, now_ns, now_ns, 1);
    ASSERT_TRUE(coherent) << coherent.error().message.view();

    const RecordingLog::Entry* term_entry_0 = log.find_term_entry(0);
    ASSERT_NE(nullptr, term_entry_0);
    EXPECT_EQ(initial_term_base_log_position, term_entry_0->term_base_log_position);
    EXPECT_EQ(term_base_log_position, term_entry_0->log_position);

    const RecordingLog::Entry* term_entry_1 = log.find_term_entry(1);
    ASSERT_NE(nullptr, term_entry_1);
    EXPECT_EQ(leadership_term_id, term_entry_1->leadership_term_id);
    EXPECT_EQ(term_base_log_position, term_entry_1->term_base_log_position);
    EXPECT_EQ(-1, term_entry_1->log_position);

    EXPECT_EQ(2U, log.entries().size());
}

TEST_F(RecordingLogTest, shouldThrowIfLastTermIsUnfinishedAndTermBaseLogPositionIsNotSpecified)
{
    constexpr std::int64_t leadership_term_id = 4;
    constexpr std::int64_t log_position = 500;
    constexpr std::int64_t now_ns = 1'000'000;

    auto log = open_log(true);
    append_term(log, RECORDING_ID, leadership_term_id - 1, 0, now_ns);

    auto coherent =
        log.ensure_coherent(RECORDING_ID, 0, 0, leadership_term_id, NULL_POSITION, log_position, now_ns, now_ns, 1);
    EXPECT_FALSE(coherent);
}

TEST(RecordingLogValueTest, typeAsString)
{
    EXPECT_EQ("TERM", RecordingLog::type_as_string(0));
    EXPECT_EQ("SNAPSHOT", RecordingLog::type_as_string(1));
    EXPECT_EQ("UNKNOWN", RecordingLog::type_as_string(-5));
    EXPECT_EQ("UNKNOWN", RecordingLog::type_as_string(36542364));
}

TEST(RecordingLogValueTest, entryToString)
{
    const auto entry = make_entry(42, 5, 1024, 701, 1'000'000'000'000LL, 16, ENTRY_TYPE_SNAPSHOT, true, 2);
    EXPECT_EQ(
        "Entry{recordingId=42, leadershipTermId=5, termBaseLogPosition=1024, logPosition=701, "
        "timestamp=1000000000000, serviceId=16, type=SNAPSHOT, isValid=true, entryIndex=2}",
        entry.to_string());
}

TEST(RecordingLogValueTest, shouldDetermineIfSnapshotIsInvalid)
{
    const auto valid_snapshot = make_entry(42, 5, 1024, 701, 1'000'000'000'000LL, 16, ENTRY_TYPE_SNAPSHOT, true, 2);
    const auto invalid_snapshot = make_entry(42, 5, 1024, 701, 1'000'000'000'000LL, 16, ENTRY_TYPE_SNAPSHOT, false, 2);
    const auto term = make_entry(42, 5, 1024, 701, 1'000'000'000'000LL, 16, ENTRY_TYPE_TERM, true, 2);

    EXPECT_FALSE(RecordingLog::is_invalid_snapshot(valid_snapshot));
    EXPECT_TRUE(RecordingLog::is_invalid_snapshot(invalid_snapshot));
    EXPECT_FALSE(RecordingLog::is_invalid_snapshot(term));
}

TEST_F(RecordingLogTest, shouldInsertStandbySnapshotInRecordingLog)
{
    {
        auto log = open_log(true);
        append_snapshot(log, 1, 1, 0, 1000, 1'000'000'000, SERVICE_ID);
        append_snapshot(log, 2, 1, 0, 1000, 1'000'000'000, 0);

        append_standby_snapshot(log, 3, 2, 1000, 2000, 1'000'000'000, SERVICE_ID, "remotehost.aeron.io:20002");
        append_standby_snapshot(log, 4, 2, 1000, 2000, 1'000'000'000, 0, "remotehost.aeron.io:20002");

        append_snapshot(log, 5, 3, 2000, 3000, 1'000'000'000, SERVICE_ID);
        append_snapshot(log, 6, 3, 2000, 3000, 1'000'000'000, 0);
    }

    auto log = open_log(false);

    const auto assert_log_entry = [&log](std::int64_t recording_id, std::int32_t type, std::string_view endpoint)
    {
        bool found = false;
        for (const auto& entry : log.entries())
        {
            if (entry.recording_id == recording_id && entry.type == type && entry.archive_endpoint == endpoint)
            {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found);
    };

    assert_log_entry(1, ENTRY_TYPE_SNAPSHOT, "");
    assert_log_entry(2, ENTRY_TYPE_SNAPSHOT, "");
    assert_log_entry(3, ENTRY_TYPE_STANDBY_SNAPSHOT, "remotehost.aeron.io:20002");
    assert_log_entry(4, ENTRY_TYPE_STANDBY_SNAPSHOT, "remotehost.aeron.io:20002");
    assert_log_entry(5, ENTRY_TYPE_SNAPSHOT, "");
    assert_log_entry(6, ENTRY_TYPE_SNAPSHOT, "");
}

TEST_F(RecordingLogTest, shouldInvalidateLatestAnySnapshots)
{
    {
        auto log = open_log(true);
        append_term(log, 1, 1, 0, 1'000'000'000);
        append_term(log, 1, 2, 0, 1'000'000'000);
        append_term(log, 1, 3, 0, 1'000'000'000);
        append_term(log, 1, 4, 0, 1'000'000'000);

        append_snapshot(log, 1, 1, 0, 1000, 1'000'000'000, 0);
        append_snapshot(log, 2, 1, 0, 1000, 1'000'000'000, SERVICE_ID);

        append_standby_snapshot(log, 3, 2, 1000, 2000, 1'000'000'000, 0, "remotehost.aeron.io:20002");
        append_standby_snapshot(log, 4, 2, 1000, 2000, 1'000'000'000, SERVICE_ID, "remotehost.aeron.io:20002");

        append_snapshot(log, 5, 3, 2000, 3000, 1'000'000'000, 0);
        append_snapshot(log, 6, 3, 2000, 3000, 1'000'000'000, SERVICE_ID);

        append_standby_snapshot(log, 7, 4, 1000, 4000, 1'000'000'000, 0, "remotehost.aeron.io:20002");
        append_standby_snapshot(log, 8, 4, 1000, 4000, 1'000'000'000, SERVICE_ID, "remotehost.aeron.io:20002");
        append_snapshot(log, 9, 4, 2000, 4000, 1'000'000'000, 0);
        append_snapshot(log, 10, 4, 2000, 4000, 1'000'000'000, SERVICE_ID);

        const auto next_snapshot_index = [](const RecordingLog& log_ref)
        {
            for (std::int32_t i = static_cast<std::int32_t>(log_ref.entries().size()) - 1; i >= 0; --i)
            {
                if (RecordingLog::is_valid_any_snapshot(log_ref.entries()[static_cast<std::size_t>(i)]))
                {
                    return i;
                }
            }
            return -1;
        };

        EXPECT_EQ(13, next_snapshot_index(log));
    }

    auto log = open_log(false);

    const auto next_snapshot_index = [](const RecordingLog& log_ref)
    {
        for (std::int32_t i = static_cast<std::int32_t>(log_ref.entries().size()) - 1; i >= 0; --i)
        {
            if (RecordingLog::is_valid_any_snapshot(log_ref.entries()[static_cast<std::size_t>(i)]))
            {
                return i;
            }
        }
        return -1;
    };

    auto one = log.invalidate_latest_snapshot();
    ASSERT_TRUE(one) << one.error().message.view();
    EXPECT_EQ(8, next_snapshot_index(log));

    auto two = log.invalidate_latest_snapshot();
    ASSERT_TRUE(two) << two.error().message.view();
    EXPECT_EQ(5, next_snapshot_index(log));

    auto three = log.invalidate_latest_snapshot();
    ASSERT_TRUE(three) << three.error().message.view();
    EXPECT_EQ(2, next_snapshot_index(log));
}

TEST_F(RecordingLogTest, shouldNotIncludeStandbySnapshotInRecoveryPlan)
{
    {
        auto log = open_log(true);
        append_snapshot(log, 1, 1, 0, 1000, 1'000'000'000, SERVICE_ID);
        append_snapshot(log, 2, 1, 0, 1000, 1'000'000'000, 0);

        append_standby_snapshot(log, 3, 2, 1000, 2000, 1'000'000'000, SERVICE_ID, "remotehost.aeron.io:20002");
        append_standby_snapshot(log, 4, 2, 1000, 2000, 1'000'000'000, 0, "remotehost.aeron.io:20002");
    }

    auto log = open_log(false);
    RecordingLog::RecordingExtentSource source = [](std::int64_t)
    { return std::optional<RecordingLog::RecordingExtent>{RecordingLog::RecordingExtent{}}; };

    auto plan = log.create_recovery_plan(source, 1, NULL_VALUE);
    ASSERT_TRUE(plan) << plan.error().message.view();
    ASSERT_EQ(2U, plan->snapshots.size());
    EXPECT_TRUE(plan->snapshots[0].recording_id == 1 || plan->snapshots[0].recording_id == 2);
    EXPECT_TRUE(plan->snapshots[1].recording_id == 1 || plan->snapshots[1].recording_id == 2);
}

TEST_F(RecordingLogTest, shouldGetLatestStandbySnapshotsGroupedByEndpoint)
{
    {
        auto log = open_log(true);
        append_snapshot(log, 1, 1, 0, 1000, 1'000'000'000, SERVICE_ID);
        append_snapshot(log, 2, 1, 0, 1000, 1'000'000'000, 0);

        append_standby_snapshot(log, 5, 2, 500, 800, 1'000'000'000, SERVICE_ID, "remotehost0.aeron.io:20002");
        append_standby_snapshot(log, 6, 2, 500, 800, 1'000'000'000, 0, "remotehost0.aeron.io:20002");

        append_standby_snapshot(log, 3, 2, 1000, 2000, 1'000'000'000, SERVICE_ID, "remotehost0.aeron.io:20002");
        append_standby_snapshot(log, 4, 2, 1000, 2000, 1'000'000'000, 0, "remotehost0.aeron.io:20002");

        append_standby_snapshot(log, 3, 2, 1000, 2000, 1'000'000'000, SERVICE_ID, "remotehost1.aeron.io:20002");
        append_standby_snapshot(log, 4, 2, 1000, 2000, 1'000'000'000, 0, "remotehost1.aeron.io:20002");

        append_standby_snapshot(log, 10, 2, 3000, 4000, 1'000'000'000, 0, "remotehost0.aeron.io:20002");
        append_standby_snapshot(log, 11, 2, 3000, 4000, 1'000'000'000, 0, "remotehost1.aeron.io:20002");
    }

    auto log = open_log(false);

    const auto assert_log_entry = [&log](std::int64_t recording_id, std::int32_t type, std::string_view endpoint)
    {
        bool found = false;
        for (const auto& entry : log.entries())
        {
            if (entry.recording_id == recording_id && entry.type == type && entry.archive_endpoint == endpoint)
            {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found);
    };

    assert_log_entry(1, ENTRY_TYPE_SNAPSHOT, "");
    assert_log_entry(2, ENTRY_TYPE_SNAPSHOT, "");
    assert_log_entry(3, ENTRY_TYPE_STANDBY_SNAPSHOT, "remotehost0.aeron.io:20002");
    assert_log_entry(4, ENTRY_TYPE_STANDBY_SNAPSHOT, "remotehost0.aeron.io:20002");
    assert_log_entry(3, ENTRY_TYPE_STANDBY_SNAPSHOT, "remotehost1.aeron.io:20002");
    assert_log_entry(4, ENTRY_TYPE_STANDBY_SNAPSHOT, "remotehost1.aeron.io:20002");

    const auto standby_snapshots = log.latest_standby_snapshots(1);
    ASSERT_EQ(2U, standby_snapshots.size());
    EXPECT_EQ(2U, standby_snapshots.at("remotehost0.aeron.io:20002").size());
    EXPECT_EQ(2U, standby_snapshots.at("remotehost1.aeron.io:20002").size());
}

TEST_F(RecordingLogTest, shouldInvalidateLatestSnapshotIgnoringStandbySnapshots)
{
    {
        auto log = open_log(true);
        append_term(log, 1, 0, 0, 1'000'000'000);

        append_snapshot(log, 2, 0, 0, 1000, 1'000'000'000, SERVICE_ID);
        append_snapshot(log, 3, 0, 0, 1000, 1'000'000'000, 0);

        append_snapshot(log, 4, 0, 2, 2000, 1'000'000'000, SERVICE_ID);
        append_snapshot(log, 5, 0, 2, 2000, 1'000'000'000, 0);

        append_standby_snapshot(log, 15, 0, 500, 800, 1'000'000'000, SERVICE_ID, "remotehost0.aeron.io:20002");
        append_standby_snapshot(log, 16, 0, 500, 800, 1'000'000'000, 0, "remotehost0.aeron.io:20002");
    }

    auto log = open_log(false);
    const RecordingLog::Entry* latest = log.get_latest_snapshot(SERVICE_ID);
    ASSERT_NE(nullptr, latest);
    EXPECT_EQ(4, latest->recording_id);
    const auto pre_invalidate = log.latest_standby_snapshots(2);

    auto invalidated = log.invalidate_latest_snapshot();
    ASSERT_TRUE(invalidated) << invalidated.error().message.view();

    latest = log.get_latest_snapshot(SERVICE_ID);
    ASSERT_NE(nullptr, latest);
    EXPECT_EQ(2, latest->recording_id);

    const auto post_invalidate = log.latest_standby_snapshots(2);
    EXPECT_EQ(pre_invalidate, post_invalidate);
}

TEST_F(RecordingLogTest, shouldHandleEntriesStraddlingPageBoundary)
{
    const std::string endpoint(3079, 'a');

    {
        auto log = open_log(true);
        append_standby_snapshot(log, 1, 2, 1000, 2000, 1'000'000'000, SERVICE_ID, endpoint);
        append_standby_snapshot(log, 2, 2, 1000, 2000, 1'000'000'000, 0, endpoint);
        append_standby_snapshot(log, 3, 2, 1000, 2000, 1'000'000'000, 1, endpoint);
        auto appended =
            log.append(ENTRY_TYPE_TERM, 4, 3, 10000, 11111, 2'000'000'000, SERVICE_ID, RecordingLog::FILE_NAME);
        ASSERT_TRUE(appended) << appended.error().message.view();
        append_snapshot(log, 5, 4, 20000, 22222, 3'000'000'000, SERVICE_ID);
        append_snapshot(log, 6, 4, 20000, 22222, 3'000'000'000, 0);
    }

    auto log = open_log(false);

    const auto assert_log_entry =
        [&log](std::int64_t recording_id, std::int32_t type, std::string_view expected_endpoint)
    {
        bool found = false;
        for (const auto& entry : log.entries())
        {
            if (entry.recording_id == recording_id && entry.type == type && entry.archive_endpoint == expected_endpoint)
            {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found);
    };

    assert_log_entry(1, ENTRY_TYPE_STANDBY_SNAPSHOT, endpoint);
    assert_log_entry(2, ENTRY_TYPE_STANDBY_SNAPSHOT, endpoint);
    assert_log_entry(3, ENTRY_TYPE_STANDBY_SNAPSHOT, endpoint);
    assert_log_entry(4, ENTRY_TYPE_TERM, "");
    assert_log_entry(5, ENTRY_TYPE_SNAPSHOT, "");
    assert_log_entry(6, ENTRY_TYPE_SNAPSHOT, "");
}

TEST_F(RecordingLogTest, shouldFindOnlyStandbySnapshotsNewerThanTheLocalSnapshot)
{
    auto log = open_log(true);
    append_term(log, 1, 1, 0, 0);
    append_snapshot(log, 1, 1, 0, 1000, 0, SERVICE_ID);
    append_snapshot(log, 2, 1, 0, 1000, 0, 0);

    append_standby_snapshot(log, 3, 1, 0, 800, 0, SERVICE_ID, "old.aeron.io:20002");
    append_standby_snapshot(log, 4, 1, 0, 800, 0, 0, "old.aeron.io:20002");

    append_standby_snapshot(log, 5, 1, 0, 2000, 0, SERVICE_ID, "new.aeron.io:20002");
    append_standby_snapshot(log, 6, 1, 0, 2000, 0, 0, "new.aeron.io:20002");

    const auto newer = log.newer_standby_snapshots(1);
    ASSERT_EQ(1U, newer.size());
    EXPECT_EQ(2U, newer.at("new.aeron.io:20002").size());
}

TEST_F(RecordingLogTest, shouldRejectSnapshotEntryIfEndpointIsTooLong)
{
    const std::string endpoint(5000, 'a');
    auto log = open_log(true);

    auto result = log.append_standby_snapshot(1, 2, 1000, 2000, 1'000'000'000, SERVICE_ID, endpoint);
    ASSERT_FALSE(result);
    EXPECT_EQ("Endpoint is too long: 5000 vs 4044", result.error().message.view());
}

}  // namespace
