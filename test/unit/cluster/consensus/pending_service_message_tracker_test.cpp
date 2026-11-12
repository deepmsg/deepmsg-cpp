// C4c: `PendingServiceMessageTracker` (1.54.0) -- renumbering, the 20-per-poll
// limit, back pressure, the two sweeps and the snapshot load/verify path.
// Ported from the reference's `PendingServiceMessageTrackerTest` where the
// cases are the same, plus the poll/back-pressure shapes the Java test leaves
// to the agent.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "cluster/consensus/pending_service_message_tracker.h"

namespace
{

using deepmsg::cluster::consensus::LogAppender;
using deepmsg::cluster::consensus::PendingServiceMessageTracker;
using deepmsg::cluster::consensus::SESSION_HEADER_LENGTH;

constexpr std::size_t CLUSTER_SESSION_ID_OFFSET = 16;
constexpr std::size_t APPEND_POSITION_OFFSET = 24;
constexpr std::int64_t NOT_APPENDED_YET = std::numeric_limits<std::int64_t>::max();

class FakeLogPublisher final : public LogAppender
{
public:
    struct Append
    {
        std::int64_t leadership_term_id;
        std::int64_t cluster_session_id;
        std::int64_t timestamp;
        std::vector<std::uint8_t> payload;
    };

    std::vector<Append> appends;
    long next_position{100};
    long result{100};
    std::int64_t last_position{0};

    [[nodiscard]] long append_message(std::int64_t leadership_term_id, std::int64_t cluster_session_id,
                                      std::int64_t timestamp, const std::uint8_t* payload,
                                      std::size_t payload_length) override
    {
        if (result <= 0)
        {
            return result;
        }
        appends.push_back(Append{leadership_term_id, cluster_session_id, timestamp,
                                 std::vector<std::uint8_t>(payload, payload + payload_length)});
        last_position = result;
        return next_position++;
    }

    [[nodiscard]] long append_session_open(const deepmsg::cluster::consensus::ClusterSession&, std::int64_t,
                                           std::int64_t) override
    {
        return 0;
    }

    [[nodiscard]] bool append_session_close(deepmsg::cluster::consensus::ClusterSession&, std::int64_t,
                                            std::int64_t) override
    {
        return false;
    }

    [[nodiscard]] std::int64_t position() const override
    {
        return 0;
    }
};

[[nodiscard]] std::vector<std::uint8_t> message_with_payload(std::string_view payload)
{
    std::vector<std::uint8_t> message(SESSION_HEADER_LENGTH + payload.size(), 0);
    std::memcpy(message.data() + SESSION_HEADER_LENGTH, payload.data(), payload.size());
    return message;
}

void write_i64(std::vector<std::uint8_t>& message, std::size_t offset, std::int64_t value)
{
    const auto bits = static_cast<std::uint64_t>(value);
    for (std::size_t i = 0; i < sizeof(std::uint64_t); i++)
    {
        message[offset + i] = static_cast<std::uint8_t>(bits >> (8 * i));
    }
}

[[nodiscard]] std::int64_t read_stored(const PendingServiceMessageTracker& tracker, std::size_t offset,
                                       std::size_t index = 0)
{
    std::int64_t value = 0;
    std::size_t seen = 0;
    (void)tracker.pending_messages().for_each(
        0,
        [&value, &seen, offset, index](std::span<const std::uint8_t> message, std::size_t)
        {
            if (seen == index)
            {
                std::uint64_t bits = 0;
                for (std::size_t i = 0; i < sizeof(std::uint64_t); i++)
                {
                    bits |= static_cast<std::uint64_t>(message[offset + i]) << (8 * i);
                }
                value = static_cast<std::int64_t>(bits);
                return false;
            }
            ++seen;
            return true;
        },
        index + 1);
    return value;
}

class PendingServiceMessageTrackerTest : public ::testing::Test
{
protected:
    FakeLogPublisher publisher_{};
    PendingServiceMessageTracker tracker_{0, publisher_, [] { return std::int64_t{200}; }};
};

TEST_F(PendingServiceMessageTrackerTest, enqueueRenumbersTheSessionAndMarksItNotAppended)
{
    const auto first = message_with_payload("first");
    const auto second = message_with_payload("second");
    ASSERT_TRUE(tracker_.enqueue_message(first));
    ASSERT_TRUE(tracker_.enqueue_message(second));

    EXPECT_EQ(NOT_APPENDED_YET, read_stored(tracker_, APPEND_POSITION_OFFSET));

    EXPECT_EQ(2, tracker_.poll());
    ASSERT_EQ(2u, publisher_.appends.size());
    EXPECT_EQ(PendingServiceMessageTracker::service_session_id(0, std::numeric_limits<std::int64_t>::min()) + 1,
              publisher_.appends[0].cluster_session_id);
    EXPECT_EQ(PendingServiceMessageTracker::service_session_id(0, std::numeric_limits<std::int64_t>::min()) + 2,
              publisher_.appends[1].cluster_session_id);
    EXPECT_EQ(std::int64_t{200}, publisher_.appends[0].timestamp);
    EXPECT_EQ("first", std::string(publisher_.appends[0].payload.begin(), publisher_.appends[0].payload.end()));

    // The stored timestamp now carries the append position of each message.
    EXPECT_EQ(std::int64_t{100}, read_stored(tracker_, APPEND_POSITION_OFFSET, 0));
    EXPECT_EQ(std::int64_t{101}, read_stored(tracker_, APPEND_POSITION_OFFSET, 1));
}

TEST_F(PendingServiceMessageTrackerTest, pollAppendsAtMostTheReferenceLimit)
{
    for (int i = 0; i < 25; i++)
    {
        ASSERT_TRUE(tracker_.enqueue_message(message_with_payload("x")));
    }

    EXPECT_EQ(20, tracker_.poll());
    EXPECT_EQ(20u, publisher_.appends.size());
    EXPECT_EQ(5, tracker_.poll());
    EXPECT_EQ(25u, publisher_.appends.size());
}

TEST_F(PendingServiceMessageTrackerTest, aBackPressuredAppendIsRetriedFromTheSameMessage)
{
    publisher_.result = 0;  // back pressure
    ASSERT_TRUE(tracker_.enqueue_message(message_with_payload("a")));
    ASSERT_TRUE(tracker_.enqueue_message(message_with_payload("b")));

    EXPECT_EQ(0, tracker_.poll());
    EXPECT_TRUE(publisher_.appends.empty());

    publisher_.result = 100;
    EXPECT_EQ(2, tracker_.poll());
    ASSERT_EQ(2u, publisher_.appends.size());
    EXPECT_EQ("a", std::string(publisher_.appends[0].payload.begin(), publisher_.appends[0].payload.end()));
    EXPECT_EQ("b", std::string(publisher_.appends[1].payload.begin(), publisher_.appends[1].payload.end()));
}

TEST_F(PendingServiceMessageTrackerTest, sweepLeaderMessagesDropsOnlyTheCommittedPrefix)
{
    ASSERT_TRUE(tracker_.enqueue_message(message_with_payload("a")));
    ASSERT_TRUE(tracker_.enqueue_message(message_with_payload("b")));
    ASSERT_EQ(2, tracker_.poll());

    // Position 100 (the first) is committed, 101 is not.
    tracker_.sweep_leader_messages(100);
    EXPECT_EQ(48u, tracker_.size());
    EXPECT_EQ(PendingServiceMessageTracker::service_session_id(0, std::numeric_limits<std::int64_t>::min()) + 1,
              tracker_.log_service_session_id());

    tracker_.sweep_leader_messages(101);
    EXPECT_EQ(0u, tracker_.size());
}

TEST_F(PendingServiceMessageTrackerTest, sweepFollowerMessagesDropsAtOrBelow)
{
    auto first = message_with_payload("a");
    auto second = message_with_payload("b");
    const std::int64_t first_id = PendingServiceMessageTracker::service_session_id(0, 1);
    const std::int64_t second_id = PendingServiceMessageTracker::service_session_id(0, 2);
    write_i64(first, CLUSTER_SESSION_ID_OFFSET, first_id);
    write_i64(second, CLUSTER_SESSION_ID_OFFSET, second_id);
    ASSERT_TRUE(tracker_.append_message(first));
    ASSERT_TRUE(tracker_.append_message(second));

    tracker_.sweep_follower_messages(first_id);
    EXPECT_EQ(48u, tracker_.size());
    tracker_.sweep_follower_messages(second_id);
    EXPECT_EQ(0u, tracker_.size());
}

TEST_F(PendingServiceMessageTrackerTest, restoreUncommittedMessagesResetsTheQueue)
{
    ASSERT_TRUE(tracker_.enqueue_message(message_with_payload("a")));
    ASSERT_EQ(1, tracker_.poll());
    ASSERT_EQ(std::int64_t{100}, read_stored(tracker_, APPEND_POSITION_OFFSET));

    tracker_.restore_uncommitted_messages(0);
    EXPECT_EQ(NOT_APPENDED_YET, read_stored(tracker_, APPEND_POSITION_OFFSET));

    publisher_.appends.clear();
    EXPECT_EQ(1, tracker_.poll());
    ASSERT_EQ(1u, publisher_.appends.size());
}

TEST_F(PendingServiceMessageTrackerTest, snapshotEmpty)
{
    const std::int64_t log_service_session_id =
        PendingServiceMessageTracker::service_session_id(0, std::numeric_limits<std::int64_t>::min());
    PendingServiceMessageTracker loaded(0, publisher_, [] { return std::int64_t{200}; });
    ASSERT_TRUE(loaded.load_state(log_service_session_id + 1, log_service_session_id, 0));
    auto verified = loaded.verify();
    EXPECT_TRUE(verified) << verified.error().message.view();
}

TEST_F(PendingServiceMessageTrackerTest, loadValid)
{
    PendingServiceMessageTracker loaded(0, publisher_, [] { return std::int64_t{200}; });
    ASSERT_TRUE(loaded.load_state(4, 1, 64));

    auto first = message_with_payload("a");
    auto second = message_with_payload("b");
    write_i64(first, CLUSTER_SESSION_ID_OFFSET, PendingServiceMessageTracker::service_session_id(0, 2));
    write_i64(second, CLUSTER_SESSION_ID_OFFSET, PendingServiceMessageTracker::service_session_id(0, 3));
    ASSERT_TRUE(loaded.append_message(first));
    ASSERT_TRUE(loaded.append_message(second));

    auto verified = loaded.verify();
    EXPECT_TRUE(verified) << verified.error().message.view();
}

TEST_F(PendingServiceMessageTrackerTest, loadInvalid)
{
    PendingServiceMessageTracker loaded(0, publisher_, [] { return std::int64_t{200}; });
    ASSERT_TRUE(loaded.load_state(4, 1, 64));

    auto message = message_with_payload("a");
    write_i64(message, CLUSTER_SESSION_ID_OFFSET, PendingServiceMessageTracker::service_session_id(0, 9));
    ASSERT_TRUE(loaded.append_message(message));

    auto verified = loaded.verify();
    ASSERT_FALSE(verified);
    EXPECT_NE(std::string::npos, verified.error().message.view().find("snapshot has incorrect pending message"));
}

TEST_F(PendingServiceMessageTrackerTest, theServiceIdPackingsAreTheReferences)
{
    const std::int64_t log_message = PendingServiceMessageTracker::service_session_id(3, 0x1234);
    EXPECT_EQ(3, PendingServiceMessageTracker::service_id_from_log_message(log_message));
    EXPECT_EQ(3, PendingServiceMessageTracker::service_id_from_service_message(3));
    EXPECT_EQ(-1, PendingServiceMessageTracker::service_id_from_service_message(-1));
}

}  // namespace
