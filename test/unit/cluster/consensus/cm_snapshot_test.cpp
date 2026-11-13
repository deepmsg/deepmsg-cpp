// C4d: the consensus module's snapshot taker and adapter
// (`ConsensusModuleSnapshotTaker.java`, `ConsensusModuleSnapshotAdapter.java`,
// Aeron 1.54.0). The reference tests drive the taker through a mocked
// `ExclusivePublication`; here the seam is `SnapshotPublicationSink`.

#include <gtest/gtest.h>

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "cluster/common/cluster_codecs.h"
#include "cluster/consensus/cm_snapshot.h"
#include "cluster/consensus/log_publisher.h"
#include "cluster/consensus/pending_service_message_tracker.h"
#include "io_aeron_cluster_codecs/ClusterSession.h"
#include "io_aeron_cluster_codecs/ConsensusModule.h"
#include "io_aeron_cluster_codecs/PendingMessageTracker.h"
#include "io_aeron_cluster_codecs/SessionMessageHeader.h"
#include "io_aeron_cluster_codecs/SnapshotMarker.h"
#include "io_aeron_cluster_codecs/Timer.h"

namespace
{

using axc::core::Error;
using axc::core::ErrorHandler;
using deepmsg::cluster::common::message_header;
using deepmsg::cluster::common::MESSAGE_HEADER_LENGTH;
using deepmsg::cluster::common::wrap_body;
using deepmsg::cluster::consensus::CM_SNAPSHOT_TYPE_ID;
using deepmsg::cluster::consensus::ConsensusModuleSnapshotAdapter;
using deepmsg::cluster::consensus::ConsensusModuleSnapshotTaker;
using deepmsg::cluster::consensus::SnapshotLoadHandler;
using deepmsg::cluster::consensus::SnapshotPublicationSink;
using deepmsg::cluster::service::TimeUnit;

constexpr std::int64_t OFFER_BACK_PRESSURED = AERON_PUBLICATION_BACK_PRESSURED;

class FakePublication final : public SnapshotPublicationSink
{
public:
    explicit FakePublication(std::size_t max_payload_length = 1024) : max_payload_length_(max_payload_length) {}

    [[nodiscard]] std::expected<std::span<std::uint8_t>, std::int64_t> try_claim(std::size_t length) override
    {
        ++claim_calls_;
        if (length > max_payload_length_)
        {
            return std::unexpected(OFFER_BACK_PRESSURED);
        }
        claim_.assign(length, 0);
        claimed_ = true;
        return std::span<std::uint8_t>(claim_);
    }

    [[nodiscard]] std::expected<void, Error> commit() override
    {
        if (!claimed_)
        {
            return std::unexpected(axc::core::make_error(axc::core::ErrorCode::internal_error, "commit without claim"));
        }
        claimed_ = false;
        records_.push_back(claim_);
        return {};
    }

    [[nodiscard]] std::int64_t offer(std::span<const std::uint8_t> payload) override
    {
        ++offer_calls_;
        records_.emplace_back(payload.begin(), payload.end());
        return static_cast<std::int64_t>(payload.size());
    }

    [[nodiscard]] std::size_t max_payload_length() const override
    {
        return max_payload_length_;
    }

    [[nodiscard]] std::int64_t position() const override
    {
        std::int64_t position = 0;
        for (const auto& record : records_)
        {
            position += static_cast<std::int64_t>(record.size());
        }
        return position;
    }

    void idle() override
    {
        ++idle_calls_;
    }

    [[nodiscard]] const std::vector<std::vector<std::uint8_t>>& records() const
    {
        return records_;
    }

    [[nodiscard]] int claim_calls() const
    {
        return claim_calls_;
    }

    [[nodiscard]] int offer_calls() const
    {
        return offer_calls_;
    }

private:
    std::size_t max_payload_length_;
    std::vector<std::uint8_t> claim_{};
    bool claimed_{false};
    std::vector<std::vector<std::uint8_t>> records_{};
    int claim_calls_{0};
    int offer_calls_{0};
    int idle_calls_{0};
};

class FakeLogAppender final : public deepmsg::cluster::consensus::LogAppender
{
public:
    [[nodiscard]] long append_message(std::int64_t, std::int64_t, std::int64_t, const std::uint8_t*,
                                      std::size_t) override
    {
        return 1;
    }
    [[nodiscard]] long append_session_open(const deepmsg::cluster::consensus::ClusterSession&, std::int64_t,
                                           std::int64_t) override
    {
        return 1;
    }
    [[nodiscard]] bool append_session_close(deepmsg::cluster::consensus::ClusterSession&, std::int64_t,
                                            std::int64_t) override
    {
        return true;
    }
    [[nodiscard]] std::int64_t position() const override
    {
        return 0;
    }
};

class CountingErrorHandler final : public ErrorHandler
{
public:
    void on_error(const Error& error) noexcept override
    {
        ++count_;
        last_ = error;
    }

    [[nodiscard]] int count() const
    {
        return count_;
    }
    [[nodiscard]] const Error& last() const
    {
        return last_;
    }

private:
    int count_{0};
    Error last_{};
};

class RecordingLoadHandler final : public SnapshotLoadHandler
{
public:
    [[nodiscard]] std::expected<void, Error> on_load_begin_snapshot(std::int32_t app_version,
                                                                    TimeUnit time_unit) override
    {
        begin_snapshot = true;
        snapshot_app_version = app_version;
        snapshot_time_unit = time_unit;
        return {};
    }
    [[nodiscard]] std::expected<void, Error> on_load_end_snapshot() override
    {
        end_snapshot = true;
        return {};
    }
    [[nodiscard]] std::expected<void, Error> on_load_cluster_session(
        std::int64_t cluster_session_id, std::int64_t correlation_id, std::int64_t opened_log_position,
        std::int64_t time_of_last_activity, io::aeron::cluster::codecs::CloseReason::Value close_reason,
        std::int32_t response_stream_id, std::string_view response_channel) override
    {
        ++sessions;
        this->loaded_cluster_session_id = cluster_session_id;
        this->loaded_correlation_id = correlation_id;
        this->loaded_opened_log_position = opened_log_position;
        this->loaded_time_of_last_activity = time_of_last_activity;
        this->loaded_close_reason = close_reason;
        this->loaded_response_stream_id = response_stream_id;
        this->loaded_response_channel = std::string{response_channel};
        return {};
    }
    [[nodiscard]] std::expected<void, Error> on_load_consensus_module_state(
        std::int64_t next_session_id, std::int64_t next_service_session_id, std::int64_t log_service_session_id,
        std::int32_t pending_message_capacity) override
    {
        ++module_states;
        this->loaded_next_session_id = next_session_id;
        this->loaded_next_service_session_id = next_service_session_id;
        this->loaded_log_service_session_id = log_service_session_id;
        this->loaded_pending_message_capacity = pending_message_capacity;
        return {};
    }
    [[nodiscard]] std::expected<void, Error> on_load_pending_message_tracker(std::int64_t next_service_session_id,
                                                                             std::int64_t log_service_session_id,
                                                                             std::int32_t pending_message_capacity,
                                                                             std::int32_t service_id) override
    {
        ++trackers;
        tracker_next_service_session_id = next_service_session_id;
        tracker_log_service_session_id = log_service_session_id;
        tracker_pending_message_capacity = pending_message_capacity;
        tracker_service_id = service_id;
        return {};
    }
    [[nodiscard]] std::expected<void, Error> on_load_pending_message(std::int64_t cluster_session_id,
                                                                     std::span<const std::uint8_t> message) override
    {
        ++pending_messages;
        loaded_pending_message_session_id = cluster_session_id;
        loaded_pending_message_size = message.size();
        return {};
    }
    [[nodiscard]] std::expected<void, Error> on_load_timer(std::int64_t correlation_id, std::int64_t deadline) override
    {
        ++timers;
        timer_correlation_id = correlation_id;
        timer_deadline = deadline;
        return {};
    }

    bool begin_snapshot{false};
    bool end_snapshot{false};
    std::int32_t snapshot_app_version{0};
    TimeUnit snapshot_time_unit{TimeUnit::milliseconds};
    int sessions{0};
    std::int64_t loaded_cluster_session_id{0};
    std::int64_t loaded_correlation_id{0};
    std::int64_t loaded_opened_log_position{0};
    std::int64_t loaded_time_of_last_activity{0};
    io::aeron::cluster::codecs::CloseReason::Value loaded_close_reason{};
    std::int32_t loaded_response_stream_id{0};
    std::string loaded_response_channel{};
    int module_states{0};
    std::int64_t loaded_next_session_id{0};
    std::int64_t loaded_next_service_session_id{0};
    std::int64_t loaded_log_service_session_id{0};
    std::int32_t loaded_pending_message_capacity{0};
    int trackers{0};
    std::int64_t tracker_next_service_session_id{0};
    std::int64_t tracker_log_service_session_id{0};
    std::int32_t tracker_pending_message_capacity{0};
    std::int32_t tracker_service_id{0};
    int pending_messages{0};
    std::int64_t loaded_pending_message_session_id{0};
    std::size_t loaded_pending_message_size{0};
    int timers{0};
    std::int64_t timer_correlation_id{0};
    std::int64_t timer_deadline{0};
};

template <class Decoder>
Decoder decode(const std::vector<std::uint8_t>& record)
{
    auto header = message_header(record);
    EXPECT_TRUE(header.has_value());
    EXPECT_EQ(io::aeron::cluster::codecs::MessageHeader::sbeSchemaId(), header->schemaId());
    return wrap_body<Decoder>(std::span<const std::uint8_t>(record), *header);
}

ConsensusModuleSnapshotTaker make_taker(FakePublication& publication)
{
    return ConsensusModuleSnapshotTaker(publication, TimeUnit::milliseconds, 7);
}

TEST(ConsensusModuleSnapshotTakerTest, snapshotConsensusModuleState)
{
    FakePublication publication;
    auto taker = make_taker(publication);

    ASSERT_TRUE(taker.snapshot_consensus_module_state(11, 22, 33, 44));
    ASSERT_EQ(1u, publication.records().size());

    auto state = decode<io::aeron::cluster::codecs::ConsensusModule>(publication.records()[0]);
    EXPECT_EQ(11, state.nextSessionId());
    EXPECT_EQ(22, state.nextServiceSessionId());
    EXPECT_EQ(33, state.logServiceSessionId());
    EXPECT_EQ(44, state.pendingMessageCapacity());
}

TEST(ConsensusModuleSnapshotTakerTest, snapshotTimer)
{
    FakePublication publication;
    auto taker = make_taker(publication);

    taker.snapshot_timer(42, 1000);
    ASSERT_FALSE(taker.take_error().has_value());
    ASSERT_EQ(1u, publication.records().size());

    auto timer = decode<io::aeron::cluster::codecs::Timer>(publication.records()[0]);
    EXPECT_EQ(42, timer.correlationId());
    EXPECT_EQ(1000, timer.deadline());
}

TEST(ConsensusModuleSnapshotTakerTest, snapshotSessionShouldUseTryClaimIfDataFitsIntoMaxPayloadSize)
{
    FakePublication publication(1024);
    auto taker = make_taker(publication);
    deepmsg::cluster::consensus::ClusterSession session(0, 5, 7, "aeron:ipc", "client");
    session.opened_log_position(100);

    ASSERT_TRUE(taker.snapshot_session(session));
    ASSERT_EQ(1u, publication.records().size());
    EXPECT_EQ(1, publication.claim_calls());
    EXPECT_EQ(0, publication.offer_calls());

    auto record = decode<io::aeron::cluster::codecs::ClusterSession>(publication.records()[0]);
    EXPECT_EQ(5, record.clusterSessionId());
    EXPECT_EQ(0, record.correlationId());
    EXPECT_EQ(100, record.openedLogPosition());
    EXPECT_EQ(-1, record.timeOfLastActivity());
    EXPECT_EQ(7, record.responseStreamId());
    EXPECT_EQ("aeron:ipc", record.getResponseChannelAsString());
}

TEST(ConsensusModuleSnapshotTakerTest, snapshotSessionShouldUseOfferIfDataDoesNotFitIntoMaxPayloadSize)
{
    FakePublication publication(8);
    auto taker = make_taker(publication);
    deepmsg::cluster::consensus::ClusterSession session(0, 5, 7, "aeron:ipc", "client");

    ASSERT_TRUE(taker.snapshot_session(session));
    ASSERT_EQ(1u, publication.records().size());
    EXPECT_EQ(0, publication.claim_calls());
    EXPECT_EQ(1, publication.offer_calls());
}

TEST(ConsensusModuleSnapshotTakerTest, snapshotPendingServiceMessageTracker)
{
    FakePublication publication;
    auto taker = make_taker(publication);
    FakeLogAppender appender;
    deepmsg::cluster::consensus::PendingServiceMessageTracker tracker(1, appender, [] { return 0; });
    CountingErrorHandler error_handler;

    std::uint8_t message[MESSAGE_HEADER_LENGTH + io::aeron::cluster::codecs::SessionMessageHeader::SBE_BLOCK_LENGTH]{};
    io::aeron::cluster::codecs::SessionMessageHeader encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(message), 0, sizeof(message));
    encoder.clusterSessionId(0).timestamp(0);
    ASSERT_TRUE(tracker.enqueue_message(std::span<const std::uint8_t>(message, sizeof(message))));

    ASSERT_TRUE(taker.snapshot_tracker(tracker, error_handler));
    ASSERT_EQ(2u, publication.records().size());
    EXPECT_EQ(0, error_handler.count());

    auto header = decode<io::aeron::cluster::codecs::PendingMessageTracker>(publication.records()[0]);
    EXPECT_EQ(tracker.next_service_session_id(), header.nextServiceSessionId());
    EXPECT_EQ(tracker.log_service_session_id(), header.logServiceSessionId());
    EXPECT_EQ(tracker.size(), static_cast<std::size_t>(header.pendingMessageCapacity()));
    EXPECT_EQ(1, header.serviceId());

    auto pending = decode<io::aeron::cluster::codecs::SessionMessageHeader>(publication.records()[1]);
    EXPECT_EQ(tracker.log_service_session_id() + 1, pending.clusterSessionId());
}

TEST(ConsensusModuleSnapshotTakerTest, snapshotPendingServiceMessageTrackerWithServiceMessagesMissedByFollower)
{
    FakePublication publication;
    auto taker = make_taker(publication);
    FakeLogAppender appender;
    deepmsg::cluster::consensus::PendingServiceMessageTracker tracker(0, appender, [] { return 0; });
    CountingErrorHandler error_handler;

    // nextServiceSessionId=5, logServiceSessionId=10: the follower has missed
    // six, which is the reference's warning branch.
    ASSERT_TRUE(tracker.load_state(5, 10, 0));
    ASSERT_TRUE(taker.snapshot_tracker(tracker, error_handler));

    EXPECT_EQ(1, error_handler.count());
    EXPECT_NE(std::string::npos, error_handler.last().message.view().find("Follower has missed 6 service message(s)"));

    auto header = decode<io::aeron::cluster::codecs::PendingMessageTracker>(publication.records()[0]);
    EXPECT_EQ(11, header.nextServiceSessionId());
}

TEST(ConsensusModuleSnapshotAdapterTest, theTakersRecordsRoundTripThroughTheAdapter)
{
    FakePublication publication;
    auto taker = make_taker(publication);
    FakeLogAppender appender;

    deepmsg::cluster::consensus::PendingServiceMessageTracker tracker(0, appender, [] { return 0; });
    CountingErrorHandler error_handler;
    deepmsg::cluster::consensus::ClusterSession session(0, 5, 7, "aeron:ipc", "client");
    session.opened_log_position(100);

    std::uint8_t message[MESSAGE_HEADER_LENGTH + io::aeron::cluster::codecs::SessionMessageHeader::SBE_BLOCK_LENGTH]{};
    io::aeron::cluster::codecs::SessionMessageHeader encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(message), 0, sizeof(message));
    encoder.clusterSessionId(0).timestamp(0);
    ASSERT_TRUE(tracker.enqueue_message(std::span<const std::uint8_t>(message, sizeof(message))));

    ASSERT_TRUE(taker.mark_begin(100, 2));
    ASSERT_TRUE(taker.snapshot_consensus_module_state(11, 22, 33, 44));
    ASSERT_TRUE(taker.snapshot_session(session));
    taker.snapshot_timer(42, 1000);
    ASSERT_TRUE(taker.snapshot_tracker(tracker, error_handler));
    ASSERT_TRUE(taker.mark_end(100, 2));
    ASSERT_FALSE(taker.take_error().has_value());

    RecordingLoadHandler handler;
    ConsensusModuleSnapshotAdapter adapter;
    adapter.handler(handler);
    for (const auto& record : publication.records())
    {
        auto dispatched = adapter.dispatch(std::span<const std::uint8_t>(record));
        ASSERT_TRUE(dispatched) << dispatched.error().message.view();
    }

    EXPECT_TRUE(adapter.is_done());
    EXPECT_TRUE(handler.begin_snapshot);
    EXPECT_TRUE(handler.end_snapshot);
    EXPECT_EQ(7, handler.snapshot_app_version);
    EXPECT_EQ(TimeUnit::milliseconds, handler.snapshot_time_unit);
    EXPECT_EQ(1, handler.module_states);
    EXPECT_EQ(11, handler.loaded_next_session_id);
    EXPECT_EQ(1, handler.sessions);
    EXPECT_EQ(5, handler.loaded_cluster_session_id);
    EXPECT_EQ(100, handler.loaded_opened_log_position);
    EXPECT_EQ(1, handler.timers);
    EXPECT_EQ(42, handler.timer_correlation_id);
    EXPECT_EQ(1, handler.trackers);
    EXPECT_EQ(0, handler.tracker_service_id);
    EXPECT_EQ(1, handler.pending_messages);
}

TEST(ConsensusModuleSnapshotAdapterTest, aForeignSnapshotTypeIsRejected)
{
    FakePublication publication;
    auto taker = make_taker(publication);
    ASSERT_TRUE(taker.mark_begin(100, 2));

    auto record = publication.records()[0];
    // The container's snapshot type id is 2; rewrite it.
    auto header = message_header(record);
    ASSERT_TRUE(header.has_value());
    auto marker = wrap_body<io::aeron::cluster::codecs::SnapshotMarker>(std::span<const std::uint8_t>(record), *header);
    marker.typeId(2);

    RecordingLoadHandler handler;
    ConsensusModuleSnapshotAdapter adapter;
    adapter.handler(handler);
    auto dispatched = adapter.dispatch(std::span<const std::uint8_t>(record));
    ASSERT_FALSE(dispatched);
    EXPECT_NE(std::string::npos, dispatched.error().message.view().find("unexpected snapshot type: 2"));
}

TEST(ConsensusModuleSnapshotAdapterTest, theMarkersFollowTheReferenceOrder)
{
    FakePublication publication;
    auto taker = make_taker(publication);
    ASSERT_TRUE(taker.mark_begin(100, 2));
    ASSERT_TRUE(taker.mark_end(100, 2));

    EXPECT_EQ(CM_SNAPSHOT_TYPE_ID,
              decode<io::aeron::cluster::codecs::SnapshotMarker>(publication.records()[0]).typeId());
    EXPECT_EQ(io::aeron::cluster::codecs::SnapshotMark::BEGIN,
              decode<io::aeron::cluster::codecs::SnapshotMarker>(publication.records()[0]).mark());
    EXPECT_EQ(io::aeron::cluster::codecs::SnapshotMark::END,
              decode<io::aeron::cluster::codecs::SnapshotMarker>(publication.records()[1]).mark());
}

}  // namespace
