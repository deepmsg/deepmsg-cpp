// A4b-5 criteria: `ReplicationSession` (`ReplicationSession.java:60-1010`,
// 1.54.0 = 454da1e4e6). The C suite drives the real thing end to end; these
// cases pin the state machine, the destination row and the signal order
// without a second archive.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "archive/server/channel_uri.h"
#include "archive/server/replication_session.h"

namespace
{

using deepmsg::archive::server::CachedEpochClock;
using deepmsg::archive::server::ExtendRecordingOutcome;
using deepmsg::archive::server::replication_extend_channel;
using deepmsg::archive::server::replication_replay_channel;
using deepmsg::archive::server::ReplicationConfig;
using deepmsg::archive::server::ReplicationControl;
using deepmsg::archive::server::ReplicationSession;
using deepmsg::archive::server::ReplicationSource;
using deepmsg::archive::server::SourceRecordingDescriptor;
using State = ReplicationSession::State;
using Signal = deepmsg::archive::server::archive_codecs::RecordingSignal::Value;

constexpr std::int64_t NULL_VALUE = -1;

SourceRecordingDescriptor descriptor(std::int64_t start, std::int64_t stop)
{
    SourceRecordingDescriptor value;
    value.recording_id = 7;
    value.control_session_id = 42;
    value.start_timestamp = 1000;
    value.stop_timestamp = 2000;
    value.start_position = start;
    value.stop_position = stop;
    value.initial_term_id = 11;
    value.segment_file_length = 128 * 1024;
    value.term_buffer_length = 64 * 1024;
    value.mtu_length = 1408;
    value.session_id = 33;
    value.stream_id = 10;
    value.stripped_channel = "aeron:udp?endpoint=localhost:8000";
    value.original_channel = "aeron:udp?endpoint=localhost:8000";
    value.source_identity = "";
    return value;
}

class FakeSource final : public ReplicationSource
{
public:
    int do_work() override
    {
        ++work_calls;
        connected = work_calls >= connect_on_pass;
        return connected ? 1 : 0;
    }

    [[nodiscard]] bool is_connected() const noexcept override
    {
        return connected;
    }

    [[nodiscard]] bool is_failed() const noexcept override
    {
        return failed;
    }

    [[nodiscard]] std::string_view error_message() const noexcept override
    {
        return error;
    }

    [[nodiscard]] std::int64_t control_session_id() const override
    {
        return control_session;
    }

    [[nodiscard]] std::int64_t next_correlation_id() override
    {
        return ++correlation;
    }

    bool list_recording(std::int64_t correlation_id, std::int64_t recording_id) override
    {
        ++list_calls;
        list_correlation = correlation_id;
        list_recording_id = recording_id;
        return list_result;
    }

    bool replay(std::int64_t correlation_id, std::int64_t recording_id, std::string_view replay_channel,
                std::int32_t replay_stream_id, std::int64_t position, std::int64_t length,
                std::int32_t file_io_max_length) override
    {
        ++replay_calls;
        replay_correlation = correlation_id;
        replay_channel_value = std::string{replay_channel};
        replay_stream = replay_stream_id;
        replay_position = position;
        replay_length = length;
        replay_file_io_max_length = file_io_max_length;
        return replay_result;
    }

    bool stop_replay(std::int64_t correlation_id, std::int64_t replay_session_id) override
    {
        (void)correlation_id;
        ++stop_replay_calls;
        stopped_replay_session_id = replay_session_id;
        return true;
    }

    bool get_recording_position(std::int64_t correlation_id, std::int64_t recording_id) override
    {
        ++position_calls;
        position_correlation = correlation_id;
        position_recording_id = recording_id;
        return true;
    }

    [[nodiscard]] std::expected<Response, axc::core::Error> poll_response() override
    {
        if (response_index >= responses.size())
        {
            return Response{};
        }
        return responses[response_index++];
    }

    void reset_descriptor_poller(std::int64_t correlation_id, std::int32_t record_count,
                                 std::function<void(SourceRecordingDescriptor&)> consumer) override
    {
        descriptor_correlation = correlation_id;
        descriptor_count = record_count;
        descriptor_consumer = std::move(consumer);
    }

    [[nodiscard]] std::expected<int, axc::core::Error> poll_descriptors() override
    {
        int polled = 0;
        while (!descriptors.empty())
        {
            SourceRecordingDescriptor next = descriptors.front();
            descriptors.erase(descriptors.begin());
            if (descriptor_consumer)
            {
                descriptor_consumer(next);
            }
            ++polled;
        }
        return polled;
    }

    [[nodiscard]] bool descriptor_dispatch_complete() const noexcept override
    {
        return dispatch_complete;
    }

    [[nodiscard]] std::int32_t descriptor_remaining_record_count() const noexcept override
    {
        return remaining_record_count;
    }

    std::vector<Response> responses;
    std::vector<SourceRecordingDescriptor> descriptors;
    int connect_on_pass{1};
    int work_calls{0};
    bool connected{false};
    bool failed{false};
    std::string error;
    std::int64_t control_session{42};
    std::int64_t correlation{0};
    int list_calls{0};
    bool list_result{true};
    std::int64_t list_correlation{0};
    std::int64_t list_recording_id{0};
    std::int64_t descriptor_correlation{0};
    std::int32_t descriptor_count{0};
    bool dispatch_complete{true};
    std::int32_t remaining_record_count{0};
    std::function<void(SourceRecordingDescriptor&)> descriptor_consumer;
    size_t response_index{0};
    int replay_calls{0};
    bool replay_result{true};
    std::int64_t replay_correlation{0};
    std::string replay_channel_value;
    std::int32_t replay_stream{0};
    std::int64_t replay_position{0};
    std::int64_t replay_length{0};
    std::int32_t replay_file_io_max_length{0};
    int stop_replay_calls{0};
    std::int64_t stopped_replay_session_id{0};
    int position_calls{0};
    std::int64_t position_correlation{0};
    std::int64_t position_recording_id{0};
};

class FakeControl final : public ReplicationControl
{
public:
    void send_replication_signal(std::int64_t control_session_id, std::int64_t replication_id,
                                 std::int64_t recording_id, std::int64_t subscription_id, std::int64_t position,
                                 Signal signal) override
    {
        signals.push_back({control_session_id, replication_id, recording_id, subscription_id, position, signal});
    }

    void send_replication_error(std::int64_t control_session_id, std::int64_t replication_id, std::int64_t error_code,
                                std::string_view message) override
    {
        errors.push_back({control_session_id, replication_id, error_code, std::string{message}});
    }

    [[nodiscard]] bool stop_recording_subscription(std::int64_t registration_id) override
    {
        stopped_subscriptions.push_back(registration_id);
        return true;
    }

    [[nodiscard]] std::int64_t catalog_stop_position(std::int64_t recording_id) override
    {
        (void)recording_id;
        return catalog_stop;
    }

    [[nodiscard]] std::expected<std::int64_t, std::string> add_new_recording(
        std::int64_t start_position, std::int64_t stop_position, std::int64_t start_timestamp,
        std::int64_t stop_timestamp, std::int32_t image_initial_term_id, std::int32_t segment_file_length,
        std::int32_t term_buffer_length, std::int32_t mtu_length, std::int32_t session_id, std::int32_t stream_id,
        std::string_view stripped_channel, std::string_view original_channel, std::string_view source_identity) override
    {
        AddedRow row;
        row.start_position = start_position;
        row.stop_position = stop_position;
        row.start_timestamp = start_timestamp;
        row.stop_timestamp = stop_timestamp;
        row.initial_term_id = image_initial_term_id;
        row.segment_file_length = segment_file_length;
        row.term_buffer_length = term_buffer_length;
        row.mtu_length = mtu_length;
        row.session_id = session_id;
        row.stream_id = stream_id;
        row.stripped_channel = std::string{stripped_channel};
        row.original_channel = std::string{original_channel};
        row.source_identity = std::string{source_identity};
        added_rows.push_back(row);
        return next_recording_id++;
    }

    [[nodiscard]] std::expected<void, std::string> replace_recording(
        std::int64_t recording_id, std::int64_t start_position, std::int64_t stop_position,
        std::int64_t start_timestamp, std::int64_t stop_timestamp, std::int32_t image_initial_term_id,
        std::int32_t segment_file_length, std::int32_t term_buffer_length, std::int32_t mtu_length,
        std::int32_t session_id, std::int32_t stream_id, std::string_view stripped_channel,
        std::string_view original_channel, std::string_view source_identity) override
    {
        (void)start_timestamp;
        (void)stop_timestamp;
        (void)image_initial_term_id;
        (void)segment_file_length;
        (void)term_buffer_length;
        (void)mtu_length;
        (void)session_id;
        (void)stream_id;
        (void)stripped_channel;
        (void)original_channel;
        (void)source_identity;
        replace_calls.push_back({recording_id, start_position, stop_position});
        return {};
    }

    [[nodiscard]] std::expected<std::int64_t, std::string> start_extend_recording(
        std::int64_t control_session_id, std::int64_t correlation_id, std::int64_t recording_id, std::int32_t stream_id,
        std::int32_t source_location, bool auto_stop, std::string_view original_channel) override
    {
        extend_control_session_id = control_session_id;
        extend_correlation_id = correlation_id;
        extend_recording_id = recording_id;
        extend_stream_id = stream_id;
        extend_source_location = source_location;
        extend_auto_stop = auto_stop;
        extend_channel = std::string{original_channel};
        return extend_token;
    }

    [[nodiscard]] std::optional<ExtendRecordingOutcome> take_extend_recording_outcome(std::int64_t token) override
    {
        if (token != extend_token || !outcome_ready)
        {
            return std::nullopt;
        }
        return outcome;
    }

    void cancel_extend_recording(std::int64_t token) override
    {
        cancelled_tokens.push_back(token);
    }

    [[nodiscard]] aeron_image_t* image_by_session_id(std::int64_t registration_id, std::int32_t session_id) override
    {
        image_registration_id = registration_id;
        image_session_id = session_id;
        return image;
    }

    [[nodiscard]] std::int64_t image_position(aeron_image_t* value) override
    {
        EXPECT_EQ(image, value);
        return position_value;
    }

    [[nodiscard]] bool image_is_closed(aeron_image_t* value) override
    {
        EXPECT_EQ(image, value);
        return image_closed;
    }

    [[nodiscard]] bool image_is_end_of_stream(aeron_image_t* value) override
    {
        EXPECT_EQ(image, value);
        return image_end_of_stream;
    }

    [[nodiscard]] std::optional<std::string> subscription_resolved_endpoint(std::int64_t registration_id) override
    {
        (void)registration_id;
        return endpoint;
    }

    struct SignalEvent
    {
        std::int64_t control_session_id{0};
        std::int64_t replication_id{0};
        std::int64_t recording_id{0};
        std::int64_t subscription_id{0};
        std::int64_t position{0};
        Signal signal{deepmsg::archive::server::archive_codecs::RecordingSignal::START};
    };

    struct ErrorEvent
    {
        std::int64_t control_session_id{0};
        std::int64_t replication_id{0};
        std::int64_t error_code{0};
        std::string message;
    };

    struct AddedRow
    {
        std::int64_t start_position{0};
        std::int64_t stop_position{0};
        std::int64_t start_timestamp{0};
        std::int64_t stop_timestamp{0};
        std::int32_t initial_term_id{0};
        std::int32_t segment_file_length{0};
        std::int32_t term_buffer_length{0};
        std::int32_t mtu_length{0};
        std::int32_t session_id{0};
        std::int32_t stream_id{0};
        std::string stripped_channel;
        std::string original_channel;
        std::string source_identity;
    };

    struct ReplaceCall
    {
        std::int64_t recording_id{0};
        std::int64_t start_position{0};
        std::int64_t stop_position{0};
    };

    std::vector<SignalEvent> signals;
    std::vector<ErrorEvent> errors;
    std::vector<std::int64_t> stopped_subscriptions;
    std::vector<AddedRow> added_rows;
    std::vector<ReplaceCall> replace_calls;
    std::vector<std::int64_t> cancelled_tokens;

    std::int64_t catalog_stop{0};
    std::int64_t next_recording_id{100};
    std::int64_t extend_token{5};
    bool outcome_ready{false};
    ExtendRecordingOutcome outcome{55, {}};
    std::int64_t extend_control_session_id{0};
    std::int64_t extend_correlation_id{0};
    std::int64_t extend_recording_id{0};
    std::int32_t extend_stream_id{0};
    std::int32_t extend_source_location{0};
    bool extend_auto_stop{false};
    std::string extend_channel;

    aeron_image_t* image{reinterpret_cast<aeron_image_t*>(1)};
    std::int64_t position_value{0};
    bool image_closed{false};
    bool image_end_of_stream{false};
    std::int64_t image_registration_id{0};
    std::int32_t image_session_id{0};
    std::optional<std::string> endpoint{"127.0.0.1:9000"};
};

ReplicationConfig make_config()
{
    ReplicationConfig config;
    config.control_session_id = 11;
    config.replication_id = 3;
    config.src_recording_id = 7;
    config.dst_recording_id = NULL_VALUE;
    config.stop_position = NULL_VALUE;
    config.file_io_max_length = 4096;
    config.replication_session_id = 1;
    config.replication_channel = "aeron:udp?endpoint=localhost:8012";
    config.action_timeout_ms = 1000;
    return config;
}

struct Fixture
{
    Fixture()
    {
        config.clock = &clock;
        create();
    }

    /// Rebuilds the session from the (possibly mutated) config.
    void recreate()
    {
        session.reset();
        create();
    }

    void create()
    {
        auto fake = std::make_unique<FakeSource>();
        source = fake.get();
        session = std::make_unique<ReplicationSession>(config, std::move(fake), control);
    }

    CachedEpochClock clock{1000};
    ReplicationConfig config{make_config()};
    FakeControl control;
    FakeSource* source{nullptr};
    std::unique_ptr<ReplicationSession> session;
};

TEST(ReplicationSessionTest, connectThenDescriptorBuildsTheDestinationRowAndReplicates)
{
    Fixture fixture;
    fixture.source->descriptors.push_back(descriptor(0, 128));
    fixture.session->do_work();  // CONNECT
    ASSERT_EQ(State::replicate_descriptor, fixture.session->state());

    fixture.session->do_work();  // listRecording
    ASSERT_EQ(State::replicate_descriptor, fixture.session->state());
    EXPECT_EQ(1, fixture.source->list_calls);
    EXPECT_EQ(7, fixture.source->list_recording_id);

    fixture.session->do_work();  // descriptor poll → add row, REPLICATE, EXTEND
    ASSERT_EQ(State::extend, fixture.session->state());
    ASSERT_EQ(1u, fixture.control.added_rows.size());
    EXPECT_EQ(0, fixture.control.added_rows[0].start_position);
    EXPECT_EQ(0, fixture.control.added_rows[0].stop_position);
    EXPECT_EQ(33, fixture.control.added_rows[0].session_id);
    EXPECT_EQ(10, fixture.control.added_rows[0].stream_id);
    EXPECT_EQ(1408, fixture.control.added_rows[0].mtu_length);
    EXPECT_EQ(100, fixture.session->dst_recording_id());

    ASSERT_EQ(1u, fixture.control.signals.size());
    EXPECT_EQ(deepmsg::archive::server::archive_codecs::RecordingSignal::REPLICATE, fixture.control.signals[0].signal);
    EXPECT_EQ(0, fixture.control.signals[0].position);
    EXPECT_EQ(100, fixture.control.signals[0].recording_id);
    EXPECT_EQ(NULL_VALUE, fixture.control.signals[0].subscription_id);
    EXPECT_EQ(3, fixture.control.signals[0].replication_id);
}

TEST(ReplicationSessionTest, extendStartsTheDestinationSubscriptionAndReplayFollowsTheSource)
{
    Fixture fixture;
    fixture.source->descriptors.push_back(descriptor(0, 128));
    fixture.session->do_work();
    fixture.session->do_work();
    fixture.session->do_work();
    ASSERT_EQ(State::extend, fixture.session->state());

    fixture.session->do_work();  // start the add, outcome still pending
    ASSERT_EQ(State::extend, fixture.session->state());
    EXPECT_EQ(11, fixture.control.extend_control_session_id);
    EXPECT_EQ(3, fixture.control.extend_correlation_id);  // the replication id
    EXPECT_EQ(100, fixture.control.extend_recording_id);
    EXPECT_EQ(10, fixture.control.extend_stream_id);
    EXPECT_EQ(1, fixture.control.extend_source_location);  // REMOTE
    EXPECT_TRUE(fixture.control.extend_auto_stop);
    EXPECT_NE(std::string::npos, fixture.control.extend_channel.find("session-id=1"));
    EXPECT_NE(std::string::npos, fixture.control.extend_channel.find("rejoin=false"));

    fixture.control.outcome_ready = true;
    fixture.session->do_work();  // outcome → REPLAY_TOKEN, then passes through
    ASSERT_EQ(State::replay_token, fixture.session->state());
    EXPECT_EQ(55, fixture.session->recording_subscription_id());

    fixture.session->do_work();  // → GET_ARCHIVE_PROXY
    fixture.session->do_work();  // → REPLAY
    ASSERT_EQ(State::replay, fixture.session->state());

    fixture.session->do_work();  // offer the replay
    EXPECT_EQ(1, fixture.source->replay_calls);
    EXPECT_EQ(0, fixture.source->replay_position);
    EXPECT_EQ(NULL_VALUE, fixture.source->replay_length);  // follow the source
    EXPECT_EQ(4096, fixture.source->replay_file_io_max_length);
    EXPECT_NE(std::string::npos, fixture.source->replay_channel_value.find("session-id=1"));

    fixture.source->responses.push_back(
        {ReplicationSource::Response::Kind::ok, 42, fixture.source->replay_correlation, 44, 0, {}, {}});
    fixture.session->do_work();  // OK → AWAIT_IMAGE
    ASSERT_EQ(State::await_image, fixture.session->state());

    fixture.session->do_work();  // image found → REPLICATE
    ASSERT_EQ(State::replicate, fixture.session->state());
    EXPECT_EQ(55, fixture.control.image_registration_id);
    EXPECT_EQ(44, fixture.control.image_session_id);

    fixture.control.position_value = 128;
    fixture.session->do_work();  // caught up with the source stop → SYNC → DONE
    ASSERT_EQ(State::done, fixture.session->state());
    EXPECT_TRUE(fixture.session->is_done());

    const auto& last_signal = fixture.control.signals.back();
    EXPECT_EQ(deepmsg::archive::server::archive_codecs::RecordingSignal::SYNC, last_signal.signal);
    EXPECT_EQ(128, last_signal.position);

    fixture.session->close();
    EXPECT_EQ(1u, fixture.control.stopped_subscriptions.size());
    EXPECT_EQ(55, fixture.control.stopped_subscriptions[0]);
    EXPECT_EQ(0, fixture.source->stop_replay_calls);  // REPLICATE cleared the id before DONE

    const auto replicate_end_count = std::count_if(
        fixture.control.signals.begin(), fixture.control.signals.end(), [](const FakeControl::SignalEvent& event)
        { return event.signal == deepmsg::archive::server::archive_codecs::RecordingSignal::REPLICATE_END; });
    EXPECT_EQ(1, replicate_end_count);

    fixture.session->close();  // idempotent
    EXPECT_EQ(1u, fixture.control.stopped_subscriptions.size());
}

TEST(ReplicationSessionTest, anEmptyDestinationRecordingIsReplacedWithoutAReplicateSignal)
{
    Fixture fixture;
    fixture.config.dst_recording_id = 77;
    fixture.config.has_dst_recording = true;
    fixture.config.dst_start_position = 0;
    fixture.config.dst_stop_position = 0;
    fixture.config.dst_stream_id = 10;
    fixture.recreate();
    fixture.source->descriptors.push_back(descriptor(0, 128));

    fixture.session->do_work();
    fixture.session->do_work();
    fixture.session->do_work();  // descriptor → replace

    ASSERT_EQ(1u, fixture.control.replace_calls.size());
    EXPECT_EQ(77, fixture.control.replace_calls[0].recording_id);
    // An existing (empty) destination row is replaced, not added, so there is
    // no REPLICATE signal (`ReplicationSession.java:378-394`).
    EXPECT_TRUE(fixture.control.signals.empty());
    EXPECT_EQ(State::extend, fixture.session->state());
}

TEST(ReplicationSessionTest, anInSyncDescriptorSignalsSyncAndStops)
{
    Fixture fixture;
    fixture.source->descriptors.push_back(descriptor(0, 0));
    fixture.session->do_work();
    fixture.session->do_work();
    fixture.session->do_work();

    ASSERT_EQ(State::done, fixture.session->state());
    ASSERT_EQ(2u, fixture.control.signals.size());
    EXPECT_EQ(deepmsg::archive::server::archive_codecs::RecordingSignal::REPLICATE, fixture.control.signals[0].signal);
    EXPECT_EQ(deepmsg::archive::server::archive_codecs::RecordingSignal::SYNC, fixture.control.signals[1].signal);
    EXPECT_EQ(0, fixture.source->replay_calls);
}

TEST(ReplicationSessionTest, anUnknownSourceRecordingFailsWithUnknownRecording)
{
    Fixture fixture;
    fixture.source->remaining_record_count = 1;  // dispatch complete with a record left over
    fixture.session->do_work();
    fixture.session->do_work();
    fixture.session->do_work();

    ASSERT_EQ(State::done, fixture.session->state());
    ASSERT_EQ(1u, fixture.control.errors.size());
    EXPECT_EQ(5, fixture.control.errors[0].error_code);  // UNKNOWN_RECORDING
    EXPECT_EQ("unknown src recording id 7", fixture.control.errors[0].message);
    EXPECT_EQ(3, fixture.control.errors[0].replication_id);  // the replication id
}

TEST(ReplicationSessionTest, aFileIoMaxLengthBelowTheMtuFailsBeforeTheRowIsBuilt)
{
    Fixture fixture;
    fixture.config.file_io_max_length = 1024;
    fixture.recreate();
    fixture.source->descriptors.push_back(descriptor(0, 128));

    fixture.session->do_work();
    fixture.session->do_work();
    fixture.session->do_work();

    ASSERT_EQ(State::done, fixture.session->state());
    EXPECT_TRUE(fixture.control.added_rows.empty());
    ASSERT_EQ(1u, fixture.control.errors.size());
    EXPECT_NE(std::string::npos, fixture.control.errors[0].message.find("fileIoMaxLength"));
}

TEST(ReplicationSessionTest, closeSignalsReplicateEndOnceAndStopsTheSourceReplay)
{
    Fixture fixture;
    fixture.source->descriptors.push_back(descriptor(0, 128));
    fixture.session->do_work();
    fixture.session->do_work();
    fixture.session->do_work();
    fixture.session->do_work();
    fixture.control.outcome_ready = true;
    fixture.session->do_work();
    fixture.session->do_work();
    fixture.session->do_work();
    fixture.session->do_work();  // replay offer
    fixture.source->responses.push_back(
        {ReplicationSource::Response::Kind::ok, 42, fixture.source->replay_correlation, 44, 0, {}, {}});
    fixture.session->do_work();  // → AWAIT_IMAGE
    fixture.session->do_work();  // → REPLICATE

    fixture.session->abort("stop replication");
    ASSERT_TRUE(fixture.session->is_done());
    fixture.session->close();

    EXPECT_EQ(1, fixture.source->stop_replay_calls);
    EXPECT_EQ(44, fixture.source->stopped_replay_session_id);
    EXPECT_EQ(1u, fixture.control.stopped_subscriptions.size());

    const auto replicate_end_count = std::count_if(
        fixture.control.signals.begin(), fixture.control.signals.end(), [](const FakeControl::SignalEvent& event)
        { return event.signal == deepmsg::archive::server::archive_codecs::RecordingSignal::REPLICATE_END; });
    EXPECT_EQ(1, replicate_end_count);

    fixture.session->close();
    EXPECT_EQ(1, fixture.source->stop_replay_calls);
    EXPECT_EQ(1u, fixture.control.stopped_subscriptions.size());
}

TEST(ReplicationSessionTest, closeCancelsAnInFlightExtend)
{
    Fixture fixture;
    fixture.source->descriptors.push_back(descriptor(0, 128));
    fixture.session->do_work();
    fixture.session->do_work();
    fixture.session->do_work();
    fixture.session->do_work();  // extend started, outcome pending
    ASSERT_EQ(State::extend, fixture.session->state());
    EXPECT_TRUE(fixture.control.cancelled_tokens.empty());

    fixture.session->close();
    EXPECT_EQ(1u, fixture.control.cancelled_tokens.size());
}

TEST(ReplicationChannelTest, theExtendChannelForcesRejoinFalseAndTheSessionId)
{
    auto channel = replication_extend_channel("aeron:udp?endpoint=localhost:8012", 33, false);
    ASSERT_TRUE(channel.has_value()) << channel.error().message.view();
    EXPECT_NE(std::string::npos, channel->find("rejoin=false"));
    EXPECT_NE(std::string::npos, channel->find("session-id=33"));
    EXPECT_NE(std::string::npos, channel->find("endpoint=localhost:8012"));

    auto response_channel =
        replication_extend_channel("aeron:udp?endpoint=localhost:8012|control-mode=response", 33, true);
    ASSERT_TRUE(response_channel.has_value());
    EXPECT_NE(std::string::npos, response_channel->find("rejoin=false"));
    EXPECT_EQ(std::string::npos, response_channel->find("session-id=33"));
}

TEST(ReplicationChannelTest, theReplayChannelReplacesAWildcardPortAndCarriesTheSessionId)
{
    // The channel's host is kept and only the wildcard port is replaced
    // (`ChannelUri.java:630-635`).
    auto channel = replication_replay_channel("aeron:udp?endpoint=localhost:0", "127.0.0.1:9000", 33, false);
    ASSERT_TRUE(channel.has_value()) << channel.error().message.view();
    EXPECT_NE(std::string::npos, channel->find("endpoint=localhost:9000"));
    EXPECT_NE(std::string::npos, channel->find("session-id=33"));

    auto fixed = replication_replay_channel("aeron:udp?endpoint=localhost:8012", "127.0.0.1:9000", 33, false);
    ASSERT_TRUE(fixed.has_value());
    EXPECT_NE(std::string::npos, fixed->find("endpoint=localhost:8012"));

    auto wildcard_resolved = replication_replay_channel("aeron:udp?endpoint=localhost:0", "127.0.0.1:0", 33, false);
    EXPECT_FALSE(wildcard_resolved.has_value());
}

}  // namespace
