#include "archive/server/replication_session.h"

#include <utility>

#include "archive/server/channel_uri.h"
#include "core/error/error.h"

namespace deepmsg::archive::server
{
namespace
{

/// `Aeron.NULL_VALUE` / `AeronArchive.NULL_POSITION` / `NULL_LENGTH`.
constexpr std::int64_t NULL_VALUE = -1;
constexpr std::int64_t NULL_POSITION = -1;
constexpr std::int64_t NULL_LENGTH = -1;

/// `ArchiveException` codes the replication path sends.
constexpr std::int64_t ERROR_GENERIC = 0;
constexpr std::int64_t UNKNOWN_RECORDING = 5;
constexpr std::int64_t REPLICATION_CONNECTION_FAILURE = 14;

/// `SourceLocation.REMOTE` (`SourceLocation.java`): the image comes from the
/// source archive's replay publication, not a local publication.
constexpr std::int32_t SOURCE_LOCATION_REMOTE = 1;

}  // namespace

ReplicationSession::ReplicationSession(const ReplicationConfig& config, std::unique_ptr<ReplicationSource> source,
                                       ReplicationControl& control)
    : config_(config),
      source_(std::move(source)),
      control_(&control),
      dst_stop_position_(config.stop_position),
      dst_recording_id_(config.dst_recording_id)
{
    time_of_last_action_ms_ = now_ms();

    // `:152-172`: the destination summary seeds the replay point and the
    // "empty recording is replaceable" flag.
    if (config_.has_dst_recording)
    {
        replay_position_ = config_.dst_stop_position;
        replay_stream_id_ = config_.dst_stream_id;
        is_destination_recording_empty_ = config_.dst_start_position == config_.dst_stop_position;
    }
}

std::int64_t ReplicationSession::now_ms() const
{
    return nullptr == config_.clock ? 0 : config_.clock->time_ms();
}

bool ReplicationSession::timed_out() const
{
    return now_ms() >= (time_of_last_action_ms_ + config_.action_timeout_ms);
}

std::int64_t ReplicationSession::track_action(std::int64_t correlation_id)
{
    time_of_last_action_ms_ = now_ms();
    active_correlation_id_ = correlation_id;
    return 1;
}

void ReplicationSession::enter(State next, std::string reason)
{
    (void)reason;
    // `:975-985`: every transition clears the correlation and restarts the
    // action timer.
    state_ = next;
    active_correlation_id_ = NULL_VALUE;
    time_of_last_action_ms_ = now_ms();
}

void ReplicationSession::abort(std::string_view reason)
{
    (void)reason;
    // `:200-215`: the stop path only ends the session; `close()` does the
    // teardown and sends `REPLICATE_END`.
    enter(State::done);
}

void ReplicationSession::error(std::string_view message, std::int64_t error_code)
{
    // `:1000-1010`: the correlation is the replication id, not the request's.
    control_->send_replication_error(config_.control_session_id, config_.replication_id, error_code, message);
}

bool ReplicationSession::fail(std::string_view message, std::int64_t error_code)
{
    error(message, error_code);
    enter(State::done, std::string{message});
    return false;
}

void ReplicationSession::signal(std::int64_t position, archive_codecs::RecordingSignal::Value recording_signal)
{
    // `:1003-1009`: the subscription id is `NULL_VALUE` until the destination
    // recording subscription exists.
    const std::int64_t subscription_id =
        NULL_VALUE == recording_subscription_id_ ? NULL_VALUE : recording_subscription_id_;
    control_->send_replication_signal(config_.control_session_id, config_.replication_id, dst_recording_id_,
                                      subscription_id, position, recording_signal);
}

void ReplicationSession::stop_recording()
{
    // `:940-955`: the destination subscription stops its recording session.
    if (NULL_VALUE != recording_subscription_id_)
    {
        (void)control_->stop_recording_subscription(recording_subscription_id_);
        recording_subscription_id_ = NULL_VALUE;
    }
}

void ReplicationSession::stop_replay_session()
{
    // `:925-938`: tell the source to stop the replay it started for us.
    if (NULL_VALUE != src_replay_session_id_ && nullptr != source_)
    {
        (void)source_->stop_replay(source_->next_correlation_id(), src_replay_session_id_);
        src_replay_session_id_ = NULL_VALUE;
    }
}

void ReplicationSession::close()
{
    if (closed_)
    {
        return;
    }
    closed_ = true;

    // `:196-213`: stop the destination subscription and the source replay,
    // release the source client, then signal `REPLICATE_END` exactly once.
    stop_recording();
    stop_replay_session();

    if (0 != extend_token_)
    {
        // The add was still in flight; nobody waits for its OK.
        control_->cancel_extend_recording(extend_token_);
        extend_token_ = 0;
    }
    source_.reset();

    signal(NULL_POSITION, archive_codecs::RecordingSignal::REPLICATE_END);
}

int ReplicationSession::do_work()
{
    int work_count = 0;

    switch (state_)
    {
        case State::connect:
            work_count += connect();
            break;
        case State::replicate_descriptor:
            work_count += replicate_descriptor();
            break;
        case State::src_recording_position:
            work_count += src_recording_position();
            break;
        case State::extend:
            work_count += extend();
            break;
        case State::replay_token:
            work_count += replay_token();
            break;
        case State::get_archive_proxy:
            work_count += get_archive_proxy();
            break;
        case State::replay:
            work_count += replay();
            break;
        case State::await_image:
            work_count += await_image();
            break;
        case State::replicate:
            work_count += replicate();
            break;
        case State::catchup:
            work_count += catchup();
            break;
        case State::attempt_live_join:
            work_count += attempt_live_join();
            break;
        case State::done:
            break;
    }

    return work_count;
}

int ReplicationSession::connect()
{
    // `:400-443`: async connect, one step per pass.
    if (nullptr == source_)
    {
        (void)fail("Replication connection failed=no source archive client", REPLICATION_CONNECTION_FAILURE);
        return 1;
    }

    if (source_->is_connected())
    {
        enter(State::replicate_descriptor, "connected to the Archive");
        return 1;
    }

    int work_count = source_->do_work();

    if (source_->is_failed())
    {
        (void)fail("Replication connection failed=" + std::string{source_->error_message()},
                   REPLICATION_CONNECTION_FAILURE);
        return work_count;
    }

    if (source_->is_connected())
    {
        enter(State::replicate_descriptor, "connected to the Archive");
        work_count += 1;
    }

    return work_count;
}

int ReplicationSession::replicate_descriptor()
{
    // `:444-490`.
    int work_count = 0;

    if (NULL_VALUE == active_correlation_id_)
    {
        const std::int64_t correlation_id = source_->next_correlation_id();
        if (source_->list_recording(correlation_id, config_.src_recording_id))
        {
            work_count += track_action(correlation_id);
            source_->reset_descriptor_poller(correlation_id, 1, [this](SourceRecordingDescriptor& descriptor)
                                             { on_recording_descriptor(descriptor); });
        }
        else if (timed_out())
        {
            (void)fail("failed to list remote recording descriptor", ERROR_GENERIC);
        }
        return work_count;
    }

    auto polled = source_->poll_descriptors();
    if (!polled)
    {
        (void)fail(polled.error().message.view(), ERROR_GENERIC);
        return work_count;
    }
    work_count += *polled;

    if (source_->descriptor_dispatch_complete() && source_->descriptor_remaining_record_count() > 0)
    {
        (void)fail("unknown src recording id " + std::to_string(config_.src_recording_id), UNKNOWN_RECORDING);
        return work_count;
    }

    if (!is_done() && 0 == *polled && timed_out())
    {
        (void)fail("failed to fetch remote recording descriptor", ERROR_GENERIC);
    }

    return work_count;
}

void ReplicationSession::on_recording_descriptor(SourceRecordingDescriptor& descriptor)
{
    // `:290-399` (`RecordingDescriptorConsumer`).
    src_stop_position_ = descriptor.stop_position;
    replay_stream_id_ = descriptor.stream_id;

    if (config_.live_destination.empty() && NULL_VALUE != config_.replication_session_id)
    {
        replay_session_id_ = config_.replication_session_id;
    }
    else
    {
        replay_session_id_ = descriptor.session_id;
    }

    if (NULL_VALUE != config_.file_io_max_length && config_.file_io_max_length < descriptor.mtu_length)
    {
        (void)fail("Replication fileIoMaxLength (" + std::to_string(config_.file_io_max_length) +
                       ") is less than than the recording mtuLength (" + std::to_string(descriptor.mtu_length) + ")",
                   ERROR_GENERIC);
        return;
    }

    if (NULL_VALUE == dst_recording_id_)
    {
        replay_position_ = descriptor.start_position;
        auto added = control_->add_new_recording(
            descriptor.start_position, descriptor.start_position, descriptor.start_timestamp,
            descriptor.start_timestamp, descriptor.initial_term_id, descriptor.segment_file_length,
            descriptor.term_buffer_length, descriptor.mtu_length, descriptor.session_id, descriptor.stream_id,
            descriptor.stripped_channel, descriptor.original_channel, descriptor.source_identity);
        if (!added)
        {
            (void)fail(added.error(), ERROR_GENERIC);
            return;
        }
        dst_recording_id_ = *added;

        signal(descriptor.start_position, archive_codecs::RecordingSignal::REPLICATE);
    }
    else if (is_destination_recording_empty_)
    {
        replay_position_ = descriptor.start_position;
        auto replaced = control_->replace_recording(
            dst_recording_id_, descriptor.start_position, descriptor.start_position, descriptor.start_timestamp,
            descriptor.start_timestamp, descriptor.initial_term_id, descriptor.segment_file_length,
            descriptor.term_buffer_length, descriptor.mtu_length, descriptor.session_id, descriptor.stream_id,
            descriptor.stripped_channel, descriptor.original_channel, descriptor.source_identity);
        if (!replaced)
        {
            (void)fail(replaced.error(), ERROR_GENERIC);
            return;
        }
    }

    State next_state = State::extend;

    if (!config_.live_destination.empty())
    {
        if (NULL_POSITION != descriptor.stop_position)
        {
            (void)fail("cannot live merge without active source recording", ERROR_GENERIC);
            return;
        }

        next_state = State::src_recording_position;
    }

    if (descriptor.start_position == descriptor.stop_position ||
        (NULL_VALUE != dst_recording_id_ &&
         descriptor.stop_position == control_->catalog_stop_position(dst_recording_id_)))
    {
        signal(descriptor.stop_position, archive_codecs::RecordingSignal::SYNC);
        next_state = State::done;
    }

    enter(next_state);
}

int ReplicationSession::src_recording_position()
{
    // `:492-525`: only a live destination reaches this state.
    int work_count = 0;

    if (NULL_VALUE == active_correlation_id_)
    {
        const std::int64_t correlation_id = source_->next_correlation_id();
        if (source_->get_recording_position(correlation_id, config_.src_recording_id))
        {
            work_count += track_action(correlation_id);
        }
        else if (timed_out())
        {
            (void)fail("failed to send recording position request", ERROR_GENERIC);
        }
        return work_count;
    }

    auto response = source_->poll_response();
    if (!response)
    {
        (void)fail(response.error().message.view(), ERROR_GENERIC);
        return work_count;
    }
    work_count += 1;

    if (has_response(*response))
    {
        src_recording_position_ = response->relevant_id;

        if (NULL_POSITION == src_recording_position_)
        {
            (void)fail("cannot live merge without active source recording", ERROR_GENERIC);
            return work_count;
        }

        enter(State::extend);
    }
    else if (is_done())
    {
        return work_count;
    }
    else if (timed_out())
    {
        (void)fail("failed to get recording position", ERROR_GENERIC);
    }

    return work_count;
}

int ReplicationSession::extend()
{
    // `:527-568`: rewrite the replication channel, ask the conductor for the
    // destination recording subscription and wait for its registration id.
    if (0 == extend_token_)
    {
        auto channel = replication_extend_channel(config_.replication_channel, replay_session_id_,
                                                  is_mds_response_channel(config_.replication_channel));
        if (!channel)
        {
            (void)fail(channel.error().message.view(), ERROR_GENERIC);
            return 1;
        }
        extend_channel_ = *channel;

        auto started =
            control_->start_extend_recording(config_.control_session_id, config_.replication_id, dst_recording_id_,
                                             replay_stream_id_, SOURCE_LOCATION_REMOTE, true, extend_channel_);
        if (!started)
        {
            // The conductor already sent the ERROR under `replicationId`.
            enter(State::done, started.error());
            return 1;
        }

        extend_token_ = *started;
    }

    auto outcome = control_->take_extend_recording_outcome(extend_token_);
    if (!outcome)
    {
        if (timed_out())
        {
            (void)fail("failed to extend recording " + std::to_string(dst_recording_id_), ERROR_GENERIC);
        }
        return 0;
    }

    extend_token_ = 0;
    if (!outcome->error.empty())
    {
        // The ERROR already went out with this replication's correlation.
        enter(State::done, outcome->error);
        return 1;
    }

    recording_subscription_id_ = outcome->registration_id;

    if (!config_.live_destination.empty() || (NULL_VALUE != config_.channel_tag_id) ||
        (NULL_VALUE != config_.subscription_tag_id))
    {
        // Tagged / live replication needs the extra destination add
        // (`:559-566`); no criterion drives it (recorded gap).
        (void)fail("tagged or live replication is not implemented", ERROR_GENERIC);
        return 1;
    }

    enter(State::replay_token, "recording subscription started");
    return 1;
}

int ReplicationSession::replay_token()
{
    // `:570-603`: only a response-mode replication channel asks for a token.
    if (!is_mds_response_channel(config_.replication_channel))
    {
        enter(State::get_archive_proxy);
        return 1;
    }

    (void)fail("response-channel replication is not implemented", ERROR_GENERIC);
    return 1;
}

int ReplicationSession::get_archive_proxy()
{
    // `:605-654`: a response-mode replay needs a second publication; the
    // judged path passes straight through.
    if (is_mds_response_channel(config_.replication_channel))
    {
        (void)fail("response-channel replication is not implemented", ERROR_GENERIC);
        return 1;
    }

    enter(State::replay);
    return 1;
}

int ReplicationSession::replay()
{
    // `:656-748`.
    int work_count = 0;

    if (NULL_VALUE == active_correlation_id_)
    {
        if (NULL_VALUE == recording_subscription_id_)
        {
            (void)fail("replay without a destination recording subscription", ERROR_GENERIC);
            return work_count;
        }

        auto resolved_endpoint = control_->subscription_resolved_endpoint(recording_subscription_id_);
        if (!resolved_endpoint)
        {
            if (timed_out())
            {
                (void)fail("failed to resolve subscription endpoint: channel=" + extend_channel_, ERROR_GENERIC);
            }
            return work_count;
        }

        auto replay_channel =
            replication_replay_channel(config_.replication_channel, *resolved_endpoint, replay_session_id_,
                                       is_mds_response_channel(config_.replication_channel));
        if (!replay_channel)
        {
            (void)fail(replay_channel.error().message.view(), ERROR_GENERIC);
            return work_count;
        }

        const std::int64_t replay_length =
            NULL_POSITION == dst_stop_position_ ? NULL_LENGTH : dst_stop_position_ - replay_position_;
        const std::int64_t correlation_id = source_->next_correlation_id();

        if (source_->replay(correlation_id, config_.src_recording_id, *replay_channel, replay_stream_id_,
                            replay_position_, replay_length, config_.file_io_max_length))
        {
            work_count += track_action(correlation_id);
        }
        else if (timed_out())
        {
            (void)fail("failed to send replay request", ERROR_GENERIC);
        }
        return work_count;
    }

    auto response = source_->poll_response();
    if (!response)
    {
        (void)fail(response.error().message.view(), ERROR_GENERIC);
        return work_count;
    }
    work_count += 1;

    if (has_response(*response))
    {
        src_replay_session_id_ = response->relevant_id;
        enter(State::await_image);
    }
    else if (!is_done() && timed_out())
    {
        (void)fail("failed get acknowledgement of replay request to: " + config_.replication_channel, ERROR_GENERIC);
    }

    return work_count;
}

int ReplicationSession::await_image()
{
    // `:750-790`: the destination subscription must expose the replay
    // publication's image under the source replay session id (low 32 bits).
    image_ =
        control_->image_by_session_id(recording_subscription_id_, static_cast<std::int32_t>(src_replay_session_id_));
    if (nullptr != image_)
    {
        if (config_.live_destination.empty())
        {
            enter(State::replicate);
        }
        else
        {
            enter(State::catchup);
        }
        return 1;
    }

    if (timed_out())
    {
        (void)fail("failed get replay image for sessionId=" +
                       std::to_string(static_cast<std::int32_t>(src_replay_session_id_)) +
                       " on channel=" + config_.replication_channel,
                   ERROR_GENERIC);
    }

    return 0;
}

int ReplicationSession::replicate()
{
    // `:792-830`.
    if (nullptr == image_)
    {
        (void)fail("replication image is missing", ERROR_GENERIC);
        return 1;
    }

    const bool is_closed = control_->image_is_closed(image_);
    const bool is_end_of_stream = control_->image_is_end_of_stream(image_);
    const std::int64_t position = control_->image_position(image_);
    const bool is_synced = NULL_POSITION != src_stop_position_ && position >= src_stop_position_;
    const bool is_destination_complete = NULL_POSITION != dst_stop_position_ && position >= dst_stop_position_;

    if (!is_synced && !is_destination_complete && !is_end_of_stream && !is_closed)
    {
        return 0;
    }

    if (is_synced)
    {
        signal(position, archive_codecs::RecordingSignal::SYNC);
    }

    src_replay_session_id_ = NULL_VALUE;
    enter(State::done, is_synced ? "sync" : "done");
    return 1;
}

int ReplicationSession::catchup()
{
    (void)fail("live merge is not implemented", ERROR_GENERIC);
    return 1;
}

int ReplicationSession::attempt_live_join()
{
    (void)fail("live merge is not implemented", ERROR_GENERIC);
    return 1;
}

bool ReplicationSession::has_response(const ReplicationSource::Response& response)
{
    // `:900-915`.
    if (ReplicationSource::Response::Kind::none == response.kind ||
        ReplicationSource::Response::Kind::challenged == response.kind)
    {
        return false;
    }

    if (response.control_session_id != source_->control_session_id())
    {
        return false;
    }

    if (ReplicationSource::Response::Kind::error == response.kind)
    {
        (void)fail(response.error_message, response.code);
        return false;
    }

    return response.correlation_id == active_correlation_id_;
}

}  // namespace deepmsg::archive::server
