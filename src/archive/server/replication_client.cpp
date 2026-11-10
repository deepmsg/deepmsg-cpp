#include "archive/server/replication_client.h"

#include <utility>

// The internal C archive client headers carry no `extern "C"` guard.
extern "C"
{
#include "client/aeron_archive.h"
#include "client/aeron_archive_async_connect.h"
#include "client/aeron_archive_client.h"
#include "client/aeron_archive_context.h"
#include "client/aeron_archive_control_response_poller.h"
#include "client/aeron_archive_proxy.h"
#include "client/aeron_archive_recording_descriptor_poller.h"
}

namespace deepmsg::archive::server
{

struct SourceArchiveClient::Credentials
{
    std::string data;
    aeron_archive_encoded_credentials_t view{};
};

namespace
{

[[nodiscard]] aeron_archive_encoded_credentials_t* supply_credentials(void* clientd)
{
    auto* credentials = static_cast<SourceArchiveClient::Credentials*>(clientd);
    return nullptr == credentials ? nullptr : &credentials->view;
}

[[nodiscard]] aeron_archive_encoded_credentials_t* answer_challenge(aeron_archive_encoded_credentials_t* challenge,
                                                                    void* clientd)
{
    // `ReplicationCredentialsSupplier` hands the same credentials back for a
    // simple (non-challenge) source (`ArchiveConductor.java:1443-1446`).
    (void)challenge;
    return supply_credentials(clientd);
}

void free_credentials(aeron_archive_encoded_credentials_t* credentials, void* clientd)
{
    // The bytes belong to the replication request; the client only borrows them.
    (void)credentials;
    (void)clientd;
}

}  // namespace

std::expected<std::unique_ptr<SourceArchiveClient>, axc::core::Error> SourceArchiveClient::create(aeron_t* aeron,
                                                                                                  const Config& config)
{
    if (nullptr == aeron)
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::invalid_argument,
                                                     "the source client needs the archive's Aeron client"));
    }

    auto client = std::unique_ptr<SourceArchiveClient>(new SourceArchiveClient(aeron, config));
    if (!client->error_.empty())
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::io_error, client->error_));
    }
    return client;
}

SourceArchiveClient::SourceArchiveClient(aeron_t* archive_aeron, const Config& config)
{
    if (0 != aeron_archive_context_init(&context_))
    {
        error_ = aeron_errmsg();
        return;
    }

    (void)aeron_archive_context_set_control_request_channel(context_, config.control_request_channel.c_str());
    (void)aeron_archive_context_set_control_request_stream_id(context_, config.control_request_stream_id);
    (void)aeron_archive_context_set_control_response_channel(context_, config.control_response_channel.c_str());
    (void)aeron_archive_context_set_control_response_stream_id(context_, config.control_response_stream_id);
    if (!config.client_name.empty())
    {
        (void)aeron_archive_context_set_client_name(context_, config.client_name.c_str());
    }

    // Share the archive conductor's client (`Archive.java:1620`): the context
    // then neither creates nor closes an Aeron client of its own.
    context_->aeron = archive_aeron;
    context_->owns_aeron_client = false;

    if (!config.encoded_credentials.empty())
    {
        credentials_ = std::make_unique<Credentials>();
        credentials_->data = config.encoded_credentials;
        credentials_->view.data = credentials_->data.data();
        credentials_->view.length = static_cast<std::uint32_t>(credentials_->data.size());
        (void)aeron_archive_context_set_credentials_supplier(context_, &supply_credentials, &answer_challenge,
                                                             &free_credentials, credentials_.get());
    }

    if (0 != aeron_archive_async_connect(&async_, context_))
    {
        error_ = aeron_errmsg();
    }
}

SourceArchiveClient::~SourceArchiveClient()
{
    if (nullptr != archive_)
    {
        (void)aeron_archive_close(archive_);
        archive_ = nullptr;
    }
    // A consumed async object is deleted by the poll itself.
    if (nullptr != async_)
    {
        aeron_archive_async_connect_delete(async_);
        async_ = nullptr;
    }
    if (nullptr != context_)
    {
        (void)aeron_archive_context_close(context_);
        context_ = nullptr;
    }
}

int SourceArchiveClient::do_work()
{
    if (failed_)
    {
        return 0;
    }

    int work_count = 0;

    if (nullptr != context_ && 0 != aeron_archive_context_invoke_aeron_client(context_))
    {
        error_ = aeron_errmsg();
        failed_ = true;
        return work_count;
    }

    if (nullptr != async_)
    {
        const int poll_result = aeron_archive_async_connect_poll(&archive_, async_);
        if (0 != poll_result)
        {
            // The library deletes the async object on both outcomes.
            async_ = nullptr;
        }
        if (poll_result < 0)
        {
            error_ = aeron_errmsg();
            failed_ = true;
        }
        else if (poll_result > 0)
        {
            work_count += 1;
        }
    }

    return work_count;
}

std::int64_t SourceArchiveClient::control_session_id() const
{
    return nullptr == archive_ ? -1 : aeron_archive_control_session_id(archive_);
}

std::int64_t SourceArchiveClient::next_correlation_id()
{
    return nullptr == archive_ ? -1 : aeron_archive_next_correlation_id(archive_);
}

bool SourceArchiveClient::list_recording(std::int64_t correlation_id, std::int64_t recording_id)
{
    return nullptr != archive_ &&
           aeron_archive_proxy_list_recording(aeron_archive_proxy(archive_), correlation_id, recording_id);
}

bool SourceArchiveClient::replay(std::int64_t correlation_id, std::int64_t recording_id,
                                 std::string_view replay_channel, std::int32_t replay_stream_id, std::int64_t position,
                                 std::int64_t length, std::int32_t file_io_max_length)
{
    if (nullptr == archive_)
    {
        return false;
    }

    aeron_archive_replay_params_t params;
    (void)aeron_archive_replay_params_init(&params);
    params.position = position;
    params.length = length;
    params.file_io_max_length = file_io_max_length;

    const std::string channel{replay_channel};
    return aeron_archive_proxy_replay(aeron_archive_proxy(archive_), correlation_id, recording_id, channel.c_str(),
                                      replay_stream_id, &params);
}

bool SourceArchiveClient::stop_replay(std::int64_t correlation_id, std::int64_t replay_session_id)
{
    return nullptr != archive_ &&
           aeron_archive_proxy_stop_replay(aeron_archive_proxy(archive_), correlation_id, replay_session_id);
}

bool SourceArchiveClient::get_recording_position(std::int64_t correlation_id, std::int64_t recording_id)
{
    return nullptr != archive_ &&
           aeron_archive_proxy_get_recording_position(aeron_archive_proxy(archive_), correlation_id, recording_id);
}

std::expected<SourceArchiveClient::Response, axc::core::Error> SourceArchiveClient::poll_response()
{
    if (nullptr == archive_)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::protocol_error, "source archive is not connected"));
    }

    auto* poller = aeron_archive_control_response_poller(archive_);
    if (nullptr == poller)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::internal_error, "source control response poller is missing"));
    }

    if (aeron_archive_control_response_poller_poll(poller) < 0)
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::protocol_error, aeron_errmsg()));
    }

    Response response;
    if (!poller->is_poll_complete)
    {
        return response;
    }

    response.control_session_id = poller->control_session_id;
    response.correlation_id = poller->correlation_id;
    response.relevant_id = poller->relevant_id;
    response.code = poller->code_value;

    if (poller->was_challenged)
    {
        response.kind = Response::Kind::challenged;
        if (nullptr != poller->encoded_challenge_buffer)
        {
            response.encoded_challenge =
                std::string{poller->encoded_challenge_buffer, poller->encoded_challenge.length};
        }
    }
    else if (poller->is_code_error)
    {
        response.kind = Response::Kind::error;
        if (nullptr != poller->error_message)
        {
            response.error_message = poller->error_message;
        }
    }
    else if (poller->is_code_ok)
    {
        response.kind = Response::Kind::ok;
    }

    return response;
}

void SourceArchiveClient::reset_descriptor_poller(std::int64_t correlation_id, std::int32_t record_count,
                                                  std::function<void(SourceRecordingDescriptor&)> consumer)
{
    descriptor_consumer_ = std::move(consumer);
    descriptors_.clear();
    if (nullptr == archive_)
    {
        return;
    }

    aeron_archive_recording_descriptor_poller_reset(archive_->recording_descriptor_poller, correlation_id, record_count,
                                                    &SourceArchiveClient::on_descriptor, this);
}

std::expected<int, axc::core::Error> SourceArchiveClient::poll_descriptors()
{
    if (nullptr == archive_)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::protocol_error, "source archive is not connected"));
    }

    const int polled = aeron_archive_recording_descriptor_poller_poll(archive_->recording_descriptor_poller);
    if (polled < 0)
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::protocol_error, aeron_errmsg()));
    }

    for (SourceRecordingDescriptor& descriptor : descriptors_)
    {
        if (descriptor_consumer_)
        {
            descriptor_consumer_(descriptor);
        }
    }
    descriptors_.clear();

    return polled;
}

bool SourceArchiveClient::descriptor_dispatch_complete() const noexcept
{
    return nullptr != archive_ && archive_->recording_descriptor_poller->is_dispatch_complete;
}

std::int32_t SourceArchiveClient::descriptor_remaining_record_count() const noexcept
{
    return nullptr == archive_ ? 0 : archive_->recording_descriptor_poller->remaining_record_count;
}

void SourceArchiveClient::on_descriptor(aeron_archive_recording_descriptor_stct* descriptor, void* clientd)
{
    auto* self = static_cast<SourceArchiveClient*>(clientd);
    if (nullptr == self || nullptr == descriptor)
    {
        return;
    }

    SourceRecordingDescriptor copy;
    copy.control_session_id = descriptor->control_session_id;
    copy.correlation_id = descriptor->correlation_id;
    copy.recording_id = descriptor->recording_id;
    copy.start_timestamp = descriptor->start_timestamp;
    copy.stop_timestamp = descriptor->stop_timestamp;
    copy.start_position = descriptor->start_position;
    copy.stop_position = descriptor->stop_position;
    copy.initial_term_id = descriptor->initial_term_id;
    copy.segment_file_length = descriptor->segment_file_length;
    copy.term_buffer_length = descriptor->term_buffer_length;
    copy.mtu_length = descriptor->mtu_length;
    copy.session_id = descriptor->session_id;
    copy.stream_id = descriptor->stream_id;
    if (nullptr != descriptor->stripped_channel)
    {
        copy.stripped_channel.assign(descriptor->stripped_channel, descriptor->stripped_channel_length);
    }
    if (nullptr != descriptor->original_channel)
    {
        copy.original_channel.assign(descriptor->original_channel, descriptor->original_channel_length);
    }
    if (nullptr != descriptor->source_identity)
    {
        copy.source_identity.assign(descriptor->source_identity, descriptor->source_identity_length);
    }

    self->descriptors_.push_back(std::move(copy));
}

}  // namespace deepmsg::archive::server
