#include "archive/server/archive_conductor.h"

#include <unistd.h>

#include <cerrno>
#include <utility>

#include <algorithm>
#include <span>

#include "archive/server/archive_counters.h"
#include "archive/server/channel_uri.h"
#include "archive/server/recording_pos.h"
#include "core/deepmsg/image_handlers.h"
#include "core/io/fs.h"
#include "core/time/clock.h"

// The subscription's registration id has no public accessor in 1.54, and the
// internal header's `volatile` parameters trip a warning under -Werror (the
// group is spelled differently per compiler).
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-volatile"
#else
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wvolatile"
#endif
extern "C"
{
#include "aeron_subscription.h"
}
#if defined(__clang__)
#pragma clang diagnostic pop
#else
#pragma GCC diagnostic pop
#endif

namespace deepmsg::archive::server
{
namespace
{

/// `SemanticVersion.major`.
[[nodiscard]] std::int32_t major_version(std::int32_t version) noexcept
{
    return (version >> 16) & 0xFFFF;
}

[[nodiscard]] std::string version_to_string(std::int32_t version)
{
    return std::to_string((version >> 16) & 0xFFFF) + "." + std::to_string((version >> 8) & 0xFF) + "." +
           std::to_string(version & 0xFF);
}

/// `ArchiveException` error codes carried as the relevant id of an ERROR
/// response (`ArchiveException.java:29-84`); `GENERIC` (0) is the two-argument
/// `send_error_response` overload.
constexpr std::int64_t ACTIVE_SUBSCRIPTION = 3;
constexpr std::int64_t MAX_RECORDINGS = 8;
constexpr std::int64_t STORAGE_SPACE = 11;

/// `SourceLocation.LOCAL` (`SourceLocation.java`).
constexpr std::int32_t SOURCE_LOCATION_LOCAL = 0;

}  // namespace

ArchiveConductor::ArchiveConductor(const ArchiveConductorContext& context)
    : context_(context),
      authenticator_(nullptr != context.authenticator ? context.authenticator : &default_authenticator_),
      adapter_(*this)
{
}

ArchiveConductor::~ArchiveConductor()
{
    close();
}

std::expected<std::unique_ptr<ArchiveConductor>, axc::core::Error> ArchiveConductor::create(
    const ArchiveConductorContext& context)
{
    auto conductor = std::unique_ptr<ArchiveConductor>(new ArchiveConductor(context));
    if (auto initialized = conductor->init(); !initialized)
    {
        return std::unexpected(initialized.error());
    }
    return conductor;
}

std::expected<void, axc::core::Error> ArchiveConductor::init()
{
    axc::core::ClientConfig config;
    config.dir = context_.aeron_dir;
    config.use_conductor_agent_invoker = true;

    auto client = axc::core::DeepmsgClient::connect(config);
    if (!client)
    {
        return std::unexpected(client.error());
    }
    client_ = std::move(*client);

    axc::core::ImageHandlers handlers;
    handlers.on_unavailable = &ArchiveConductor::unavailable_image_trampoline;
    handlers.on_unavailable_clientd = this;

    auto subscription = client_.add_subscription(context_.control_channel, context_.control_stream_id, handlers);
    for (int attempt = 0; attempt < 5000; attempt++)
    {
        auto polled = subscription.poll();
        if (!polled)
        {
            return std::unexpected(polled.error());
        }
        if (polled->has_value())
        {
            control_subscription_ = std::move(**polled);
            break;
        }

        // The driver has to answer the async add, so the conductor has to keep
        // turning between polls (`铁律 2`: never block inside an add).
        const int work = client_.do_work();
        (void)work;
        ::usleep(1000);
    }

    if (!control_subscription_)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::io_error, "control subscription did not complete"));
    }

    cached_clock_.update(epoch_clock_.time_ms());

    // `Archive.Context.conclude` (`Archive.java:1570-1700`): the catalog and the
    // four archive-level counters, allocated before the conductor serves.
    CatalogOptions catalog_options;
    catalog_options.archive_dir = context_.archive_dir;
    catalog_options.file_sync_level = context_.catalog_file_sync_level;
    catalog_options.catalog_capacity = context_.catalog_capacity;
    catalog_options.clock = &epoch_clock_;

    auto catalog = Catalog::create(catalog_options);
    if (!catalog)
    {
        return std::unexpected(catalog.error());
    }
    catalog_ = std::move(*catalog);

    // `Archive.java:1656-1667`: one recorder max-write-time counter per archive.
    if (AERON_NULL_COUNTER_ID !=
        ArchiveCounters::find(client_.counters_reader(), ArchiveCounters::MAX_WRITE_TIME_TYPE_ID, context_.archive_id))
    {
        return std::unexpected(axc::core::make_error(
            axc::core::ErrorCode::config_error,
            "existing max write time counter detected for archiveId=" + std::to_string(context_.archive_id)));
    }

    const auto archive_id_key = ArchiveCounters::archive_id_key(context_.archive_id);

    auto max_write_time =
        allocate_counter(ArchiveCounters::MAX_WRITE_TIME_TYPE_ID,
                         std::span<const std::uint8_t>{archive_id_key.data(), archive_id_key.size()},
                         ArchiveCounters::make_label(ArchiveCounters::MAX_WRITE_TIME_NAME, context_.archive_id));
    if (!max_write_time)
    {
        return std::unexpected(max_write_time.error());
    }
    max_write_time_counter_.emplace(std::move(*max_write_time));

    auto total_write_bytes =
        allocate_counter(ArchiveCounters::TOTAL_WRITE_BYTES_TYPE_ID,
                         std::span<const std::uint8_t>{archive_id_key.data(), archive_id_key.size()},
                         ArchiveCounters::make_label(ArchiveCounters::TOTAL_WRITE_BYTES_NAME, context_.archive_id));
    if (!total_write_bytes)
    {
        return std::unexpected(total_write_bytes.error());
    }
    total_write_bytes_counter_.emplace(std::move(*total_write_bytes));

    auto total_write_time =
        allocate_counter(ArchiveCounters::TOTAL_WRITE_TIME_TYPE_ID,
                         std::span<const std::uint8_t>{archive_id_key.data(), archive_id_key.size()},
                         ArchiveCounters::make_label(ArchiveCounters::TOTAL_WRITE_TIME_NAME, context_.archive_id));
    if (!total_write_time)
    {
        return std::unexpected(total_write_time.error());
    }
    total_write_time_counter_.emplace(std::move(*total_write_time));

    auto recording_sessions = allocate_counter(
        ArchiveCounters::RECORDING_SESSION_COUNT_TYPE_ID,
        std::span<const std::uint8_t>{archive_id_key.data(), archive_id_key.size()},
        ArchiveCounters::make_label(ArchiveCounters::RECORDING_SESSION_COUNT_NAME, context_.archive_id));
    if (!recording_sessions)
    {
        return std::unexpected(recording_sessions.error());
    }
    recording_sessions_counter_.emplace(std::move(*recording_sessions));

    recorder_.set_counters(&*total_write_bytes_counter_, &*total_write_time_counter_, &*max_write_time_counter_);
    return {};
}

std::expected<axc::core::PositionWriter, axc::core::Error> ArchiveConductor::allocate_counter(
    std::int32_t type_id, std::span<const std::uint8_t> key, std::string_view label)
{
    // Two-phase (`铁律 2`), driven by the client's own conductor while waiting.
    auto async = client_.add_counter(type_id, key, label);
    for (int attempt = 0; attempt < 5000; attempt++)
    {
        auto polled = async.poll();
        if (!polled)
        {
            return std::unexpected(polled.error());
        }
        if (polled->has_value())
        {
            return axc::core::PositionWriter{std::move(**polled)};
        }

        (void)client_.do_work();
        ::usleep(1000);
    }

    return std::unexpected(axc::core::make_error(axc::core::ErrorCode::io_error, "counter add did not complete"));
}

void ArchiveConductor::close()
{
    // Sessions first: their writers own segment files and their counters.
    recorder_.close();
    sessions_ = SessionWorker<ControlSession>{};
    publications_.clear();
    control_subscription_.reset();
    if (!catalog_.is_closed())
    {
        (void)catalog_.close();
    }
    recording_sessions_counter_.reset();
    total_write_time_counter_.reset();
    total_write_bytes_counter_.reset();
    max_write_time_counter_.reset();
    client_.close();
}

int ArchiveConductor::do_work()
{
    // `ArchiveConductor.doWork` (`ArchiveConductor.java:363-395`) for the control
    // plane: drive the client conductor, read the control subscription, run the
    // sessions.
    cached_clock_.update(epoch_clock_.time_ms());

    int work_count = client_.do_work();
    work_count += poll_control_subscription();
    work_count += poll_pending_publications();
    work_count += poll_pending_recording_subscriptions();
    work_count += poll_recording_images();
    work_count += recorder_.do_work([this](RecordingSession& session) { close_recording_session(session); });
    work_count += sessions_.do_work(
        [this](ControlSession& session)
        {
            adapter_.unregister_session(session.session_id());
            sessions_by_id_.erase(session.session_id());
            publications_.erase(session.session_id());

            // A session that ended before its publication add completed must not
            // leave the async registration behind: the client conductor walks
            // it on close.
            for (std::size_t index = 0; index < pending_publications_.size();)
            {
                if (pending_publications_[index].control_session_id == session.session_id())
                {
                    if (0 != aeron_async_add_exclusive_publication_cancel(client_.handle(),
                                                                          pending_publications_[index].async))
                    {
                        log_warning("failed to cancel the control publication add of an ended session");
                    }
                    pending_publications_.erase(pending_publications_.begin() + static_cast<std::ptrdiff_t>(index));
                }
                else
                {
                    index++;
                }
            }
        });

    return work_count;
}

int ArchiveConductor::poll_control_subscription()
{
    if (!control_subscription_)
    {
        return 0;
    }

    int work_count = 0;
    const int image_count = aeron_subscription_image_count(control_subscription_.get());
    for (int index = 0; index < image_count; index++)
    {
        aeron_image_t* image =
            aeron_subscription_image_at_index(control_subscription_.get(), static_cast<std::size_t>(index));
        if (nullptr == image)
        {
            continue;
        }

        PollContext poll_context{this, image};
        work_count += aeron_image_poll(image, &ArchiveConductor::fragment_trampoline, &poll_context,
                                       static_cast<std::size_t>(context_.fragment_limit));
        aeron_image_release(image);
    }

    return work_count;
}

void ArchiveConductor::fragment_trampoline(void* clientd, const std::uint8_t* buffer, size_t length,
                                           aeron_header_t* header)
{
    (void)header;
    auto* poll_context = static_cast<PollContext*>(clientd);
    poll_context->conductor->adapter_.on_fragment(buffer, static_cast<std::int32_t>(length), poll_context->image);
}

void ArchiveConductor::unavailable_image_trampoline(void* clientd, aeron_subscription_t* subscription,
                                                    aeron_image_t* image)
{
    (void)subscription;
    auto* conductor = static_cast<ArchiveConductor*>(clientd);
    conductor->log_warning("control image unavailable");
    conductor->adapter_.abort_sessions_for_image(image, "control image unavailable");
}

ControlSession* ArchiveConductor::on_connect_request(const ConnectRequest& request)
{
    // `ArchiveConductor.newControlSession` (`ArchiveConductor.java:447-520`).
    std::string invalid_version_message;
    if (major_version(request.protocol_version) != PROTOCOL_MAJOR_VERSION)
    {
        invalid_version_message = "invalid client version " + version_to_string(request.protocol_version) +
                                  ", archive is " + version_to_string(PROTOCOL_SEMANTIC_VERSION);
    }

    ControlSession::Config config;
    config.control_session_id = next_session_id_++;
    config.correlation_id = request.correlation_id;
    config.connect_timeout_ms = context_.connect_timeout_ms;
    config.session_liveness_check_interval_ms = context_.session_liveness_check_interval_ms;
    config.invalid_version_message = std::move(invalid_version_message);

    // `ArchiveConductor.newControlSession` (`ArchiveConductor.java:457-482`):
    // strip the client's channel, then apply the archive's term length, sparse
    // flag and MTU (the channel's own values win), plus the correlation id for
    // an MDS response channel.
    ChannelUriOptions options;
    options.term_length = context_.control_term_buffer_length;
    options.mtu_length = context_.control_mtu_length;
    options.sparse = context_.control_term_buffer_sparse;

    aeron_image_constants_t image_constants{};
    if (nullptr != request.image &&
        0 == aeron_image_constants(const_cast<aeron_image_t*>(static_cast<const aeron_image_t*>(request.image)),
                                   &image_constants) &&
        is_mds_response_channel(request.response_channel))
    {
        options.has_response_correlation_id = true;
        options.response_correlation_id = image_constants.correlation_id;
    }

    auto response_channel = control_response_channel(request.response_channel, options);
    if (!response_channel)
    {
        log_warning("failed to rewrite the control response channel " + request.response_channel + ": " +
                    std::string{response_channel.error().message.view()});
        return nullptr;
    }

    // `ControlSession.init` adds the publication asynchronously and waits for it
    // in INIT (`ControlSession.java:859-908`); the add is started here and
    // completed in `poll_pending_publications`.
    aeron_async_add_exclusive_publication_t* async = nullptr;
    if (0 != aeron_async_add_exclusive_publication(&async, client_.handle(), response_channel->c_str(),
                                                   request.response_stream_id))
    {
        log_warning("failed to add the control publication on " + *response_channel);
        return nullptr;
    }

    auto session = std::make_unique<ControlSession>(config, proxy_, *authenticator_, cached_clock_);
    ControlSession* raw_session = session.get();

    sessions_by_id_[config.control_session_id] = raw_session;
    pending_publications_.push_back(PendingPublication{config.control_session_id, async});
    sessions_.add(std::move(session));

    authenticator_->on_connect_request(config.control_session_id, request.encoded_credentials, cached_clock_.time_ms());
    return raw_session;
}

int ArchiveConductor::poll_pending_publications()
{
    int work_count = 0;

    for (std::size_t index = 0; index < pending_publications_.size();)
    {
        PendingPublication& pending = pending_publications_[index];
        aeron_exclusive_publication_t* publication = nullptr;

        // `aeron_client_conductor_async_resource_poll`: 0 = still awaiting the
        // driver, 1 = registered (complete), -1 = error or timeout.
        const int add_result = aeron_async_add_exclusive_publication_poll(&publication, pending.async);
        if (add_result < 0)
        {
            log_warning(std::string{"control publication add failed: "} + aeron_errmsg());
            if (0 != aeron_async_add_exclusive_publication_cancel(client_.handle(), pending.async))
            {
                log_warning("failed to cancel the control publication add");
            }
            pending_publications_.erase(pending_publications_.begin() + static_cast<std::ptrdiff_t>(index));
            continue;
        }

        if (1 != add_result || nullptr == publication)
        {
            index++;
            continue;
        }

        auto wrapper = std::make_unique<DeepmsgExclusivePublication>(publication);
        const auto found = sessions_by_id_.find(pending.control_session_id);
        if (found != sessions_by_id_.end())
        {
            found->second->set_publication(wrapper.get());
            publications_[pending.control_session_id] = std::move(wrapper);
        }

        pending_publications_.erase(pending_publications_.begin() + static_cast<std::ptrdiff_t>(index));
        work_count++;
    }

    return work_count;
}

ControlSession* ArchiveConductor::control_session_by_id(std::int64_t id) noexcept
{
    const auto found = sessions_by_id_.find(id);
    return found == sessions_by_id_.end() ? nullptr : found->second;
}

void ArchiveConductor::increment_recording_sessions() noexcept
{
    if (recording_sessions_counter_)
    {
        recording_sessions_counter_->set_value(recording_sessions_counter_->value() + 1);
    }
}

void ArchiveConductor::decrement_recording_sessions() noexcept
{
    if (recording_sessions_counter_)
    {
        recording_sessions_counter_->set_value(recording_sessions_counter_->value() - 1);
    }
}

std::int64_t ArchiveConductor::recording_sessions_counter_value() const noexcept
{
    return recording_sessions_counter_ ? recording_sessions_counter_->value() : -1;
}

void ArchiveConductor::on_start_recording(const StartRecordingRequest& request)
{
    // `ArchiveConductor.startRecording` (`ArchiveConductor.java:530-583`).
    ControlSession* session = request.session;
    if (nullptr == session)
    {
        return;
    }

    const auto max_recordings = static_cast<std::size_t>(context_.recording.max_concurrent_recordings);
    if (recording_subscriptions_.size() >= max_recordings || recorder_.size() >= max_recordings)
    {
        session->send_error_response(
            request.correlation_id, MAX_RECORDINGS,
            "max concurrent recordings reached at " + std::to_string(context_.recording.max_concurrent_recordings));
        return;
    }

    const std::int64_t usable = axc::core::usable_space(context_.archive_dir);
    if (usable <= context_.recording.low_storage_space_threshold)
    {
        session->send_error_response(
            request.correlation_id, STORAGE_SPACE,
            "low storage threshold=" + std::to_string(context_.recording.low_storage_space_threshold) +
                " <= usableSpace=" + std::to_string(usable));
        return;
    }

    auto key = make_key(request.stream_id, request.channel);
    if (!key)
    {
        session->send_error_response(request.correlation_id, std::string{key.error().message.view()});
        return;
    }

    if (recording_subscriptions_.find(*key) != recording_subscriptions_.end())
    {
        session->send_error_response(
            request.correlation_id, ACTIVE_SUBSCRIPTION,
            "recording exists for streamId=" + std::to_string(request.stream_id) + " channel=" + request.channel);
        return;
    }

    auto stripped = stripped_channel(request.channel);
    if (!stripped)
    {
        session->send_error_response(request.correlation_id, std::string{stripped.error().message.view()});
        return;
    }

    const std::string subscription_channel =
        SOURCE_LOCATION_LOCAL == request.source_location && is_udp_channel(request.channel) ? "aeron-spy:" + *stripped
                                                                                            : *stripped;

    PendingRecordingSubscription pending;
    pending.key = *key;
    pending.stripped_channel = *stripped;
    pending.original_channel = request.channel;
    pending.stream_id = request.stream_id;
    pending.control_session_id = session->session_id();
    pending.correlation_id = request.correlation_id;
    pending.auto_stop = request.auto_stop;
    pending.subscription_add = client_.add_subscription(subscription_channel, request.stream_id, {});
    pending_recording_subscriptions_.push_back(std::move(pending));
}

int ArchiveConductor::poll_pending_recording_subscriptions()
{
    int work_count = 0;

    for (auto it = pending_recording_subscriptions_.begin(); it != pending_recording_subscriptions_.end();)
    {
        auto polled = it->subscription_add.poll();
        if (!polled)
        {
            log_warning(std::string{"recording subscription add failed: "} +
                        std::string{polled.error().message.view()});
            if (auto* session = control_session_by_id(it->control_session_id))
            {
                session->send_error_response(it->correlation_id, std::string{polled.error().message.view()});
            }
            it = pending_recording_subscriptions_.erase(it);
            continue;
        }

        if (!polled->has_value())
        {
            ++it;
            continue;
        }

        auto handle = std::move(**polled);

        RecordingSubscription subscription;
        subscription.key = it->key;
        subscription.stripped_channel = it->stripped_channel;
        subscription.original_channel = it->original_channel;
        subscription.stream_id = it->stream_id;
        subscription.control_session_id = it->control_session_id;
        subscription.correlation_id = it->correlation_id;
        subscription.auto_stop = it->auto_stop;
        subscription.registration_id = handle->registration_id;
        subscription.ref_count = 1;
        subscription.subscription = std::move(handle);

        recording_subscription_keys_[subscription.registration_id] = subscription.key;
        recording_subscriptions_[subscription.key] = std::move(subscription);

        // The OK carries the registration id and is sent before any image
        // arrives (`ArchiveConductor.java:570`).
        if (auto* session = control_session_by_id(it->control_session_id))
        {
            session->send_ok_response(it->correlation_id, recording_subscriptions_[it->key].registration_id);
        }

        it = pending_recording_subscriptions_.erase(it);
        work_count++;
    }

    return work_count;
}

int ArchiveConductor::poll_recording_images()
{
    int work_count = 0;
    std::vector<aeron_image_t*> seen;

    for (auto& [key, subscription] : recording_subscriptions_)
    {
        (void)key;
        if (!subscription.subscription)
        {
            continue;
        }

        const int image_count = aeron_subscription_image_count(subscription.subscription.get());
        for (int index = 0; index < image_count; index++)
        {
            aeron_image_t* image =
                aeron_subscription_image_at_index(subscription.subscription.get(), static_cast<std::size_t>(index));
            if (nullptr == image)
            {
                continue;
            }

            seen.push_back(image);
            if (sessions_by_image_.find(image) == sessions_by_image_.end())
            {
                work_count += start_recording_session(subscription, image);
            }

            aeron_image_release(image);
        }
    }

    // A session whose image is no longer in the subscription has no stream left
    // to record; it is aborted before anything can touch the freed image.
    for (auto it = sessions_by_image_.begin(); it != sessions_by_image_.end();)
    {
        if (std::find(seen.begin(), seen.end(), it->first) == seen.end())
        {
            it->second->abort("image unavailable");
            it = sessions_by_image_.erase(it);
        }
        else
        {
            ++it;
        }
    }

    return work_count;
}

int ArchiveConductor::start_recording_session(RecordingSubscription& subscription, aeron_image_t* image)
{
    // `ArchiveConductor.startRecordingSession` (`ArchiveConductor.java:1998-2075`).
    aeron_image_constants_t constants{};
    if (0 != aeron_image_constants(image, &constants))
    {
        log_warning(std::string{"image constants unavailable: "} + aeron_errmsg());
        return 0;
    }

    if (recorder_.size() >= static_cast<std::size_t>(context_.recording.max_concurrent_recordings))
    {
        log_warning("max concurrent recordings reached at " +
                    std::to_string(context_.recording.max_concurrent_recordings));
        return 0;
    }

    const std::int64_t usable = axc::core::usable_space(context_.archive_dir);
    if (usable <= context_.recording.low_storage_space_threshold)
    {
        log_warning("low storage threshold=" + std::to_string(context_.recording.low_storage_space_threshold) +
                    " <= usableSpace=" + std::to_string(usable));
        return 0;
    }

    const auto term_buffer_length = static_cast<std::int32_t>(constants.term_buffer_length);
    const auto segment_file_length = std::max(context_.recording.segment_file_length, term_buffer_length);
    const std::string source_identity = nullptr == constants.source_identity ? "" : constants.source_identity;

    auto recording_id = catalog_.add_new_recording(
        constants.join_position, epoch_clock_.time_ms(), constants.initial_term_id, segment_file_length,
        term_buffer_length, static_cast<std::int32_t>(constants.mtu_length), constants.session_id,
        subscription.stream_id, subscription.stripped_channel, subscription.original_channel, source_identity);
    if (!recording_id)
    {
        log_warning(std::string{"catalog addNewRecording failed: "} + std::string{recording_id.error().message.view()});
        return 0;
    }

    // `RecordingPos.allocate` (`RecordingPos.java:93-132`), two-phase.
    const auto position_key =
        RecordingPos::make_key(context_.archive_id, *recording_id, constants.session_id, source_identity);
    const auto position_label = RecordingPos::make_label(context_.archive_id, *recording_id, constants.session_id,
                                                         subscription.stream_id, subscription.stripped_channel);

    auto counter = allocate_counter(RecordingPos::TYPE_ID, position_key.view(), position_label);
    if (!counter)
    {
        log_warning(std::string{"recording position counter failed: "} + std::string{counter.error().message.view()});
        (void)catalog_.recording_stopped(*recording_id, constants.join_position, epoch_clock_.time_ms());
        return 0;
    }

    axc::core::PositionWriter position = std::move(*counter);
    position.set_release(constants.join_position);

    RecordingWriterContext writer_context;
    writer_context.archive_dir = context_.archive_dir;
    writer_context.file_sync_level = context_.recording.file_sync_level;
    writer_context.archive_dir_fd = -1;

    RecordingSession::Config config;
    config.correlation_id = subscription.correlation_id;
    config.recording_id = *recording_id;
    config.control_session_id = subscription.control_session_id;
    config.subscription_registration_id = subscription.registration_id;
    config.start_position = constants.join_position;
    config.join_position = constants.join_position;
    config.segment_length = segment_file_length;
    config.term_length = term_buffer_length;
    config.block_length_limit = std::min(term_buffer_length, context_.recording.file_io_max_length);
    config.session_id = constants.session_id;
    config.stream_id = subscription.stream_id;
    config.image = image;
    config.auto_stop = subscription.auto_stop;
    config.original_channel = subscription.original_channel;
    config.stripped_channel = subscription.stripped_channel;
    config.source_identity = source_identity;

    RecordingWriter writer(*recording_id, constants.join_position, segment_file_length, term_buffer_length,
                           constants.join_position, std::move(writer_context), &recorder_);

    auto session = std::make_unique<RecordingSession>(
        std::move(config), std::move(writer), std::make_unique<axc::core::PositionWriterTarget>(std::move(position)),
        std::make_unique<AeronRecordingImage>(image));

    sessions_by_image_[image] = session.get();
    recorder_.add_session(std::move(session));
    subscription.ref_count++;
    increment_recording_sessions();

    // `ArchiveConductor.java:2046-2051`: the START signal at the join position.
    if (auto* control = control_session_by_id(subscription.control_session_id))
    {
        control->send_signal(subscription.correlation_id, *recording_id, subscription.registration_id,
                             constants.join_position, archive_codecs::RecordingSignal::START);
    }

    return 1;
}

void ArchiveConductor::close_recording_session(RecordingSession& session)
{
    // `ArchiveConductor.closeRecordingSession` (`ArchiveConductor.java:1329-1363`).
    sessions_by_image_.erase(session.image_handle());

    const std::int64_t position = session.recorded_position();

    (void)catalog_.recording_stopped(session.recording_id(), position, epoch_clock_.time_ms());

    if (auto* control = control_session_by_id(session.control_session_id()))
    {
        if (!session.error_message().empty())
        {
            control->send_error_response(session.correlation_id(), session.error_message());
        }
        control->send_signal(session.correlation_id(), session.recording_id(), session.subscription_registration_id(),
                             position, archive_codecs::RecordingSignal::STOP);
    }

    const auto found = recording_subscription_keys_.find(session.subscription_registration_id());
    if (found != recording_subscription_keys_.end())
    {
        auto subscription = recording_subscriptions_.find(found->second);
        if (subscription != recording_subscriptions_.end())
        {
            subscription->second.ref_count--;
            if (subscription->second.ref_count <= 0 || session.auto_stop())
            {
                recording_subscription_keys_.erase(found);
                recording_subscriptions_.erase(subscription);
            }
        }
    }

    decrement_recording_sessions();
}

void ArchiveConductor::log_warning(std::string_view message)
{
    // `ArchiveConductor.logWarning` (`ArchiveConductor.java:442-445`) feeds the
    // error log; the distinct error log lands with A2b, so the message is kept
    // for the caller (and the tests) in the meantime.
    warnings_.emplace_back(message);
}

}  // namespace deepmsg::archive::server
