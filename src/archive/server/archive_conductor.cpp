#include "archive/server/archive_conductor.h"

#include <unistd.h>

#include <cerrno>
#include <utility>

#include "core/deepmsg/image_handlers.h"
#include "core/time/clock.h"

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
            cached_clock_.update(epoch_clock_.time_ms());
            return {};
        }

        // The driver has to answer the async add, so the conductor has to keep
        // turning between polls (`铁律 2`: never block inside an add).
        const int work = client_.do_work();
        (void)work;
        ::usleep(1000);
    }

    return std::unexpected(
        axc::core::make_error(axc::core::ErrorCode::io_error, "control subscription did not complete"));
}

void ArchiveConductor::close()
{
    sessions_ = SessionWorker<ControlSession>{};
    publications_.clear();
    control_subscription_.reset();
    client_ = axc::core::DeepmsgClient{};
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

    // `ControlSession.init` adds the publication asynchronously and waits for it
    // in INIT (`ControlSession.java:859-908`); the add is started here and
    // completed in `poll_pending_publications`.
    aeron_async_add_exclusive_publication_t* async = nullptr;
    if (0 != aeron_async_add_exclusive_publication(&async, client_.handle(), request.response_channel.c_str(),
                                                   request.response_stream_id))
    {
        log_warning("failed to add the control publication on " + request.response_channel);
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

        auto wrapper = std::make_unique<AeronExclusivePublication>(publication);
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

void ArchiveConductor::log_warning(std::string_view message)
{
    // `ArchiveConductor.logWarning` (`ArchiveConductor.java:442-445`) feeds the
    // error log; the distinct error log lands with A2b, so the message is kept
    // for the caller (and the tests) in the meantime.
    warnings_.emplace_back(message);
}

}  // namespace deepmsg::archive::server
