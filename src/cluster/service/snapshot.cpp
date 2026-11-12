#include "cluster/service/snapshot.h"

#include <cstring>
#include <string>
#include <utility>

#include "cluster/service/codecs.h"
#include "cluster/service/consensus_module_proxy.h"
#include "io_aeron_cluster_codecs/ClientSession.h"
#include "io_aeron_cluster_codecs/SnapshotMarker.h"

namespace deepmsg::cluster::service
{
namespace
{

constexpr std::int64_t NULL_POSITION = -1;

[[nodiscard]] Error publication_error(std::int64_t code, const char* what)
{
    return axc::core::make_error(axc::core::ErrorCode::no_capacity,
                                 std::string{what} + " refused: " + std::to_string(code));
}

}  // namespace

// -- SnapshotImage ----------------------------------------------------------

namespace
{

void on_image_fragment(void* clientd, const std::uint8_t* buffer, std::size_t length, aeron_header_t*)
{
    static_cast<SnapshotFragmentHandler*>(clientd)->on_snapshot_fragment(buffer, length);
}

}  // namespace

int SnapshotImage::poll(SnapshotFragmentHandler& handler, std::size_t fragment_limit)
{
    return aeron_image_poll(image_, &on_image_fragment, &handler, fragment_limit);
}

bool SnapshotImage::is_end_of_stream() const noexcept
{
    return aeron_image_is_end_of_stream(image_);
}

bool SnapshotImage::is_closed() const noexcept
{
    return aeron_image_is_closed(image_);
}

std::int64_t SnapshotImage::position() const noexcept
{
    return aeron_image_position(image_);
}

// -- SnapshotPublication ----------------------------------------------------

std::int64_t SnapshotPublication::offer(std::span<const std::uint8_t> payload) noexcept
{
    return aeron_exclusive_publication_offer(publication_, payload.data(), payload.size(), nullptr, nullptr);
}

std::expected<SnapshotPublication::Claim, std::int64_t> SnapshotPublication::try_claim(std::size_t length) noexcept
{
    aeron_buffer_claim_t claim{};
    const std::int64_t position = aeron_exclusive_publication_try_claim(publication_, length, &claim);
    if (position < 0)
    {
        return std::unexpected(position);
    }
    return Claim{claim, position, std::span<std::uint8_t>(claim.data, claim.length)};
}

void SnapshotPublication::idle() noexcept
{
    auto idled = context_->idle(*client_, 0);
    if (!idled)
    {
        // A terminated client is noticed by the agent's next do_work.
    }
}

std::int64_t SnapshotPublication::position() const noexcept
{
    return aeron_exclusive_publication_position(publication_);
}

std::size_t SnapshotPublication::max_payload_length() const noexcept
{
    aeron_publication_constants_t constants{};
    if (0 != aeron_exclusive_publication_constants(publication_, &constants))
    {
        return 0;
    }
    return constants.max_payload_length;
}

std::int32_t SnapshotPublication::session_id() const noexcept
{
    aeron_publication_constants_t constants{};
    if (0 != aeron_exclusive_publication_constants(publication_, &constants))
    {
        return 0;
    }
    return constants.session_id;
}

std::expected<void, Error> SnapshotPublication::Claim::commit() noexcept
{
    if (0 != aeron_buffer_claim_commit(&claim_))
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::internal_error, "failed to commit the snapshot claim"));
    }
    return {};
}

// -- ServiceSnapshotTaker ---------------------------------------------------

std::expected<void, Error> ServiceSnapshotTaker::mark(std::int32_t mark, std::int64_t log_position,
                                                      std::int64_t leadership_term_id)
{
    std::uint8_t buffer[MESSAGE_HEADER_LENGTH + codecs::SnapshotMarker::SBE_BLOCK_LENGTH]{};

    codecs::SnapshotMarker encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer), 0, sizeof(buffer));
    encoder.typeId(SNAPSHOT_TYPE_ID)
        .logPosition(log_position)
        .leadershipTermId(leadership_term_id)
        .index(0)
        .mark(static_cast<codecs::SnapshotMark::Value>(mark))
        .timeUnit(static_cast<codecs::ClusterTimeUnit::Value>(time_unit_code(time_unit_)))
        .appVersion(app_version_);

    return claim(std::span<const std::uint8_t>(buffer, sizeof(buffer)));
}

std::expected<void, Error> ServiceSnapshotTaker::mark_begin(std::int64_t log_position, std::int64_t leadership_term_id)
{
    return mark(static_cast<std::int32_t>(codecs::SnapshotMark::BEGIN), log_position, leadership_term_id);
}

std::expected<void, Error> ServiceSnapshotTaker::mark_end(std::int64_t log_position, std::int64_t leadership_term_id)
{
    return mark(static_cast<std::int32_t>(codecs::SnapshotMark::END), log_position, leadership_term_id);
}

std::expected<void, Error> ServiceSnapshotTaker::snapshot_session(const ContainerClientSession& session)
{
    const std::size_t length = MESSAGE_HEADER_LENGTH + codecs::ClientSession::SBE_BLOCK_LENGTH + sizeof(std::int32_t) +
                               session.response_channel().size() + sizeof(std::int32_t) +
                               session.encoded_principal().size();

    if (length <= publication_->max_payload_length())
    {
        auto claimed = publication_->try_claim(length);
        if (!claimed.has_value())
        {
            if (OFFER_BACK_PRESSURED == claimed.error() || OFFER_ADMIN_ACTION == claimed.error())
            {
                publication_->idle();
                return snapshot_session(session);
            }
            return std::unexpected(publication_error(claimed.error(), "snapshot session claim"));
        }

        codecs::ClientSession encoder;
        encoder.wrapAndApplyHeader(reinterpret_cast<char*>(claimed->payload().data()), 0, claimed->payload().size());
        encoder.clusterSessionId(session.id())
            .responseStreamId(session.response_stream_id())
            .putResponseChannel(session.response_channel())
            .putEncodedPrincipal(reinterpret_cast<const char*>(session.encoded_principal().data()),
                                 session.encoded_principal().size());
        return claimed->commit();
    }

    std::vector<std::uint8_t> buffer(length, 0);
    codecs::ClientSession encoder;
    encoder.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    encoder.clusterSessionId(session.id())
        .responseStreamId(session.response_stream_id())
        .putResponseChannel(session.response_channel())
        .putEncodedPrincipal(reinterpret_cast<const char*>(session.encoded_principal().data()),
                             session.encoded_principal().size());
    return offer(buffer);
}

std::expected<void, Error> ServiceSnapshotTaker::claim(std::span<const std::uint8_t> record)
{
    while (true)
    {
        auto claimed = publication_->try_claim(record.size());
        if (claimed.has_value())
        {
            std::memcpy(claimed->payload().data(), record.data(), record.size());
            return claimed->commit();
        }
        if (OFFER_BACK_PRESSURED == claimed.error() || OFFER_ADMIN_ACTION == claimed.error())
        {
            publication_->idle();
            continue;
        }
        return std::unexpected(publication_error(claimed.error(), "failed to claim snapshot buffer"));
    }
}

std::expected<void, Error> ServiceSnapshotTaker::offer(std::span<const std::uint8_t> record)
{
    while (true)
    {
        const std::int64_t position = publication_->offer(record);
        if (position > 0)
        {
            return {};
        }
        if (OFFER_BACK_PRESSURED == position || OFFER_ADMIN_ACTION == position)
        {
            publication_->idle();
            continue;
        }
        return std::unexpected(publication_error(position, "failed to offer snapshot buffer"));
    }
}

// -- ServiceSnapshotLoader --------------------------------------------------

ServiceSnapshotLoader::ServiceSnapshotLoader()
{
    if (0 != aeron_controlled_fragment_assembler_create(&assembler_, &ServiceSnapshotLoader::on_fragment, this))
    {
        assembler_ = nullptr;
    }
}

ServiceSnapshotLoader::~ServiceSnapshotLoader()
{
    if (nullptr != assembler_)
    {
        (void)aeron_controlled_fragment_assembler_delete(assembler_);
    }
}

aeron_controlled_fragment_handler_action_t ServiceSnapshotLoader::on_fragment(void* clientd, const std::uint8_t* buffer,
                                                                              std::size_t length, aeron_header_t*)
{
    return static_cast<ServiceSnapshotLoader*>(clientd)->dispatch(std::span<const std::uint8_t>(buffer, length));
}

aeron_controlled_fragment_handler_action_t ServiceSnapshotLoader::dispatch(std::span<const std::uint8_t> message)
{
    if (done_)
    {
        return AERON_ACTION_BREAK;
    }

    auto header = message_header(message);
    if (!header)
    {
        error_ = header.error();
        return AERON_ACTION_ABORT;
    }

    switch (header->templateId())
    {
        case codecs::SnapshotMarker::SBE_TEMPLATE_ID:
        {
            auto marker = wrap_body<codecs::SnapshotMarker>(message, *header);
            if (SNAPSHOT_TYPE_ID != marker.typeId())
            {
                error_ = axc::core::make_error(axc::core::ErrorCode::protocol_error,
                                               "unexpected snapshot type: " + std::to_string(marker.typeId()));
                return AERON_ACTION_ABORT;
            }

            if (codecs::SnapshotMark::BEGIN == marker.mark())
            {
                if (in_snapshot_)
                {
                    error_ = axc::core::make_error(axc::core::ErrorCode::protocol_error, "already in snapshot");
                    return AERON_ACTION_ABORT;
                }
                in_snapshot_ = true;
                app_version_ = marker.appVersion();
                time_unit_ = time_unit_from_code(static_cast<std::int32_t>(marker.timeUnit()));
                auto handled = handler_->on_load_begin_snapshot(app_version_, time_unit_);
                if (!handled)
                {
                    error_ = handled.error();
                    return AERON_ACTION_ABORT;
                }
                return AERON_ACTION_CONTINUE;
            }

            if (codecs::SnapshotMark::END == marker.mark())
            {
                if (!in_snapshot_)
                {
                    error_ = axc::core::make_error(axc::core::ErrorCode::protocol_error, "missing begin snapshot");
                    return AERON_ACTION_ABORT;
                }
                done_ = true;
                auto handled = handler_->on_load_end_snapshot();
                if (!handled)
                {
                    error_ = handled.error();
                    return AERON_ACTION_ABORT;
                }
                return AERON_ACTION_BREAK;
            }
            return AERON_ACTION_CONTINUE;
        }
        case codecs::ClientSession::SBE_TEMPLATE_ID:
        {
            auto record = wrap_body<codecs::ClientSession>(message, *header);
            const std::string channel = record.getResponseChannelAsString();
            const std::string principal = record.getEncodedPrincipalAsString();
            auto handled = handler_->on_load_cluster_session(
                record.clusterSessionId(), record.responseStreamId(), channel,
                std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(principal.data()),
                                              principal.size()));
            if (!handled)
            {
                error_ = handled.error();
                return AERON_ACTION_ABORT;
            }
            return AERON_ACTION_CONTINUE;
        }
        default:
            return AERON_ACTION_CONTINUE;
    }
}

std::expected<int, Error> ServiceSnapshotLoader::poll(aeron_image_t* image, SnapshotLoadHandler& handler)
{
    if (done_ || nullptr == assembler_)
    {
        return 0;
    }

    handler_ = &handler;
    error_.reset();
    const int work = aeron_image_bounded_controlled_poll(image, aeron_controlled_fragment_assembler_handler, assembler_,
                                                         INT64_MAX, SNAPSHOT_FRAGMENT_LIMIT);
    handler_ = nullptr;

    if (error_.has_value())
    {
        return std::unexpected(*error_);
    }
    if (work < 0)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::internal_error, "snapshot image poll failed"));
    }
    return work;
}

// -- ArchiveClient ----------------------------------------------------------

std::expected<ArchiveClient, Error> ArchiveClient::connect(axc::core::DeepmsgClient& client,
                                                           const ServiceContainerConfig& config)
{
    ArchiveClient result;

    if (0 != aeron_archive_context_init(&result.context_))
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::internal_error, "aeron_archive_context_init failed"));
    }

    (void)aeron_archive_context_set_aeron(result.context_, client.handle());
    (void)aeron_archive_context_set_owns_aeron_client(result.context_, false);
    (void)aeron_archive_context_set_control_request_channel(result.context_,
                                                            config.archive_local_control_channel.c_str());
    (void)aeron_archive_context_set_control_request_stream_id(result.context_, config.archive_local_control_stream_id);
    (void)aeron_archive_context_set_control_response_channel(result.context_,
                                                             config.archive_local_control_channel.c_str());
    (void)aeron_archive_context_set_control_response_stream_id(result.context_,
                                                               config.archive_control_response_stream_id);
    (void)aeron_archive_context_set_message_timeout_ns(result.context_, 5'000'000'000ULL);

    if (0 != aeron_archive_connect(&result.archive_, result.context_))
    {
        const std::string message = aeron_errmsg();
        (void)aeron_archive_context_close(result.context_);
        result.context_ = nullptr;
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::internal_error, "archive connection failed: " + message));
    }

    result.archive_id_ = aeron_archive_get_archive_id(result.archive_);
    return result;
}

ArchiveClient::ArchiveClient(ArchiveClient&& other) noexcept
    : context_(other.context_), archive_(other.archive_), archive_id_(other.archive_id_)
{
    other.context_ = nullptr;
    other.archive_ = nullptr;
}

ArchiveClient& ArchiveClient::operator=(ArchiveClient&& other) noexcept
{
    if (this != &other)
    {
        if (nullptr != archive_)
        {
            (void)aeron_archive_close(archive_);
        }
        if (nullptr != context_)
        {
            (void)aeron_archive_context_close(context_);
        }
        context_ = other.context_;
        archive_ = other.archive_;
        archive_id_ = other.archive_id_;
        other.context_ = nullptr;
        other.archive_ = nullptr;
    }
    return *this;
}

ArchiveClient::~ArchiveClient()
{
    if (nullptr != archive_)
    {
        (void)aeron_archive_close(archive_);
    }
    if (nullptr != context_)
    {
        (void)aeron_archive_context_close(context_);
    }
}

std::expected<void, Error> ArchiveClient::check_error()
{
    char buffer[1024]{};
    const int result = aeron_archive_poll_for_error_response(archive_, buffer, sizeof(buffer));
    if (result < 0)
    {
        return std::unexpected(
            axc::core::make_error(axc::core::ErrorCode::internal_error,
                                  "archive error response poll failed: " + std::string{aeron_errmsg()}));
    }
    if ('\0' != buffer[0])
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::protocol_error, buffer));
    }
    return {};
}

std::expected<std::int64_t, Error> ArchiveClient::start_recording(std::string_view channel, std::int32_t stream_id)
{
    std::int64_t subscription_id = 0;
    const std::string channel_string{channel};
    if (0 != aeron_archive_start_recording(&subscription_id, archive_, channel_string.c_str(), stream_id,
                                           AERON_ARCHIVE_SOURCE_LOCATION_LOCAL, true))
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::internal_error,
                                                     "archive startRecording failed: " + std::string{aeron_errmsg()}));
    }
    return subscription_id;
}

std::expected<std::int64_t, Error> ArchiveClient::start_replay(std::int64_t recording_id, std::string_view channel,
                                                               std::int32_t stream_id,
                                                               std::int64_t subscription_registration_id)
{
    aeron_archive_replay_params_t params{};
    (void)aeron_archive_replay_params_init(&params);
    params.position = 0;
    params.length = NULL_POSITION;
    params.subscription_registration_id = subscription_registration_id;

    const std::string channel_string{channel};
    std::int64_t replay_session_id = 0;
    if (0 != aeron_archive_start_replay(&replay_session_id, archive_, recording_id, channel_string.c_str(), stream_id,
                                        &params))
    {
        return std::unexpected(axc::core::make_error(axc::core::ErrorCode::internal_error,
                                                     "archive startReplay failed: " + std::string{aeron_errmsg()}));
    }
    return replay_session_id;
}

}  // namespace deepmsg::cluster::service
