#include "cluster/consensus/ingress_adapter.h"

#include <string_view>
#include <utility>

#include "io_aeron_cluster_codecs/AdminRequest.h"
#include "io_aeron_cluster_codecs/ChallengeResponse.h"
#include "io_aeron_cluster_codecs/SessionCloseRequest.h"
#include "io_aeron_cluster_codecs/SessionConnectRequest.h"
#include "io_aeron_cluster_codecs/SessionKeepAlive.h"
#include "io_aeron_cluster_codecs/SessionMessageHeader.h"

namespace deepmsg::cluster::consensus
{
namespace
{

using axc::core::ErrorCode;
using axc::core::make_error;
namespace header_codec = deepmsg::cluster::common::codecs;

constexpr std::size_t SESSION_MESSAGE_HEADER_LENGTH =
    deepmsg::cluster::common::MESSAGE_HEADER_LENGTH + header_codec::SessionMessageHeader::sbeBlockLength();

}  // namespace

IngressAdapter::IngressAdapter(IngressHandler& handler, std::int32_t fragment_poll_limit)
    : handler_(handler), fragment_poll_limit_(fragment_poll_limit)
{
    if (0 != aeron_controlled_fragment_assembler_create(&udp_assembler_, &IngressAdapter::on_fragment, this))
    {
        udp_assembler_ = nullptr;
    }
    if (0 != aeron_controlled_fragment_assembler_create(&ipc_assembler_, &IngressAdapter::on_fragment, this))
    {
        ipc_assembler_ = nullptr;
    }
}

IngressAdapter::~IngressAdapter()
{
    if (nullptr != udp_assembler_)
    {
        (void)aeron_controlled_fragment_assembler_delete(udp_assembler_);
    }
    if (nullptr != ipc_assembler_)
    {
        (void)aeron_controlled_fragment_assembler_delete(ipc_assembler_);
    }
}

void IngressAdapter::connect(axc::core::SubPtr udp_subscription, axc::core::SubPtr ipc_subscription)
{
    udp_subscription_ = std::move(udp_subscription);
    ipc_subscription_ = std::move(ipc_subscription);
}

std::expected<int, Error> IngressAdapter::poll()
{
    if (nullptr == udp_assembler_)
    {
        return std::unexpected(make_error(ErrorCode::internal_error, "ingress assembler was not created"));
    }

    error_.reset();
    int fragments = 0;

    if (udp_subscription_)
    {
        current_subscription_ = udp_subscription_.get();
        current_is_ipc_ = false;
        const int work = aeron_subscription_controlled_poll(
            udp_subscription_.get(), aeron_controlled_fragment_assembler_handler, udp_assembler_, fragment_poll_limit_);
        if (work < 0)
        {
            error_ = make_error(ErrorCode::internal_error, "ingress UDP subscription poll failed");
        }
        else
        {
            fragments += work;
        }
    }

    if (!error_.has_value() && ipc_subscription_ && nullptr != ipc_assembler_)
    {
        current_subscription_ = ipc_subscription_.get();
        current_is_ipc_ = true;
        const int work = aeron_subscription_controlled_poll(
            ipc_subscription_.get(), aeron_controlled_fragment_assembler_handler, ipc_assembler_, fragment_poll_limit_);
        if (work < 0)
        {
            error_ = make_error(ErrorCode::internal_error, "ingress IPC subscription poll failed");
        }
        else
        {
            fragments += work;
        }
    }

    current_subscription_ = nullptr;
    if (error_.has_value())
    {
        return std::unexpected(*error_);
    }
    return fragments;
}

void IngressAdapter::free_session_buffer(std::int32_t image_session_id, bool is_ipc)
{
    aeron_controlled_fragment_assembler_t* assembler = is_ipc ? ipc_assembler_ : udp_assembler_;
    if (nullptr != assembler)
    {
        (void)aeron_controlled_fragment_assembler_delete_session_buffer(assembler, image_session_id);
    }
}

IngressImageInfo IngressAdapter::image_info_for(std::int32_t image_session_id) const
{
    IngressImageInfo info;
    if (nullptr == current_subscription_)
    {
        return info;
    }

    aeron_image_t* image = aeron_subscription_image_by_session_id(current_subscription_, image_session_id);
    if (nullptr == image)
    {
        return info;
    }

    aeron_image_constants_t constants{};
    if (0 != aeron_image_constants(image, &constants))
    {
        return info;
    }

    info.correlation_id = constants.correlation_id;
    info.source_identity = nullptr == constants.source_identity ? std::string{} : constants.source_identity;
    info.session_id = image_session_id;
    return info;
}

aeron_controlled_fragment_handler_action_t IngressAdapter::on_fragment(void* clientd, const std::uint8_t* buffer,
                                                                       std::size_t length, aeron_header_t* header)
{
    auto* adapter = static_cast<IngressAdapter*>(clientd);
    return adapter->dispatch(std::span<const std::uint8_t>(buffer, length), header);
}

aeron_controlled_fragment_handler_action_t IngressAdapter::dispatch(std::span<const std::uint8_t> message,
                                                                    aeron_header_t* header)
{
    auto decoded_header = deepmsg::cluster::common::raw_message_header(message);
    if (!decoded_header)
    {
        error_ = decoded_header.error();
        return AERON_ACTION_ABORT;
    }

    aeron_header_values_t values{};
    const bool has_values = 0 == aeron_header_values(header, &values);
    const std::int32_t image_session_id = has_values ? values.frame.session_id : 0;

    if (decoded_header->schemaId() != deepmsg::cluster::common::SBE_SCHEMA_ID)
    {
        handler_.on_extension_message(decoded_header->schemaId(), decoded_header->templateId(), message);
        return AERON_ACTION_CONTINUE;
    }

    switch (decoded_header->templateId())
    {
        case header_codec::SessionMessageHeader::SBE_TEMPLATE_ID:
        {
            if (message.size() < SESSION_MESSAGE_HEADER_LENGTH)
            {
                error_ = make_error(ErrorCode::protocol_error, "a session message is shorter than its header");
                return AERON_ACTION_ABORT;
            }
            auto decoder =
                deepmsg::cluster::common::wrap_body<header_codec::SessionMessageHeader>(message, *decoded_header);
            const bool continuous = handler_.on_ingress_message(decoder.leadershipTermId(), decoder.clusterSessionId(),
                                                                message.data() + SESSION_MESSAGE_HEADER_LENGTH,
                                                                message.size() - SESSION_MESSAGE_HEADER_LENGTH);
            return continuous ? AERON_ACTION_CONTINUE : AERON_ACTION_ABORT;
        }

        case header_codec::SessionConnectRequest::SBE_TEMPLATE_ID:
        {
            auto decoder =
                deepmsg::cluster::common::wrap_body<header_codec::SessionConnectRequest>(message, *decoded_header);
            // The generated var-string accessor advances the decoder's position,
            // so the length must be read (and kept) before the pointer.
            const std::uint32_t response_channel_length = decoder.responseChannelLength();
            const char* response_channel_ptr = decoder.responseChannel();
            const std::string_view response_channel{response_channel_ptr, response_channel_length};
            const std::string_view credentials = decoder.getEncodedCredentialsAsStringView();
            const std::uint32_t client_info_length = decoder.clientInfoLength();
            const char* client_info_ptr = decoder.clientInfo();
            const std::string_view client_info{client_info_ptr, client_info_length};
            const IngressImageInfo image_info = image_info_for(image_session_id);

            handler_.on_session_connect(
                decoder.correlationId(), decoder.responseStreamId(), decoder.version(), response_channel,
                std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(credentials.data()),
                                              credentials.size()),
                client_info, image_info);
            break;
        }

        case header_codec::SessionCloseRequest::SBE_TEMPLATE_ID:
        {
            auto decoder =
                deepmsg::cluster::common::wrap_body<header_codec::SessionCloseRequest>(message, *decoded_header);
            handler_.on_session_close(decoder.leadershipTermId(), decoder.clusterSessionId());
            break;
        }

        case header_codec::SessionKeepAlive::SBE_TEMPLATE_ID:
        {
            auto decoder =
                deepmsg::cluster::common::wrap_body<header_codec::SessionKeepAlive>(message, *decoded_header);
            handler_.on_session_keep_alive(decoder.leadershipTermId(), decoder.clusterSessionId(),
                                           image_info_for(image_session_id).correlation_id);
            break;
        }

        case header_codec::ChallengeResponse::SBE_TEMPLATE_ID:
        {
            auto decoder =
                deepmsg::cluster::common::wrap_body<header_codec::ChallengeResponse>(message, *decoded_header);
            const std::string_view credentials = decoder.getEncodedCredentialsAsStringView();
            handler_.on_challenge_response(
                decoder.correlationId(), decoder.clusterSessionId(),
                std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(credentials.data()),
                                              credentials.size()));
            break;
        }

        case header_codec::AdminRequest::SBE_TEMPLATE_ID:
        {
            auto decoder = deepmsg::cluster::common::wrap_body<header_codec::AdminRequest>(message, *decoded_header);
            // `payload` is optional (`AdminRequest` schema, id 5). The Java
            // decoder reads its length header past the fragment end into the
            // larger client buffer; an exact-size span must not. A message
            // that stops after the block means an empty payload.
            std::span<const std::uint8_t> payload{};
            if (message.size() >= decoder.sbePosition() + header_codec::AdminRequest::payloadHeaderLength())
            {
                const std::string_view view = decoder.getPayloadAsStringView();
                payload =
                    std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(view.data()), view.size());
            }
            handler_.on_admin_request(decoder.leadershipTermId(), decoder.clusterSessionId(), decoder.correlationId(),
                                      static_cast<std::int32_t>(decoder.requestType()), payload);
            break;
        }

        default:
            break;
    }

    return AERON_ACTION_CONTINUE;
}

}  // namespace deepmsg::cluster::consensus
