#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>

#include "cluster/common/cluster_codecs.h"
#include "cluster/consensus/consensus_constants.h"
#include "core/deepmsg/handles.h"
#include "core/error/error.h"

namespace deepmsg::cluster::consensus
{

using axc::core::Error;

/// The image a connect request arrived on (`Header.context()` in the
/// reference): source identity and session id are part of `sessionInfo`.
struct IngressImageInfo
{
    std::int64_t correlation_id{NULL_VALUE};
    std::string source_identity{};
    std::int32_t session_id{0};
};

/// `IngressAdapter`'s callbacks; `ConsensusModuleAgent` implements them.
class IngressHandler
{
public:
    IngressHandler() = default;
    IngressHandler(const IngressHandler&) = delete;
    IngressHandler& operator=(const IngressHandler&) = delete;
    virtual ~IngressHandler() = default;

    /// `onIngressMessage` (`ConsensusModuleAgent.java:828-853`); false aborts
    /// the poll (the log publication was back-pressured).
    [[nodiscard]] virtual bool on_ingress_message(std::int64_t leadership_term_id, std::int64_t cluster_session_id,
                                                  const std::uint8_t* payload, std::size_t payload_length) = 0;
    virtual void on_session_connect(std::int64_t correlation_id, std::int32_t response_stream_id, std::int32_t version,
                                    std::string_view response_channel,
                                    std::span<const std::uint8_t> encoded_credentials, std::string_view client_info,
                                    const IngressImageInfo& image_info) = 0;
    virtual void on_session_close(std::int64_t leadership_term_id, std::int64_t cluster_session_id) = 0;
    virtual void on_session_keep_alive(std::int64_t leadership_term_id, std::int64_t cluster_session_id,
                                       std::int64_t image_correlation_id) = 0;
    virtual void on_challenge_response(std::int64_t correlation_id, std::int64_t cluster_session_id,
                                       std::span<const std::uint8_t> encoded_credentials) = 0;
    virtual void on_admin_request(std::int64_t leadership_term_id, std::int64_t cluster_session_id,
                                  std::int64_t correlation_id, std::int32_t request_type,
                                  std::span<const std::uint8_t> payload) = 0;
    virtual void on_extension_message(std::int32_t schema_id, std::int32_t template_id,
                                      std::span<const std::uint8_t> message) = 0;
};

/// `IngressAdapter` (`IngressAdapter.java`): the UDP and IPC ingress
/// subscriptions, both polled through a controlled fragment assembler.
///
/// C4b transcribes all six session templates. `AdminRequest` and foreign
/// schemas are handed to the handler; the agent reports them as not
/// transcribed yet (C4e) instead of silently dropping them.
class IngressAdapter
{
public:
    IngressAdapter(IngressHandler& handler, std::int32_t fragment_poll_limit);
    IngressAdapter(const IngressAdapter&) = delete;
    IngressAdapter& operator=(const IngressAdapter&) = delete;
    IngressAdapter(IngressAdapter&&) = delete;
    IngressAdapter& operator=(IngressAdapter&&) = delete;
    ~IngressAdapter();

    void connect(axc::core::SubPtr udp_subscription, axc::core::SubPtr ipc_subscription);

    [[nodiscard]] std::expected<int, Error> poll();

    void free_session_buffer(std::int32_t image_session_id, bool is_ipc);

private:
    static aeron_controlled_fragment_handler_action_t on_fragment(void* clientd, const std::uint8_t* buffer,
                                                                  std::size_t length, aeron_header_t* header);
    [[nodiscard]] aeron_controlled_fragment_handler_action_t dispatch(std::span<const std::uint8_t> message,
                                                                      aeron_header_t* header);
    [[nodiscard]] IngressImageInfo image_info_for(std::int32_t image_session_id) const;

    IngressHandler& handler_;
    std::int32_t fragment_poll_limit_{50};
    axc::core::SubPtr udp_subscription_{};
    axc::core::SubPtr ipc_subscription_{};
    aeron_controlled_fragment_assembler_t* udp_assembler_{nullptr};
    aeron_controlled_fragment_assembler_t* ipc_assembler_{nullptr};
    aeron_subscription_t* current_subscription_{nullptr};
    bool current_is_ipc_{false};
    std::optional<Error> error_{};
};

}  // namespace deepmsg::cluster::consensus
