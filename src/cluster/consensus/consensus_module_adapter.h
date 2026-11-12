#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>

#include <aeronc.h>

#include "core/deepmsg/handles.h"
#include "core/error/error.h"

namespace deepmsg::cluster::consensus
{

using axc::core::Error;

/// `ConsensusModuleAdapter.FRAGMENT_LIMIT` (`ConsensusModuleAdapter.java:30`).
inline constexpr std::int32_t FRAGMENT_LIMIT = 25;

/// The six dispatches a service can make (`ConsensusModuleAdapter.java:59-151`).
///
/// `on_service_message` receives the **whole** queued message (SBE header +
/// `SessionMessageHeader` + payload), which is what the tracker stores.
/// `on_service_ack` returning successfully makes the adapter stop the poll
/// (`BREAK`): an ack changes the module's state (snapshot, termination), and
/// the reference stops reading at that point so the rest of the messages are
/// handled against the new state.
class ServiceControlHandler
{
public:
    ServiceControlHandler() = default;
    ServiceControlHandler(const ServiceControlHandler&) = delete;
    ServiceControlHandler& operator=(const ServiceControlHandler&) = delete;
    virtual ~ServiceControlHandler() = default;

    [[nodiscard]] virtual std::expected<void, Error> on_service_message(std::int64_t cluster_session_id,
                                                                        std::span<const std::uint8_t> message) = 0;
    [[nodiscard]] virtual std::expected<void, Error> on_service_close_session(std::int64_t cluster_session_id) = 0;
    virtual void on_schedule_timer(std::int64_t correlation_id, std::int64_t deadline) = 0;
    virtual void on_cancel_timer(std::int64_t correlation_id) = 0;
    [[nodiscard]] virtual std::expected<void, Error> on_service_ack(std::int64_t log_position, std::int64_t timestamp,
                                                                    std::int64_t ack_id, std::int64_t relevant_id,
                                                                    std::int32_t service_id) = 0;
    [[nodiscard]] virtual std::expected<void, Error> on_cluster_members_query(std::int64_t correlation_id,
                                                                              bool is_extended_request) = 0;
};

/// `ConsensusModuleAdapter` (`ConsensusModuleAdapter.java`): the container ->
/// consensus module direction of S2, one subscription on the control channel's
/// consensus-module stream, polled through a controlled fragment assembler.
class ConsensusModuleAdapter
{
public:
    explicit ConsensusModuleAdapter(ServiceControlHandler& handler, std::int32_t fragment_poll_limit = FRAGMENT_LIMIT);
    ConsensusModuleAdapter(const ConsensusModuleAdapter&) = delete;
    ConsensusModuleAdapter& operator=(const ConsensusModuleAdapter&) = delete;
    ~ConsensusModuleAdapter();

    void connect(axc::core::SubPtr subscription);

    [[nodiscard]] std::expected<int, Error> poll();

    /// Decode and dispatch one whole message -- the body of `onFragment`, and
    /// what the dispatch tests drive directly.
    [[nodiscard]] std::expected<aeron_controlled_fragment_handler_action_t, Error> dispatch_message(
        std::span<const std::uint8_t> message);

    [[nodiscard]] bool is_connected() const noexcept
    {
        return nullptr != subscription_;
    }

private:
    static aeron_controlled_fragment_handler_action_t on_fragment(void* clientd, const std::uint8_t* buffer,
                                                                  std::size_t length, aeron_header_t* header);

    ServiceControlHandler& handler_;
    std::int32_t fragment_poll_limit_{FRAGMENT_LIMIT};
    axc::core::SubPtr subscription_{};
    aeron_controlled_fragment_assembler_t* assembler_{nullptr};
    std::optional<Error> error_{};
};

}  // namespace deepmsg::cluster::consensus
