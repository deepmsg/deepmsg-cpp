#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <aeronc.h>

#include "cluster/client/client_config.h"
#include "cluster/common/cluster_codecs.h"
#include "io_aeron_cluster_codecs/EventCode.h"

namespace deepmsg::cluster::client
{

/// `EgressPoller` (`EgressPoller.java:31-333`): one slot, `is_poll_complete`,
/// four cluster templates and `BREAK`/`ABORT` back-pressure.
///
/// The poller does not own the subscription (the reference does): the caller's
/// `DeepmsgClient` owns it, as in the archive's control-response poller.
class EgressPoller
{
public:
    explicit EgressPoller(std::size_t fragment_limit);
    ~EgressPoller();

    EgressPoller(const EgressPoller&) = delete;
    EgressPoller& operator=(const EgressPoller&) = delete;
    EgressPoller(EgressPoller&&) = delete;
    EgressPoller& operator=(EgressPoller&&) = delete;

    /// `egressImage()` (`EP:82-85`); the image the last `SessionEvent` /
    /// `NewLeaderEvent` arrived on.
    [[nodiscard]] aeron_image_t* egress_image() const noexcept
    {
        return egress_image_;
    }

    [[nodiscard]] std::int32_t template_id() const noexcept
    {
        return template_id_;
    }

    [[nodiscard]] std::int64_t cluster_session_id() const noexcept
    {
        return cluster_session_id_;
    }

    [[nodiscard]] std::int64_t correlation_id() const noexcept
    {
        return correlation_id_;
    }

    [[nodiscard]] std::int64_t leadership_term_id() const noexcept
    {
        return leadership_term_id_;
    }

    [[nodiscard]] std::int32_t leader_member_id() const noexcept
    {
        return leader_member_id_;
    }

    [[nodiscard]] std::optional<common::codecs::EventCode::Value> event_code() const noexcept
    {
        return event_code_;
    }

    [[nodiscard]] std::int32_t version() const noexcept
    {
        return version_;
    }

    [[nodiscard]] std::int64_t leader_heartbeat_timeout_ns() const noexcept
    {
        return leader_heartbeat_timeout_ns_;
    }

    [[nodiscard]] std::string_view detail() const noexcept
    {
        return detail_;
    }

    [[nodiscard]] std::span<const std::uint8_t> encoded_challenge() const noexcept
    {
        return encoded_challenge_;
    }

    [[nodiscard]] bool is_poll_complete() const noexcept
    {
        return is_poll_complete_;
    }

    /// `isChallenged()` (`EP:202-205`).
    [[nodiscard]] bool is_challenged() const noexcept;

    /// Reset the slot when the last poll completed, then poll the subscription
    /// (`EP:212-230`). Returns fragments consumed, negative on error.
    [[nodiscard]] int poll(aeron_subscription_t* subscription);

    /// One assembled message (`EP:235-320`). Exposed so the state machine can
    /// be driven without a driver.
    [[nodiscard]] aeron_controlled_fragment_handler_action_t on_message(std::span<const std::uint8_t> message,
                                                                        aeron_image_t* image);

private:
    static aeron_controlled_fragment_handler_action_t on_fragment(void* clientd, const std::uint8_t* buffer,
                                                                  std::size_t length, aeron_header_t* header);

    void reset_slot();

    aeron_controlled_fragment_assembler_t* assembler_{nullptr};
    std::size_t fragment_limit_{10};
    aeron_image_t* egress_image_{nullptr};
    std::int64_t cluster_session_id_{NULL_VALUE};
    std::int64_t correlation_id_{NULL_VALUE};
    std::int64_t leadership_term_id_{NULL_VALUE};
    std::int32_t leader_member_id_{NULL_VALUE};
    std::int32_t template_id_{NULL_VALUE};
    std::int32_t version_{0};
    std::int64_t leader_heartbeat_timeout_ns_{NULL_VALUE};
    bool is_poll_complete_{false};
    std::optional<common::codecs::EventCode::Value> event_code_{};
    std::string detail_{};
    std::vector<std::uint8_t> encoded_challenge_{};
};

}  // namespace deepmsg::cluster::client
