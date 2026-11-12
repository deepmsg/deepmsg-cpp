#pragma once

#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <vector>

#include <aeronc.h>

#include "core/deepmsg/deepmsg_client.h"
#include "core/error/error.h"

namespace deepmsg::cluster::service
{

using axc::core::Error;

/// `ServiceAdapter.FRAGMENT_LIMIT`.
inline constexpr std::size_t SERVICE_FRAGMENT_LIMIT = 1;

/// One `JoinLog(40)` (`CSA.onJoinLog`, `:449-471`).
struct JoinLogEvent
{
    std::int64_t log_position{-1};
    std::int64_t max_log_position{-1};
    std::int32_t member_id{-1};
    std::int32_t log_session_id{0};
    std::int32_t log_stream_id{0};
    bool is_startup{false};
    std::int32_t role{0};
    bool is_standby{false};
    std::string log_channel{};
};

/// What a poll over the service stream found.
struct ServiceEvents
{
    std::optional<JoinLogEvent> join_log{};
    std::optional<std::pair<std::int64_t, bool>> termination_position{};
    std::optional<std::int64_t> requested_ack_position{};
};

/// `ServiceAdapter` (`ServiceAdapter.java:27-110`): one subscription on the
/// control channel's service stream, dispatching three message kinds.
class ServiceAdapter
{
public:
    explicit ServiceAdapter(aeron_subscription_t* subscription);
    ~ServiceAdapter();

    ServiceAdapter(const ServiceAdapter&) = delete;
    ServiceAdapter& operator=(const ServiceAdapter&) = delete;
    ServiceAdapter(ServiceAdapter&& other) noexcept;
    ServiceAdapter& operator=(ServiceAdapter&& other) noexcept;

    [[nodiscard]] std::expected<std::size_t, Error> poll(ServiceEvents& events);

    /// Decode one whole message into `events` (the body of `onFragment`).
    [[nodiscard]] static std::expected<void, Error> dispatch_message(ServiceEvents& events,
                                                                     std::span<const std::uint8_t> message);

private:
    static aeron_controlled_fragment_handler_action_t on_fragment(void* clientd, const std::uint8_t* buffer,
                                                                  std::size_t length, aeron_header_t* header);

    aeron_subscription_t* subscription_{nullptr};
    aeron_controlled_fragment_assembler_t* assembler_{nullptr};
    ServiceEvents* events_{nullptr};
    std::optional<Error> error_{};
};

}  // namespace deepmsg::cluster::service
