#pragma once

#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include <aeronc.h>

#include "cluster/service/clustered_service.h"
#include "core/error/error.h"

namespace deepmsg::cluster::service
{

using axc::core::Error;

/// `ConsensusModule.CLUSTER_ACTION_FLAGS_DEFAULT`.
inline constexpr std::int32_t CLUSTER_ACTION_FLAGS_DEFAULT = 0;
/// `ConsensusModule.CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT`.
inline constexpr std::int32_t CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT = 1;

/// One log message, decoded (`BoundedLogAdapter.java:136-260`).
struct LogMessage
{
    enum class Kind
    {
        session_message,
        timer_event,
        session_open,
        session_close,
        cluster_action,
        new_leadership_term
    };

    Kind kind{Kind::session_message};
    std::int64_t cluster_session_id{-1};
    std::int64_t timestamp{0};
    std::int64_t correlation_id{0};
    std::int64_t leadership_term_id{-1};
    std::int64_t log_position{-1};
    std::int64_t term_base_log_position{0};
    std::int32_t leader_member_id{-1};
    std::int32_t log_session_id{0};
    std::int32_t time_unit_code{0};
    std::int32_t app_version{0};
    std::int32_t response_stream_id{0};
    std::int32_t action{0};
    std::int32_t flags{CLUSTER_ACTION_FLAGS_DEFAULT};
    CloseReason close_reason{CloseReason::CLIENT_ACTION};
    std::string response_channel{};
    std::vector<std::uint8_t> encoded_principal{};
};

/// One message and where it ends.
struct LogEvent
{
    std::int64_t position{0};
    std::int32_t session_id{-1};
    std::int32_t stream_id{-1};
    LogMessage message{};
    std::span<const std::uint8_t> payload{};
};

/// What the adapter calls for each decoded message.
class LogHandler
{
public:
    virtual ~LogHandler() = default;
    [[nodiscard]] virtual std::expected<void, Error> on_log_event(const LogEvent& event) = 0;
};

/// `BoundedLogAdapter` (`BoundedLogAdapter.java`): reads the log image up to
/// the commit counter, never beyond.
class BoundedLogAdapter
{
public:
    explicit BoundedLogAdapter(std::size_t fragment_limit);
    ~BoundedLogAdapter();

    BoundedLogAdapter(const BoundedLogAdapter&) = delete;
    BoundedLogAdapter& operator=(const BoundedLogAdapter&) = delete;
    BoundedLogAdapter(BoundedLogAdapter&& other) noexcept;
    BoundedLogAdapter& operator=(BoundedLogAdapter&& other) noexcept;

    void set_max_log_position(std::int64_t position) noexcept
    {
        max_log_position_ = position;
    }

    [[nodiscard]] bool has_image() const noexcept
    {
        return nullptr != image_;
    }

    [[nodiscard]] aeron_image_t* image() const noexcept
    {
        return image_;
    }

    void set_image(aeron_subscription_t* subscription, aeron_image_t* image) noexcept;

    [[nodiscard]] bool is_done() const noexcept;

    [[nodiscard]] std::expected<std::size_t, Error> poll(std::int64_t limit, LogHandler& handler);

    /// The subscription the image came from, for the close path.
    [[nodiscard]] aeron_subscription_t* subscription() const noexcept
    {
        return subscription_;
    }

    void clear_image() noexcept;

    /// Decode one whole message; exposed for tests.
    [[nodiscard]] static std::expected<LogMessage, Error> decode(std::span<const std::uint8_t> message);

private:
    static aeron_controlled_fragment_handler_action_t on_fragment(void* clientd, const std::uint8_t* buffer,
                                                                  std::size_t length, aeron_header_t* header);

    aeron_subscription_t* subscription_{nullptr};
    aeron_image_t* image_{nullptr};
    std::int64_t max_log_position_{0};
    std::size_t fragment_limit_{50};
    aeron_controlled_fragment_assembler_t* assembler_{nullptr};
    LogHandler* handler_{nullptr};
    std::optional<Error> error_{};
};

}  // namespace deepmsg::cluster::service
