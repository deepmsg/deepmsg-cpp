#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string_view>

#include <aeronc.h>

#include "cluster/common/cluster_codecs.h"
#include "core/deepmsg/deepmsg_client.h"
#include "core/error/error.h"
#include "io_aeron_cluster_codecs/CloseReason.h"

namespace deepmsg::cluster::consensus
{

using axc::core::Error;

namespace codecs = ::io::aeron::cluster::codecs;

/// What the consensus module does with each log record
/// (`ConsensusModuleAgent.onReplay*`, Aeron 1.54.0 `:1512-1653`).
class LogMessageHandler
{
public:
    LogMessageHandler() = default;
    LogMessageHandler(const LogMessageHandler&) = delete;
    LogMessageHandler& operator=(const LogMessageHandler&) = delete;
    virtual ~LogMessageHandler() = default;

    /// `onReplaySessionMessage` (`:1512-1524`).
    virtual void on_replay_session_message(std::int64_t cluster_session_id, std::int64_t timestamp) = 0;
    /// `onReplayTimerEvent` (`:1555-1561`).
    virtual void on_replay_timer_event(std::int64_t correlation_id) = 0;
    /// `onReplaySessionOpen` (`:1563-1573`).
    virtual void on_replay_session_open(std::int64_t log_position, std::int64_t correlation_id,
                                        std::int64_t cluster_session_id, std::int64_t timestamp,
                                        std::int32_t response_stream_id, std::string_view response_channel) = 0;
    /// `onReplaySessionClose` (`:1575-1578`).
    virtual void on_replay_session_close(std::int64_t cluster_session_id, codecs::CloseReason::Value close_reason) = 0;
    /// `onReplayClusterAction` (`:1580-1607`); the caller's handler can refuse
    /// (the replay poll then aborts, as the reference's `BREAK`/throw does).
    [[nodiscard]] virtual std::expected<void, Error> on_replay_cluster_action(std::int64_t leadership_term_id,
                                                                              std::int64_t log_position,
                                                                              std::int64_t timestamp,
                                                                              std::int32_t action,
                                                                              std::int32_t flags) = 0;
    /// `onReplayNewLeadershipTermEvent` (`:1609-1653`).
    [[nodiscard]] virtual std::expected<void, Error> on_replay_new_leadership_term_event(
        std::int64_t leadership_term_id, std::int64_t log_position, std::int64_t timestamp,
        std::int64_t term_base_log_position, std::int32_t time_unit_code, std::int32_t app_version) = 0;
};

/// `LogAdapter` (`LogAdapter.java`): the consensus module's view of the leader
/// log. The container's `BoundedLogAdapter` (`service/log_adapter.h`) is a
/// different class with a different message set; do not merge them.
class LogAdapter
{
public:
    LogAdapter(LogMessageHandler& handler, std::size_t fragment_limit);
    ~LogAdapter();

    LogAdapter(const LogAdapter&) = delete;
    LogAdapter& operator=(const LogAdapter&) = delete;

    /// `disconnect` (`:55-84`): release the image and answer the subscription
    /// registration id (`NULL_VALUE` when there is none).
    [[nodiscard]] std::int64_t disconnect();

    /// `position()` (`:86-94`): the image's position, or 0 without one.
    [[nodiscard]] std::int64_t position() const;

    /// `poll(boundPosition)` (`:96-104`).
    [[nodiscard]] std::expected<int, Error> poll(std::int64_t bound_position);

    [[nodiscard]] bool is_image_closed() const;
    /// `isLogEndOfStream()` (`:111-114`).
    [[nodiscard]] bool is_log_end_of_stream() const;
    /// `isLogEndOfStreamAt(position)` (`:116-119`).
    [[nodiscard]] bool is_log_end_of_stream_at(std::int64_t position) const;

    [[nodiscard]] aeron_image_t* image() const
    {
        return image_;
    }
    /// `image(Image)` (`:126-134`): take the subscription's image, release any
    /// previous one.
    void image(aeron_image_t* image);

    /// The log subscription the image belongs to (the host owns it).
    void subscription(aeron_subscription_t* subscription)
    {
        subscription_ = subscription;
    }
    /// The client that owns the subscription; only `async_remove_destination`
    /// needs it.
    void client(axc::core::DeepmsgClient* client)
    {
        client_ = client;
    }
    [[nodiscard]] aeron_subscription_t* subscription() const
    {
        return subscription_;
    }

    /// `asyncRemoveDestination` (`:136-142`).
    [[nodiscard]] bool async_remove_destination(std::string_view destination);

private:
    static aeron_controlled_fragment_handler_action_t on_fragment(void* clientd, const std::uint8_t* buffer,
                                                                  std::size_t length, aeron_header_t* header);
    [[nodiscard]] aeron_controlled_fragment_handler_action_t dispatch(std::span<const std::uint8_t> message,
                                                                      std::int64_t log_position);

    aeron_controlled_fragment_assembler_t* assembler_{nullptr};
    LogMessageHandler* handler_{nullptr};
    aeron_image_t* image_{nullptr};
    aeron_subscription_t* subscription_{nullptr};
    axc::core::DeepmsgClient* client_{nullptr};
    aeron_async_destination_t* async_destination_{nullptr};
    std::optional<Error> error_{};
    std::size_t fragment_limit_;
};

}  // namespace deepmsg::cluster::consensus
