#include "cluster/consensus/pending_service_message_tracker.h"

#include <array>
#include <limits>
#include <optional>
#include <string>
#include <vector>

namespace deepmsg::cluster::consensus
{
namespace
{

using axc::core::ErrorCode;
using axc::core::make_error;

/// Where the cluster session id lives in a queued message: the SBE header (8)
/// plus `SessionMessageHeader.clusterSessionId` (block offset 8).
constexpr std::size_t CLUSTER_SESSION_ID_OFFSET = 16;
/// Where the append position is stashed: the header's `timestamp` field
/// (`SessionMessageHeader.timestampEncodingOffset()` = 16, plus the 8-byte SBE
/// header).
constexpr std::size_t APPEND_POSITION_OFFSET = 24;

[[nodiscard]] std::int64_t read_i64(std::span<const std::uint8_t> message, std::size_t offset) noexcept
{
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < sizeof(std::uint64_t); i++)
    {
        value |= static_cast<std::uint64_t>(message[offset + i]) << (8 * i);
    }
    return static_cast<std::int64_t>(value);
}

void write_i64(std::span<std::uint8_t> message, std::size_t offset, std::int64_t value) noexcept
{
    const auto bits = static_cast<std::uint64_t>(value);
    for (std::size_t i = 0; i < sizeof(std::uint64_t); i++)
    {
        message[offset + i] = static_cast<std::uint8_t>(bits >> (8 * i));
    }
}

}  // namespace

PendingServiceMessageTracker::PendingServiceMessageTracker(std::int32_t service_id, LogAppender& log_publisher,
                                                           std::function<std::int64_t()> cluster_clock)
    : service_id_(service_id), log_publisher_(log_publisher), cluster_clock_(std::move(cluster_clock))
{
    log_service_session_id_ = service_session_id(service_id, std::numeric_limits<std::int64_t>::min());
    next_service_session_id_ = log_service_session_id_ + 1;
}

std::expected<void, Error> PendingServiceMessageTracker::enqueue_message(std::span<const std::uint8_t> message)
{
    const std::int64_t cluster_session_id = next_service_session_id_++;
    if (cluster_session_id > log_service_session_id_)
    {
        if (message.size() < SESSION_HEADER_LENGTH)
        {
            return std::unexpected(
                make_error(ErrorCode::protocol_error, "a service message is shorter than its SessionMessageHeader"));
        }

        // The reference rewrites the id and the timestamp in the control
        // channel's buffer before copying into the ring; a copy of our own
        // keeps the caller's buffer const.
        std::vector<std::uint8_t> stored(message.begin(), message.end());
        write_i64(stored, CLUSTER_SESSION_ID_OFFSET, cluster_session_id);
        write_i64(stored, APPEND_POSITION_OFFSET, NOT_APPENDED_YET);

        if (!pending_messages_.append(stored))
        {
            return std::unexpected(
                make_error(ErrorCode::no_capacity,
                           "pending service message buffer at capacity=" + std::to_string(pending_messages_.size()) +
                               " for serviceId=" + std::to_string(service_id_)));
        }
    }
    return {};
}

void PendingServiceMessageTracker::sweep_follower_messages(std::int64_t cluster_session_id)
{
    log_service_session_id_ = cluster_session_id;
    (void)pending_messages_.consume([this](std::span<const std::uint8_t> message, std::size_t)
                                    { return read_i64(message, CLUSTER_SESSION_ID_OFFSET) <= log_service_session_id_; },
                                    std::numeric_limits<std::size_t>::max());
}

void PendingServiceMessageTracker::sweep_leader_messages(std::int64_t commit_position)
{
    if (uncommitted_messages_ <= 0)
    {
        return;
    }

    const std::size_t consumed = pending_messages_.consume(
        [this, commit_position](std::span<const std::uint8_t> message, std::size_t)
        {
            const std::int64_t append_position = read_i64(message, APPEND_POSITION_OFFSET);
            if (commit_position >= append_position)
            {
                log_service_session_id_ = read_i64(message, CLUSTER_SESSION_ID_OFFSET);
                --uncommitted_messages_;
                return true;
            }
            return false;
        },
        std::numeric_limits<std::size_t>::max());

    pending_message_head_offset_ =
        pending_message_head_offset_ > consumed ? pending_message_head_offset_ - consumed : 0;
}

void PendingServiceMessageTracker::restore_uncommitted_messages(std::int64_t commit_position)
{
    if (uncommitted_messages_ <= 0)
    {
        return;
    }

    sweep_leader_messages(commit_position);
    (void)pending_messages_.for_each_mut(
        0,
        [](std::span<std::uint8_t> message, std::size_t)
        {
            write_i64(message, APPEND_POSITION_OFFSET, NOT_APPENDED_YET);
            return true;
        },
        std::numeric_limits<std::size_t>::max());
    uncommitted_messages_ = 0;
    pending_message_head_offset_ = 0;
}

bool PendingServiceMessageTracker::load_state(std::int64_t next_service_session_id, std::int64_t log_service_session_id,
                                              std::size_t pending_message_capacity)
{
    next_service_session_id_ = next_service_session_id;
    log_service_session_id_ = log_service_session_id;
    return pending_messages_.reset(pending_message_capacity);
}

int PendingServiceMessageTracker::poll()
{
    const std::size_t appended = pending_messages_.for_each_mut(
        pending_message_head_offset_,
        [this](std::span<std::uint8_t> message, std::size_t head_offset)
        {
            const std::int64_t cluster_session_id = read_i64(message, CLUSTER_SESSION_ID_OFFSET);
            const long append_position = log_publisher_.append_message(
                leadership_term_id_, cluster_session_id, cluster_clock_(), message.data() + SESSION_HEADER_LENGTH,
                message.size() - SESSION_HEADER_LENGTH);

            if (append_position > 0)
            {
                ++uncommitted_messages_;
                pending_message_head_offset_ = head_offset;
                write_i64(message, APPEND_POSITION_OFFSET, append_position);
                return true;
            }

            return false;
        },
        SERVICE_MESSAGE_LIMIT);

    return static_cast<int>(appended);
}

std::expected<void, Error> PendingServiceMessageTracker::verify() const
{
    std::int64_t message_count = 0;
    std::optional<Error> error;

    (void)pending_messages_.for_each(
        0,
        [this, &message_count, &error](std::span<const std::uint8_t> message, std::size_t)
        {
            // `messageCount` is one-based in the reference's check.
            ++message_count;
            const std::int64_t cluster_session_id = read_i64(message, CLUSTER_SESSION_ID_OFFSET);
            if (cluster_session_id != (log_service_session_id_ + message_count))
            {
                error = make_error(ErrorCode::protocol_error,
                                   "snapshot has incorrect pending message: serviceId=" + std::to_string(service_id_) +
                                       " nextServiceSessionId=" + std::to_string(next_service_session_id_) +
                                       " logServiceSessionId=" + std::to_string(log_service_session_id_) +
                                       " clusterSessionId=" + std::to_string(cluster_session_id) +
                                       " pendingMessageIndex=" + std::to_string(message_count));
                return false;
            }
            return true;
        },
        std::numeric_limits<std::size_t>::max());

    if (error.has_value())
    {
        return std::unexpected(*error);
    }

    if (next_service_session_id_ != (log_service_session_id_ + message_count + 1))
    {
        return std::unexpected(make_error(
            ErrorCode::protocol_error,
            "snapshot has incorrect pending message state: serviceId=" + std::to_string(service_id_) +
                " nextServiceSessionId=" + std::to_string(next_service_session_id_) + " logServiceSessionId=" +
                std::to_string(log_service_session_id_) + " pendingMessageCount=" + std::to_string(message_count)));
    }

    return {};
}

void PendingServiceMessageTracker::reset()
{
    (void)pending_messages_.for_each_mut(
        0,
        [](std::span<std::uint8_t> message, std::size_t)
        {
            write_i64(message, APPEND_POSITION_OFFSET, NOT_APPENDED_YET);
            return true;
        },
        std::numeric_limits<std::size_t>::max());
}

std::int32_t PendingServiceMessageTracker::service_id_from_log_message(std::int64_t cluster_session_id) noexcept
{
    return static_cast<std::int32_t>((static_cast<std::uint64_t>(cluster_session_id) >> 56) & 0x7F);
}

std::int32_t PendingServiceMessageTracker::service_id_from_service_message(std::int64_t cluster_session_id) noexcept
{
    return static_cast<std::int32_t>(cluster_session_id);
}

std::int64_t PendingServiceMessageTracker::service_session_id(std::int32_t service_id, std::int64_t session_id) noexcept
{
    return (static_cast<std::int64_t>(service_id) << 56) | session_id;
}

}  // namespace deepmsg::cluster::consensus
