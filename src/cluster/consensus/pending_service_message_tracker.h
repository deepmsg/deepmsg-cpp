#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <limits>
#include <span>

#include "cluster/consensus/consensus_constants.h"
#include "cluster/consensus/log_publisher.h"
#include "core/collections/expandable_ring_buffer.h"
#include "core/error/error.h"

namespace deepmsg::cluster::consensus
{

using axc::core::Error;

/// `PendingServiceMessageTracker.SERVICE_MESSAGE_LIMIT` (`:35`): the most
/// queued service messages one `poll` hands to the log.
inline constexpr std::size_t SERVICE_MESSAGE_LIMIT = 20;

/// `Long.MAX_VALUE`: "not appended yet", stashed in the queued message's
/// `timestamp` field.
inline constexpr std::int64_t NOT_APPENDED_YET = std::numeric_limits<std::int64_t>::max();

/// `PendingServiceMessageTracker` (`PendingServiceMessageTracker.java`).
///
/// Messages a service emits are not appended to the log by the service: they
/// are handed to the consensus module on the control channel, **renumbered**
/// here, queued in a ring buffer, and appended by the leader at its own pace
/// (`poll`, at most `SERVICE_MESSAGE_LIMIT` per pass). The leader then sweeps
/// the appended ones as the commit position passes them; a follower sweeps
/// them as the replayed log reaches them.
///
/// Two byte-level facts about the queue:
///  * the `timestamp` field of a stored `SessionMessageHeader` is not a
///    timestamp: the tracker overwrites it with the log position the message
///    was appended at, or `NOT_APPENDED_YET` before it has one;
///  * a service's log messages carry a cluster session id whose top seven bits
///    are the service id, while a message *from* a service uses the service id
///    itself as the cluster session id.
class PendingServiceMessageTracker
{
public:
    /// `new PendingServiceMessageTracker(serviceId, commitPosition,
    /// logPublisher, clusterClock)`. The commit position is passed to the
    /// sweepers instead of being read from a counter here (the caller has just
    /// written the same number).
    PendingServiceMessageTracker(std::int32_t service_id, LogAppender& log_publisher,
                                 std::function<std::int64_t()> cluster_clock);

    PendingServiceMessageTracker(const PendingServiceMessageTracker&) = delete;
    PendingServiceMessageTracker& operator=(const PendingServiceMessageTracker&) = delete;
    /// The agent keeps one per service in a `std::vector`; the reference has an
    /// array. Moving keeps the log publisher reference intact.
    PendingServiceMessageTracker(PendingServiceMessageTracker&&) noexcept = default;

    /// `leadershipTermId(long)` (`:67-70`).
    void leadership_term_id(std::int64_t leadership_term_id) noexcept
    {
        leadership_term_id_ = leadership_term_id;
    }

    [[nodiscard]] std::int32_t service_id() const noexcept
    {
        return service_id_;
    }

    [[nodiscard]] std::int64_t next_service_session_id() const noexcept
    {
        return next_service_session_id_;
    }

    [[nodiscard]] std::int64_t log_service_session_id() const noexcept
    {
        return log_service_session_id_;
    }

    /// `enqueueMessage(...)` (`:87-105`). `message` is the whole queued
    /// message: the SBE header, the `SessionMessageHeader` and the payload, as
    /// it arrived on the control channel.
    [[nodiscard]] std::expected<void, Error> enqueue_message(std::span<const std::uint8_t> message);

    /// `sweepFollowerMessages(clusterSessionId)` (`:107-111`).
    void sweep_follower_messages(std::int64_t cluster_session_id);

    /// `sweepLeaderMessages()` (`:113-120`), with the commit position passed in.
    void sweep_leader_messages(std::int64_t commit_position);

    /// `restoreUncommittedMessages()` (`:122-131`), with the commit position
    /// passed in.
    void restore_uncommitted_messages(std::int64_t commit_position);

    /// `appendMessage(...)` (`:133-136`): the raw snapshot-load path.
    [[nodiscard]] bool append_message(std::span<const std::uint8_t> message)
    {
        return pending_messages_.append(message);
    }

    /// `loadState(...)` (`:138-143`). Returns false when the snapshot's
    /// capacity exceeds the ring's maximum.
    [[nodiscard]] bool load_state(std::int64_t next_service_session_id, std::int64_t log_service_session_id,
                                  std::size_t pending_message_capacity);

    /// `poll()` (`:145-148`): append up to `SERVICE_MESSAGE_LIMIT` queued
    /// messages the log has not taken yet.
    [[nodiscard]] int poll();

    /// `size()` (`:150-153`): the ring's used bytes.
    [[nodiscard]] std::size_t size() const noexcept
    {
        return pending_messages_.size();
    }

    /// `verify()` (`:155-195`): the snapshot's messages must be the
    /// consecutive session ids after `logServiceSessionId`.
    [[nodiscard]] std::expected<void, Error> verify() const;

    /// `reset()` (`:197-200`).
    void reset();

    [[nodiscard]] const axc::core::ExpandableRingBuffer& pending_messages() const noexcept
    {
        return pending_messages_;
    }

    /// `serviceIdFromLogMessage` (`:279-282`): the top seven bits.
    [[nodiscard]] static std::int32_t service_id_from_log_message(std::int64_t cluster_session_id) noexcept;

    /// `serviceIdFromServiceMessage` (`:291-294`): the low 32 bits.
    [[nodiscard]] static std::int32_t service_id_from_service_message(std::int64_t cluster_session_id) noexcept;

    /// `serviceSessionId(serviceId, sessionId)` (`:296-299`).
    [[nodiscard]] static std::int64_t service_session_id(std::int32_t service_id, std::int64_t session_id) noexcept;

private:
    std::int32_t service_id_{0};
    std::size_t pending_message_head_offset_{0};
    std::int64_t uncommitted_messages_{0};
    std::int64_t next_service_session_id_{0};
    std::int64_t log_service_session_id_{0};
    std::int64_t leadership_term_id_{NULL_VALUE};

    LogAppender& log_publisher_;
    std::function<std::int64_t()> cluster_clock_;
    axc::core::ExpandableRingBuffer pending_messages_{};
};

}  // namespace deepmsg::cluster::consensus
