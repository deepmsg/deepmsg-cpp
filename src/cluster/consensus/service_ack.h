#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <expected>
#include <functional>
#include <span>
#include <string_view>
#include <vector>

#include "core/error/error.h"

namespace deepmsg::cluster::consensus
{

using axc::core::Error;

/// State holder for ACKs from each of the `ClusteredService`s
/// (`ServiceAck.java`, Aeron 1.54.0).
///
/// Every service answers a `JoinLog` (and later a snapshot, and a termination)
/// with an ack carrying the same `(logPosition, ackId)` it was asked about. The
/// per-service queues hold one answer each; "reached" means every queue's head
/// matches exactly, and an answer out of sequence is an invariant failure, not
/// something to skip.
struct ServiceAck
{
    std::int64_t ack_id{0};
    std::int64_t log_position{0};
    std::int64_t relevant_id{0};
};

/// `ArrayDeque<ServiceAck>[]`, one per service.
using ServiceAckQueue = std::deque<ServiceAck>;
using ServiceAckQueues = std::vector<ServiceAckQueue>;

/// `ServiceAck.newArrayOfQueues(serviceCount)` (`:146-157`).
[[nodiscard]] ServiceAckQueues new_array_of_queues(std::size_t service_count);

/// `ServiceAck.hasReached(logPosition, ackId, queues)` (`:59-79`).
[[nodiscard]] std::expected<bool, Error> has_reached(std::int64_t log_position, std::int64_t ack_id,
                                                     const ServiceAckQueues& queues);

/// `ServiceAck.removeHead(queues)` (`:81-87`).
void remove_head(ServiceAckQueues& queues);

/// `ServiceAck.pollServiceAcks(queues)` (`:89-104`).
[[nodiscard]] std::expected<std::vector<ServiceAck>, Error> poll_service_acks(ServiceAckQueues& queues);

/// `ServiceAck.pollServiceAcks(logPosition, serviceId, queues)` (`:106-125`):
/// every queue's head must name `logPosition`; an empty queue is an error.
[[nodiscard]] std::expected<std::vector<ServiceAck>, Error> poll_service_acks(std::int64_t log_position,
                                                                              std::int32_t service_id,
                                                                              ServiceAckQueues& queues);

/// `ServiceAck.areAllRelevantIdsNonNull(message, serviceAcks, distinctErrorLog)`
/// (`:127-144`): records `service=<id> <message>` for every ack with a null
/// relevant id, highest service id first.
[[nodiscard]] bool are_all_relevant_ids_non_null(std::string_view message, std::span<const ServiceAck> acks,
                                                 const std::function<void(const Error&)>& report);

}  // namespace deepmsg::cluster::consensus
