#include "cluster/consensus/service_ack.h"

#include <string>

#include "cluster/consensus/consensus_constants.h"

namespace deepmsg::cluster::consensus
{
namespace
{

using axc::core::ErrorCode;
using axc::core::make_error;

}  // namespace

ServiceAckQueues new_array_of_queues(std::size_t service_count)
{
    return ServiceAckQueues(service_count);
}

std::expected<bool, Error> has_reached(std::int64_t log_position, std::int64_t ack_id, const ServiceAckQueues& queues)
{
    for (std::size_t service_id = 0; service_id < queues.size(); service_id++)
    {
        const ServiceAckQueue& queue = queues[service_id];
        if (queue.empty())
        {
            return false;
        }

        const ServiceAck& service_ack = queue.front();
        if (service_ack.ack_id != ack_id || service_ack.log_position != log_position)
        {
            return std::unexpected(
                make_error(ErrorCode::protocol_error, "ack out of sequence: expected [ackId=" + std::to_string(ack_id) +
                                                          ", logPosition=" + std::to_string(log_position) +
                                                          "] vs received [ackId=" + std::to_string(service_ack.ack_id) +
                                                          ", logPosition=" + std::to_string(service_ack.log_position) +
                                                          ", relevantId=" + std::to_string(service_ack.relevant_id) +
                                                          ", serviceId=" + std::to_string(service_id) + "]"));
        }
    }

    return true;
}

void remove_head(ServiceAckQueues& queues)
{
    for (ServiceAckQueue& queue : queues)
    {
        if (!queue.empty())
        {
            queue.pop_front();
        }
    }
}

std::expected<std::vector<ServiceAck>, Error> poll_service_acks(ServiceAckQueues& queues)
{
    std::vector<ServiceAck> acks;
    acks.reserve(queues.size());

    for (std::size_t service_id = 0; service_id < queues.size(); service_id++)
    {
        if (queues[service_id].empty())
        {
            return std::unexpected(
                make_error(ErrorCode::protocol_error, "invalid ack for serviceId=" + std::to_string(service_id)));
        }
        acks.push_back(queues[service_id].front());
        queues[service_id].pop_front();
    }

    return acks;
}

std::expected<std::vector<ServiceAck>, Error> poll_service_acks(std::int64_t log_position, std::int32_t service_id,
                                                                ServiceAckQueues& queues)
{
    std::vector<ServiceAck> acks;
    acks.reserve(queues.size());

    for (std::size_t id = 0; id < queues.size(); id++)
    {
        ServiceAckQueue& queue = queues[id];
        if (queue.empty() || queue.front().log_position != log_position)
        {
            return std::unexpected(
                make_error(ErrorCode::protocol_error,
                           "invalid ack for serviceId=" + std::to_string(service_id) +
                               " logPosition=" + std::to_string(log_position) + " " +
                               (queue.empty() ? std::string{"<empty>"} : std::to_string(queue.front().ack_id))));
        }
        acks.push_back(queue.front());
        queue.pop_front();
    }

    return acks;
}

bool are_all_relevant_ids_non_null(std::string_view message, std::span<const ServiceAck> acks,
                                   const std::function<void(const Error&)>& report)
{
    bool is_set_complete = true;

    for (std::size_t service_id = acks.size(); service_id-- > 0;)
    {
        if (NULL_VALUE == acks[service_id].relevant_id)
        {
            report(make_error(ErrorCode::protocol_error,
                              "service=" + std::to_string(service_id) + " " + std::string{message}));
            is_set_complete = false;
        }
    }

    return is_set_complete;
}

}  // namespace deepmsg::cluster::consensus
