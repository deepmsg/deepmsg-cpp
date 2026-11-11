// C4c: `ServiceAck` and its per-service queues (`ServiceAck.java`, 1.54.0).

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "cluster/consensus/service_ack.h"

namespace
{

using deepmsg::cluster::consensus::are_all_relevant_ids_non_null;
using deepmsg::cluster::consensus::has_reached;
using deepmsg::cluster::consensus::new_array_of_queues;
using deepmsg::cluster::consensus::poll_service_acks;
using deepmsg::cluster::consensus::remove_head;
using deepmsg::cluster::consensus::ServiceAck;
using deepmsg::cluster::consensus::ServiceAckQueues;

TEST(ServiceAckTest, everyQueueMustHoldTheExactHead)
{
    ServiceAckQueues queues = new_array_of_queues(2);
    queues[0].push_back(ServiceAck{5, 100, 0});

    auto first = has_reached(100, 5, queues);
    ASSERT_TRUE(first);
    EXPECT_FALSE(*first) << "the second queue is empty";

    queues[1].push_back(ServiceAck{5, 100, 1});
    auto both = has_reached(100, 5, queues);
    ASSERT_TRUE(both);
    EXPECT_TRUE(*both);

    queues[1].front().ack_id = 4;
    auto mismatch = has_reached(100, 5, queues);
    ASSERT_FALSE(mismatch);
    EXPECT_NE(std::string::npos, mismatch.error().message.view().find("ack out of sequence"));
}

TEST(ServiceAckTest, removeHeadPopsEveryQueue)
{
    ServiceAckQueues queues = new_array_of_queues(2);
    queues[0].push_back(ServiceAck{1, 10, 0});
    queues[1].push_back(ServiceAck{1, 10, 1});

    remove_head(queues);
    EXPECT_TRUE(queues[0].empty());
    EXPECT_TRUE(queues[1].empty());
}

TEST(ServiceAckTest, pollServiceAcksErrorsOnAnEmptyQueue)
{
    ServiceAckQueues queues = new_array_of_queues(2);
    queues[0].push_back(ServiceAck{1, 10, 0});

    auto polled = poll_service_acks(queues);
    ASSERT_FALSE(polled);
    EXPECT_NE(std::string::npos, polled.error().message.view().find("invalid ack for serviceId=1"));
}

TEST(ServiceAckTest, pollServiceAcksAtChecksEveryQueuesPosition)
{
    // Java (`ServiceAck.java:106-125`) checks `logPosition` on every queue, not
    // only on the service id the ack came from.
    ServiceAckQueues queues = new_array_of_queues(2);
    queues[0].push_back(ServiceAck{1, 10, 0});
    queues[1].push_back(ServiceAck{1, 11, 1});

    auto polled = poll_service_acks(10, 0, queues);
    ASSERT_FALSE(polled);
    EXPECT_NE(std::string::npos, polled.error().message.view().find("invalid ack for serviceId=0"));
}

TEST(ServiceAckTest, aNullRelevantIdIsReportedFromTheHighestServiceDown)
{
    const std::vector<ServiceAck> acks = {ServiceAck{0, 0, -1}, ServiceAck{0, 0, 7}, ServiceAck{0, 0, -1}};
    std::vector<std::string> messages;

    const bool complete =
        are_all_relevant_ids_non_null("failed to take snapshot", acks, [&messages](const axc::core::Error& error)
                                      { messages.emplace_back(error.message.view()); });

    EXPECT_FALSE(complete);
    ASSERT_EQ(2u, messages.size());
    EXPECT_EQ("service=2 failed to take snapshot", messages[0]);
    EXPECT_EQ("service=0 failed to take snapshot", messages[1]);
}

}  // namespace
