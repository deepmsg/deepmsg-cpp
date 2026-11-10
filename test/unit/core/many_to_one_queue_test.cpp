// A5a-1 criteria: `ManyToOneQueue`
// (`org.agrona.concurrent.ManyToOneConcurrentLinkedQueue`), the handoff the
// archive's DEDICATED mode uses between the conductor, the recorder and the
// replayer.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <thread>
#include <vector>

#include "core/collections/many_to_one_queue.h"

namespace
{

TEST(ManyToOneQueueTest, popsNullWhenEmpty)
{
    axc::core::ManyToOneQueue<int> queue;
    EXPECT_TRUE(queue.empty());
    EXPECT_EQ(nullptr, queue.pop());
}

TEST(ManyToOneQueueTest, keepsFifoOrderForOneProducer)
{
    axc::core::ManyToOneQueue<int> queue;
    int values[] = {1, 2, 3, 4, 5};

    for (int& value : values)
    {
        ASSERT_TRUE(queue.push(&value));
    }
    EXPECT_FALSE(queue.empty());

    for (int& value : values)
    {
        EXPECT_EQ(&value, queue.pop());
    }
    EXPECT_EQ(nullptr, queue.pop());
    EXPECT_TRUE(queue.empty());
}

TEST(ManyToOneQueueTest, everyProducersValueArrivesExactlyOnce)
{
    constexpr int PRODUCERS = 4;
    constexpr int PER_PRODUCER = 2000;

    axc::core::ManyToOneQueue<int> queue;
    std::vector<int> values(static_cast<std::size_t>(PRODUCERS * PER_PRODUCER));
    for (int index = 0; index < static_cast<int>(values.size()); ++index)
    {
        values[static_cast<std::size_t>(index)] = index;
    }

    std::vector<std::int64_t> pushed(PRODUCERS, 0);
    std::vector<std::thread> threads;
    threads.reserve(PRODUCERS);
    for (int producer = 0; producer < PRODUCERS; ++producer)
    {
        threads.emplace_back(
            [&queue, &values, &pushed, producer]()
            {
                const int from = producer * PER_PRODUCER;
                for (int index = from; index < from + PER_PRODUCER; ++index)
                {
                    while (!queue.push(&values[static_cast<std::size_t>(index)]))
                    {
                        std::this_thread::yield();
                    }
                    ++pushed[static_cast<std::size_t>(producer)];
                }
            });
    }

    std::vector<int> received;
    received.reserve(values.size());
    while (received.size() < values.size())
    {
        int* value = queue.pop();
        if (nullptr != value)
        {
            received.push_back(*value);
        }
        else
        {
            std::this_thread::yield();
        }
    }

    for (std::thread& thread : threads)
    {
        thread.join();
    }

    std::sort(received.begin(), received.end());
    EXPECT_EQ(values, received);
    EXPECT_EQ(nullptr, queue.pop());
}

}  // namespace
