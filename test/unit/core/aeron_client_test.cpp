#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <optional>
#include <string>
#include <thread>

#include <aeronmd.h>

#include "core/aeron/aeron_client.h"
#include "core/counters/position_view.h"
#include "core/counters/position_writer.h"

namespace
{

using axc::core::AeronClient;
using axc::core::CounterPtr;
using axc::core::ExPubPtr;
using axc::core::PositionView;
using axc::core::PositionWriter;
using axc::core::SubPtr;

/// Embedded C media driver on a private temp directory.
class EmbeddedDriver
{
public:
    EmbeddedDriver()
    {
        char pattern[] = "/tmp/axc-aeron-XXXXXX";
        if (nullptr == ::mkdtemp(pattern))
        {
            return;
        }
        dir_ = pattern;

        if (0 != aeron_driver_context_init(&context_))
        {
            return;
        }
        if (0 != aeron_driver_context_set_dir(context_, dir_.c_str()))
        {
            return;
        }
        aeron_driver_context_set_dir_delete_on_start(context_, true);
        aeron_driver_context_set_dir_delete_on_shutdown(context_, true);

        if (0 != aeron_driver_init(&driver_, context_))
        {
            return;
        }

        started_ = 0 == aeron_driver_start(driver_, false);
    }

    ~EmbeddedDriver()
    {
        if (nullptr != driver_)
        {
            aeron_driver_close(driver_);
        }
        if (nullptr != context_)
        {
            aeron_driver_context_close(context_);
        }
        if (!dir_.empty())
        {
            ::rmdir(dir_.c_str());
        }
    }

    EmbeddedDriver(const EmbeddedDriver&) = delete;
    EmbeddedDriver& operator=(const EmbeddedDriver&) = delete;

    [[nodiscard]] bool ok() const noexcept
    {
        return started_;
    }

    [[nodiscard]] const std::string& dir() const noexcept
    {
        return dir_;
    }

private:
    std::string dir_;
    aeron_driver_context_t* context_{nullptr};
    aeron_driver_t* driver_{nullptr};
    bool started_{false};
};

std::optional<AeronClient> connect_with_retry(const std::string& dir)
{
    for (int attempt = 0; attempt < 200; ++attempt)
    {
        auto connected = AeronClient::connect({dir, true});
        if (connected.has_value())
        {
            return std::optional<AeronClient>{std::move(*connected)};
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return std::nullopt;
}

template <class Predicate>
bool pump_until(AeronClient& client, Predicate predicate, std::chrono::milliseconds timeout = std::chrono::seconds(10))
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;

    while (!predicate())
    {
        if (std::chrono::steady_clock::now() >= deadline)
        {
            return false;
        }
        // aeron_main_do_work returns the work count, negative on error.
        if (0 > client.do_work())
        {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }

    return true;
}

struct ImageCapture
{
    std::atomic<aeron_image_t*> image{nullptr};
};

void on_available_image(void* clientd, aeron_subscription_t*, aeron_image_t* image)
{
    static_cast<ImageCapture*>(clientd)->image.store(image, std::memory_order_release);
}

void on_unavailable_image(void* clientd, aeron_subscription_t*, aeron_image_t*)
{
    static_cast<ImageCapture*>(clientd)->image.store(nullptr, std::memory_order_release);
}

struct MessageCapture
{
    int count{0};
    std::size_t last_length{0};
};

void on_fragment(void* clientd, const std::uint8_t*, std::size_t length, aeron_header_t*)
{
    auto* capture = static_cast<MessageCapture*>(clientd);
    ++capture->count;
    capture->last_length = length;
}

}  // namespace

TEST(AeronClient, ConnectsAndExposesCountersReader)
{
    EmbeddedDriver driver;
    ASSERT_TRUE(driver.ok());

    auto client = connect_with_retry(driver.dir());
    ASSERT_TRUE(client.has_value());

    ASSERT_TRUE(pump_until(*client, [&] { return nullptr != client->counters_reader(); }));
    EXPECT_GE(aeron_counters_reader_max_counter_id(client->counters_reader()), 0);
}

TEST(AeronClient, AddsCounterAndReadsPositionByAddress)
{
    EmbeddedDriver driver;
    ASSERT_TRUE(driver.ok());

    auto connected = connect_with_retry(driver.dir());
    ASSERT_TRUE(connected.has_value());
    AeronClient client = std::move(*connected);
    ASSERT_TRUE(pump_until(client, [&] { return nullptr != client.counters_reader(); }));

    const std::array<std::uint8_t, 8> key{1, 2, 3, 4, 5, 6, 7, 8};
    auto add = client.add_counter(999, key, "axc-test-counter");

    std::optional<CounterPtr> counter;
    std::string poll_error;
    ASSERT_TRUE(pump_until(client,
                           [&]
                           {
                               auto polled = add.poll();
                               if (!polled)
                               {
                                   poll_error = polled.error().message.c_str();
                                   return true;
                               }
                               if (polled->has_value())
                               {
                                   counter = std::move(**polled);
                                   return true;
                               }
                               return false;
                           }));
    ASSERT_TRUE(counter.has_value()) << poll_error;

    aeron_counter_constants_t constants{};
    ASSERT_EQ(0, aeron_counter_constants(counter->get(), &constants));

    PositionWriter writer{std::move(*counter)};
    ASSERT_TRUE(writer.valid());
    EXPECT_GE(writer.id(), 0);

    writer.set_release(1234);
    EXPECT_EQ(1234, writer.get());

    const PositionView view = PositionView::at(client.counters_reader(), writer.id());
    ASSERT_TRUE(view.valid());
    EXPECT_EQ(1234, view.get_acquire());
    EXPECT_TRUE(view.is_allocated_by(constants.registration_id));
    EXPECT_FALSE(view.is_allocated_by(constants.registration_id + 1));
    EXPECT_FALSE(PositionView::at(client.counters_reader(), 100000).valid());
}

TEST(AeronClient, PublishesAndReceivesOverIpc)
{
    EmbeddedDriver driver;
    ASSERT_TRUE(driver.ok());

    auto connected = connect_with_retry(driver.dir());
    ASSERT_TRUE(connected.has_value());
    AeronClient client = std::move(*connected);
    ASSERT_TRUE(pump_until(client, [&] { return nullptr != client.counters_reader(); }));

    ImageCapture images;
    const axc::core::ImageHandlers handlers{on_available_image, &images, on_unavailable_image, &images};

    auto sub_add = client.add_subscription("aeron:ipc", 1001, handlers);
    auto pub_add = client.add_exclusive_publication("aeron:ipc", 1001);

    std::optional<SubPtr> subscription;
    std::optional<ExPubPtr> publication;
    std::string poll_error;

    ASSERT_TRUE(pump_until(client,
                           [&]
                           {
                               // Poll each async add exactly until it completes; polling a completed
                               // AsyncAdd again is an error by design.
                               if (!subscription.has_value())
                               {
                                   auto sub = sub_add.poll();
                                   if (!sub)
                                   {
                                       poll_error = sub.error().message.c_str();
                                       return true;
                                   }
                                   if (sub->has_value())
                                   {
                                       subscription = std::move(**sub);
                                   }
                               }

                               if (!publication.has_value())
                               {
                                   auto pub = pub_add.poll();
                                   if (!pub)
                                   {
                                       poll_error = pub.error().message.c_str();
                                       return true;
                                   }
                                   if (pub->has_value())
                                   {
                                       publication = std::move(**pub);
                                   }
                               }

                               return subscription.has_value() && publication.has_value();
                           }));
    ASSERT_TRUE(subscription.has_value()) << poll_error;
    ASSERT_TRUE(publication.has_value()) << poll_error;

    const std::array<std::uint8_t, 9> message{'h', 'e', 'l', 'l', 'o', '-', 'x', '0', '1'};
    bool offered = false;
    ASSERT_TRUE(pump_until(client,
                           [&]
                           {
                               const auto result = aeron_exclusive_publication_offer(publication->get(), message.data(),
                                                                                     message.size(), nullptr, nullptr);
                               if (result > 0)
                               {
                                   offered = true;
                                   return true;
                               }
                               return false;
                           }));
    ASSERT_TRUE(offered);

    MessageCapture messages;
    ASSERT_TRUE(pump_until(client,
                           [&]
                           {
                               auto* image = images.image.load(std::memory_order_acquire);
                               if (nullptr == image)
                               {
                                   return false;
                               }
                               return aeron_image_poll(image, on_fragment, &messages, 10) > 0 && messages.count > 0;
                           }));
    EXPECT_GT(messages.count, 0);
    EXPECT_GE(messages.last_length, message.size());
}
