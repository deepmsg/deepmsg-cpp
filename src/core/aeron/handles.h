#pragma once

#include <memory>

#include <aeronc.h>

namespace axc::core
{

// Close errors are not actionable in a deleter; they surface through the
// driver's error log. Everything else in the code base must not ignore errors.

struct AeronContextCloser
{
    void operator()(aeron_context_t* context) const noexcept
    {
        if (nullptr != context)
        {
            aeron_context_close(context);
        }
    }
};

struct AeronCloser
{
    void operator()(aeron_t* client) const noexcept
    {
        if (nullptr != client)
        {
            aeron_close(client);
        }
    }
};

struct PublicationCloser
{
    void operator()(aeron_publication_t* publication) const noexcept
    {
        if (nullptr != publication)
        {
            aeron_publication_close(publication, nullptr, nullptr);
        }
    }
};

struct ExclusivePublicationCloser
{
    void operator()(aeron_exclusive_publication_t* publication) const noexcept
    {
        if (nullptr != publication)
        {
            aeron_exclusive_publication_close(publication, nullptr, nullptr);
        }
    }
};

struct SubscriptionCloser
{
    void operator()(aeron_subscription_t* subscription) const noexcept
    {
        if (nullptr != subscription)
        {
            aeron_subscription_close(subscription, nullptr, nullptr);
        }
    }
};

struct CounterCloser
{
    void operator()(aeron_counter_t* counter) const noexcept
    {
        if (nullptr != counter)
        {
            aeron_counter_close(counter, nullptr, nullptr);
        }
    }
};

using AeronContextPtr = std::unique_ptr<aeron_context_t, AeronContextCloser>;
using AeronPtr = std::unique_ptr<aeron_t, AeronCloser>;
using PubPtr = std::unique_ptr<aeron_publication_t, PublicationCloser>;
using ExPubPtr = std::unique_ptr<aeron_exclusive_publication_t, ExclusivePublicationCloser>;
using SubPtr = std::unique_ptr<aeron_subscription_t, SubscriptionCloser>;
using CounterPtr = std::unique_ptr<aeron_counter_t, CounterCloser>;

}  // namespace axc::core
