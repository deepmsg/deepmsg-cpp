/*
 * Shared helpers for the P6 probes.
 *
 * The probes are plain C against the reference client library (`aeronc.h`,
 * pinned 1.54.0) so that the thing measured on the far side of the cable is
 * the reference driver and the reference client, not our code. They run
 * against any driver's directory: ours for the near side, the reference
 * `aeronmd` for the far side.
 */
#ifndef DEEPM_DEEPDMSG_P6_PROBES_COMMON_H
#define DEEPM_DEEPDMSG_P6_PROBES_COMMON_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "aeronc.h"

#define P6_POLL_TIMEOUT_NS (60LL * 1000 * 1000 * 1000)

static inline int64_t p6_now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
}

static inline void p6_fail(const char* what)
{
    fprintf(stderr, "%s: %s\n", what, aeron_errmsg());
    exit(1);
}

static inline aeron_t* p6_connect(const char* directory)
{
    aeron_context_t* context = NULL;
    if (0 != aeron_context_init(&context) || 0 != aeron_context_set_dir(context, directory))
    {
        p6_fail("context");
    }
    // The launcher loop below is the conductor; without this the async adds
    // never complete (the driver's own agents do not run in this client).
    (void)aeron_context_set_use_conductor_agent_invoker(context, true);
    aeron_t* client = NULL;
    if (0 != aeron_init(&client, context))
    {
        p6_fail("aeron_init");
    }
    return client;
}

static inline aeron_subscription_t* p6_subscription(aeron_t* client, const char* channel, int32_t stream)
{
    aeron_async_add_subscription_t* async = NULL;
    if (0 != aeron_async_add_subscription(&async, client, channel, stream, NULL, NULL, NULL, NULL))
    {
        p6_fail("async add subscription");
    }
    aeron_subscription_t* subscription = NULL;
    const int64_t deadline = p6_now_ns() + P6_POLL_TIMEOUT_NS;
    while (NULL == subscription && p6_now_ns() < deadline)
    {
        (void)aeron_main_do_work(client);
        if (1 == aeron_async_add_subscription_poll(&subscription, async))
        {
            break;
        }
        usleep(100);
    }
    if (NULL == subscription)
    {
        p6_fail("subscription never completed");
    }
    return subscription;
}

static inline aeron_publication_t* p6_publication(aeron_t* client, const char* channel, int32_t stream)
{
    aeron_async_add_publication_t* async = NULL;
    if (0 != aeron_async_add_publication(&async, client, channel, stream))
    {
        p6_fail("async add publication");
    }
    aeron_publication_t* publication = NULL;
    const int64_t deadline = p6_now_ns() + P6_POLL_TIMEOUT_NS;
    while (NULL == publication && p6_now_ns() < deadline)
    {
        (void)aeron_main_do_work(client);
        if (1 == aeron_async_add_publication_poll(&publication, async))
        {
            break;
        }
        usleep(100);
    }
    if (NULL == publication)
    {
        p6_fail("publication never completed");
    }
    return publication;
}

static inline void p6_wait_connected(aeron_t* client, aeron_publication_t* publication, int64_t timeout_ns)
{
    const int64_t deadline = p6_now_ns() + timeout_ns;
    while (!aeron_publication_is_connected(publication) && p6_now_ns() < deadline)
    {
        (void)aeron_main_do_work(client);
        usleep(200);
    }
}

/// Offer with the conductor pumped; `-1` from a disconnected publication is
/// retried, anything else negative is a failure the caller should see.
static inline int64_t p6_offer(aeron_t* client, aeron_publication_t* publication, const uint8_t* buffer, size_t length,
                               int64_t deadline_ns)
{
    while (p6_now_ns() < deadline_ns)
    {
        const int64_t position = aeron_publication_offer(publication, buffer, length, NULL, NULL);
        if (position >= 0)
        {
            return position;
        }
        (void)aeron_main_do_work(client);
        usleep(100);
    }
    return -1;
}

#endif  // DEEPM_DEEPDMSG_P6_PROBES_COMMON_H
