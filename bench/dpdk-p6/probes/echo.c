/*
 * The far side of the P6 interop and performance runs: a plain C echo on the
 * reference driver.
 *
 *     echo <aeron-dir> <in-channel> <in-stream> <out-channel> <out-stream> [count] [ready-file]
 *
 * Subscribes on `<in-channel>`, and for every fragment publishes the same
 * bytes on `<out-channel>` -- nothing else, so the round trip the near side
 * measures is the two drivers, the cable and this copy. Prints
 * `ECHOED <n>` when `count` is reached (or on SIGTERM/SIGINT), then lingers
 * 500 ms to let the last echo leave.
 *
 * With a `ready-file`, the subscription is created, `READY` is printed, and the
 * publication waits for the file to appear. The near side starts first and the
 * trigger is touched once its subscription exists: a publication created before
 * that keeps re-sending SETUP frames, which the media counts as data-plane
 * discards (the port has no binding yet).
 */
#include "common.h"

#include <signal.h>

typedef struct
{
    aeron_t* client;
    aeron_publication_t* publication;
    int64_t echoed;
    int64_t expected;
    volatile sig_atomic_t* running;
} Echo;

static void on_fragment(void* clientd, const uint8_t* buffer, size_t length, aeron_header_t* header)
{
    (void)header;
    Echo* echo = (Echo*)clientd;
    if (p6_offer(echo->client, echo->publication, buffer, length, p6_now_ns() + P6_POLL_TIMEOUT_NS) < 0)
    {
        fprintf(stderr, "echo: the reply could not be offered\n");
        *echo->running = 0;
        return;
    }
    ++echo->echoed;
    if (echo->expected > 0 && echo->echoed >= echo->expected)
    {
        *echo->running = 0;
    }
}

static volatile sig_atomic_t g_running = 1;

static void on_signal(int signal_number)
{
    (void)signal_number;
    g_running = 0;
}

int main(int argc, char** argv)
{
    if (argc < 6)
    {
        fprintf(stderr, "usage: %s <aeron-dir> <in-channel> <in-stream> <out-channel> <out-stream> [count]\n", argv[0]);
        return 2;
    }
    const int64_t expected = argc > 6 ? atoll(argv[6]) : 0;
    const char* const ready_file = argc > 7 ? argv[7] : NULL;

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

    Echo echo = {0};
    echo.client = p6_connect(argv[1]);
    echo.expected = expected;
    echo.running = &g_running;
    aeron_subscription_t* subscription = p6_subscription(echo.client, argv[2], (int32_t)atoi(argv[3]));
    printf("READY\n");
    fflush(stdout);
    if (NULL != ready_file)
    {
        const int64_t deadline = p6_now_ns() + P6_POLL_TIMEOUT_NS;
        while (0 != access(ready_file, F_OK) && p6_now_ns() < deadline)
        {
            (void)aeron_main_do_work(echo.client);
            usleep(1000);
        }
    }
    echo.publication = p6_publication(echo.client, argv[4], (int32_t)atoi(argv[5]));
    p6_wait_connected(echo.client, echo.publication, P6_POLL_TIMEOUT_NS);

    while (g_running)
    {
        // Busy spin, for the same reason as `ping`: a sleeping echo is a
        // sleeping half of the round trip.
        (void)aeron_main_do_work(echo.client);
        (void)aeron_subscription_poll(subscription, on_fragment, &echo, 10);
    }

    printf("ECHOED %lld\n", (long long)echo.echoed);
    fflush(stdout);
    usleep(500 * 1000);
    return 0;
}
