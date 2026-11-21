/*
 * Near side of the P6 performance run: a ping-pong over the cable.
 *
 *     ping <aeron-dir> <out-channel> <out-stream> <in-channel> <in-stream>
 *          <count> [warmup] [rate]
 *
 * Offers 64-byte messages ("<seq> <monotonic-ns>" plus padding) and records
 * `receive - send` for each from the same `CLOCK_MONOTONIC` (both ends are on
 * one host in this rig). The first `warmup` round trips are discarded.
 *
 * Without a `rate` (0 or absent) it is a strict ping-pong: one message
 * outstanding, the next offered only after the echo of the last. With a
 * `rate` in messages per second it keeps up to P6_WINDOW messages outstanding
 * and offers one every 1/rate seconds, so the same cell can be run at a pace
 * below the round trip (idle between messages, which the 10GBASE-T PHY's
 * low-power state makes visible) or above it (a queue forms, and the round
 * trip includes it). Either way each message's own round trip is what is
 * measured -- the inter-send gap is not part of it.
 *
 * Prints, on success:
 *
 *     SENT <n> RECEIVED <n> WARMUP <w>
 *     RATE <requested> ACHIEVED <measured>
 *     P50 <ns> P99 <ns> P999 <ns> MIN <ns> MAX <ns>
 *
 * and exits non-zero if any message times out, the counts disagree, or the
 * requested pace could not be kept (an arm that ran slower than it was asked
 * to is not the arm the cell says it measured).
 */
#include "common.h"

/* How many messages may be in flight at once. At 501K/s and a 25 us round
 * trip, about thirteen are; a thousand is headroom for the paced runs and
 * small enough that the ring cannot hide a stalled echo. */
#define P6_WINDOW 1024

typedef struct
{
    int64_t* send_ns;      /* by sequence; 0 = not offered */
    int64_t* round_trip;   /* by sequence; valid when seen */
    uint8_t* seen;
    int64_t total;
    int64_t received;
    int64_t first_send_ns;
    int64_t last_send_ns;
    int64_t last_receive_ns;
} Pong;

typedef struct
{
    uint64_t sequence;
    int64_t send_ns;
    uint8_t padding[48];
} Message;

static void on_fragment(void* clientd, const uint8_t* buffer, size_t length, aeron_header_t* header)
{
    (void)header;
    Pong* pong = (Pong*)clientd;
    if (length != sizeof(Message))
    {
        return;
    }
    Message message;
    memcpy(&message, buffer, sizeof(message));
    const uint64_t sequence = message.sequence;
    if (sequence >= (uint64_t)pong->total || 0 == pong->send_ns[sequence] || 0 != pong->seen[sequence])
    {
        return;
    }
    pong->seen[sequence] = 1;
    pong->round_trip[sequence] = p6_now_ns() - pong->send_ns[sequence];
    pong->last_receive_ns = p6_now_ns();
    ++pong->received;
}

static int compare_ns(const void* left, const void* right)
{
    const int64_t a = *(const int64_t*)left;
    const int64_t b = *(const int64_t*)right;
    return a < b ? -1 : (a > b ? 1 : 0);
}

int main(int argc, char** argv)
{
    if (argc < 7)
    {
        fprintf(stderr,
                "usage: %s <aeron-dir> <out-channel> <out-stream> <in-channel> <in-stream> <count> [warmup] [rate]\n",
                argv[0]);
        return 2;
    }
    const int64_t count = atoll(argv[6]);
    const int64_t warmup = argc > 7 ? atoll(argv[7]) : 0;
    const int64_t rate = argc > 8 ? atoll(argv[8]) : 0;
    const int64_t total = count + warmup;
    const int64_t interval_ns = rate > 0 ? 1000000000LL / rate : 0;

    aeron_t* client = p6_connect(argv[1]);
    aeron_publication_t* publication = p6_publication(client, argv[2], (int32_t)atoi(argv[3]));
    aeron_subscription_t* subscription = p6_subscription(client, argv[4], (int32_t)atoi(argv[5]));
    p6_wait_connected(client, publication, P6_POLL_TIMEOUT_NS);

    int64_t* round_trips = NULL;
    if (count > 0)
    {
        round_trips = (int64_t*)calloc((size_t)count, sizeof(int64_t));
    }
    Pong pong = {0};
    if (total > 0)
    {
        pong.send_ns = (int64_t*)calloc((size_t)total, sizeof(int64_t));
        pong.round_trip = (int64_t*)calloc((size_t)total, sizeof(int64_t));
        pong.seen = (uint8_t*)calloc((size_t)total, sizeof(uint8_t));
    }
    if (NULL == round_trips || (total > 0 && (NULL == pong.send_ns || NULL == pong.round_trip || NULL == pong.seen)))
    {
        fprintf(stderr, "ping: out of memory\n");
        free(round_trips);
        free(pong.send_ns);
        free(pong.round_trip);
        free(pong.seen);
        return 1;
    }
    pong.total = total;

    int64_t next_sequence = 0;
    int64_t oldest_outstanding = 0;
    int64_t next_send_ns = p6_now_ns();
    int result = 0;

    /* The rig triggers the echo a second after this client starts, so a paced
     * run that began at once would spend its first second queued at the echo
     * and measure a rate the cell never asked for. One unpaced message,
     * waited for, is the handshake: the paced clock starts when the far side
     * is actually answering. */
    if (0 == result && rate > 0 && total > 0)
    {
        Message message = {0};
        message.sequence = 0;
        message.send_ns = p6_now_ns();
        if (p6_offer(client, publication, (const uint8_t*)&message, sizeof(message),
                     p6_now_ns() + 5 * 1000 * 1000 * 1000LL) < 0)
        {
            fprintf(stderr, "ping: offer failed at 0\n");
            result = 1;
        }
        else
        {
            pong.send_ns[0] = message.send_ns;
            const int64_t deadline = p6_now_ns() + 5 * 1000 * 1000 * 1000LL;
            while (0 == pong.seen[0] && p6_now_ns() < deadline)
            {
                (void)aeron_main_do_work(client);
                (void)aeron_subscription_poll(subscription, on_fragment, &pong, 64);
            }
            if (0 == pong.seen[0])
            {
                fprintf(stderr, "ping: no echo for 0\n");
                result = 1;
            }
        }
        next_sequence = 1;
        oldest_outstanding = 1;
        next_send_ns = p6_now_ns();
    }

    while (pong.received < total && 0 == result)
    {
        const int64_t now = p6_now_ns();

        if (next_sequence < total)
        {
            /* One outstanding without a rate; the window with one. */
            const int needs_send = rate > 0 ? (now >= next_send_ns && next_sequence - pong.received < P6_WINDOW)
                                            : (next_sequence == pong.received);
            if (needs_send)
            {
                /* A full publication is back pressure, not a failure: the
                 * slot stays and the loop comes back (the reference's own
                 * sender does the same). Sleeping here would be part of the
                 * reading. */
                Message message = {0};
                message.sequence = (uint64_t)next_sequence;
                message.send_ns = now;
                const int64_t position = aeron_publication_offer(publication, (const uint8_t*)&message,
                                                                 sizeof(message), NULL, NULL);
                if (position >= 0)
                {
                    pong.send_ns[next_sequence] = now;
                    if (0 == pong.first_send_ns)
                    {
                        pong.first_send_ns = now;
                    }
                    pong.last_send_ns = now;
                    ++next_sequence;
                    if (rate > 0)
                    {
                        next_send_ns += interval_ns;
                        /* Fallen more than one interval behind: resynchronise
                         * rather than emit a catch-up burst the rate did not
                         * ask for. The achieved rate says it happened. */
                        if (next_send_ns < now)
                        {
                            next_send_ns = now;
                        }
                    }
                }
            }
        }

        (void)aeron_main_do_work(client);
        (void)aeron_subscription_poll(subscription, on_fragment, &pong, 64);

        /* The echo comes back in order, so the oldest outstanding is the one
         * after the ones already seen. */
        while (oldest_outstanding < next_sequence && 0 != pong.seen[oldest_outstanding])
        {
            ++oldest_outstanding;
        }
        if (oldest_outstanding < next_sequence &&
            p6_now_ns() - pong.send_ns[oldest_outstanding] > 5 * 1000 * 1000 * 1000LL)
        {
            fprintf(stderr, "ping: no echo for %lld\n", (long long)oldest_outstanding);
            result = 1;
        }
    }

    int64_t measured = 0;
    for (int64_t sequence = warmup; sequence < total; ++sequence)
    {
        if (0 != pong.seen[sequence])
        {
            round_trips[measured++] = pong.round_trip[sequence];
        }
    }

    const char* const dump = getenv("P6_DUMP_RTTS");
    if (NULL != dump && dump[0] != '\0')
    {
        FILE* out = fopen(dump, "w");
        if (NULL != out)
        {
            for (int64_t index = 0; index < measured; ++index)
            {
                fprintf(out, "%lld\n", (long long)round_trips[index]);
            }
            fclose(out);
        }
    }

    printf("SENT %lld RECEIVED %lld WARMUP %lld\n", (long long)total, (long long)pong.received, (long long)warmup);
    if (0 == result && measured != count)
    {
        fprintf(stderr, "ping: measured %lld of %lld\n", (long long)measured, (long long)count);
        result = 1;
    }
    if (0 == result)
    {
        /* Only a paced run has a rate to check: without one the client goes as
         * fast as the round trip lets it, and the span would include the wait
         * the rig spends before it triggers the echo. */
        int64_t achieved = 0;
        if (rate > 0)
        {
            /* The send-side span: `last_receive_ns - first_send_ns` includes
             * the last message's round trip and understates a small sample. */
            const int64_t span = pong.last_send_ns - pong.first_send_ns;
            /* The first message was the handshake; the paced count is what
             * went out after it. */
            const int64_t paced = total > 1 ? total - 1 : total;
            if (span > 0)
            {
                achieved = (paced * 1000000000LL) / span;
            }
        }
        printf("RATE %lld ACHIEVED %lld\n", (long long)rate, (long long)achieved);
        if (rate > 0 && achieved * 100 < rate * 90)
        {
            fprintf(stderr, "ping: asked for %lld/s, achieved %lld/s\n", (long long)rate, (long long)achieved);
            result = 1;
        }
    }
    if (0 == result)
    {
        qsort(round_trips, (size_t)measured, sizeof(int64_t), compare_ns);
        const int64_t p50 = round_trips[(measured * 50) / 100];
        const int64_t p99 = round_trips[(measured * 99) / 100];
        const int64_t p999 = round_trips[(measured * 999) / 1000];
        printf("P50 %lld P99 %lld P999 %lld MIN %lld MAX %lld\n", (long long)p50, (long long)p99, (long long)p999,
               (long long)round_trips[0], (long long)round_trips[measured - 1]);
    }
    fflush(stdout);
    free(round_trips);
    free(pong.send_ns);
    free(pong.round_trip);
    free(pong.seen);
    return result;
}
