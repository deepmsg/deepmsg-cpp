/*
 * Near side of the P6 performance run: a strict ping-pong over the cable.
 *
 *     ping <aeron-dir> <out-channel> <out-stream> <in-channel> <in-stream> <count> [warmup]
 *
 * Offers one 64-byte message ("<seq> <monotonic-ns>" plus padding), waits for
 * the echo of that sequence, and records `receive - send` from the same
 * `CLOCK_MONOTONIC` (both ends are on one host in this rig). The first
 * `warmup` round trips are discarded. Prints, on success:
 *
 *     SENT <n> RECEIVED <n> WARMUP <w>
 *     P50 <ns> P99 <ns> P999 <ns> MIN <ns> MAX <ns>
 *
 * and exits non-zero if any message times out or the counts disagree: an arm
 * that sent fewer messages than asked is not a fast arm.
 */
#include "common.h"

typedef struct
{
    int64_t waiting_for;
    int64_t receive_ns;
    int64_t received;
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
    if ((int64_t)message.sequence != pong->waiting_for)
    {
        return;
    }
    pong->receive_ns = p6_now_ns();
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
        fprintf(stderr, "usage: %s <aeron-dir> <out-channel> <out-stream> <in-channel> <in-stream> <count> [warmup]\n",
                argv[0]);
        return 2;
    }
    const int64_t count = atoll(argv[6]);
    const int64_t warmup = argc > 7 ? atoll(argv[7]) : 0;

    aeron_t* client = p6_connect(argv[1]);
    aeron_publication_t* publication = p6_publication(client, argv[2], (int32_t)atoi(argv[3]));
    aeron_subscription_t* subscription = p6_subscription(client, argv[4], (int32_t)atoi(argv[5]));
    p6_wait_connected(client, publication, P6_POLL_TIMEOUT_NS);

    int64_t* round_trips = NULL;
    if (count > 0)
    {
        round_trips = (int64_t*)calloc((size_t)count, sizeof(int64_t));
        if (NULL == round_trips)
        {
            fprintf(stderr, "ping: out of memory\n");
            return 1;
        }
    }

    Pong pong = {0};
    int64_t measured = 0;
    int result = 0;
    for (int64_t sequence = 0; sequence < count + warmup; ++sequence)
    {
        Message message = {0};
        message.sequence = (uint64_t)sequence;
        message.send_ns = p6_now_ns();
        if (p6_offer(client, publication, (const uint8_t*)&message, sizeof(message),
                     p6_now_ns() + 5 * 1000 * 1000 * 1000LL) < 0)
        {
            fprintf(stderr, "ping: offer failed at %lld\n", (long long)sequence);
            result = 1;
            break;
        }

        pong.waiting_for = sequence;
        pong.receive_ns = 0;
        const int64_t deadline = p6_now_ns() + 5 * 1000 * 1000 * 1000LL;
        // Busy spin: a sleep here is part of the measurement (E4's void rounds
        // were exactly this), so the wait costs the loop and nothing else.
        while (0 == pong.receive_ns && p6_now_ns() < deadline)
        {
            (void)aeron_main_do_work(client);
            (void)aeron_subscription_poll(subscription, on_fragment, &pong, 10);
        }
        if (0 == pong.receive_ns)
        {
            fprintf(stderr, "ping: no echo for %lld\n", (long long)sequence);
            result = 1;
            break;
        }
        if (sequence >= warmup)
        {
            round_trips[measured++] = pong.receive_ns - message.send_ns;
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

    printf("SENT %lld RECEIVED %lld WARMUP %lld\n", (long long)(count + warmup), (long long)pong.received,
           (long long)warmup);
    if (0 == result && measured != count)
    {
        fprintf(stderr, "ping: measured %lld of %lld\n", (long long)measured, (long long)count);
        result = 1;
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
    return result;
}
