/*
 * Near side of the P6 interop runs: receive `<count>` messages and check that
 * message `i` is exactly "<prefix> <i>".
 *
 *     recv <aeron-dir> <channel> <stream> <count> <prefix> [timeout-seconds]
 *
 * Prints `RECEIVED <n> BAD <b>` and exits non-zero unless every message
 * arrived, in order, byte for byte. That is the whole interop claim: the
 * reference side's bytes are the bytes.
 */
#include "common.h"

typedef struct
{
    const char* prefix;
    int64_t count;
    int64_t received;
    int64_t bad;
} Receiver;

static void on_fragment(void* clientd, const uint8_t* buffer, size_t length, aeron_header_t* header)
{
    (void)header;
    Receiver* receiver = (Receiver*)clientd;
    char expected[256];
    const int expected_length =
        snprintf(expected, sizeof(expected), "%s %lld", receiver->prefix, (long long)receiver->received);
    if (length != (size_t)expected_length || 0 != memcmp(buffer, expected, length))
    {
        if (receiver->bad < 5)
        {
            fprintf(stderr, "recv: message %lld was '%.*s', expected '%s'\n", (long long)receiver->received,
                    (int)length, (const char*)buffer, expected);
        }
        ++receiver->bad;
    }
    ++receiver->received;
}

int main(int argc, char** argv)
{
    if (argc < 6)
    {
        fprintf(stderr, "usage: %s <aeron-dir> <channel> <stream> <count> <prefix> [timeout-seconds]\n", argv[0]);
        return 2;
    }
    const char* timeout_text = argc > 6 ? argv[6] : "60";

    Receiver receiver = {0};
    receiver.prefix = argv[5];
    receiver.count = atoll(argv[4]);

    aeron_t* client = p6_connect(argv[1]);
    aeron_subscription_t* subscription = p6_subscription(client, argv[2], (int32_t)atoi(argv[3]));

    const int64_t deadline = p6_now_ns() + atoll(timeout_text) * 1000 * 1000 * 1000LL;
    while (receiver.received < receiver.count && p6_now_ns() < deadline)
    {
        (void)aeron_main_do_work(client);
        if (0 == aeron_subscription_poll(subscription, on_fragment, &receiver, 10))
        {
            usleep(100);
        }
    }

    printf("RECEIVED %lld BAD %lld\n", (long long)receiver.received, (long long)receiver.bad);
    fflush(stdout);
    return (receiver.received == receiver.count && 0 == receiver.bad) ? 0 : 1;
}
