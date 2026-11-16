/*
 * Near side of the P6 interop runs: publish `<count>` messages of the form
 * "<prefix> <i>" and report what left.
 *
 *     send <aeron-dir> <channel> <stream> <count> <prefix>
 *
 * Prints `SENT <n>` and exits non-zero if any offer could not land or the far
 * side never connected: the interop criteria are "the reference saw them", and
 * a silent publisher is the easiest way to fake that.
 */
#include "common.h"

int main(int argc, char** argv)
{
    if (argc < 6)
    {
        fprintf(stderr, "usage: %s <aeron-dir> <channel> <stream> <count> <prefix>\n", argv[0]);
        return 2;
    }
    const int64_t count = atoll(argv[4]);
    const char* prefix = argv[5];

    aeron_t* client = p6_connect(argv[1]);
    aeron_publication_t* publication = p6_publication(client, argv[2], (int32_t)atoi(argv[3]));

    int64_t sent = 0;
    int result = 0;
    char payload[256];
    for (int64_t index = 0; index < count; ++index)
    {
        const int length = snprintf(payload, sizeof(payload), "%s %lld", prefix, (long long)index);
        if (p6_offer(client, publication, (const uint8_t*)payload, (size_t)length,
                     p6_now_ns() + 10 * 1000 * 1000 * 1000LL) < 0)
        {
            fprintf(stderr, "send: offer failed at %lld\n", (long long)index);
            result = 1;
            break;
        }
        ++sent;
    }

    printf("SENT %lld\n", (long long)sent);
    fflush(stdout);
    // Let the last frames leave before the publication closes.
    usleep(500 * 1000);
    return result;
}
