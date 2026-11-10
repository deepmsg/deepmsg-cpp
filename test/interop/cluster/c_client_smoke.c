/*
 * C3b's C-ABI smoke: a C (not C++) program that connects through
 * `deepmsg_cluster_client.h`, offers one application message, waits for the
 * echo and closes the session. The interop test runs it against the reference
 * `ClusteredMediaDriver` and this build's echo container.
 *
 * usage: c-client-smoke <aeron-dir> [payload]
 * exit 0 on "RECEIVED <payload>", 1 otherwise; status lines on stderr.
 */
#include <stdio.h>
#include <string.h>
#include <time.h>

#include <stdint.h>

#include "deepmsg_cluster_client.h"

static int received = 0;

static void on_message(void* clientd, int64_t cluster_session_id, int64_t timestamp, const uint8_t* buffer,
                       size_t length)
{
    (void)clientd;
    (void)cluster_session_id;
    (void)timestamp;
    printf("RECEIVED %.*s\n", (int)length, (const char*)buffer);
    fflush(stdout);
    received = 1;
}

int main(int argc, char** argv)
{
    if (argc < 2)
    {
        fprintf(stderr, "usage: %s <aeron-dir> [payload]\n", argv[0]);
        return 2;
    }

    const char* payload = argc > 2 ? argv[2] : "hello-from-c";
    deepmsg_cluster_client_t* client = NULL;

    if (0 != deepmsg_cluster_connect(&client, argv[1], "aeron:ipc", "aeron:ipc"))
    {
        fprintf(stderr, "connect failed: %s\n", deepmsg_cluster_error());
        return 1;
    }
    fprintf(stderr, "CONNECTED session=%lld\n", (long long)deepmsg_cluster_session_id(client));

    const int64_t position = deepmsg_cluster_offer(client, (const uint8_t*)payload, strlen(payload));
    if (position < 0)
    {
        fprintf(stderr, "offer failed: %lld\n", (long long)position);
        (void)deepmsg_cluster_close(client);
        return 1;
    }

    for (int attempt = 0; attempt < 5000 && !received; ++attempt)
    {
        if (deepmsg_cluster_poll_egress(client, on_message, NULL, 10) < 0)
        {
            fprintf(stderr, "poll failed: %s\n", deepmsg_cluster_error());
            break;
        }
        const struct timespec pause = {0, 1000000};
        (void)nanosleep(&pause, NULL);
    }

    (void)deepmsg_cluster_close(client);

    if (!received)
    {
        fprintf(stderr, "no echo within 5 s\n");
        return 1;
    }
    return 0;
}
