/*
 * The achieved-rate arithmetic of the P6 ping probe, kept apart from the
 * probe's I/O so it has an independent reading (`test/unit/core/ping_rate_test.cpp`).
 */
#ifndef DEEPMSG_BENCH_P6_RATE_H
#define DEEPMSG_BENCH_P6_RATE_H

#include <stdint.h>

/*
 * Messages per second over the send-side span.
 *
 * `total` is the probe's count including the unpaced handshake message, so the
 * paced count is `total - 1`. The span is the last offer's timestamp minus the
 * first: a receive-side span (`last_receive - first_send`) would include the
 * last message's round trip and understate a small sample.
 */
static inline int64_t p6_achieved_rate(int64_t total, int64_t first_send_ns, int64_t last_send_ns)
{
    const int64_t span = last_send_ns - first_send_ns;
    const int64_t paced = total > 1 ? total - 1 : total;
    return span > 0 ? (paced * 1000000000LL) / span : 0;
}

#endif /* DEEPMSG_BENCH_P6_RATE_H */
