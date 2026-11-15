# Benchmarks

Nothing here is a result yet. The rules live in
`/home/gavin/trader/analysis/cpp/spec-2026-11-07-cpp-performance-and-acceptance.md`;
the batch scripts land with A6a in `bench/cpp-arms/`.

Principles, repeated because they are easy to lose:

- only `X-C / J-C` answers "is the C++ server faster than the Java server";
- every cell records `fingerprint.txt` and `RUN-INFO.txt`, including every
  performance switch (io_uring backend, fsync level, recorder threads);
- a cell whose instrument was silently disabled is a failed cell, not a fast one;
- thresholds are pre-registered in `bench/cpp-arms/PREREG.md` before measuring.

## DPDK media vs kernel UDP: driver-level round trip (2026-11-15)

Pre-registered in `analysis/cpp/plan-dpdk-p6.md` §3 (grid, invariants, floors);
raw readings and the failures in `analysis/cpp/records/dpdk-p6.md`. Rig: the
bench pair (`03:00.0` ↔ `03:00.1`, X540-AT2, one Cat6a), the reference `aeronmd`
plus a reference-client echo on `10.10.0.2` **inside their own network
namespace**, our driver on `10.10.0.1` in both arms, and both drivers on the
`spin` idle strategy (the DPDK port cannot wake a parked thread; the `backoff`
default parks up to 1 ms and measured 1.07 ms here). Strict ping-pong of 64-byte
messages, 2,000 warmup, 20,000 measured, `K M M K` over two sessions.

| arm | p50 | p99 | p99.9 |
|---|---|---|---|
| kernel UDP (K) | 31.69–31.88 µs | 40.9–41.6 µs | 94–128 µs |
| DPDK media (M) | 28.71–29.47 µs (one cell 36.24) | 39.0–45.1 µs | 106–133 µs |

Session 1: M/K = **0.915 / 0.900**, both outside the same-arm floor (K 1.006,
M 1.010) — the media about 10% faster at the driver level, the same direction
and size as the media-level loop (E4, M/K 0.82). Session 2: one media cell ran
at 36.2 µs with a 187 ms tail, which inflated the media floor to 1.23 and put
M/K inside it — **no signal, by the pre-registered rule**. So the reading is
"media is not slower; a stable ratio is not yet established".

The earlier 4.7× (K 6.0 µs) was an instrument error: with both addresses in the
host's namespace the kernel's route to the peer was `local … dev lo`, so the
kernel arm never crossed the cable. Moving the far side into a namespace takes
K to 31.7–31.9 µs, which matches the Rust track's independent kernel-over-cable
measurement of 31–34 µs.

Open, and what makes the media arm unstable: the reference's image NAKs a few
times per 20,000 messages and Aeron recovers with ~100–190 ms stalls. A capture
on the peer port shows every frame present, in order, with valid UDP checksums,
and no counter records a drop (`analysis/cpp/records/dpdk-p6.md` §2.4).
