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
raw readings in `analysis/cpp/records/dpdk-p6.md` and its `p6-2026-11-15/`
folder. Rig: the bench pair (`03:00.0` ↔ `03:00.1`, X540-AT2, one Cat6a), the
reference `aeronmd` plus a reference-client echo on `10.10.0.2`, our driver on
`10.10.0.1` in both arms, and both drivers on the `spin` idle strategy (the DPDK
port cannot wake a parked thread; the `backoff` default parks up to 1 ms and
measured 1.07 ms here). Strict ping-pong of 64-byte messages, 2,000 warmup,
20,000 measured, `K M M K` over two sessions.

| arm | p50 | p99 | p99.9 |
|---|---|---|---|
| kernel UDP (K) | 6.01–6.14 µs | 10.5–12.2 µs | 44–81 µs |
| DPDK media (M) | 28.77–29.30 µs | 38.7–39.3 µs | 117–640 µs |

M/K at p50 = **4.81 / 4.69** (session 1) and **4.84 / 4.76** (session 2); the
same-arm floor was ≤1.02. At the **driver** level the current media
implementation is about **4.7× slower** than kernel UDP on the same port. This
is a different measurement from the media-level closed loop (E4, M/K 0.82): that
one excludes Aeron's sender/receiver, and this one is dominated by them plus the
per-frame copies in the userspace stack.

Recorded, not fixed: the reference's image NAKs 5–10 times per 20,000 messages
and Aeron recovers with ~100 ms stalls. A capture on the peer port shows every
frame present, in order, with valid UDP checksums, and neither the kernel nor
our port counted a drop — the cause is open (`analysis/cpp/records/dpdk-p6.md`
§2.4).
