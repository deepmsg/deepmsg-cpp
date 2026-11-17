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

**Build type: Debug (`-O0`).** These cells were measured before anyone checked
`CMAKE_BUILD_TYPE`; the `dpdk` preset is Debug, and the reference driver in the
same build directory is too. Every cell now records its build type in
`RUN-INFO.txt` and a non-Release arm says so. Re-measured with the same grid on
the same tree at Release (`analysis/cpp/records/dpdk-release.md`): the media's
p50 is **25.2–25.6 µs**, M/K **0.80–0.82**; our own per-frame cost falls from
5.26 µs (Debug) to **1.21 µs**. Cite the Release numbers for anything outside
this note.

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

The NAKs that made the media arm unstable are **closed** (2026-11-16): a few
times per 20,000 messages a frame the burst answered for never left our NIC,
the driver's idle heartbeat pushed the reference's high-water mark past it, and
the reference NAK'd exactly that frame — Aeron's recovery working, with a
~100 ms heartbeat-period stall. The cause was **one DPDK transmit queue shared
by two driver threads**: the reference initialises a transport once per
affinity, and our `dpdk_send` ignored the affinity, so the receiver agent's
SM/NAK raced the sender agent's data. Each agent now bursts on its own queue
(#51); four sessions afterwards have **zero NAKs in every media cell**.

**Core placement matters for any quoted number.** Both drivers run their agents
on `spin`, so our media holds its cores at 100%, and left to the scheduler it
moves a whole media cell's median by 13 µs in about one cell in twelve (5 of 60
cells unpinned). Pin the media with `P6_OURS_CPUSET=0-3`: 0 of 32 cells slow.
Starving the echo alone does not reproduce it, so the contention is with the
far side's other threads, not the echo.

Re-run at Release with the fix and the media pinned (`analysis/cpp/records/t7-stability.md`):
media p50 **25.3–25.5 µs**, and a clean session gives **M/K 0.803 / 0.809**
(the other session's kernel cells ran at 36.8 µs — the same one-cell-in-twelve
effect, on the kernel arm, which that session does not pin). The P6
pre-registration did not pin cores; runs that do are a recorded deviation and
write their core sets into `RUN-INFO.txt`.

Pin the far side as well when a kernel-arm number is meant to be quoted. A cell
has six spinning threads — our sender and receiver, the reference's two, the
echo and the client — and with the four sets below each pair gets its own cores
on this eight-core machine: `P6_OURS_CPUSET=0-3 P6_PEER_CPUSET=4-5
P6_ECHO_CPUSET=6 P6_CLIENT_CPUSET=7`. Measured 2026-11-17 over two sessions:
kernel p50 **30.8–31.6 µs**, media **25.2–25.6 µs**, M/K 0.809 / 0.809 and
0.820 / 0.818; the same run with the far side free also stayed clean, so the
36.8 µs session is placement luck rather than a per-session law. Cramming every
thread onto 0-3 instead gives a 6 ms kernel median — placement decides the arm's
order of magnitude.
