# The DPDK media bench rig

Three scripts share one topology and one library (`lib.sh`). They run on the
bench machine, as root, with a card whose two ports are cabled together.

```
03:00.0  our driver's port        media arm: vfio-pci → EAL → deepmsg-media-driver
                                  kernel arm: ixgbe, 10.10.0.1
03:00.1  the peer, kernel side    10.10.0.2 inside the `p6-peer` namespace:
                                  the reference `aeronmd` + the echo probes
```

The peer lives in its own namespace because otherwise the kernel arm's route to
`10.10.0.2` is `local … dev lo` and never crosses the cable — that error once
produced a fake "kernel is 4.7x faster" reading. The scripts move the peer port
into the namespace, run the cell, and put everything back in a trap
(`p6_teardown`: processes, namespace, addresses, `dpdk-devbind.py --bind=ixgbe`,
hugepage files, NetworkManager managed state).

## What is here

| file | what it does |
|---|---|
| `lib.sh` | topology, preflight, probes, driver start/stop, counters, teardown |
| `run-perf.sh` | driver-level round trip, `K M M K` per session (the T7 grid) |
| `run-flood.sh` | one-way flood, 1,000,000 frames, to raise the density of a rare loss (the lost-frame card's arm B) |
| `run-interop.sh` | a reference client against our driver over the real cable (T5) |
| `run-archive-suite.sh` | the archive interop suite twice, kernel arm vs media arm (I6) |
| `probes/` | the C clients (reference C client library): `ping`, `echo`, `send`, `recv` |

## Before running

- Our driver and the reference **must be built Release** for any number that is
  meant to be quoted: the `dpdk` preset is Debug (`-O0`), and every reading
  taken on it was off by ~4x in our own per-frame cost. Build
  `build/dpdk-release` by hand (see `records/dpdk-release.md` §7), pass it as
  `AXC_BUILD_DIR`, and read `RUN-INFO.txt` in each cell — a non-Release arm
  prints a four-line warning.
- Both drivers run the `spin` idle strategy. The DPDK port cannot wake a parked
  thread, and the C driver's default `backoff` parks up to 1 ms (it measured
  1.07 ms here).
- The reference `aeron-all` jar must exist under `third_party/aeron/aeron-all`;
  `AXC_AERON_JAR` overrides the path.
- `run-archive-suite.sh` needs the archive interop tests' fixtures,
  `AXC_AERON_TESTS=<reference tests checkout>`.

## Running

```bash
build=build/dpdk-release          # Release, DPDK ON
sudo env AXC_BUILD_DIR=$PWD/$build \
     AXC_AERON_JAR=$PWD/third_party/aeron/aeron-all/build/libs/aeron-all-1.54.0.jar \
     bench/dpdk-p6/run-perf.sh                       # P6_SESSIONS=2 by default
```

Knobs: `P6_COUNT` (messages per cell, 20,000), `P6_WARMUP` (2,000),
`P6_SESSIONS`, `P6_IDLE_STRATEGY`, `AXC_DPDK_BENCH_PCI` / `_LOCAL` / `_PEER`
(topology overrides).

Every cell writes `RUN-INFO.txt` (build type, segments flag, git revision),
`ours.log`, `peer-before.txt` / `peer-after.txt` (the peer's counters with both
drivers stopped, so the delta is exact -- NIC counters and its namespace's UDP
counters, including `Udp InErrors`, which the earlier NAK round did not read),
`rtts.txt`, and the driver's CnC counters. A cell fails loudly when a media
invariant moves: `dpdk error`, `rx receiver discard`, `missed packets`, or a
non-zero peer-side NAK count with no matching retransmit.

The flood arm judges the same way one way only: the publisher offers the count,
the subscriber receives every message with the payload intact, and neither the
peer's NIC error/drop counters nor its UDP error counters move. It is the arm
that found the shared-transmit-queue race (2026-11-16): at 20,000 frames per
cell the loss was one to six frames, which a flood turns into something a run
can see.

## Pitfalls that have bitten

- Each cell leaves a ~48 MB `cnc.dat` under `/tmp/axc-p6-*`; a few dozen fills
  the root filesystem and the unit tests start timing out. Clean them after a
  run (they are root-owned).
- `vfio` binding needs the interface down and its addresses flushed first;
  NetworkManager must be told to leave both ports alone (the rig does this).
- The reference's last frame of a burst is occasionally lost between
  `rte_eth_tx_burst` returning success and the NIC sending; the idle heartbeat
  then makes the reference NAK it and recover after ~100 ms. It shows up as a
  spike in one cell, not as a ratio. That is the open item in
  `analysis/cpp/notes-2026-11-16-the-outside-is-the-wire.md` §4.

The measurements and their records live outside this repository in
`/home/gavin/trader/analysis/cpp/` (`plan-dpdk-p6.md`, `records/dpdk-p6.md`,
`records/dpdk-release.md`). The segment-timestamp instrument is **not** part of
this rig: it is a product-code build switch (`AXC_DPDK_SEGMENTS`) kept on a
local branch; this rig only records whether the binary it measured had it on.
