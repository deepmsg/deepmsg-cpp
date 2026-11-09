# PREREG — A6a archive grid (frozen 2026-11-09, before any baseline cell)

This file is written **before** the baseline is measured and is not edited
afterwards. If a later change is needed it goes in `records/a6a.md`, with the
reason, and the old grid stays here beside it.

## Question

Performance spec §6: does the C++ archive, on the reference C driver, answer
S1/S2 at least as well as the reference Java archive on the same driver?

    headline ratio:  X-C / J-C          (the archive, one driver under both)

The grid also prints `J-C / J` (the driver, one archive under both),
`X-J / J` (the archive under the Java driver) and `X-J / X-C` (the driver under
our archive). `X-J / J` must agree in sign with the headline; a disagreement is
a finding, not a number.

## Arms

| arm | archive | driver |
|---|---|---|
| `j`   | reference `io.aeron.archive.Archive` (launched in the node's JVM) | reference Java `MediaDriver` |
| `j-c` | reference `io.aeron.archive.Archive` (external process)         | C `aeronmd` |
| `x-c` | `deepmsg-archive-server` (external process)                    | C `aeronmd` |
| `x-j` | `deepmsg-archive-server` (external process)                    | reference Java `MediaDriver` |

Every arm is the reference's own `ArchiveNode` + `live-replay-client` out of the
same built `benchmarks.jar`; only the three processes in the table move.

## Machine

Per the 2026-11-09 decision: **all cells on the WSL2 box** (GavinPC,
i7-10875H 8 physical cores / 16 vCPUs), no bench-machine arm in A6a. Cores:
driver agents at cpuset indices 2/4/6, node at vCPU 8, rig at vCPU 10 — one
process per physical core (`lscpu -e`'s `CORE` column; even vCPUs are siblings).

Consequences that are part of the reading, not excuses: `net.core.{r,w}mem_max`
is 212992, so the three socket/window settings are 208k (the bench machine's 2m
is refused by the driver), and fsync cells carry more noise than they would on
the NVMe bench machine.

## Grid

    scenario:  s2 (live-replay) and s1 (live-recording)
    fsync:     0, 2          (aeron.archive.{file,catalog}.file.sync.level)
    workload:  25K:32  →  101K:1344  →  301K:288   (msg/s : bytes)
    runs:      1 per cell (the harness's own iterations; no per-cell repeats)

The 2026-11-09 decision is to do the **25K:32 band first**; the other two bands
follow in the same file and under the same rules. `501K:32` is explicitly A6b
(throughput), not A6a.

Rounds are ABBA: `round-a` runs the arms in the order above, `round-b` runs them
reversed. A ratio is quoted only when both rounds agree in sign; the pair of
ratios is always printed together.

## Reading

From each cell's `<*.hdr>`:

- main reading: `E[X | X < p99]` (ns)
- also reported: p50, p90, p99, p99.9, p99.99, max
- floor: each arm against itself, `round-b / round-a` — a ratio inside the
  floor is not a difference

The archive's own counters (`archive-recorder/replayer total …`, per-agent max
cycle time) and the archive directory's bytes are read for *explanation* only;
they never override a latency reading.

## Pass criteria

**P1 (the only one in A6a)** — for every cell:

    X-C / J-C  ≤  1 + floor relative noise

with the two rounds either both ≤ that bound or of the same sign, and `X-J / J`
of the same sign as the headline. A cell outside this is a **defect hunt**, not
a tuning opportunity: the performance spec says find the defect first, and this
PREREG says it in advance so that no result can be argued into a pass.

P2–P5 (tails, throughput, warm-up, resources) are A6b and are not read here.

## Out of scope, by decision

- two-host shapes (WSL2's virtual network is milliseconds per round trip)
- `501K:32` throughput band; cluster `XC-7`
- changing the harness's *business* code: only the two test-side patches
  (`ArchiveNode` external archive; `LiveRecordingMessageTransceiver` external
  archive) may be applied, each through `patches/install.sh --check/--apply/--revert`
- comparing against the Rust track's historical absolute numbers (different
  machine and harness version = a different experiment)

## Frozen facts of the instrument

- reference Aeron **1.54.0**, gitlink `454da1e4e6`
- harness checkout `benchmarks` at `1b11bfc`, locally: `aeron = "1.54.0"`,
  `agrona = "2.7.0"`, Gradle wrapper on the mirror, `external-archive` patch
- builder: gcc 13.3 / clang 20, preset `release`, `-march=x86-64-v2`, PGO off
- `fingerprint.txt` per batch and `RUN-INFO.txt` per cell carry the rest
