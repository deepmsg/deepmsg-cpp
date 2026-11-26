# deepmsg-cpp

C++23 reimplementation of the Aeron **archive server** and **cluster** on top of
the official Aeron C client, C media driver and C archive client. The reference
is Aeron **1.54.2**, pinned as the submodule gitlink `481d4271ad`
(`third_party/aeron`, our fork `github.com/deepmsg/aeron`).

Goals, in two separate judgments:

- **Interchangeable** — wire bytes, disk bytes, counter type ids/key layouts and
  configuration names follow the reference; acceptance lines XA-0…XA-5 (archive)
  and XC-0…XC-7 (cluster) each pair with an oracle we did not write.
- **Fast** — only where the design points say so, always pre-registered and
  measured as `X-C / J-C` (same driver, same client, same machine).

## Status

Delivered on `main` (2026-11-19):

- **Archive** — A1a…A7 plus the read-only `archive-tool-ro` (T1): catalog and
  mark-file lifecycle (liveness claim, the previous process's error log,
  `archive-mark.lnk`), control/recording/replay/replication sessions, segment
  management, SHARED and DEDICATED runtimes, several recorders, and the
  C++-only storage and read-backend switches (`deepmsg.archive.file.io.*`,
  `deepmsg.archive.replay.io.*`). A6b (the io_uring write backend) stays on its
  branch, unmerged: the pre-registered reading said it is slower.
- **Cluster** — C1a…C9 plus the 18-command `cluster-tool` (T2): recording log,
  node state and mark files, timer wheels, election, the consensus module and
  its snapshots/recovery/termination, the service container and the business
  ABI (`echo`, `basic-auction`, `noop`, `.so` plugins), the cluster client,
  three-node catchup and leader change, mixed Java/C++ clusters, and cluster
  backup — including a node started from a reference-written backup directory.
- **Media** — the C driver as `deepmsg-media-driver`, plus a DPDK media
  (`-DAXC_WITH_DPDK=ON`) of our own: the reference's DPDK media is closed.
  Experimental, off by default.
- **Bench** — the `x-c` / `j-c` / `x-j` archive arms, the stream/storage/read
  grids and the DPDK paddock. Pre-registrations and readings live in
  `docs/benchmarks.md` and the delivery records under
  `/home/gavin/trader/analysis/cpp/records/`.

Gate: `scripts/check.sh debug release debug-clang` — **1059/1059 cases per
preset** (2026-11-19), plus clang-format, clang-tidy, the hot-path symbol audit,
the dependency-direction and forbidden-symbol checks. Java interop cases
(`ctest -L interop`) need a reference checkout; see `test/interop/README.md`.

The plan, work packages and delivery records live **outside** this repository in
`/home/gavin/trader/analysis/cpp/` (same rule as the Rust track).

## Build

Prerequisites: **CMake ≥ 3.30** (Aeron 1.54.2 requires it), JDK 21, gcc 13.3 for
all presets and **clang-20** for the `debug-clang` preset (clang 18 reports
`__cpp_concepts=201907`, so libstdc++ 13 does not expose `std::expected`),
`libdpdk-dev` for the DPDK presets.

```sh
git submodule update --init --recursive
scripts/fetch-sbe.sh                      # sbe-all 1.42.0 (the version Aeron 1.54.2 uses)
cmake --preset debug
cmake --build build/debug --parallel
ctest --test-dir build/debug --output-on-failure
```

One command for everything (format, build, tests, hot-path audit, dependency
direction, forbidden symbols), over the presets the delivery rule names:

```sh
scripts/check.sh debug release debug-clang
```

Presets: `debug`, `release`, `dpdk`, `asan`, `tsan`, `release-pgo`, plus
`-clang` variants (`debug-clang`, …). `release-pgo` generates profiles into
`build/pgo`; `release-pgo-use` consumes them. `dpdk` and `asan` compile the
DPDK media in (`libdpdk-dev` required); every other preset builds without it.

## Layout

| Path | Contents |
|---|---|
| `third_party/aeron` | the pinned reference, built through `add_subdirectory`; never patched |
| `src/core` | agents, idle strategies, clocks, counters, collections, config, the embedded driver and the client ABI (`axc::core`) |
| `src/core/media/dpdk` | the DPDK media (`axc::core::media::dpdk`), compiled only with `AXC_WITH_DPDK=ON` |
| `src/sbe` | generated codec headers + the guarded decode entry point |
| `src/archive/io` | segment writer and the `pread` / `mmap` readers |
| `src/archive/server` | conductor, control/recording/replay/replication sessions, catalog and its index |
| `src/cluster/*` | `common`, `election`, `consensus` (CM), `service` (container), `client`, `backup` |
| `apps/` | `deepmsg-archive-server`, `deepmsg-media-driver`, `deepmsg-cluster-node`, `deepmsg-consensus-module`, `deepmsg-clustered-service-container`, `deepmsg-cluster-backup`, sample services |
| `tools/` | `cluster-tool` (18 commands), `archive-tool-ro`, `repo/` self-checks |
| `test/` | `unit/` (gtest), `interop/` (reference Java/C peers), `golden/` (reference-written bytes), `alloc/`, `fuzz/` |
| `bench/` | micro benchmarks, the `x-c` / `j-c` / `x-j` arms, the `dpdk-*` paddock |
| `schemas/` | where the five SBE schemas come from and their sha256 (they are not copied in) |
| `include/` | the exported C cluster-client ABI; the C++ business API is `src/cluster/service/clustered_service.h` |
| `docs/` | `reference.md` (binary ↔ Java main), `compat.md` (byte contracts), `benchmarks.md`, `adr/` |
| `cmake/` | options, warnings, target helpers, Aeron/SBE/sanitizer/PGO/DPDK wiring |

Hot-path translation units are listed in `src/**/HOTPATH`; the audit fails the
build if one of them references `operator new`, `pthread_mutex_lock`,
`__cxa_allocate_exception` or `aeron_counters_reader_foreach_*`.

## License

Apache-2.0. Sources are transcribed from Aeron 1.54.2; see `NOTICE`.
