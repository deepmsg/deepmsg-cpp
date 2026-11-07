# deepmsg-cpp

C++23 reimplementation of the Aeron **archive server** and **cluster** on top of
the official Aeron C client, C media driver and C archive client. The reference
is Aeron **1.54.0**, pinned as the submodule gitlink `454da1e4e6`
(`third_party/aeron`, our fork `github.com/deepmsg/aeron`).

Goals, in two separate judgments:

- **Interchangeable** — wire bytes, disk bytes, counter type ids/key layouts and
  configuration names follow the reference; acceptance lines XA-0…XA-5 (archive)
  and XC-0…XC-7 (cluster) each pair with an oracle we did not write.
- **Fast** — only where the design points say so, always pre-registered and
  measured as `X-C / J-C` (same driver, same client, same machine).

## Status

**X0-1 skeleton.** Build system, pinned submodule, SBE codec generation, empty
module libraries, placeholder binaries, gates and directory layout. No archive or
cluster behaviour yet.

The plan, work packages and delivery records live **outside** this repository in
`/home/gavin/trader/analysis/cpp/` (same rule as the Rust track).

## Build

Prerequisites: **CMake ≥ 3.30** (Aeron 1.54.0 requires it), JDK 21, gcc 13.3 for
all presets and **clang-20** for the `debug-clang` preset (clang 18 reports
`__cpp_concepts=201907`, so libstdc++ 13 does not expose `std::expected`),
`liburing-dev` optional.

```sh
git submodule update --init --recursive
scripts/fetch-sbe.sh                      # sbe-all 1.42.0 (the version Aeron 1.54.0 uses)
cmake --preset debug
cmake --build build/debug --parallel
ctest --test-dir build/debug --output-on-failure
```

One command for everything (format, build, tests, hot-path audit, dependency
direction):

```sh
scripts/check.sh debug release
```

Presets: `debug`, `release`, `asan`, `tsan`, `release-pgo`, plus `-clang`
variants (`debug-clang`, …). `release-pgo` generates profiles into
`build/pgo`; `release-pgo-use` consumes them.

## Layout

| Path | Contents |
|---|---|
| `src/core` | agents, idle strategies, clocks, counters, collections, config (`axc::core`) |
| `src/sbe` | generated codec headers + the guarded decode entry point |
| `src/archive/io` | segment write/read backends (sync, io_uring, pread, mmap) |
| `src/archive/server` | conductor, control/recording/replay/replication sessions, catalog |
| `src/cluster/*` | common, election, consensus (CM), service container, client, backup |
| `apps/` | `aeron-archive-server`, `aeron-cluster-node`, `aeron-consensus-module`, `aeron-clustered-service-container`, `aeron-cluster-backup`, sample services |
| `tools/` | `cluster-tool`, `archive-tool-ro`, repository self-checks |
| `test/` | `unit/` (gtest), `interop/` (Java side, X0-4), `alloc/`, `fuzz/` |
| `bench/` | micro benchmarks and the `x-c` / `j-c` / `x-j` batch arms |
| `cmake/` | options, warnings, target helpers, Aeron/SBE/sanitizer/PGO/uring wiring |

Hot-path translation units are listed in `src/**/HOTPATH`; the audit fails the
build if one of them references `operator new`, `pthread_mutex_lock`,
`__cxa_allocate_exception` or `aeron_counters_reader_foreach_*`.

## License

Apache-2.0. Sources are transcribed from Aeron 1.54.0; see `NOTICE`.
