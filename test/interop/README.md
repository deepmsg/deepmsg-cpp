# Interop tests

Everything here needs a JDK; the Java peers are the reference `aeron-all` jar
(1.54.0). The fixture and the shim are self-contained in this repo.

## Shims

`shim/java` is a java launcher with two modes:

| `AXC_SHIM_MODE` | Behaviour |
|---|---|
| `transparent` (default) | log the invocation and `exec` the real JVM unchanged |
| `mapped` | if the main class is in `shim/main-class-map.tsv`, `exec` our binary; **an unknown main class fails loudly** |

Environment: `AXC_REAL_JAVA` (default `/usr/bin/java`), `AXC_SHIM_MAP`,
`AXC_BIN_DIR` (prefix for relative commands), `AXC_SHIM_LOG`.

The map contains the reference mains and their binaries
(`io.aeron.archive.ArchivingMediaDriver` -> `deepmsg-archive-server
--embedded-driver`, cluster equivalents, `io.aeron.MediaDriver` -> `aeronmd`).
When mapped, the binary receives every `-D` argument plus every argument after
the main class (the Aeron launch style).

`shim/shim_test.sh` self-tests both modes, the unknown-class failure and the
`-jar` refusal; the interop gtest runs it.

Rehearsal (transparent mode over the reference C suite):

```sh
PATH="$PWD/test/interop/shim:$PATH" cmake -S third_party/aeron -B build/reference-shim \
    -DAERON_TESTS=ON -DAERON_UNIT_TESTS=ON -DAERON_SYSTEM_TESTS=ON \
    -DAERON_BUILD_SAMPLES=OFF -DAERON_BUILD_DOCUMENTATION=OFF
cmake --build build/reference-shim --target aeron_archive_test
build/reference-shim/binaries/aeron_archive_test --gtest_filter='AeronCArchiveTest.*'
```

## C++ fixtures

- `Process` (`run_process`): fork/exec with timeout, combined output and an exit
  code; `ChildProcess` for long-running peers.
- `JavaProcess`: `java` with a mandatory timeout; exit code 143 (SIGTERM) is the
  normal way a JVM peer stops.
- `CDriver`: `aeronmd` on a caller-owned directory; the destructor stops the
  driver and **never deletes the directory** (it is shared with the peers).

## Reference test code

The reference C/Java test code does **not** live here: it lives in the fork
`deepmsg/aeron` (branch `deepmsg-tests`) and, for the upstream C suite, in the
pinned submodule. Point `AXC_AERON_TESTS` at it; a missing checkout is SKIP with
a printed reason, never a silent pass.
