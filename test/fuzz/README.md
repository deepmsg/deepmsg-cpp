# Fuzz targets

Two modes, one source file per target:

| target | surface |
| --- | --- |
| `archive_control_fuzz` | archive control requests (schema 101) and channel URIs |
| `cluster_ingress_fuzz` | every cluster ingress adapter (schema 111): ingress, consensus, consensus-module, egress, service, both snapshot loaders |
| `recording_log_fuzz` | `recording.log`, `node-state.dat`, `cluster-mark.dat` |
| `archive_catalog_fuzz` | `archive.catalog` (with and without CRC32) and `archive-mark.dat` |
| `dpdk_frame_fuzz` | Ethernet / ARP / IPv4 / UDP / ICMP parsing and the reply builders |

Every decode goes through `axc::sbe::decode_guarded` (src/sbe/decode_guarded.h)
or the checked var-data readers in `cluster/common/cluster_codecs.h`; the
harness never catches an exception, so a decoder that lets one escape is a
finding, not a silent pass.

## PR regression (default presets, gcc and clang)

With `AXC_FUZZ=OFF` each target links `standalone_fuzz_main.cpp` and is
registered with ctest, so `scripts/check.sh` replays it on every run:

1. the target's own structurally valid seeds (`emit_seeds`);
2. the golden files in `test/golden/` (README/expected/txt skipped);
3. for every seed: the full input, every prefix up to 512 bytes, sampled
   power-of-two and tail prefixes for larger files, and a one-byte XOR flip
   (`0xFF`, `0x7F`) over the first 64 bytes.

File targets write to `/dev/shm/axc-fuzz-<pid>/`; nothing touches a disk.
`recording_log_fuzz` and `archive_catalog_fuzz` are file-bound, so their
throughput is thousands (not millions) of execs per second by design.

## libFuzzer (nightly)

`clang-20` only:

```
scripts/run-fuzz.sh                 # 15 s per target
scripts/run-fuzz.sh --duration 600  # nightly
```

The `fuzz` preset builds with `-fsanitize=fuzzer,address,undefined`. Every
target's `LLVMFuzzerInitialize` writes its generated seeds when
`AXC_FUZZ_EMIT_SEEDS` is set, which is how `run-fuzz.sh` seeds the corpus.

A crash artifact under `build/fuzz/crashes/<target>-<sha1>` is replayed with:

```
build/fuzz/test/fuzz/<target> <artifact>
```

## Known coverage boundaries

- `archive_control_fuzz` decodes each template's fixed block, then the session
  gate refuses the request (no control session is registered). Var-data fields
  after the gate are not reached; the block and the URI parser are.
- `cluster_ingress_fuzz` feeds one message per iteration. Multi-message session
  state machines are not modelled.
