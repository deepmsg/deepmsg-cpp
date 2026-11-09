# cluster goldens

Every file here was written by the **reference Java implementation** (Aeron
1.54.0, commit `454da1e4e6`), never by our code. `expected.txt` is the same
run's field dump; the C++ tests compare against both.

| Item | Value |
|---|---|
| generator (recording.log) | `analysis/cpp/records/c1a/RecordingLogGoldenGen.java` |
| generator (node-state.dat) | `analysis/cpp/records/c1a/NodeStateGoldenGen.java` |
| generator (cluster-mark.dat) | `analysis/cpp/records/c1b/ClusterMarkGoldenGen.java` |
| reference | Aeron 1.54.0 = `454da1e4e6`; jar `third_party/aeron/aeron-all/build/libs/aeron-all-1.54.0.jar` |
| compile | `javac -proc:none -cp <jar> -d /tmp/c1a-gen <generator>.java` |
| recording.log | `java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED -cp <jar>:/tmp/c1a-gen io.aeron.cluster.RecordingLogGoldenGen test/golden/cluster/recording-log` |
| node-state.dat | `java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED -cp <jar>:/tmp/c1a-gen io.aeron.cluster.NodeStateGoldenGen test/golden/cluster/node-state` |
| cluster-mark.dat | `java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED -cp <jar>:/tmp/c1b-gen io.aeron.cluster.service.ClusterMarkGoldenGen test/golden/cluster/mark-file` |

## recording.log

| file | sha256 |
|---|---|
| `recording-log/mixed/recording.log` | `7ff2a2656faab8dc258c7cc0a51571b8eb3c532cb3eef68f5057d887340e0179` |
| `recording-log/mixed/expected.txt` | `90db7d6ebb012cebd671f7b6ef0338a5217daaa7479d47d25cad31525a69fcd1` |
| `recording-log/backfill/recording.log` | `086df5825481caba7559c66969811af1f2232af139f3fc9b1ec2872d2d17c8c3` |
| `recording-log/backfill/expected.txt` | `bca246742df2d487c458f10641d856d1608259636d486fe24d68b43f53ccc267` |

`mixed` has a TERM (committed by the next term), a CM snapshot group, an
invalidated CM snapshot, a standby-snapshot group, and a second term.
`backfill` appends terms 3, 1, 2 in that disk order, so the file order is not
the logical order — the in-memory sort is what produces `expected.txt`.
`expected.txt` keys are `entryCount`, `nextEntryIndex` and, per entry,
`entry.<n>.<field>`. `position` is intentionally absent: it is the disk offset,
not a serialised field, and the reference has no accessor for it.

`test/unit/cluster/common/recording_log_golden_test.cpp` reads both files
field by field. `test/interop/cluster/...` runs the reference
`ClusterTool recording-log` on a file our code wrote and compares the printed
entries with the same tool's output on `mixed`.

## node-state.dat

| file | sha256 |
|---|---|
| `node-state/fresh/node-state.dat` | `e6adc4c60b87591245b4668131086120d7f899f6a51b0a845a58c1256cd7b17c` |
| `node-state/fresh/expected.txt` | `a2acad0ee1d608b6cef5bed8ecf216c5bc5041c2fa1686e3c7320b68924ce12a` |
| `node-state/updated/node-state.dat` | `76c51f447681e95c7ab013f1c0fa0254830281662a20dd53b76c9df2078aa46a` |
| `node-state/updated/expected.txt` | `77dc9ce2f9d8864e9f08b17b4c9e398ae0800c09cb2d319130e747bc9de2911f` |
| `node-state/v1_42_x/node-state.dat` | `5e29c27f27972c33c03e108035a5fa1efc891f925ac1aa8d293e007d55b70186` |

`fresh` is a newly created 1 MiB file (all three records present, candidate
term NULL); `updated` has `updateCandidateTermId(832234, 8923423, 324234)`
applied. Our writer is expected to reproduce both **byte for byte** — the
padding bytes are zero in the reference too, because the file is newly mapped.
`v1_42_x` is the reference test resource
`aeron-cluster/src/test/resources/v1_42_x/node-state.dat` (written by 1.42.x,
it also carries a `ClusterMembers` record our scanner must skip); it pins
forward compatibility.

Regenerate only by re-running the reference generators; never edit any of these
files by hand.

## cluster-mark.dat

| file | sha256 |
|---|---|
| `mark-file/consensus-module/cluster-mark.dat` | `cac71edae2aaefae4b92d790249374846b68a19edc33ba4d28866837b043a7aa` |
| `mark-file/consensus-module/expected.txt` | `03d7b7f07964227076a262db6be420f41050e148c18148bcc91336fb00aca22d` |
| `mark-file/container/cluster-mark.dat` | `fdc29ede4e254790302f1ac7f354d6eaa3d6faee4661b07be37033f29a8f6e0c` |
| `mark-file/container/expected.txt` | `977c79adfb98141646c959ba25bf7a33da2ccec734f93941aeb5a3fa2524e9eb` |
| `mark-file/backup/cluster-mark.dat` | `6e61ff82d2f83722cebc091dff3a5470af40c01c7ebac0b68d78d9293825e2b7` |
| `mark-file/backup/expected.txt` | `65052b89b63d0c9c70416a0b060d9839aa37a509d1e0c91f79e75027472c4433` |

Each case was created with `now = 123456`, page size 4096 and a 1 MiB error
buffer, then `signalReady(123456)`. `pid`/`startTimestamp` are therefore fixed
in `expected.txt` for the reference run (the pid is the generator JVM's); the
C++ test reads the file, it does not reproduce it byte for byte because a live
component writes its own pid and start time.

`test/unit/cluster/common/cluster_mark_file_golden_test.cpp` checks every
field, and `axc_cluster_interop_tests` has `ClusterTool pid` read a mark file
written by our code.
