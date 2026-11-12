# Byte contracts

Every wire format, disk format, counter key layout and configuration name that
must match Aeron 1.54.0 goes here, with the test that pins it. A new format or
key is not mergeable without a row **and** a golden or interop test in the same
commit.

| Contract | Follows | Pinned by |
|---|---|---|
| `archive.catalog` descriptors | `aeron-archive-codecs.xml` (schema 101, `CatalogHeader`/`RecordingDescriptorHeader`/`RecordingDescriptor`) | A1a: `test/golden/archive/catalog/` (reference-written file) + `CatalogTest`/`CatalogGoldenTest` + `ArchiveTool describe/count-entries` interop |
| `archive-mark.dat` | `aeron-archive-mark-codecs.xml` (message header + `MarkFileHeader` + var-ascii channels + error buffer) | A1b: `test/golden/archive/mark-file/` (reference-written file) + `ArchiveMarkFileTest`/`MarkFileGoldenTest` + `ArchiveTool describe-all/verify` interop |
| `archive-mark.lnk` | Agrona `MarkFile.ensureMarkFileLink` (2.6.1) + `Archive.java:1392-1397`: US-ASCII canonical path of the **mark-file directory** (not the file); absent when it equals the archive directory | A7: `ArchiveMarkFileTest.theLinkFileNamesTheMarkDirectory` + `ArchiveTool resolveMarkFileDir` interop |
| `<dir>/archive-<uuuu-MM-dd-HH-mm-ss-SSSSSSZ>-error.log` | `CommonContext.saveExistingErrors` (`CommonContext.java:1316-1355`): the previous process's distinct-error-log rendering, written only when it has observations, before the mark-file header is overwritten | A7: `ArchiveMarkFileTest.anExistingErrorLogIsSavedAndCleared` |
| segment files `<id>-<base>.rec` | `Archive.segmentFileName`, `AeronArchive.segmentFileBasePosition`, length `max(segment, term)` | A1b: `SegmentFilesTest`/`SyncWriterTest` + `ArchiveTool verify` interop over files we wrote |
| archive counters 100–117 key layout | `ArchiveCounters` / `RecordingPos` | A2b |
| archive counters 102 (`Archive Control Sessions`), 108/109/110 (`archive-replayer max read time in ns`, `archive-replayer total read bytes`, `archive-replayer total read time in ns`) and 112 (`Archive Replay Sessions`): key = 8-byte archive id, label `<name> - archiveId=<id>` | `AeronCounters.java:751-818`, `Archive.java:1626-1730`, `ArchiveCounters.java:52-68` | A3b: `ArchiveCountersTest` (ids/key/label bytes); the C suite exercises 102 (`shouldCloseArchiveConnectionOnFailureInCaseApplicationKeepsPolling`) and the replay family |
| duty-cycle counters 103 (`... max cycle time in ns: <MODE>`) / 104 (`... work cycle time exceeded count: threshold=<duration> <MODE>`) for `archive-conductor` in every mode and `archive-recorder` / `archive-replayer` only in DEDICATED; key = 8-byte archive id | `AeronCounters.java:757,764`, `Archive.java:1470-1545`, `DutyCycleStallTracker.java:76-90` | A5a: `ThreadingInteropTest` (labels, pairs per mode) + `DutyCycleTracker` unit test |
| `deepmsg.archive.recorder.threads` (C++ extension, default 1): how many recorder agents DEDICATED mode starts; N > 1 adds `-<i>` to the recorder thread name and duty-cycle label; nothing on the wire or on disk changes | none (P-A9: the reference always starts one recorder) | A6f: `ThreadingTest` (default/range/mode) + the N-recorder criteria in `analysis/cpp/records/a6f.md` |
| control protocol request/response | `aeron-archive-codecs.xml` | A2a |
| `recording.log` | `RecordingLog.java` | C1a: `test/golden/cluster/recording-log/{mixed,backfill}` (reference-written) + `RecordingLogTest`/`RecordingLogGoldenTest` + `ClusterTool recording-log` interop over a file we wrote |
| `node-state.dat` | `aeron-cluster-node-state-codecs.xml` | C1a: `test/golden/cluster/node-state/{fresh,updated}` (reference-written, reproduced byte for byte) + `v1_42_x` forward-compat resource + `NodeStateFileTest` |
| `ReplicateRequest2` (66) / `StopReplicationRequest` (51): v7 `fileIoMaxLength`, v8 `replicationSessionId`/`encodedCredentials`, v10 `srcResponseChannel`, read in schema order | `aeron-archive-codecs.xml` (schema 101) | A4b: adapter arm tests + `ReplicationInteropTest`; the four C replication cases |
| replication channel rewrites: `rejoin=false`, `session-id=<replaySessionId>`, wildcard-port substitution from the subscription's resolved endpoint | `ReplicationSession.java:527-568`, `:727-744`; `ChannelUri.java:613-636` | A4b: `ReplicationSessionTest` channel cases + the four C replication cases |
| destination recording descriptor copied from the source descriptor (`addNewRecording` / `replaceRecording`) | `ReplicationSession.java:349-394` | A4b: `ReplicationSessionTest` (row fields) + the C cases (the destination replay reads the frames back) |
| `cluster-mark*.dat` | `aeron-cluster-mark-codecs.xml` (message header + `MarkFileHeader` + var-ascii fields + error buffer) | C1b: `test/golden/cluster/mark-file/{consensus-module,container,backup}` (reference-written) + `ClusterMarkFileTest`/golden + `ClusterTool pid` interop over a file we wrote |
| cluster counters 200–241 key layout | `ClusterCounters` / `AeronCounters.java:860-1106` | C1b: `ClusterCountersTest` (key bytes and label text for cluster, service and versioned forms; full type-id table) |
| timer snapshot order | Agrona `Long2LongHashMap` entry-iterator order (wheel) / heap array level order (priority heap); `WheelTimerService.snapshot` | C1c: `test/golden/cluster/timer/{five,many,heap-after-poll}.txt` (reference-written) + `TimerGoldenTest`; the two suppliers are deliberately **not** snapshot-byte-compatible with each other |
| timer expiry order | `DeadlineTimerWheel.poll` / `PriorityHeapTimerService.poll` | C1c: `test/golden/cluster/timer/{wheel-expiry,wheel-tick,heap-expiry}.txt` + `TimerGoldenTest` + `PriorityHeapTimerServiceTest` |
| ingress/log/egress/consensus/S2 frames | `aeron-cluster-codecs.xml` (schema 111) | C2a: the container half of S2 (`JoinLog` 40, `ServiceTerminationPosition` 42, `RequestServiceAck` 108, `ServiceAck` 33, `ScheduleTimer` 31, `CancelTimer` 32) against the reference CM (`ContainerInteropTest`); full family C4 |

## C++-only switches (A6c/A6d, 2026-11-10)

These never join the reference's 48-name table (`archive_config_names()`), whose
test asserts every entry starts with `aeron.archive.`. The defaults reproduce the
reference behaviour (sparse `setLength`, `FileChannel.read` into a buffer, no
advice).

| Name | Default | Meaning | Pinned by |
|---|---|---|---|
| `deepmsg.archive.file.io.fallocate` | `false` | Best-effort preallocation after the reference's `ftruncate`/`setLength` (`RecordingWriter.java:219`): `fallocate(FALLOC_FL_KEEP_SIZE)`. A refusal (unsupported filesystem, `ENOSPC`) leaves the segment sparse and defers a full-disk failure to the write, with one warning. | `ArchiveConfigTest`, `SyncWriterTest` |
| `deepmsg.archive.replay.io.backend` | `pread` | How `ReplaySession` reads segment files: `pread` copies into the session's read window (the reference's shape) or `mmap` maps the file `MAP_PRIVATE` so the frame loop walks the mapping (frame-header rewrites COW and never reach the file). A replay bounded by a position counter (a live recording) always reads through `pread`: the first header rewrite freezes its 4 KiB page, so the recorder's later appends into that page would be invisible to the mapping. | `ArchiveConfigTest`, `SegmentReaderTest`, `ReplaySessionTest` |
| `deepmsg.archive.replay.io.fadvise` | `none` | `willneed` issues `POSIX_FADV_WILLNEED` when a segment is opened and `POSIX_FADV_DONTNEED` before it is dropped; a refused advice (`EINVAL`/`ENOSYS`) is not an error. | `ArchiveConfigTest`, `SegmentReaderTest` |

## Known gaps (A4b, 2026-11-09)

Deliberately not implemented; none of them is reachable from the reference C
cases this card is judged by, and each fails loudly rather than silently:

- `ReplicateRequest` (50) and `TaggedReplicateRequest` (62) arms — the C client
  sends `ReplicateRequest2` (66) only.
- live merge: `CATCHUP` / `ATTEMPT_LIVE_JOIN` and the `MERGE` signal — a
  `live_destination` request ends with "live merge is not implemented".
- response-channel replication: `REPLAY_TOKEN` (105) and the second
  publication `GET_ARCHIVE_PROXY` builds — a response-mode replication channel
  ends with "response-channel replication is not implemented".
- the `encodedCredentials` challenge/response chain for the source client: the
  request's credentials are supplied as-is (the reference's
  `ReplicationCredentialsSupplier` does the same for a non-challenging source).
