# Byte contracts

Every wire format, disk format, counter key layout and configuration name that
must match Aeron 1.54.0 goes here, with the test that pins it. A new format or
key is not mergeable without a row **and** a golden or interop test in the same
commit.

| Contract | Follows | Pinned by |
|---|---|---|
| `archive.catalog` descriptors | `aeron-archive-codecs.xml` (schema 101, `CatalogHeader`/`RecordingDescriptorHeader`/`RecordingDescriptor`) | A1a: `test/golden/archive/catalog/` (reference-written file) + `CatalogTest`/`CatalogGoldenTest` + `ArchiveTool describe/count-entries` interop |
| `archive-mark.dat` | `aeron-archive-mark-codecs.xml` (message header + `MarkFileHeader` + var-ascii channels + error buffer) | A1b: `test/golden/archive/mark-file/` (reference-written file) + `ArchiveMarkFileTest`/`MarkFileGoldenTest` + `ArchiveTool describe-all/verify` interop |
| segment files `<id>-<base>.rec` | `Archive.segmentFileName`, `AeronArchive.segmentFileBasePosition`, length `max(segment, term)` | A1b: `SegmentFilesTest`/`SyncWriterTest` + `ArchiveTool verify` interop over files we wrote |
| archive counters 100–117 key layout | `ArchiveCounters` / `RecordingPos` | A2b |
| archive counters 102 (`Archive Control Sessions`), 108/109/110 (`archive-replayer max read time in ns`, `archive-replayer total read bytes`, `archive-replayer total read time in ns`) and 112 (`Archive Replay Sessions`): key = 8-byte archive id, label `<name> - archiveId=<id>` | `AeronCounters.java:751-818`, `Archive.java:1626-1730`, `ArchiveCounters.java:52-68` | A3b: `ArchiveCountersTest` (ids/key/label bytes); the C suite exercises 102 (`shouldCloseArchiveConnectionOnFailureInCaseApplicationKeepsPolling`) and the replay family |
| duty-cycle counters 103 (`... max cycle time in ns: <MODE>`) / 104 (`... work cycle time exceeded count: threshold=<duration> <MODE>`) for `archive-conductor` in every mode and `archive-recorder` / `archive-replayer` only in DEDICATED; key = 8-byte archive id | `AeronCounters.java:757,764`, `Archive.java:1470-1545`, `DutyCycleStallTracker.java:76-90` | A5a: `ThreadingInteropTest` (labels, pairs per mode) + `DutyCycleTracker` unit test |
| control protocol request/response | `aeron-archive-codecs.xml` | A2a |
| `recording.log` | `RecordingLog.java` | C1a: `test/golden/cluster/recording-log/{mixed,backfill}` (reference-written) + `RecordingLogTest`/`RecordingLogGoldenTest` + `ClusterTool recording-log` interop over a file we wrote |
| `node-state.dat` | `aeron-cluster-node-state-codecs.xml` | C1a: `test/golden/cluster/node-state/{fresh,updated}` (reference-written, reproduced byte for byte) + `v1_42_x` forward-compat resource + `NodeStateFileTest` |
| `ReplicateRequest2` (66) / `StopReplicationRequest` (51): v7 `fileIoMaxLength`, v8 `replicationSessionId`/`encodedCredentials`, v10 `srcResponseChannel`, read in schema order | `aeron-archive-codecs.xml` (schema 101) | A4b: adapter arm tests + `ReplicationInteropTest`; the four C replication cases |
| replication channel rewrites: `rejoin=false`, `session-id=<replaySessionId>`, wildcard-port substitution from the subscription's resolved endpoint | `ReplicationSession.java:527-568`, `:727-744`; `ChannelUri.java:613-636` | A4b: `ReplicationSessionTest` channel cases + the four C replication cases |
| destination recording descriptor copied from the source descriptor (`addNewRecording` / `replaceRecording`) | `ReplicationSession.java:349-394` | A4b: `ReplicationSessionTest` (row fields) + the C cases (the destination replay reads the frames back) |
| `cluster-mark*.dat` | `aeron-cluster-mark-codecs.xml` | C1b |
| cluster counters 200–241 key layout | `ClusterCounters` | C1b |
| timer snapshot order | Agrona `Long2LongHashMap` iteration order | C1c |
| ingress/log/egress/consensus/S2 frames | `aeron-cluster-codecs.xml` | C4 |

## C++-only switches (A6c, 2026-11-10)

These never join the reference's 48-name table (`archive_config_names()`), whose
test asserts every entry starts with `aeron.archive.`. The defaults reproduce the
reference behaviour.

| Name | Default | Meaning | Pinned by |
|---|---|---|---|
| `deepmsg.archive.file.io.fallocate` | `false` | Best-effort preallocation after the reference's `ftruncate`/`setLength` (`RecordingWriter.java:219`): `fallocate(FALLOC_FL_KEEP_SIZE)`. A refusal (unsupported filesystem, `ENOSPC`) leaves the segment sparse and defers a full-disk failure to the write, with one warning. | `ArchiveConfigTest`, `SyncWriterTest` |

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
