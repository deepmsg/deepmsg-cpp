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
| control protocol request/response | `aeron-archive-codecs.xml` | A2a |
| `recording.log` | `RecordingLog.java` | C1a |
| `node-state.dat` | `aeron-cluster-node-state-codecs.xml` | C1a |
| `cluster-mark*.dat` | `aeron-cluster-mark-codecs.xml` | C1b |
| cluster counters 200–241 key layout | `ClusterCounters` | C1b |
| timer snapshot order | Agrona `Long2LongHashMap` iteration order | C1c |
| ingress/log/egress/consensus/S2 frames | `aeron-cluster-codecs.xml` | C4 |
