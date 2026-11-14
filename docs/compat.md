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
| business plugin ABI (`deepmsg_cluster_service_create`; `deepmsg.cluster.service` = echo/basic-auction/noop or `deepmsg.cluster.service.library=<path>`) | Q8 (`analysis/cpp/modules-2026-11-07-cpp-interfaces.md` §5.4) | C2c: `aBusinessLoadedFromASharedLibraryEchoes` + the container binary scenes |
| `recording.log` | `RecordingLog.java` | C1a: `test/golden/cluster/recording-log/{mixed,backfill}` (reference-written) + `RecordingLogTest`/`RecordingLogGoldenTest` + `ClusterTool recording-log` interop over a file we wrote |
| `node-state.dat` | `aeron-cluster-node-state-codecs.xml` | C1a: `test/golden/cluster/node-state/{fresh,updated}` (reference-written, reproduced byte for byte) + `v1_42_x` forward-compat resource + `NodeStateFileTest` |
| `ReplicateRequest2` (66) / `StopReplicationRequest` (51): v7 `fileIoMaxLength`, v8 `replicationSessionId`/`encodedCredentials`, v10 `srcResponseChannel`, read in schema order | `aeron-archive-codecs.xml` (schema 101) | A4b: adapter arm tests + `ReplicationInteropTest`; the four C replication cases |
| replication channel rewrites: `rejoin=false`, `session-id=<replaySessionId>`, wildcard-port substitution from the subscription's resolved endpoint | `ReplicationSession.java:527-568`, `:727-744`; `ChannelUri.java:613-636` | A4b: `ReplicationSessionTest` channel cases + the four C replication cases |
| destination recording descriptor copied from the source descriptor (`addNewRecording` / `replaceRecording`) | `ReplicationSession.java:349-394` | A4b: `ReplicationSessionTest` (row fields) + the C cases (the destination replay reads the frames back) |
| `cluster-mark*.dat` | `aeron-cluster-mark-codecs.xml` (message header + `MarkFileHeader` + var-ascii fields + error buffer) | C1b: `test/golden/cluster/mark-file/{consensus-module,container,backup}` (reference-written) + `ClusterMarkFileTest`/golden + `ClusterTool pid` interop over a file we wrote |
| cluster counters 200–241 key layout | `ClusterCounters` / `AeronCounters.java:860-1106` | C1b: `ClusterCountersTest` (key bytes and label text for cluster, service and versioned forms; full type-id table) |
| timer snapshot order | Agrona `Long2LongHashMap` entry-iterator order (wheel) / heap array level order (priority heap); `WheelTimerService.snapshot` | C1c: `test/golden/cluster/timer/{five,many,heap-after-poll}.txt` (reference-written) + `TimerGoldenTest`; the two suppliers are deliberately **not** snapshot-byte-compatible with each other |
| timer expiry order | `DeadlineTimerWheel.poll` / `PriorityHeapTimerService.poll` | C1c: `test/golden/cluster/timer/{wheel-expiry,wheel-tick,heap-expiry}.txt` + `TimerGoldenTest` + `PriorityHeapTimerServiceTest` |
| container snapshot records (`SnapshotMarker` 100 with typeId **2**, `ClientSession` 102, then the business's records after END) | `aeron-cluster-codecs.xml`; `ClusteredServiceAgent.snapshotState` / `ServiceSnapshotLoader` | C2b: `theTwinsSnapshotIsTheJavaOnesByteForByte` (Java BasicAuction vs our twin, same log, the two recordings byte-equal) + `aSnapshotSurvivesAContainerRestart` |
| ingress/log/egress/consensus/S2 frames | `aeron-cluster-codecs.xml` (schema 111) | C2a: the container half of S2 (`JoinLog` 40, `ServiceTerminationPosition` 42, `RequestServiceAck` 108, `ServiceAck` 33, `ScheduleTimer` 31, `CancelTimer` 32) against the reference CM (`ContainerInteropTest`); full family C4 |
| session messages (32-byte `SessionMessageHeader` = 8-byte SBE header + 24-byte block, `clusterSessionId` + `leadershipTermId` + `timestamp`) and the two control messages written by the client: `SessionKeepAlive` (template 5) and `SessionCloseRequest` (template 4), both `leadershipTermId` + `clusterSessionId` | `AeronCluster.java:99-100` (`SESSION_HEADER_LENGTH`), `:487-526` (keep-alive), `:1129-1160` (close) | C3b: `cluster_client_test.cpp::offerWritesTheSessionHeaderAndThePayload` / `tryClaimFillsThePayloadAfterTheHeader` / `keepAliveCarriesTheTermAndSession` / `closeSendsSessionCloseRequestWhenConnected` + the C smoke interop (`aCProgramConnectsOffersAndReceivesTheEcho`) |
| client connect request (`SessionConnectRequest` template 3: fields in schema order, `version` = protocol 0.3.0 = `768`, `clientInfo` = `name=<clientName> version=1.54.0 commit=454da1e4e6`) and the four egress templates the client reads (`SessionMessageHeader` 1, `SessionEvent` 2, `NewLeaderEvent` 6, `Challenge` 7) | `AeronCluster.java:2452-2471` (`AeronCounters.formatVersionInfo`, `AeronCounters.java:1633-1636`; `AeronClusterVersion` = 1.54.0 / `454da1e4e6`); `EgressPoller.java:235-320` | C3a: `async_connect_test.cpp::theSevenStepsReachALiveSessionAndTheRequestCarriesTheReferencesFields` (decodes the bytes our client offered) + `egress_poller_test.cpp` (each template, the unknown schema and the full-slot `ABORT`) + `client_connect_interop_test.cpp` against the reference `ClusteredMediaDriver` |
| `aeron.cluster.cpp.standby.snapshot.on.startup` (`warn`/`refuse`, default `warn`) — this build's own name, not in the reference; it gates the Q6 standby-startup check (a standby snapshot newer than every local snapshot) | Q6 (`analysis/cpp/impl-2026-11-07-cpp-cluster.md:59`) | C4a: `StandbyStartupCheckTest` (warn/refuse, newer/equal/older) + `ConsensusModuleInteropTest.aNewerStandbySnapshotWarnsAndCountsAnError` / `refuseModeStopsTheStart` |
| `cluster-mark.dat` written by the CM (component `CONSENSUS_MODULE`, `memberId`, `serviceId` = -1, `clusterId`, archive/service/CM/ingress stream ids) | `ConsensusModule.Context.concludeMarkFile` (`ConsensusModule.java:4610-4650`) | C4a: `ConsensusModuleInteropTest.theSkeletonStartsAgainstTheReferenceArchiveAndPublishesItsState` |
| cluster session counter 241: key `[clusterId int32][clusterSessionId int64]`, label `cluster-session: <sessionInfo> - clusterId=<id>` | `ClusterSession.addSessionCounter` (`ClusterSession.java:529-553`) | C4b: `SessionManagerTest.aSessionCounterUsesTheClusterAndSessionKey` (type id and the key's cluster-id prefix) |
| log session message: the 32-byte `SessionMessageHeader` + payload; `SessionOpenEvent` and `SessionCloseEvent` records | `LogPublisher.appendMessage/appendSessionOpen/appendSessionClose` (`LogPublisher.java:115-218`) | C4b: `LogPublisherTest` (all three records decoded field by field; `payload_bytes_copied() == 0`) |
| S2 control frames, CM -> container: `JoinLog` (40), `ClusterMembersResponse` (41), `ServiceTerminationPosition` (42), `ClusterMembersExtendedResponse` (43), `RequestServiceAck` (108) | `aeron-cluster-codecs.xml` (schema 111); `ServiceProxy.java` | C4c: `ServiceProxyTest` (each message decoded field by field), the container's own `ServiceAdapter` decodes the same bytes in the C4c interop test |
| S2 control frames, container -> CM: `SessionMessageHeader` (1), `CloseSession` (30), `ScheduleTimer` (31), `CancelTimer` (32), `ServiceAck` (33), `ClusterMembersQuery` (34) | `aeron-cluster-codecs.xml`; `ConsensusModuleAdapter.java` | C4c: `ConsensusModuleAdapterTest` (six templates, the `BREAK` after `ServiceAck`) + the C4c interop test against the reference container |
| `TimerEvent` log record (leadership term id, correlation id, timestamp) | `LogPublisher.appendTimer` (`LogPublisher.java:242-267`); `TimerEvent` schema 111 | C4c: `LogPublisherTest` (timer record) + the timer rounds in the C4c interop test |
| recovery-state counter 204: key `[leadershipTermId int64][logPosition int64][timestamp int64][clusterId int32][serviceCount int32][snapshotRecordingIds int64...]`, label `Cluster recovery: leadershipTermId=… logPosition=… clusterId=…` | `RecoveryState.allocate` (`service/RecoveryState.java`) | C4c: the reference container starts against our CM in `ServiceControlInteropTest` (it awaits this counter before `JoinLog`); C4d: it is filled from the recovery plan (`addRecoveryStateCounter`, `ConsensusModuleAgent.java:2868-2893`) and the restarted container finds its snapshot through it (`SnapshotRecoveryInteropTest.aSnapshotSurvivesARestartAndTheLogContinues`) |
| consensus module snapshot records (`SnapshotMarker` 100 with typeId **0**, `ClusterSession` 103, `ConsensusModule` 105, `Timer` 104, `PendingMessageTracker` 107, `ClusterMembers` 106 ignored) | `aeron-cluster-codecs.xml`; `ConsensusModuleSnapshotTaker.java` / `ConsensusModuleSnapshotAdapter.java` | C4d: `cm_snapshot_test.cpp` (each record decoded field by field, the taker→adapter round trip, marker order, a foreign typeId rejected) + `SnapshotRecoveryInteropTest.aSnapshotSurvivesARestartAndTheLogContinues` (the reference container restores through the CM snapshot it found in `RecoveryState`) |
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

## The DPDK media (C++-only, 2026-11-15)

The reference's DPDK media is Premium and closed, so there is no oracle for its
names or its log. These are our own, compiled in only with
`-DAXC_WITH_DPDK=ON`; a default build refuses `deepmsg.dpdk.enabled=true` and
names the build option. Every name is read through `PropertySource`, so the
environment form is `DEEPMSG_DPDK_*` (`DEEPMSG_DPDK_IPV4_ADDRESS` ->
`deepmsg.dpdk.ipv4.address`). An unknown `deepmsg.dpdk.*` name is refused.

| Name | Default | Meaning | Pinned by |
|---|---|---|---|
| `deepmsg.dpdk.enabled` | `false` | The media's switch. This — not the reference's `aeron.udp.channel.transport.bindings.media` — selects it: the C driver resolves that name in `aeron_driver_context_init` (`aeron_driver_context.c:1288`) before a media can be installed, and no `dpdk` symbol is exported (P5 would add the alias). | `DpdkMediaConfigTest`, `DpdkCounters` |
| `deepmsg.dpdk.eal.args` | empty | The operator's EAL arguments, whitespace-split and passed through (`-a 0000:03:00.0`, `--vdev=net_pcap0,...`, `--no-huge`). `--lcores`/`-l` is refused: the media chooses it from the process's own cpuset. | `DpdkMediaConfig` |
| `deepmsg.dpdk.port` | first probed port | The EAL port name (a PCI address or a vdev name). | `DpdkMediaConfig`, `DpdkPortTest` |
| `deepmsg.dpdk.ipv4.address` | required when enabled | This port's address/prefix, e.g. `10.10.0.1/24`. The kernel does not have it; the stack answers ARP for it. | `DpdkMediaChild` |
| `deepmsg.dpdk.ipv4.gateway` | none | Next hop for destinations off this link. | `DpdkMediaConfig` |
| `deepmsg.dpdk.static.neighbours` | empty | `address=mac,...` entries that never age out. | `DpdkMediaConfig`, `DpdkStackTest` |
| `deepmsg.dpdk.rx.descriptors` / `tx.descriptors` | 1024 / 1024 | Queue depths. | `DpdkPortTest` |
| `deepmsg.dpdk.mempool.size` | 8191 | Mbufs (`2^k - 1`, so every cache line is full). | `DpdkPortTest` |
| `deepmsg.dpdk.mempool.cache` | 256 | Per-thread mbuf cache. | `DpdkPortTest` |
| `deepmsg.dpdk.rx.burst` | 32 | Frames one `pump` takes off the port (the shim's floor is 4 on ixgbe). | `DpdkMediaConfig` |
| `deepmsg.dpdk.sender.ring` | 1024 | Frames a transport's ring holds (`2^k`; the capacity is rounded up from the receive window). | `DpdkMediaConfig` |
| `deepmsg.dpdk.ephemeral.ports` | `32768-60999` | Port range for a transport bound to port 0. | `DpdkMediaConfig` |
| `deepmsg.dpdk.arp.timeout.ms` / `arp.retry.ms` | 300000 / 1000 | Neighbour ageing and ARP throttle. | `DpdkMediaConfig` |

**Behaviour that is not configuration**

- The media adds `--no-telemetry` and `--lcores=0@(<process cpuset>)` to the
  operator's EAL arguments; an operator `--lcores`/`-l` is refused rather than
  overridden (`plan-dpdk-media.md` §4.7). The main thread's affinity is saved
  before `rte_eal_init` and restored after it.
- `open_media` waits up to 30 s for the port's link before it returns; a
  10GBASE-T link was measured at ~20 s after start, and frames sent before it
  is up are lost without an error.
- On success it writes one line to **stdout** — `dpdk media on <port>
  (<driver>), address <address>/<prefix>, mtu <mtu>, link up (<speed> Mb/s)` —
  and `dpdk media closed` on close. The reference's closed media says nothing;
  this line is deliberate, because "configured" and "open on the media" must
  not look alike. Nothing is written to stderr by our own code.
- Receives: every transport keeps its kernel socket; a wildcard or this-port
  bind is also entered in the stack's table, which is how the DPDK port acts as
  an extra NIC. Sends are routed per destination: loopback, multicast,
  broadcast and unreachable destinations fall back to the kernel; everything
  else leaves through the DPDK port. Only `aeron.threading.mode=DEDICATED` is
  accepted.
- Counters use type ids 75–95 (`aeron_counters.h:155-173`); 80/81 are unused
  and 92 is the port's extended-statistics name count. Their keys and labels
  are ours, because the reference's media is closed.

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
 (C4a: the consensus module launches, loops and watches its archive)
 (C4b: the sessions, their ingress and the log/egress records)
