# The 1.54.0 → 1.54.2 delta, and what it meant here

The reference pin moved from `454da1e4e6` (1.54.0) to `481d4271ad` (1.54.2).
Two patch releases, 21 commits; three of them change behaviour a transcription
has to follow, the rest are the C driver/client and test-only work. This file
records the delta and the C-client audit that went with it, so the next bump
starts from the survey instead of the diff.

## The three behavioural changes

| Change | Reference | What this repo did |
|---|---|---|
| `ClusterActionRequest` carries `termBaseLogPosition`; its `logPosition` becomes the claim position (the snapshot starts after the record) | `LogPublisher.java:241-275`, schema `aeron-cluster-codecs.xml` v18 | `log_publisher.{h,cpp}`, `consensus_module_agent.*` — the C client's `try_claim` answers with the frame's **end**, so the field takes the end minus the aligned frame and `last_append_position()` keeps the end |
| `SnapshotMark` carries `termBaseLogPosition`; the takers take `(leadershipTermId, termBaseLogPosition, logPosition)` | `SnapshotTaker.java: markSnapshot`, `ConsensusModuleAgent.java:3178`, `:3214-3218` | `cm_snapshot.*`, `service/snapshot.*`, `clustered_service_agent.h`, `log_adapter.cpp` — both takers write the field; the module computes the base once and reuses it for the recording-log entries |
| The torn last fragment's page boundary is found from its **file** offset, not its buffer offset | `Catalog.java:1435-1480` | `archive/server/catalog.cpp` — `isValidFragment`/`hasDataInAllPagesAfterStraddle` carry both offsets; the two reference cases are transcribed in `catalog_test.cpp` |

## The C client audit

The submodule brings the C client 1.54.0 → 1.54.2, so the audit is: every place
this repo consumes a changed answer. **No code change was needed**; each row
names the call sites that were read.

| C client change | Call sites in this repo | Verdict |
|---|---|---|
| `aeron_image_end_of_stream_position` returns `-1` on error (was `INT64_MAX`/error text) | none — the symbol is not used | nothing to do |
| `aeron_image_is_end_of_stream` / `aeron_image_is_publication_revoked` answer **false** for a null image (was true with an error set) | `archive/server/archive_conductor.cpp` (`image_is_end_of_stream`, reached from `replication_session.cpp` which guards null first); `archive/server/recording_session.cpp` (`AeronRecordingImage::is_end_of_stream`); `cluster/service/snapshot.cpp` (`SnapshotImage::is_end_of_stream`); `cluster/service/log_adapter.cpp` and `cluster/consensus/log_adapter.cpp` (both guard null); one interop test | the wrappers are only built around a live image; the paths that can hold null guard it. Where the answer changes, it changes to the reference's (`C` 1.54.2, `aeron_image.c`) |
| New error codes `AERON_CLIENT_ERROR_CLIENT_ILLEGAL_STATE (1005)` and `..._BROADCAST_RECEIVER_ILLEGAL_STATE (1006)` | no `AERON_CLIENT_ERROR_*` name is referenced anywhere (`aeron_errcode()` is passed through as a number) | no switch to extend |
| The conductor marks pending async resources `ERRORED` when it hits an unrecoverable error, instead of leaving callers to poll forever | `client_util.h::await_async` (and the `await_handle` beside it) return the poll's error immediately and otherwise stop at the deadline | already surfaces it — this is the behaviour the change makes reachable |
| `aeron_fragment_assembler_delete_session_buffer` (new) | this repo uses the **controlled** assemblers (`aeron_controlled_fragment_assembler_*`, `aeron_image_controlled_fragment_assembler_*`), not the plain `aeron_fragment_assembler_t` | not applicable |
| C++ wrapper (`Image` copy/move, `ChannelUri` fields) | the wrapper is not used; the repo links the C API directly | not applicable |

Two C driver commits in 1.54.2 (consumer/spy position updated when a local
subscriber is removed; `joinPosition` afterwards) are the submodule's own
behaviour; nothing here transcribes the driver, so nothing to follow.

## Re-checking after a bump

1. `git diff <old> <new> -- aeron-cluster/src/main/resources` — schemas first.
2. `git diff <old> <new> -- aeron-cluster/src/main/java`, `aeron-archive/src/main/java` —
   the behaviour a transcription follows.
3. `git diff <old> <new> -- aeron-client/src/main/c` — then re-run the table above:
   `grep -rn "aeron_image_\|AERON_CLIENT_ERROR" src apps tools test`.
