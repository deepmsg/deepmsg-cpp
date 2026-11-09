#!/usr/bin/env bash
# The reference's three drop-wait races are test-side
# (`/home/gavin/trader/analysis/fix/upstream-issue-drop-wait-races-the-flood.md`):
# the tests read the persistent subscription while the flood is still landing,
# which raises the over-run threshold and the image never drops. The upstream
# fix is a 100 ms wait after the far-side subscriber drains; the same commit
# (`00731eaa41` on the fork's `deepmsg-tests` branch) makes the three cases
# green:
#
#   AeronArchivePersistentSubscriptionTest.shouldDropFromLiveBackToReplayThenJoinLiveAgain
#   AeronArchivePersistentSubscriptionTest.canFallbackToReplayAfterStartingFromLive
#   PersistentSubscriptionWrapperTest.shouldCycleThroughMultipleLiveReplayTransitions
#
# XA-1's "0 red" runs with these holds, exactly like the Rust track. They are
# applied to the **submodule working tree only** and never committed: the
# gitlink is the contract (CLAUDE.md rule 3), and this script reverts them.
#
# Usage: test/interop/apply-xa1-holds.sh apply|revert|status

set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
aeron="$root/third_party/aeron"
ps_test="$aeron/aeron-archive/src/test/c/client/aeron_archive_persistent_subscription_test.cpp"
wrapper_test="$aeron/aeron-system-tests/src/test/cpp_wrapper/PersistentSubscriptionWrapperTest.cpp"
archive_wrapper_test="$aeron/aeron-archive/src/test/cpp_wrapper/AeronArchiveWrapperTest.cpp"

git -C "$aeron" rev-parse --is-inside-work-tree > /dev/null
status=$(git -C "$aeron" status --porcelain -- "$ps_test" "$wrapper_test" "$archive_wrapper_test")

case "${1:-}" in
apply)
    if [ -n "$status" ]; then
        echo "apply-xa1-holds: the two test files are already modified; revert first" >&2
        exit 1
    fi
    python3 - "$ps_test" "$wrapper_test" "$archive_wrapper_test" <<'PY'
import sys

sleep_note = """        // LOCAL HOLD (XA-1, not upstream as of 454da1e4e6): let the flood land
        // before the subscription is read -- see
        // analysis/fix/upstream-issue-drop-wait-races-the-flood.md.
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

"""

ps_path, wrapper_path, archive_wrapper_path = sys.argv[1], sys.argv[2], sys.argv[3]

with open(ps_path) as handle:
    text = handle.read()

if "#include <thread>" not in text:
    text = text.replace("#include <chrono>", "#include <chrono>\n#include <thread>", 1)

anchors = [
    "            [&] { return fast_count == 64; });\n\n        // Phase 6: persistent subscription drops back to replay",
    "            [&] { return fast_count == back_pressure_messages.size(); });\n\n        executeUntil(\n            \"drops to replaying\"",
]
for anchor in anchors:
    if text.count(anchor) != 1:
        raise SystemExit(f"anchor not found exactly once in {ps_path}: {anchor[:60]!r}")
    head, tail = anchor.split("\n\n", 1)
    text = text.replace(anchor, head + "\n\n" + sleep_note + tail, 1)

with open(ps_path, "w") as handle:
    handle.write(text)

with open(wrapper_path) as handle:
    text = handle.read()

anchor = "            [&] { return fastCount >= 64; });\n\n        executeUntil(\"drops to replaying\""
if text.count(anchor) != 1:
    raise SystemExit(f"anchor not found exactly once in {wrapper_path}")
text = text.replace(anchor, anchor.replace("\n\n", "\n\n" + sleep_note, 1), 1)
with open(wrapper_path, "w") as handle:
    handle.write(text)

# `shouldRecordAndExtend` lists the recording as soon as stopRecording returns; the
# stop's OK does not guarantee the catalog write (the reference archive wins the
# race by timing, ours can lose it). Wait for the stop position before listing.
with open(archive_wrapper_path) as handle:
    text = handle.read()

anchor = "        aeronArchive->stopRecording(publication);\n    }\n\n    int32_t initialTermId;"
hold = """        aeronArchive->stopRecording(publication);
    }

    // LOCAL HOLD (XA-1, not upstream): the stop's OK does not guarantee that the
    // catalog's stop position is written; the reference archive wins this race by
    // timing and ours can lose it. Wait for the descriptor before listing.
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (aeronArchive->getStopPosition(recordingId) == -1 &&
               std::chrono::steady_clock::now() < deadline)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    int32_t initialTermId;"""
if text.count(anchor) != 1:
    raise SystemExit(f"anchor not found exactly once in {archive_wrapper_path}")
text = text.replace(anchor, hold, 1)
with open(archive_wrapper_path, "w") as handle:
    handle.write(text)
PY
    echo "apply-xa1-holds: applied"
    ;;
revert)
    git -C "$aeron" checkout -- "$ps_test" "$wrapper_test" "$archive_wrapper_test"
    echo "apply-xa1-holds: reverted"
    ;;
status)
    if [ -n "$status" ]; then
        echo "apply-xa1-holds: applied"
    else
        echo "apply-xa1-holds: clean"
    fi
    ;;
*)
    echo "usage: apply-xa1-holds.sh apply|revert|status" >&2
    exit 2
    ;;
esac
