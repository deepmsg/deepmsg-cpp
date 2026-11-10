#!/usr/bin/env bash
# XA-3: runs each content-parity case in its own process.
#
# The reference Java media driver (and aeronmd) are children of the test process;
# a fixture that fails early can leave one behind holding a port, and the next
# fixture in the same process then sees a dead driver. One case per process makes
# each fixture's setup independent, the same way the reference C suite spawns one
# archive per case.
#
# Usage: test/interop/run-parity.sh [preset]

set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
preset=${1:-debug}
binary="$root/build/$preset/test/interop/axc_archive_interop_tests"
if [ ! -x "$binary" ]; then
    echo "run-parity: no test binary at $binary" >&2
    exit 2
fi

cases=(
    singleSegmentIsByteIdentical
    multiSegmentIsByteIdentical
    emptyRecordingDescribesTheSameStream
    twoConcurrentStreamsAreByteIdentical
    udpWithMtu512IsByteIdentical
    pacedPublishingIsByteIdentical
    acrossDriversSingleSegment
    acrossDriversMultiSegment
    acrossDriversUdpMtu512
)

passed=0
failed=0
for case in "${cases[@]}"; do
    log="/tmp/axc-parity-$case.log"
    # A timed-out case must not leave the fixture's driver/archive children
    # behind: they keep the case's UDP ports and the next run then fails with
    # "publication not connected" (measured on the A6b mutant run). `setsid`
    # makes the test a process-group leader, so the timeout kills the group.
    setsid "$binary" --gtest_filter="ContentParityTest.$case" >"$log" 2>&1 &
    pid=$!
    waited=0
    while kill -0 "$pid" 2>/dev/null; do
        if [ "$waited" -ge 180 ]; then
            kill -TERM -- -"$pid" 2>/dev/null || true
            sleep 1
            kill -KILL -- -"$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
            printf '%-40s TIMEOUT (/tmp/axc-parity-%s.log)\n' "$case" "$case"
            failed=$((failed + 1))
            continue 2
        fi
        sleep 1
        waited=$((waited + 1))
    done
    if wait "$pid"; then
        printf '%-40s PASS\n' "$case"
        passed=$((passed + 1))
    else
        printf '%-40s FAIL (/tmp/axc-parity-%s.log)\n' "$case" "$case"
        failed=$((failed + 1))
    fi
done

echo "--------------------------------------------------------------------------"
echo "content parity: $passed passed, $failed failed"
[ 0 -eq "$failed" ]
