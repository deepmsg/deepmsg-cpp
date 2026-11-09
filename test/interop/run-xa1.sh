#!/usr/bin/env bash
# XA-1: the whole reference C archive suite against our binary in one threading
# mode, one process per case (the reference harness spawns the archive per
# case), denominator = X0-6's runtime recount: 319 cases across 11 binaries
# (`analysis/cpp/records/x0-6/case-denominators.tsv`).
#
# The reference harness hardcodes `aeron.archive.threading.mode=SHARED` in
# `TestArchive.h` / `TestStandaloneArchive.h`; the shim rewrites that argument
# to `$AXC_SHIM_THREADING_MODE` for our binary only, so DEDICATED is a property
# of the run, not of a patched submodule.
#
# Usage: test/interop/run-xa1.sh [preset] [SHARED|DEDICATED] [log dir]
#   preset defaults to `debug`; all 11 binaries must already be built in
#   `build/reference-shim/binaries` (see the one-time configure in a2b.txt plus
#   `cmake --build build/reference-shim --target <names>`).

set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
preset=${1:-debug}
mode=${2:-SHARED}
log_dir=${3:-"$root/build/xa1-$(date +%Y%m%d-%H%M%S)-$mode"}

bin_dir="$root/build/$preset/apps/deepmsg-archive-server"
if [ ! -x "$bin_dir/deepmsg-archive-server" ]; then
    echo "run-xa1: no archive binary at $bin_dir (build the $preset preset first)" >&2
    exit 2
fi

# name: X0-6 runtime case count.
tests=(
    "aeron_archive_test:80"
    "aeron_archive_persistent_subscription_test:80"
    "archiveTestW:51"
    "aeron_archive_persistent_subscription_resilience_test:50"
    "aeron_archive_persistent_subscription_context_test:31"
    "persistentSubscriptionTestW:9"
    "persistentSubscriptionContextTestW:8"
    "aeron_archive_recording_pos_test:4"
    "aeron_archive_async_client_test:3"
    "archiveContextTestW:2"
    "aeron_archive_async_connect_leak_test:1"
)

mkdir -p "$log_dir"
echo "run-xa1: mode=$mode preset=$preset logs=$log_dir"

expected_total=0
seen_total=0
failed_total=0
skipped_total=0
mismatches=()

for entry in "${tests[@]}"; do
    name=${entry%%:*}
    expected=${entry##*:}
    binary="$root/build/reference-shim/binaries/$name"
    if [ ! -x "$binary" ]; then
        echo "run-xa1: missing $binary (build the reference binaries first)" >&2
        exit 2
    fi

    AXC_C_SUITE="$binary" AXC_SHIM_THREADING_MODE="$mode" \
        timeout 1200 "$root/test/interop/run-c-suite.sh" "$preset" > "$log_dir/$name.log" 2>&1 || true

    # One line per runtime case, with the elapsed time; the trailing summary
    # repeats the failed/skipped names without it, so match the timed form.
    passed=$(grep -cE '^\[       OK \] [A-Za-z].*\([0-9]+ ms\)$' "$log_dir/$name.log" || true)
    failed=$(grep -cE '^\[  FAILED  \] [A-Za-z].*\([0-9]+ ms\)$' "$log_dir/$name.log" || true)
    skipped=$(grep -cE '^\[  SKIPPED \] [A-Za-z]' "$log_dir/$name.log" || true)
    seen=$((passed + failed + skipped))

    expected_total=$((expected_total + expected))
    seen_total=$((seen_total + seen))
    failed_total=$((failed_total + failed))
    skipped_total=$((skipped_total + skipped))

    if [ "$seen" -ne "$expected" ]; then
        mismatches+=("$name: expected $expected cases, saw $seen")
    fi
    printf '%-58s %3d/%3d passed  %2d failed  %2d skipped\n' "$name" "$passed" "$expected" "$failed" "$skipped"
done

echo "--------------------------------------------------------------------------"
printf 'total: %d cases (denominator %d), %d failed, %d skipped\n' \
    "$seen_total" "$expected_total" "$failed_total" "$skipped_total"

status=0
if [ "$seen_total" -ne "$expected_total" ]; then
    echo "run-xa1: DENOMINATOR MISMATCH" >&2
    for mismatch in "${mismatches[@]}"; do echo "  $mismatch" >&2; done
    status=1
fi
if [ "$failed_total" -ne 0 ]; then
    echo "run-xa1: $failed_total case(s) failed" >&2
    grep -HE '^\[  FAILED  \] [A-Za-z]' "$log_dir"/*.log | sed 's/^/  /' >&2 || true
    status=1
fi
if [ "$skipped_total" -ne 0 ]; then
    echo "run-xa1: $skipped_total case(s) skipped (a skip is not a pass)" >&2
    status=1
fi

if [ 0 -eq "$status" ]; then
    echo "run-xa1: GREEN ($mode)"
fi
exit "$status"
