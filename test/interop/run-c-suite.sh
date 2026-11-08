#!/usr/bin/env bash
# Runs the reference C archive suite (`aeron_archive_test`) against our binary.
#
# The pinned `TestArchive.h` starts the archive with
# `posix_spawn(..., envp = NULL)`, so the child gets an **empty environment**:
# the env-driven shim cannot be used here. Instead the reference suite is
# configured once with `JAVA_EXECUTABLE` pointing at
# `test/interop/shim/archive-java`, which redirects the
# `io.aeron.archive.ArchivingMediaDriver` invocation to our binary.
#
# One-time configure (the other options are already in the cache):
#   cmake -S third_party/aeron -B build/reference-shim \
#       -DJava_JAVA_EXECUTABLE="$PWD/test/interop/shim/archive-java"
#
# Usage: test/interop/run-c-suite.sh [preset] [gtest args...]
#   preset defaults to `debug`.

set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
preset=${1:-debug}
shift || true

bin_dir="$root/build/$preset/apps/deepmsg-archive-server"
test_bin=${AXC_C_SUITE:-"$root/build/reference-shim/binaries/aeron_archive_test"}

if [ ! -x "$bin_dir/deepmsg-archive-server" ]; then
    echo "run-c-suite: no archive binary at $bin_dir (build the $preset preset first)" >&2
    exit 2
fi
if [ ! -x "$test_bin" ]; then
    echo "run-c-suite: no reference C suite at $test_bin (see the header for the one-time configure)" >&2
    exit 2
fi

# `archive-java` reads this because the spawned child cannot be given env vars.
printf 'AXC_BIN_DIR=%s\n' "$bin_dir" > "$root/test/interop/shim/mapped.conf"

exec "$test_bin" "$@"
