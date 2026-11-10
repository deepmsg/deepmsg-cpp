#!/usr/bin/env bash
#
# Every labelled counter in a CnC file, as `<label>\t<value>`.
#
#   counters.sh <cnc-file>
#
# The reader is `CounterDump.java` beside this file, run out of the harness's
# own jar so that the same class files read the Java driver and the C driver
# (both write the reference's CnC layout). See `quantiles.sh` for the same
# pattern and for where the jar comes from.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BENCH_DIR=$(dirname "$here")
# shellcheck source=../lib/checkouts.sh
. "$BENCH_DIR/lib/checkouts.sh"

if [ $# -ne 1 ]; then
    echo "usage: counters.sh <cnc-file>" >&2
    exit 2
fi
if [ ! -f "$1" ]; then
    echo "no CnC file at $1" >&2
    exit 1
fi

jar=$(ls "$BENCHMARKS_DIR"/benchmarks-all/build/libs/benchmarks.jar 2>/dev/null | head -1)
if [ -z "$jar" ]; then
    echo "no harness jar: build it with ./gradlew :benchmarks-all:shadowJar" >&2
    echo "  (see bench/lib/checkouts.sh)" >&2
    exit 1
fi

exec java \
    --add-opens java.base/java.util.zip=ALL-UNNAMED \
    --add-opens java.base/jdk.internal.misc=ALL-UNNAMED \
    -cp "$jar" "$here/CounterDump.java" "$1"
