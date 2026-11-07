#!/usr/bin/env bash
# Locate (or build) the reference aeron-all jar used by interop tests (X0-4).
# Prints the jar path on stdout.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

if [ -n "${AXC_AERON_ALL_JAR:-}" ]; then
    echo "$AXC_AERON_ALL_JAR"
    exit 0
fi

jar="$root/third_party/aeron/build/libs/aeron-all-1.54.0.jar"
if [ -f "$jar" ]; then
    echo "$jar"
    exit 0
fi

echo "building aeron-all from the pinned submodule (needs JDK 17+)" >&2
(cd "$root/third_party/aeron" && ./gradlew -q :aeron-all:jar)
echo "$jar"
