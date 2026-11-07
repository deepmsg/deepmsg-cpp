#!/usr/bin/env bash
# Dependency-direction invariant (repo-layout §4.1):
#   cluster/* never depends on archive/server -- CM accesses the local archive
#   only through the C archive client;
#   archive/* never depends on cluster/*.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$root"

status=0

if grep -rn --include=CMakeLists.txt 'axc_archive_server' src/cluster; then
    echo "check-deps: src/cluster must not reference axc_archive_server" >&2
    status=1
fi

if grep -rn --include=CMakeLists.txt 'axc_cluster_' src/archive; then
    echo "check-deps: src/archive must not reference cluster targets" >&2
    status=1
fi

if [ "$status" -eq 0 ]; then
    echo "check-deps: dependency direction OK"
fi

exit "$status"
