#!/usr/bin/env bash
# Symbol audit for hot-path translation units.
#
# Each src/**/HOTPATH lists hot-path .cpp files (one per line, relative to src/).
# After a build, the corresponding object must not reference:
#   operator new, pthread_mutex_lock, __cxa_allocate_exception,
#   any aeron_counters_reader_foreach_* (counters are read by address only).
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$root"

build_dir=${1:-build/debug}
if [ ! -d "$build_dir" ]; then
    echo "hotpath-audit: no build directory '$build_dir' (build first)" >&2
    exit 2
fi

# Note: this is an ERE. Do NOT write "operator new(unsigned long)" -- the
# parentheses would be parsed as a group and the pattern would never match.
banned='operator new|pthread_mutex_lock|__cxa_allocate_exception|aeron_counters_reader_foreach_'
status=0
checked=0

while IFS= read -r entry; do
    case "$entry" in
        ''|'#'*) continue ;;
    esac

    rel="$entry"
    object=$(find "$build_dir" -path '*/CMakeFiles/*.dir/*' -name "$(basename "${rel%.cpp}").cpp.o" -print -quit)
    if [ -z "$object" ]; then
        echo "hotpath-audit: no object for $rel (is it part of a built target?)" >&2
        status=1
        continue
    fi

    checked=$((checked + 1))
    if nm -C "$object" | grep -Eq "$banned"; then
        echo "hotpath-audit: VIOLATION in $rel" >&2
        nm -C "$object" | grep -E "$banned" >&2
        status=1
    fi
done < <(find src -name HOTPATH -print0 | xargs -0 -r cat)

echo "hotpath-audit: $checked hot-path object(s) checked"
exit "$status"
