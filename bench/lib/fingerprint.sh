#!/usr/bin/env bash
#
# What a C++-arm result set was taken on.
#
# A number without its fingerprint is an anecdote (`analysis/cpp/spec-…` §6.5):
# every batch writes one beside its results, with the C++ binary's digest, the
# compiler and the build flags. Sourced. `bench_fingerprint [output-file]`
# (default: stdout). Everything is read, not restated.

bench_fingerprint() {
    local out=${1:-/dev/stdout}

    {
        echo "# machine"
        echo "host            $(hostname)"
        echo "date            $(date --rfc-3339=seconds)"
        echo "cpu             $(awk -F': ' '/^model name/{print $2; exit}' /proc/cpuinfo)"
        echo "cores           $(nproc) logical, $(lscpu | awk -F: '/^Core\(s\) per socket/{gsub(/ /,"",$2); print $2}') per socket, $(lscpu | awk -F: '/^Socket\(s\)/{gsub(/ /,"",$2); print $2}') socket(s), $(lscpu | awk -F: '/^Thread\(s\) per core/{gsub(/ /,"",$2); print $2}') thread(s) per core"
        echo "memory          $(awk '/MemTotal/{printf "%.0f GiB\n", $2/1048576}' /proc/meminfo)"
        echo "kernel          $(uname -r)"
        echo "distro          $(. /etc/os-release && echo "$PRETTY_NAME")"
        local mount=$BENCH_AERON_DIR
        while [ ! -d "$mount" ] && [ "$mount" != / ]; do mount=$(dirname "$mount"); done
        echo "aeron dir       $BENCH_AERON_DIR (on $mount, $(df -h "$mount" 2>/dev/null | awk 'NR==2 { print $2 }') total)"
        echo "archive disk    $(df -T "$BENCH_OUT_DIR" 2>/dev/null | awk 'NR==2 { print $2 }')"

        echo
        echo "# toolchain"
        echo "gcc             $(gcc --version 2>/dev/null | head -1)"
        echo "clang           $(clang-20 --version 2>/dev/null | head -1 || clang --version 2>/dev/null | head -1)"
        echo "cmake           $(cmake --version 2>/dev/null | head -1)"
        echo "java            $("$BENCH_JAVA" -version 2>&1 | head -1)"
        echo "preset          $BENCH_PRESET"

        echo
        echo "# the checkouts"
        _rev "$REPO_ROOT" "this build"
        _rev "$BENCHMARKS_DIR" "harness"
        _rev "$AERON_REF_DIR" "reference"
        local dirty_lines
        dirty_lines=$(git -C "$BENCHMARKS_DIR" status --porcelain 2>/dev/null || true)
        echo "harness dirty   $(printf '%s\n' "$dirty_lines" | grep -c . || true) path(s)"
        [ -n "$dirty_lines" ] && printf '%s\n' "$dirty_lines" | sed 's/^/                  /'
        echo "harness aeron   $(grep -E '^aeron *=' "$BENCHMARKS_DIR/gradle/libs.versions.toml" 2>/dev/null)"

        echo
        echo "# binaries, by digest"
        _digest "$BENCH_ARCHIVE_SERVER" "archive (c++)"
        _digest "$BENCH_AERONMD" "aeronmd"
        _digest "$BENCH_JAVA_ARCHIVE_JAR" "aeron-all.jar"
        _digest "$BENCHMARKS_DIR/benchmarks-all/build/libs/benchmarks.jar" "benchmarks.jar"
        # The C++ build's own flags, read out of the CMake cache and the binary.
        local march pgo
        march=$(grep -E '^AXC_MARCH:' "$REPO_ROOT/build/$BENCH_PRESET/CMakeCache.txt" 2>/dev/null | cut -d= -f2)
        pgo=$(grep -E '^AXC_PGO:' "$REPO_ROOT/build/$BENCH_PRESET/CMakeCache.txt" 2>/dev/null | cut -d= -f2)
        echo "c++ march       ${march:-$(grep -oE -- '-march=[^ ]+' "$REPO_ROOT/build/$BENCH_PRESET/compile_commands.json" 2>/dev/null | head -1 | cut -d= -f2 || echo '(unknown)')}"
        echo "c++ pgo         ${pgo:-off}"
        echo "c++ build type  $(grep -E '^CMAKE_BUILD_TYPE:' "$REPO_ROOT/build/$BENCH_PRESET/CMakeCache.txt" 2>/dev/null | cut -d= -f2)"

        echo
        echo "# the settings both sides are given, by digest"
        _digest "$BENCHMARKS_SCRIPTS_DIR/low-latency-driver.properties" "low-latency drv"
        _digest "$BENCHMARKS_SCRIPTS_DIR/low-latency-archive.properties" "low-latency ar"
        for f in "$BENCH_DIR"/lib/conf/*.properties; do
            [ -f "$f" ] && _digest "$f" "$(basename "$f")"
        done

        echo
        echo "# pinning"
        echo "driver cpus     $BENCH_DRIVER_CPU_INDICES (indices into the allowed cpuset)"
        echo "node cpu        $BENCH_NODE_CPU"
        echo "rig cpu         $BENCH_RIG_CPU"
    } >"$out"

    [ "$out" != /dev/stdout ] && echo "fingerprint written to $out"
}

_rev() {
    local dir=$1 label=$2 rev branch
    rev=$(git -C "$dir" rev-parse HEAD 2>/dev/null) || { echo "$label  (no git checkout at $dir)"; return; }
    branch=$(git -C "$dir" rev-parse --abbrev-ref HEAD 2>/dev/null)
    printf '%-15s %s (%s)\n' "$label" "$rev" "$branch"
}

_digest() {
    local path=$1 label=$2
    printf '%-15s %s\n' "$label" "$(sha256sum "$path" 2>/dev/null | cut -d' ' -f1 || echo '(missing)')"
}
