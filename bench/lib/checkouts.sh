#!/usr/bin/env bash
#
# Where the pieces of a C++-arm batch live, and what to say when one is missing.
#
# The instrument is the reference's own `aeron-io/benchmarks` checkout (a
# sibling directory by default), and the two archives under test are external
# processes: the reference's `io.aeron.archive.Archive` and this build's
# `deepmsg-archive-server`. Sourced, never executed.
#
#   BENCHMARKS_DIR    the harness checkout (default: ../benchmarks). It must have
#                     been built once (`./gradlew :benchmarks-all:shadowJar`) and
#                     carry this tree's `external-archive.patch`.
#   AERON_REF_DIR     the pinned reference (default: third_party/aeron); only read
#                     for the fingerprint.
#   BENCH_PRESET      which CMake preset's binaries are measured (default: release)
#   BENCH_OUT_DIR     where results land (default: bench/out)
#   BENCH_ARCHIVE_SERVER / BENCH_AERONMD / BENCH_JAVA_ARCHIVE_JAR
#                     the three programs a cell starts, each overridable.

if [ -z "${BASH_VERSION:-}" ]; then
    echo "checkouts.sh needs bash" >&2
    exit 2
fi

BENCH_LIB="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(dirname "$BENCH_LIB")"
REPO_ROOT="$(dirname "$BENCH_DIR")"

BENCHMARKS_DIR="${BENCHMARKS_DIR:-$(dirname "$REPO_ROOT")/benchmarks}"
AERON_REF_DIR="${AERON_REF_DIR:-$REPO_ROOT/third_party/aeron}"
BENCH_OUT_DIR="${BENCH_OUT_DIR:-$BENCH_DIR/out}"
BENCH_PRESET="${BENCH_PRESET:-release}"

# The reference's own launchers: a Java driver, a live-replay client and the
# archive node they are measured around. `run-java` underneath them puts the
# harness jar on the classpath and honours `JVM_OPTS`.
BENCHMARKS_SCRIPTS_DIR="$BENCHMARKS_DIR/scripts/aeron"
BENCHMARKS_DRIVER_JAVA="$BENCHMARKS_SCRIPTS_DIR/media-driver"
BENCHMARKS_ECHO_CLIENT="$BENCHMARKS_SCRIPTS_DIR/live-replay-client"
BENCHMARKS_ARCHIVE_NODE="$BENCHMARKS_SCRIPTS_DIR/archive-node"
# S1's two ends: the echo node answers, the live-recording client records what it
# sends and paces itself on the archive's recording events.
BENCHMARKS_ECHO_NODE="$BENCHMARKS_SCRIPTS_DIR/echo-server"
BENCHMARKS_LIVE_RECORDING_CLIENT="$BENCHMARKS_SCRIPTS_DIR/live-recording-client"

# This build's half: one archive binary (it is the reference's `Archive.main`
# shape, A5a) and the reference C driver it runs beside.
BENCH_ARCHIVE_SERVER="${BENCH_ARCHIVE_SERVER:-$REPO_ROOT/build/$BENCH_PRESET/apps/deepmsg-archive-server/deepmsg-archive-server}"
BENCH_AERONMD="${BENCH_AERONMD:-$REPO_ROOT/build/$BENCH_PRESET/aeron/binaries/aeronmd}"
BENCH_JAVA_ARCHIVE_JAR="${BENCH_JAVA_ARCHIVE_JAR:-$AERON_REF_DIR/aeron-all/build/libs/aeron-all-1.54.0.jar}"

BENCH_AERON_DIR="${BENCH_AERON_DIR:-/dev/shm/aeron}"
BENCH_JAVA="${BENCH_JAVA:-/usr/bin/java}"

# The cores. The box these defaults were written on is an i7-10875H: **8
# physical cores with SMT**, so the even vCPUs are one per physical core
# (`lscpu -e`'s CORE column pairs 0/1, 2/3, ...). The driver's own three are
# indices into the allowed cpuset, so they are 2/4/6 (physical cores 1/2/3); the
# node and the rig are absolute vCPUs 8 and 10 (physical cores 4 and 5). See
# `analysis/cpp/plan-a6a-bench-grid.md` §1.
BENCH_DRIVER_CPU_INDICES="2,4,6"
BENCH_NODE_CPU="${BENCH_NODE_CPU:-8}"
BENCH_RIG_CPU="${BENCH_RIG_CPU:-10}"

need_dir() {
    local what=$1 path=$2
    if [ ! -d "$path" ]; then
        echo "no $what at $path" >&2
        echo "  set the environment variable named beside it in bench/lib/checkouts.sh" >&2
        return 1
    fi
}

need_file() {
    local what=$1 path=$2 hint=${3:-}
    if [ ! -x "$path" ] && [ ! -f "$path" ]; then
        echo "no $what at $path" >&2
        [ -n "$hint" ] && echo "  $hint" >&2
        return 1
    fi
}

# The harness's shadow jar: one jar carries the Java driver, the node and the
# client, so a checkout without it can produce no cell at all.
need_harness_built() {
    local jar="$BENCHMARKS_DIR/benchmarks-all/build/libs/benchmarks.jar"
    if [ -f "$jar" ]; then
        return 0
    fi
    echo "the harness has not been built: no $jar" >&2
    echo "  cd \"$BENCHMARKS_DIR\" && ./gradlew :benchmarks-all:shadowJar" >&2
    return 1
}

bench_checkouts_report() {
    cat <<EOF
benchmark harness   $BENCHMARKS_DIR
reference checkout  $AERON_REF_DIR
this build          $REPO_ROOT (preset $BENCH_PRESET)
results under       $BENCH_OUT_DIR
EOF
}
