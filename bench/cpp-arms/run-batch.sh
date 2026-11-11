#!/usr/bin/env bash
#
# One C++-arm batch: the reference's own archive benchmark harness, with the
# archive swapped.
#
#   run-batch.sh [--scenario s2] [--arms j,j-c,x-c,x-j] [--fsync 0,2]
#                [--fsync-catalog 2] [--rate 25K] [--length 32] [--runs 1]
#                [--iterations 60] [--streams N] [--recorder-threads N]
#                [--out DIR] [--list] [--quick] [--no-read]
#
# `--fsync` sets the *file* level of each cell and, unless `--fsync-catalog`
# overrides it, the catalog level too (the archive refuses catalog < file).
# The cell name keeps `fsync=<file level>`; RUN-INFO.txt carries the pair.
# `DEEPMSG_IO_FALLOCATE=true` travels to the C++ archive as
# `deepmsg.archive.file.io.fallocate` (C++ only, docs/compat.md).
#
# `--streams N` (A6f) starts N (node, client) pairs against one archive and one
# driver; the per-stream rate is unchanged, so the aggregate is N × rate.
# `--recorder-threads N` is the archive-side extension
# (`deepmsg.archive.recorder.threads`) and is written to `archive.properties`,
# so only the `x-c`/`x-j` arms act on it.
#
# The four arms (perf spec §6.1):
#
#   j     Java driver + the reference's archive, its own node/client      (baseline)
#   j-c   C aeronmd    + the reference's archive                        (same server, C driver)
#   x-c   C aeronmd    + this build's archive                           (the ratio that says it)
#   x-j   Java driver  + this build's archive                           (interaction check)
#
# The archive is always an **external process**: for `j` the reference's node
# launches it, for the other three the batch starts it and the node is patched
# (`io.aeron.benchmarks.aeron.archive.external=true`, see patches/) to connect to
# it. One machine only: WSL2's virtual path is milliseconds per round trip
# (`analysis/bench/handoff-to-wsl2.md` §0).
#
# `DEEPMSG_REPLAY_IO_BACKEND=pread|mmap` and `DEEPMSG_REPLAY_IO_FADVISE=none|willneed`
# travel to the C++ archive as `deepmsg.archive.replay.io.*` (C++ only,
# docs/compat.md); each cell also writes `rss.txt` from the archive's
# `smaps_rollup` (`Rss`/`Pss`/`Pss_File`/`Pss_Anon`/clean+dirty pages) so the
# mmap backend's page footprint is visible.
#
# The tree a batch writes is the reference harness's, plus RUN-INFO.txt per cell
# and fingerprint.txt per batch (perf spec §6.5):
#
#   <out>/fingerprint.txt
#   <out>/round-<name>/<scenario>_<arm>_fsync=<0|1|2>_length=<L>_rate=<R>/
#       run-<N>/<*.hdr> client.log RUN-INFO.txt
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BENCH_DIR=$(dirname "$here")
# shellcheck source=../lib/checkouts.sh
. "$BENCH_DIR/lib/checkouts.sh"
# shellcheck source=../lib/fingerprint.sh
. "$BENCH_DIR/lib/fingerprint.sh"
# shellcheck source=../lib/cell.sh
. "$BENCH_DIR/lib/cell.sh"

scenario=s2
arms=j,j-c,x-c,x-j
fsync_levels=0,2
catalog_fsync_levels=""
rate=25K
length=32
runs=1
iterations=60
streams=1
recorder_threads=1
warmup_iterations=30
rounds=1
quick=0
out=""
no_read=0
destination_port=13333
source_port=13334
control_port=13200

while [ $# -gt 0 ]; do
    case "$1" in
        --scenario) scenario=$2; shift 2 ;;
        --arms) arms=$2; shift 2 ;;
        --fsync) fsync_levels=$2; shift 2 ;;
        --fsync-catalog) catalog_fsync_levels=$2; shift 2 ;;
        --rate) rate=$2; shift 2 ;;
        --length) length=$2; shift 2 ;;
        --streams) streams=$2; shift 2 ;;
        --recorder-threads) recorder_threads=$2; shift 2 ;;
        --runs) runs=$2; shift 2 ;;
        --iterations) iterations=$2; shift 2 ;;
        --rounds) rounds=$2; shift 2 ;;
        --out) out=$2; shift 2 ;;
        --no-read) no_read=1; shift ;;
        --list) list=1; shift ;;
        --quick)
            quick=1
            scenario=s1
            arms=x-c
            fsync_levels=0
            rate=25K
            length=32
            runs=1; iterations=5; warmup_iterations=2
            shift ;;
        -h|--help) sed -n '3,35p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) echo "unknown option $1" >&2; exit 2 ;;
    esac
done

out=${out:-$BENCH_OUT_DIR/cpp-arms/$(date +%Y-%m-%d)-$(hostname -s)}
IFS=',' read -ra arm_list <<<"$arms"
IFS=',' read -ra fsync_list <<<"$fsync_levels"
if [ -n "$catalog_fsync_levels" ]; then
    for file_level in "${fsync_list[@]}"; do
        if [ "$catalog_fsync_levels" -lt "$file_level" ]; then
            echo "catalog sync level $catalog_fsync_levels < file level $file_level (the archive refuses it)" >&2
            exit 2
        fi
    done
fi

replay_io_backend=${DEEPMSG_REPLAY_IO_BACKEND:-pread}
replay_io_fadvise=${DEEPMSG_REPLAY_IO_FADVISE:-none}
case "$replay_io_backend" in
    pread|mmap) ;;
    *) echo "DEEPMSG_REPLAY_IO_BACKEND must be pread or mmap, not $replay_io_backend" >&2; exit 2 ;;
esac
case "$replay_io_fadvise" in
    none|willneed) ;;
    *) echo "DEEPMSG_REPLAY_IO_FADVISE must be none or willneed, not $replay_io_fadvise" >&2; exit 2 ;;
esac

if [ "$scenario" != s1 ] && [ "$scenario" != s2 ]; then
    echo "unknown scenario $scenario (s1, s2)" >&2
    exit 2
fi

if ! [[ "$streams" =~ ^[0-9]+$ ]] || [ "$streams" -lt 1 ] || [ "$streams" -gt 8 ]; then
    echo "--streams must be 1..8 (got $streams)" >&2
    exit 2
fi
if ! [[ "$recorder_threads" =~ ^[0-9]+$ ]] || [ "$recorder_threads" -lt 1 ] || [ "$recorder_threads" -gt 32 ]; then
    echo "--recorder-threads must be 1..32 (got $recorder_threads)" >&2
    exit 2
fi

# The cell directory carries the A6f axes only when they are not the reference
# shape, so the A6a tree and `read.py`'s parser stay byte-identical.
_cell_suffix() {
    local suffix=""
    [ "$streams" -ne 1 ] && suffix="${suffix}_streams=${streams}"
    [ "$recorder_threads" -ne 1 ] && suffix="${suffix}_threads=${recorder_threads}"
    echo "$suffix"
}

if [ -n "${list:-}" ]; then
    for round in $(seq 1 "$rounds"); do
        for arm in "${arm_list[@]}"; do
            for fsync in "${fsync_list[@]}"; do
                echo "round-$round ${scenario}_${arm}_fsync=${fsync}_length=${length}_rate=${rate}$(_cell_suffix)"
            done
        done
    done
    exit 0
fi

need_dir "harness checkout" "$BENCHMARKS_DIR" || exit 2
need_harness_built || exit 2
need_file "archive (c++)" "$BENCH_ARCHIVE_SERVER" "build it with: cmake --build build/$BENCH_PRESET" || exit 2
need_file "aeronmd" "$BENCH_AERONMD" "build it with: cmake --build build/$BENCH_PRESET --target aeronmd" || exit 2
need_file "aeron-all jar" "$BENCH_JAVA_ARCHIVE_JAR" "build the reference jar under third_party/aeron" || exit 2

mkdir -p "$out"
bench_checkouts_report | tee "$out/checkouts.txt"
bench_fingerprint "$out/fingerprint.txt"

# --- the cell's settings -----------------------------------------------------

_port_base() {
    echo $((20000 + ($$ % 200) * 8))
}

_write_cell_files() {
    local dir=$1 fsync=$2 arm=$3 catalog_fsync=$4
    local base
    base=$(_port_base)
    # Streaming ports (A6f): stream i uses base+10+2i / base+11+2i; the control
    # port and the archive are shared by every stream.
    cell_port_base=$base
    control_port=$((base + 1))
    destination_port=$((base + 10))
    source_port=$((base + 11))

    cat >"$dir/archive.properties" <<EOF
aeron.archive.dir=$archive_dir
aeron.archive.mark.file.dir=$BENCH_AERON_DIR
aeron.archive.control.channel=aeron:udp?endpoint=localhost:$control_port
aeron.archive.control.response.channel=aeron:udp?endpoint=localhost:0
aeron.archive.replication.channel=aeron:udp?endpoint=localhost:0
aeron.archive.file.sync.level=$fsync
aeron.archive.catalog.file.sync.level=$catalog_fsync
aeron.archive.recording.events.enabled=$([ "$scenario" = s1 ] && echo true || echo false)
aeron.archive.threading.mode=DEDICATED
aeron.archive.idle.strategy=spin
aeron.archive.recorder.idle.strategy=noop
aeron.archive.replayer.idle.strategy=noop
aeron.archive.conductor.cycle.threshold=1ms
aeron.archive.recorder.cycle.threshold=1ms
aeron.archive.replayer.cycle.threshold=1ms
deepmsg.archive.recorder.threads=$recorder_threads
EOF
    # The reference refuses `recording.events.enabled=true` with no channel
    # (`Archive.java:1240-1245`), and its own `live-recording-client` names
    # `aeron:ipc`; the archive side must name the same channel or the two ends
    # subscribe to nothing.
    if [ "$scenario" = s1 ]; then
        echo 'aeron.archive.recording.events.channel=aeron:ipc' >>"$dir/archive.properties"
    fi

    # C++-only switches (docs/compat.md); the Java arms never read them.
    if [ "$arm" = x-c ] || [ "$arm" = x-j ]; then
        echo "deepmsg.archive.file.io.fallocate=${DEEPMSG_IO_FALLOCATE:-false}" >>"$dir/archive.properties"
        echo "deepmsg.archive.replay.io.backend=$replay_io_backend" >>"$dir/archive.properties"
        echo "deepmsg.archive.replay.io.fadvise=$replay_io_fadvise" >>"$dir/archive.properties"
    fi

    cat >"$dir/driver-override.properties" <<EOF
aeron.dir=$BENCH_AERON_DIR
EOF
}

_archive_defines() {
    local file line
    # The archive is a client of the driver like every other process, and the C
    # client's default directory is `/dev/shm/aeron-<user>` — *not* the driver's
    # `/dev/shm/aeron`. Without this line the archive starts its own driver in
    # that default directory (embedded shape) or waits ten seconds for one, and
    # then none of the node's publications ever meets a subscription. Measured
    # on the first `--quick` cell: the node sat in `ArchiveNode.run:110` for the
    # full 60 s connection timeout.
    printf -- '-Daeron.dir=%s\n' "$BENCH_AERON_DIR"
    file="$BENCH_DIR/lib/conf/archive.properties"
    while IFS= read -r line || [ -n "$line" ]; do
        line=${line%$'\r'}
        [ -z "${line// }" ] && continue
        case "$line" in \#*) continue ;; esac
        printf -- '-D%s\n' "$line"
    done <"$file"
    local override
    override="$cell_dir/archive.properties"
    while IFS= read -r line || [ -n "$line" ]; do
        line=${line%$'\r'}
        [ -z "${line// }" ] && continue
        case "$line" in \#*) continue ;; esac
        printf -- '-D%s\n' "$line"
    done <"$override"
}

# --- starting the three pieces ----------------------------------------------

_start_driver() {
    local arm=$1 dir=$2
    case "$arm" in
        j|x-j)
            "$BENCHMARKS_DRIVER_JAVA" "$dir/driver-override.properties" >"$dir/driver.log" 2>&1 &
            ;;
        j-c|x-c)
            ( "$BENCH_AERONMD" \
                "$BENCHMARKS_SCRIPTS_DIR/low-latency-driver.properties" \
                "$BENCH_DIR/lib/conf/benchmark.properties" \
                "$dir/driver-override.properties" >"$dir/driver.log" 2>&1 ) &
            ;;
    esac
    _driver_pid=$!
    disown "$_driver_pid" 2>/dev/null || true
    await_cnc || { echo "  the driver never published its CnC file" >&2; return 1; }
}

_start_archive() {
    local arm=$1 dir=$2
    case "$arm" in
        j)
            _archive_pid=""
            return 0
            ;;
        j-c)
            local -a opts=(-Daeron.dir="$BENCH_AERON_DIR")
            JVM_OPTS="${opts[*]}" \
                "$BENCHMARKS_DIR/scripts/run-java" io.aeron.archive.Archive \
                "$BENCHMARKS_SCRIPTS_DIR/low-latency-archive.properties" \
                "$dir/archive.properties" >"$dir/archive.log" 2>&1 &
            ;;
        x-c|x-j)
            local -a defines=()
            while IFS= read -r line; do defines+=("$line"); done < <(_archive_defines)
            "$BENCH_ARCHIVE_SERVER" "${defines[@]}" io.aeron.archive.Archive >"$dir/archive.log" 2>&1 &
            ;;
    esac
    _archive_pid=$!
    disown "$_archive_pid" 2>/dev/null || true
    for _ in $(seq 1 200); do
        [ -f "$BENCH_AERON_DIR/archive-mark.dat" ] && { sleep 0.2; return 0; }
        sleep 0.1
    done
    echo "  the archive never wrote its mark file" >&2
    return 1
}

_start_node() {
    local arm=$1 dir=$2
    local external=""
    case "$arm" in
        j-c|x-c|x-j) external="-Dio.aeron.benchmarks.aeron.archive.external=true" ;;
    esac
    # S2's far end archives and replays; S1's just echoes, and the client owns
    # the recording. Same two channels, same two properties, different class.
    local node=$BENCHMARKS_ARCHIVE_NODE
    [ "$scenario" = s1 ] && node=$BENCHMARKS_ECHO_NODE
    # A6f: one far end per stream. The archive and the driver are shared, so the
    # control port stays the cell's; only the measured channels move.
    _node_pids=()
    local stream
    for ((stream = 0; stream < streams; stream++)); do
        local destination=$((cell_port_base + 10 + 2 * stream))
        local source=$((cell_port_base + 11 + 2 * stream))
        local stream_dir="$dir"
        local log="$dir/node.log"
        if [ "$streams" -gt 1 ]; then
            stream_dir="$dir/stream-$stream"
            log="$dir/node-$stream.log"
            mkdir -p "$stream_dir"
        fi
        # One stream keeps the A6a core map; several streams would fight over the
        # one node core, so multi-stream cells record "unpinned" instead (§Q7).
        local -a pin=()
        if [ "$streams" -eq 1 ]; then
            pin=(taskset -c "$BENCH_NODE_CPU")
        fi
        JVM_OPTS="-Daeron.dir=$BENCH_AERON_DIR \
-Dio.aeron.benchmarks.aeron.destination.channel=aeron:udp?endpoint=localhost:$destination|mtu=1408 \
-Dio.aeron.benchmarks.aeron.source.channel=aeron:udp?endpoint=localhost:$source|mtu=1408 \
-Dio.aeron.benchmarks.aeron.record.channel=aeron:ipc \
-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:$control_port \
-Daeron.archive.control.response.channel=aeron:udp?endpoint=localhost:0 \
-Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0 \
-Daeron.archive.dir=$archive_dir \
-Daeron.archive.file.sync.level=$_fsync \
-Daeron.archive.catalog.file.sync.level=$_catalog_fsync \
-Dio.aeron.benchmarks.output.directory=$stream_dir \
$external" \
            "${pin[@]}" "$node" >"$log" 2>&1 &
        _node_pids+=($!)
        disown "${_node_pids[-1]}" 2>/dev/null || true
    done
    sleep 0.5
}

_run_client() {
    local arm=$1 dir=$2 run=$3
    # S1's client is the one that owns the recording (and, for arm `j`, the
    # archive); S2's speaks the live-replay protocol to the node.
    local client=$BENCHMARKS_ECHO_CLIENT
    [ "$scenario" = s1 ] && client=$BENCHMARKS_LIVE_RECORDING_CLIENT
    # The client is the other end of the two channels the cell measures, so it
    # has to be told what they are: `LiveReplayMessageTransceiver` publishes to
    # `destinationChannel()` and subscribes to `sourceChannel()`, and without
    # these three lines it falls back to the harness defaults (13333/13334) while
    # the node listens on the cell's ports. Measured on the second `--quick`
    # cell: the node's IPC publication was linked (32 MiB window) but its
    # destination subscription never saw a publication, and both ends sat in
    # `awaitConnected` for the full 60 s.
    #
    # In S1 the same patch switch tells the client whether its recording archive
    # is one it launches (`j`) or an external process, and the archive directory
    # is the one the launch-or-connect decision reads.
    local external=""
    case "$arm" in
        j-c|x-c|x-j) external="-Dio.aeron.benchmarks.aeron.archive.external=true" ;;
    esac
    # A6f: N clients start together and are waited for together; that is what
    # makes the cell a multi-stream aggregate rather than N sequential cells.
    local -a client_pids=()
    local stream
    for ((stream = 0; stream < streams; stream++)); do
        local destination=$((cell_port_base + 10 + 2 * stream))
        local source=$((cell_port_base + 11 + 2 * stream))
        local stream_dir="$dir/run-$run"
        if [ "$streams" -gt 1 ]; then
            stream_dir="$dir/run-$run/stream-$stream"
            mkdir -p "$stream_dir"
        fi
        local -a pin=()
        if [ "$streams" -eq 1 ]; then
            pin=(taskset -c "$BENCH_RIG_CPU")
        fi
        JVM_OPTS="-Daeron.dir=$BENCH_AERON_DIR \
-Dio.aeron.benchmarks.aeron.embedded.media.driver=false \
-Dio.aeron.benchmarks.aeron.destination.channel=aeron:udp?endpoint=localhost:$destination|mtu=1408 \
-Dio.aeron.benchmarks.aeron.source.channel=aeron:udp?endpoint=localhost:$source|mtu=1408 \
-Dio.aeron.benchmarks.aeron.record.channel=aeron:ipc \
-Daeron.archive.dir=$archive_dir \
-Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0 \
$external \
-Dio.aeron.benchmarks.output.directory=$stream_dir \
-Dio.aeron.benchmarks.output.file=$scenario \
-Dio.aeron.benchmarks.message.rate=$rate \
-Dio.aeron.benchmarks.warmup.message.rate=${rate} \
-Dio.aeron.benchmarks.iterations=$iterations \
-Dio.aeron.benchmarks.warmup.iterations=$warmup_iterations \
-Dio.aeron.benchmarks.message.length=$length \
-Dio.aeron.benchmarks.batch.size=1 \
-Dio.aeron.benchmarks.aeron.use.try.claim=true \
-Dio.aeron.benchmarks.output.time.unit=MICROSECONDS \
-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:$control_port \
-Daeron.archive.control.response.channel=aeron:udp?endpoint=localhost:0" \
            "${pin[@]}" "$client" >"$stream_dir/client.log" 2>&1 &
        client_pids+=($!)
    done

    local ok=0
    local pid
    for pid in "${client_pids[@]}"; do
        wait "$pid" || ok=1
    done
    return $ok
}

# A6f: the per-stream form of `lib/cell.sh`'s `cell_validate` (that one is
# pinned to `<cell>/run-<n>`, and a multi-stream run has one directory per
# stream). The rules are the same: no `WARNING:`, no `.FAIL`, exactly one
# histogram, and at least the samples the rate and iterations asked for.
_validate_stream_dir() {
    local dir=$1 rate=$2 iterations=$3
    local log="$dir/client.log"
    local expected=$(( $(_rate_as_number "$rate") * iterations ))

    if [ ! -f "$log" ]; then
        echo "no client log at $log" >&2
        return 1
    fi
    if grep -q 'WARNING:' "$log"; then
        echo "the client printed WARNING: $(grep -m1 'WARNING:' "$log")" >&2
        return 1
    fi

    local hdrs failed
    hdrs=$(find "$dir" -maxdepth 1 -name '*.hdr' 2>/dev/null | wc -l)
    failed=$(find "$dir" -maxdepth 1 -name '*.FAIL' 2>/dev/null | wc -l)
    if [ "$failed" -gt 0 ]; then
        echo "$failed histogram(s) were written with the .FAIL suffix" >&2
        return 1
    fi
    if [ "$hdrs" -ne 1 ]; then
        echo "expected one histogram in $dir, found $hdrs" >&2
        return 1
    fi

    local hdr count
    hdr=$(find "$dir" -maxdepth 1 -name '*.hdr')
    count=$("$BENCH_DIR/tools/quantiles.sh" "$hdr" 2>/dev/null | awk 'NR == 2 { print $2 }')
    if [ -z "$count" ]; then
        echo "no count could be read out of $(basename "$hdr")" >&2
        return 1
    fi
    if [ "$count" -lt "$expected" ]; then
        echo "$(basename "$hdr") holds $count samples, fewer than the $expected asked for" >&2
        return 1
    fi
    return 0
}

_validate_run() {
    local cell=$1 run=$2 rate=$3 iterations=$4
    local ok=0
    local stream
    for ((stream = 0; stream < streams; stream++)); do
        local stream_dir="$cell/run-$run"
        if [ "$streams" -gt 1 ]; then
            stream_dir="$cell/run-$run/stream-$stream"
        fi
        if ! _validate_stream_dir "$stream_dir" "$rate" "$iterations"; then
            echo "  stream $stream did not count" >&2
            ok=1
        fi
    done
    return $ok
}

_write_run_info() {
    local dir=$1 arm=$2
    {
        echo "arm             $arm"
        echo "scenario        $scenario"
        echo "driver          $([ "$arm" = j ] || [ "$arm" = x-j ] && echo 'java MediaDriver' || echo "aeronmd ($BENCH_AERONMD)")"
        echo "archive         $([ "$arm" = j ] || [ "$arm" = j-c ] && echo "java io.aeron.archive.Archive ($BENCH_JAVA_ARCHIVE_JAR)" || echo "deepmsg-archive-server ($BENCH_ARCHIVE_SERVER)")"
        echo "control         aeron:udp?endpoint=localhost:$control_port"
        if [ "$streams" -eq 1 ]; then
            echo "destination     aeron:udp?endpoint=localhost:$destination_port|mtu=1408"
            echo "source          aeron:udp?endpoint=localhost:$source_port|mtu=1408"
        else
            local stream
            for ((stream = 0; stream < streams; stream++)); do
                echo "stream $stream       destination=aeron:udp?endpoint=localhost:$((cell_port_base + 10 + 2 * stream))|mtu=1408 source=aeron:udp?endpoint=localhost:$((cell_port_base + 11 + 2 * stream))|mtu=1408"
            done
        fi
        echo "streams         $streams (per-stream rate $rate; aggregate $streams x $rate)"
        echo "recorder        $recorder_threads thread(s) (deepmsg.archive.recorder.threads)"
        echo "record          aeron:ipc"
        echo "fsync           file=$_fsync catalog=$_catalog_fsync"
        if [ "$arm" = x-c ] || [ "$arm" = x-j ]; then
            echo "fallocate       ${DEEPMSG_IO_FALLOCATE:-false}"
            echo "replay io       backend=$replay_io_backend fadvise=$replay_io_fadvise"
        fi
        echo "archive idle    conductor=spin recorder=noop replayer=noop, threading=DEDICATED"
        echo "events          $([ "$scenario" = s1 ] && echo 'true (s1, aeron:ipc)' || echo 'false (s2)')"
        if [ "$streams" -eq 1 ]; then
            echo "cores           driver indices $BENCH_DRIVER_CPU_INDICES, node $BENCH_NODE_CPU, rig $BENCH_RIG_CPU"
        else
            echo "cores           driver indices $BENCH_DRIVER_CPU_INDICES; node and rig unpinned (multi-stream)"
        fi
        echo "rate/length     $rate msg/s, $length bytes, $iterations iterations ($warmup_iterations warmup)"
        for f in "$dir/archive.properties" "$dir/driver-override.properties"; do
            echo "settings        $(sha256sum "$f" | cut -d' ' -f1) $(basename "$f")"
        done
    } >"$dir/RUN-INFO.txt"
}

# The two files `read.py` explains a cell with (perf spec §6.5): the archive's
# labelled counters at the end of the run, and the bytes in the archive
# directory. Both are best-effort: a cell without them is still a cell, and a
# failed run should leave what it had rather than turn a diagnostic into a
# second failure. The counters are cumulative over the archive's lifetime, so a
# multi-run cell's later files mean "since the archive started"; the grid runs
# one run per cell.
_report_run() {
    local dir=$1 run=$2
    "$BENCH_DIR/tools/counters.sh" "$BENCH_AERON_DIR/cnc.dat" >"$dir/run-$run/counters.txt" 2>/dev/null || true
    du -sb "$archive_dir" 2>/dev/null | cut -f1 >"$dir/run-$run/archive-du.txt" || true
}

# A6c: the segment's allocation picture (E-D). `st_blocks` separates a sparse
# `setLength` file from a preallocated one; `filefrag` adds the extent count.
# Best effort: a missing tool or no segment is not a cell failure.
_report_filefrag() {
    local dir=$1
    command -v filefrag >/dev/null 2>&1 || return 0
    local segment
    segment=$(find "$archive_dir" -maxdepth 1 -name '*.rec' -print -quit 2>/dev/null)
    [ -n "$segment" ] || return 0
    {
        echo "file            $(basename "$segment")"
        echo "size            $(stat -c %s "$segment")"
        echo "blocks          $(stat -c %b "$segment")"
        echo "extents         $(filefrag "$segment" 2>/dev/null | sed -n 's/.*: \([0-9]*\) extent.*/\1/p')"
        filefrag -v "$segment" 2>&1 | sed -n '1,8p'
    } >"$dir/filefrag.txt"
}

# Q5 (`analysis/cpp/plan-a6d-read-backend.md` §2.4): the cell's page-cache
# footprint. The mmap backend counts the mapped segment pages in the archive's
# RSS; the pread backend only holds its per-session copy window. `smaps_rollup`
# has no plain `Mapped` line, so the file-backed share is read from `Pss_File`
# (and `Shared_Clean`/`Private_Clean`), while the pread copy window lands in
# `Pss_Anon`/`Private_Dirty`. One sample per cell, after the runs and while the
# archive is still alive. Arms whose archive is launched by the node (`j`) have
# no pid here and get no file.
_sample_rss() {
    local dir=$1
    [ -n "${_archive_pid:-}" ] || return 0
    [ -r "/proc/$_archive_pid/smaps_rollup" ] || return 0
    grep -E '^(Rss|Pss|Pss_Anon|Pss_File|Shared_Clean|Shared_Dirty|Private_Clean|Private_Dirty):' \
        "/proc/$_archive_pid/smaps_rollup" >"$dir/rss.txt" 2>/dev/null || true
}

_run_cell() {
    local round=$1 arm=$2 fsync=$3
    local catalog_fsync=${catalog_fsync_levels:-$fsync}
    local recorder_threads=${4:-1}
    cell_dir="$out/round-$round/${scenario}_${arm}_fsync=${fsync}_length=${length}_rate=${rate}$(_cell_suffix)"
    archive_dir="$cell_dir/archive"
    # A cell that is re-run must not be read beside its own leftovers: an old
    # `run-1/` histogram or an old `logs/` dump from the failed run would sit in
    # the same directory as the new one (measured on the first two `--quick`
    # attempts, whose tree still held a client log from an earlier run).
    rm -rf "$cell_dir" "$BENCH_AERON_DIR"
    mkdir -p "$archive_dir"

    wait_idle || return 1
    _write_cell_files "$cell_dir" "$fsync" "$arm" "$catalog_fsync"
    _fsync=$fsync
    _catalog_fsync=$catalog_fsync
    _write_run_info "$cell_dir" "$arm"

    _start_driver "$arm" "$cell_dir" || return 1
    _start_archive "$arm" "$cell_dir" || return 1
    _start_node "$arm" "$cell_dir" || return 1

    local ok=1
    for run in $(seq 1 "$runs"); do
        mkdir -p "$cell_dir/run-$run"
        if ! _run_client "$arm" "$cell_dir" "$run"; then
            echo "  run $run: the client failed" >&2
            ok=0
        elif ! _validate_run "$cell_dir" "$run" "$rate" "$iterations"; then
            echo "  run $run: the cell did not count (see $cell_dir/run-$run)" >&2
            ok=0
        fi
        _report_run "$cell_dir" "$run"
    done
    _report_filefrag "$cell_dir"

    _sample_rss "$cell_dir"

    _kill_nodes
    [ -n "${_archive_pid:-}" ] && kill -9 "$_archive_pid" 2>/dev/null || true
    driver_kill

    return $((1 - ok))
}

_kill_nodes() {
    local pid
    for pid in ${_node_pids[@]+"${_node_pids[@]}"}; do
        kill -9 "$pid" 2>/dev/null || true
    done
    _node_pids=()
}

_batch_cleanup() {
    _kill_nodes
    [ -n "${_archive_pid:-}" ] && kill -9 "$_archive_pid" 2>/dev/null || true
    driver_kill
}
trap _batch_cleanup EXIT

echo "cpp-arms batch: scenario=$scenario rounds=$rounds arms=$arms fsync=$fsync_levels"
echo "results: $out"

failures=0
for round in $(seq 1 "$rounds"); do
    # ABBA: odd rounds run the arms and the fsync levels in the order given, even
    # rounds run both reversed. Every cell of the pair is otherwise identical
    # (`lib/README.md`; the performance spec's §6.3 says the same).
    round_arms=("${arm_list[@]}")
    round_fsyncs=("${fsync_list[@]}")
    if [ 0 -eq $((round % 2)) ]; then
        reversed_arms=(); reversed_fsyncs=()
        for (( i=${#arm_list[@]}-1; i>=0; i-- )); do reversed_arms+=("${arm_list[$i]}"); done
        for (( i=${#fsync_list[@]}-1; i>=0; i-- )); do reversed_fsyncs+=("${fsync_list[$i]}"); done
        round_arms=("${reversed_arms[@]}")
        round_fsyncs=("${reversed_fsyncs[@]}")
    fi
    for arm in "${round_arms[@]}"; do
        for fsync in "${round_fsyncs[@]}"; do
            if _run_cell "$round" "$arm" "$fsync"; then
                printf 'round-%s %-4s fsync=%s  OK\n' "$round" "$arm" "$fsync"
            else
                printf 'round-%s %-4s fsync=%s  FAILED\n' "$round" "$arm" "$fsync"
                failures=$((failures + 1))
            fi
        done
    done
done

if [ 0 -ne "$failures" ]; then
    echo "$failures cell(s) failed; the tree is under $out" >&2
    exit 1
fi

if [ 1 -eq "$no_read" ]; then
    echo "readings skipped (--no-read)"
else
    if [ "$streams" -gt 1 ] || [ "$recorder_threads" -ne 1 ]; then
        "$here/read-streams.py" "$out" || true
    else
        "$here/read.py" "$out" || true
    fi
fi
