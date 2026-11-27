#!/usr/bin/env bash
# Nightly fuzz run: build the `fuzz` preset and run every libFuzzer target.
#
#   scripts/run-fuzz.sh                 # smoke: 15 s per target
#   scripts/run-fuzz.sh --duration 600  # nightly: 600 s per target
#
# Crash artifacts land in build/fuzz/crashes/<target>-<sha1>; replay one with
#   build/fuzz/test/fuzz/<target> <artifact>
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"

duration=15
while [ $# -gt 0 ]; do
    case "$1" in
        --smoke)
            duration=15
            ;;
        --duration)
            shift
            if [ $# -eq 0 ]; then
                echo "run-fuzz: --duration needs a number of seconds" >&2
                exit 2
            fi
            duration=$1
            ;;
        -h|--help)
            sed -n '2,9p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "run-fuzz: unknown argument '$1'" >&2
            exit 2
            ;;
    esac
    shift
done

if ! command -v clang++-20 >/dev/null 2>&1; then
    echo "run-fuzz: clang++-20 not found; libFuzzer needs clang, skipping"
    exit 0
fi

targets=(
    archive_control_fuzz
    cluster_ingress_fuzz
    recording_log_fuzz
    archive_catalog_fuzz
    dpdk_frame_fuzz
)

# Only the fuzz targets: the tests and the interop binaries are not part of a
# fuzz run, and under ASan/UBSan building them costs more than the search.
cmake --preset fuzz
cmake --build build/fuzz --parallel "$(nproc)" --target "${targets[@]}"

mkdir -p build/fuzz/crashes
shopt -s nullglob

total=$(du -sb build/fuzz/corpus 2>/dev/null | cut -f1 || true)
echo "corpus: total before ${total:-0} bytes"

copy_seeds()
{
    local target=$1
    local corpus="build/fuzz/corpus/$target"
    mkdir -p "$corpus"

    case "$target" in
        recording_log_fuzz)
            local path
            for path in test/golden/cluster/recording-log/*/recording.log; do
                cp "$path" "$corpus/recording-log-$(basename "$(dirname "$path")")"
            done
            for path in test/golden/cluster/node-state/*/node-state.dat; do
                cp "$path" "$corpus/node-state-$(basename "$(dirname "$path")")"
            done
            for path in test/golden/cluster/mark-file/*/cluster-mark.dat; do
                cp "$path" "$corpus/cluster-mark-$(basename "$(dirname "$path")")"
            done
            ;;
        archive_catalog_fuzz)
            local path
            for path in test/golden/archive/catalog/archive.catalog; do
                cp "$path" "$corpus/archive.catalog"
            done
            for path in test/golden/archive/mark-file/archive-mark.dat; do
                cp "$path" "$corpus/archive-mark.dat"
            done
            ;;
        dpdk_frame_fuzz)
            local path
            for path in test/golden/dpdk/frame/*.bin; do
                cp "$path" "$corpus/"
            done
            ;;
        *)
            # The two protocol targets synthesise their seeds.
            ;;
    esac
}

for target in "${targets[@]}"; do
    corpus="build/fuzz/corpus/$target"
    copy_seeds "$target"
    binary="build/fuzz/test/fuzz/$target"

    # The fuzzer's LLVMFuzzerInitialize writes the target's structurally valid
    # seeds when this variable is set, then exits before mutation starts.
    AXC_FUZZ_EMIT_SEEDS="$corpus" "$binary" -runs=0 "$corpus" >/dev/null 2>&1

    echo "=== ${target}: ${duration}s ==="
    if ! "$binary" \
        -dict=test/fuzz/sbe.dict \
        -max_total_time="$duration" \
        -artifact_prefix="build/fuzz/crashes/$target-" \
        -print_final_stats=1 \
        "$corpus"; then
        echo "run-fuzz: ${target} failed; artifacts in build/fuzz/crashes/${target}-*" >&2
        exit 1
    fi
    size=$(du -sb "$corpus" 2>/dev/null | cut -f1 || true)
    echo "corpus: ${target} ${size:-0} bytes"
done

echo "run-fuzz: all targets clean"
