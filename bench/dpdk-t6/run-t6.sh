#!/usr/bin/env bash
# T6: the reference Java system tests, once on the kernel driver and once on our
# media — which is open and falls back to the kernel for every channel.
#
#   bench/dpdk-t6/run-t6.sh --arm kernel|media|all [--area root|archive|cluster|driver|security|test]
#                           [--classes a,b] [--resume]
#   bench/dpdk-t6/run-t6.sh --part a          # debug vs dpdk ctest, per test
#
# Two arms, one variable: the kernel arm is the reference `aeronmd` untouched
# and the media arm is our `deepmsg-media-driver` with the media on, both with
# the threading mode the harness asks for (the media driver serves all four).
#
# The media arm runs on a `net_pcap` vdev, not on the bench card: many of the
# reference classes start two or more drivers in one test, and one vfio port
# cannot serve two processes. The media needs no root here, no huge pages and no
# machine state; each driver writes its own pcap, and every pcap staying at the
# 24-byte pcap header is the reading that the media carried nothing.
#
# Environment:
#   AXC_BUILD_DIR        the DPDK build (default build/dpdk-release)
#   AXC_T6_KERNEL_DRIVER reference aeronmd (default build/debug/aeron/binaries/aeronmd)
#   AXC_T6_DRIVER        our media driver (default $AXC_BUILD_DIR/apps/deepmsg-media-driver/...)
#   AXC_T6_WORK          work directory (default /tmp/axc-t6-<timestamp>)
#   AXC_T6_CLASS_TIMEOUT seconds per class (default 300)
#   AXC_T6_GRADLE_ARGS   extra gradle arguments (default `--offline`)
set -euo pipefail

t6_repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
t6_aeron=${AXC_T6_AERON:-$t6_repo/third_party/aeron}
t6_build=${AXC_BUILD_DIR:-$t6_repo/build/dpdk-release}
t6_media=${AXC_T6_DRIVER:-$t6_build/apps/deepmsg-media-driver/deepmsg-media-driver}
t6_kernel=${AXC_T6_KERNEL_DRIVER:-$t6_repo/build/debug/aeron/binaries/aeronmd}
t6_wrapper=${AXC_T6_WRAPPER:-$t6_repo/bench/dpdk-t6/aeronmd-wrapper.sh}
t6_compare=$t6_repo/bench/dpdk-t6/compare.py
t6_class_timeout=${AXC_T6_CLASS_TIMEOUT:-300}
t6_gradle_args=${AXC_T6_GRADLE_ARGS:---offline}
t6_work=${AXC_T6_WORK:-}
t6_arms=()
t6_area=
t6_classes=
t6_resume=0
t6_part=b

say() { printf '%s\n' "$*"; }
fail() { printf 'FAIL %s\n' "$*" >&2; exit 1; }

usage()
{
    sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --arm)
            if [[ "$2" == "all" ]]; then t6_arms+=(kernel media); else t6_arms+=("$2"); fi
            shift 2
            ;;
        --area) t6_area=$2; shift 2 ;;
        --classes) t6_classes=$2; shift 2 ;;
        --resume) t6_resume=1; shift ;;
        --part) t6_part=$2; shift 2 ;;
        -h|--help) usage ;;
        *) fail "unknown argument $1" ;;
    esac
done

if [[ "$t6_part" == "a" ]]; then
    run_part_a() {
        local out=${t6_work:-$(mktemp -d "/tmp/axc-t6-a-XXXXXX")}
        mkdir -p "$out"
        local preset xml
        for preset in debug dpdk; do
            [[ -d "$t6_repo/build/$preset" ]] || fail "no build/$preset (run scripts/check.sh $preset)"
            xml=$out/ctest-$preset.xml
            say "ctest $preset -> $xml"
            ctest --test-dir "$t6_repo/build/$preset" --output-junit "$xml" >"$out/ctest-$preset.log" 2>&1 ||
                fail "ctest $preset failed (see $out/ctest-$preset.log)"
        done
        python3 "$t6_compare" ctest --a "$out/ctest-debug.xml" --b "$out/ctest-dpdk.xml" --out "$out/part-a" ||
            fail "the two presets disagree (see $out/part-a/deltas.tsv)"
        say "PASS A: debug and dpdk agree on every test they share"
        say "work=$out"
    }
    run_part_a
    exit 0
fi

[[ ${#t6_arms[@]} -eq 0 ]] && t6_arms=(kernel media)

class_list()
{
    local root="$t6_aeron/aeron-system-tests/src/test/java"
    [[ -d "$root" ]] || fail "no reference system tests at $root (is third_party/aeron populated?)"
    local path class
    while IFS= read -r path; do
        class=${path#"$root"/}
        class=${class%.java}
        class=${class//\//.}
        if [[ -n "$t6_area" ]]; then
            case "$t6_area" in
                root) [[ "$class" =~ ^io\.aeron\.[A-Za-z0-9_]+Test$ ]] || continue ;;
                test) [[ "$class" == io.aeron.test.* ]] || continue ;;
                *) [[ "$class" == "io.aeron.$t6_area".* ]] || continue ;;
            esac
        fi
        printf '%s\n' "$class"
    done < <(find "$root" -name '*Test.java' | sort)
}

preflight()
{
    [[ -x "$t6_media" ]] || fail "no media driver at $t6_media (build the dpdk preset)"
    [[ -x "$t6_kernel" ]] || fail "no reference aeronmd at $t6_kernel (set AXC_T6_KERNEL_DRIVER or build/debug)"
    [[ -x "$t6_wrapper" ]] || fail "the media wrapper is not executable: $t6_wrapper"
    command -v python3 >/dev/null || fail "python3 not found"
    command -v md5sum >/dev/null || fail "md5sum not found"
    ( cd "$t6_aeron" && ./gradlew --version >/dev/null 2>&1 ) || fail "gradle wrapper is not usable"
}

run_class() # run_class <arm> <class>
{
    local arm=$1 class=$2 dir=$t6_work/$arm
    local row="$dir/classes.tsv"
    if [[ "$t6_resume" -eq 1 ]] && grep -q "^$class	" "$row" 2>/dev/null; then
        return 0
    fi

    local args=(-Daeron.test.system.aeronmd.path="$t6_kernel")
    case "$arm" in
        media) args=(-Daeron.test.system.aeronmd.path="$t6_wrapper") ;;
    esac

    # The harness cleans its own directories, but a killed class leaves aeron
    # shm behind and the next driver then refuses the name.
    rm -rf /dev/shm/aeron-* 2>/dev/null || true
    rm -rf "$t6_aeron/aeron-system-tests/build/test-results/test"

    local log="$dir/logs/${class}.log"
    local status=0
    ( cd "$t6_aeron" && timeout "$t6_class_timeout" ./gradlew :aeron-system-tests:test \
        --tests "$class" "${args[@]}" --console=plain --rerun-tasks $t6_gradle_args ) >"$log" 2>&1 || status=$?

    mkdir -p "$dir/xml"
    local found=0
    for xml in "$t6_aeron"/aeron-system-tests/build/test-results/test/TEST-*.xml; do
        [[ -f "$xml" ]] || continue
        cp "$xml" "$dir/xml/"
        found=1
    done
    if [[ "$found" -eq 0 ]]; then
        printf '%s\t%s\t%s\n' "$class" "$status" "no-xml" >>"$row"
    else
        printf '%s\t%s\t%s\n' "$class" "$status" "xml" >>"$row"
    fi
}

summarise_media()
{
    local dir=$t6_work/media
    local logs pcaps bad_pcap=0 bad_log=0 total_log=0
    # `ls | wc -l` under `pipefail` returns ls's failure status when nothing
    # matches and `set -e` then kills the rig instead of reporting B2 (found by
    # mutant M2).
    logs=$(find "$dir/drivers" -maxdepth 1 -name 'driver-*.log' 2>/dev/null | wc -l)
    [[ "$logs" -gt 0 ]] || fail "B2: no driver was ever started in the media arm"
    local log
    for log in "$dir"/drivers/driver-*.log; do
        total_log=$((total_log + 1))
        grep -q 'dpdk media on net_pcap0' "$log" || { bad_log=$((bad_log + 1)); say "B2 red: $log has no media line"; }
    done
    local pcap size
    for pcap in "$dir"/drivers/driver-*.pcap; do
        [[ -f "$pcap" ]] || continue
        size=$(stat -c %s "$pcap")
        if [[ "$size" -ne 24 ]]; then
            bad_pcap=$((bad_pcap + 1))
            say "B3: $pcap is $size bytes, not 24 — frames were handed to the media"
        fi
    done
    pcaps=$(find "$dir/drivers" -maxdepth 1 -name 'driver-*.pcap' 2>/dev/null | wc -l)

    # What the harness asked for, read off the wrapper's one line. The arm must
    # run the suite's own modes: a wrapper that forced one would show that mode
    # for every driver, and the diff against the kernel arm would no longer be
    # "the media".
    local mode_lines shared=0 unset_modes=0
    mode_lines=$(grep -h '^wrapper: AERON_THREADING_MODE=' "$dir"/drivers/driver-*.log 2>/dev/null || true)
    shared=$(printf '%s\n' "$mode_lines" | grep -c '=SHARED$' || true)
    local non_dedicated
    non_dedicated=$(printf '%s\n' "$mode_lines" | grep -cv '=DEDICATED$' || true)
    unset_modes=$(printf '%s\n' "$mode_lines" | grep -c '=<unset>$' || true)
    say "B2 drivers: $total_log logs, $((total_log - bad_log)) opened the media"
    say "B2 modes: $(printf '%s\n' "$mode_lines" | sed 's/^wrapper: AERON_THREADING_MODE=//' | sort | uniq -c | tr '\n' ' ')"
    say "B3 frames: $pcaps pcaps, $bad_pcap not empty"
    {
        echo "drivers=$total_log"
        echo "drivers_with_media=$((total_log - bad_log))"
        echo "drivers_shared=$shared"
        echo "drivers_not_dedicated=$non_dedicated"
        echo "drivers_unset=$unset_modes"
        echo "pcaps=$pcaps"
        echo "pcaps_with_frames=$bad_pcap"
    } >"$dir/driver-checks.txt"
    [[ "$bad_log" -eq 0 ]] || fail "B2: a driver started without the media"
    [[ "$non_dedicated" -gt 0 ]] ||
        fail "B2: every driver ran DEDICATED — the arm is not passing the harness's mode through"
    [[ "$bad_pcap" -eq 0 ]] || fail "B3: the media carried frames; see $dir/driver-checks.txt"
}

run_arm() # run_arm <arm>
{
    local arm=$1 dir=$t6_work/$arm
    mkdir -p "$dir/logs" "$dir/drivers"
    [[ "$t6_resume" -eq 1 ]] || : >"$dir/classes.tsv"
    local classes
    if [[ -n "$t6_classes" ]]; then
        classes=$(printf '%s' "$t6_classes" | tr ',' '\n')
    else
        classes=$(class_list)
    fi
    local total
    total=$(printf '%s\n' "$classes" | grep -c . || true)
    [[ "$total" -gt 0 ]] || fail "no classes selected"
    say "arm $arm: $total classes, timeout ${t6_class_timeout}s each"

    export AXC_T6_DRIVER="$t6_media"
    export AXC_T6_KERNEL_DRIVER="$t6_kernel"
    export AXC_T6_OUT="$dir/drivers"
    local class index=0
    while IFS= read -r class; do
        [[ -n "$class" ]] || continue
        index=$((index + 1))
        run_class "$arm" "$class"
        [[ $((index % 10)) -eq 0 ]] && say "arm $arm: $index/$total"
    done <<<"$classes"

    python3 "$t6_compare" collect --xml-dir "$dir/xml" --out "$dir/status.tsv"
    if [[ "$arm" == "media" ]]; then
        summarise_media
    fi
}

check_counts() # check_counts <arm>
{
    local arm=$1 file=$t6_work/$1/status.tsv
    [[ -f "$file" ]] || fail "B1: no status file for arm $arm"
    local cases classes
    cases=$(grep -c . "$file" || true)
    classes=$(cut -f1 "$file" | sort -u | grep -c . || true)
    say "B1 $arm: $classes classes, $cases cases"
    if [[ -n "$t6_classes" || -n "$t6_area" ]]; then
        say "B1 skipped: this run is filtered (classes/area), not the whole suite"
        return 0
    fi
    # 129 `*Test.java` files exist; 25 of them carry only @SlowTest methods (or
    # are excluded by the reference build), so an untagged run has 104 classes.
    [[ "$classes" -ge 100 && "$cases" -ge 700 ]] ||
        fail "B1: arm $arm ran only $classes classes / $cases cases — the suite did not run"
}

main()
{
    preflight
    if [[ -z "$t6_work" ]]; then
        t6_work=$(mktemp -d "/tmp/axc-t6-XXXXXX")
    fi
    mkdir -p "$t6_work"
    say "work=$t6_work"
    local arm
    for arm in "${t6_arms[@]}"; do
        run_arm "$arm"
        check_counts "$arm"
    done
    if [[ ${#t6_arms[@]} -ge 2 ]] && [[ " ${t6_arms[*]} " == *" media "* ]]; then
        local only_kernel only_media
        only_kernel=$(comm -23 <(cut -f1 "$t6_work/kernel/status.tsv" | sort -u) \
                              <(cut -f1 "$t6_work/media/status.tsv" | sort -u) | head -5)
        only_media=$(comm -13 <(cut -f1 "$t6_work/kernel/status.tsv" | sort -u) \
                             <(cut -f1 "$t6_work/media/status.tsv" | sort -u) | head -5)
        if [[ -n "$only_kernel" || -n "$only_media" ]]; then
            say "classes only in kernel: $only_kernel"
            say "classes only in media: $only_media"
            fail "the arms ran different classes; re-run the missing side with --resume"
        fi
        python3 "$t6_compare" diff --base "$t6_work/kernel/status.tsv" \
            --other "$t6_work/media/status.tsv" --base-label kernel --other-label media \
            --out "$t6_work/diff" ||
            fail "B4: the arms disagree; adjudicate every row in $t6_work/diff/deltas.tsv"
    fi
    say "work=$t6_work"
}

main "$@"
