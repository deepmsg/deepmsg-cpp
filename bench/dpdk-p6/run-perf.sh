#!/usr/bin/env bash
# P6 / T7: driver-level round trip, kernel UDP against the DPDK media, on the
# same port, pre-registered in plan-dpdk-p6.md §3.
#
#   sudo bench/dpdk-p6/run-perf.sh
#
# Grid: K M M K, two sessions; N messages of 64 bytes per cell after a warmup.
# The far side never changes: the reference `aeronmd` and the C echo probe on
# 10.10.0.2. Both drivers run the `spin` idle strategy (plan-dpdk-p6.md §3.2):
# the DPDK port cannot wake a parked thread, and the C driver's default
# `backoff` parks up to 1 ms, which would measure the park instead of the arm. Judged per cell (counts + media invariants) and per session
# (same-arm floor, then M/K). Writes a CSV and prints the table; exits non-zero
# if any cell failed or an invariant broke.
set -euo pipefail

p6_repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
# shellcheck source=/dev/null
source "$p6_repo/bench/dpdk-p6/lib.sh"

COUNT=${P6_COUNT:-20000}
WARMUP=${P6_WARMUP:-2000}
SESSIONS=${P6_SESSIONS:-2}
ECHO_IN_PORT=24630
ECHO_OUT_PORT=24631
STREAM=1201

failures=0
say() { printf '%s\n' "$*"; }
fail() { printf 'FAIL %s\n' "$*"; failures=$((failures + 1)); }
pass() { printf 'PASS %s\n' "$*"; }

csv=

cell() # cell <session> <arm> <position>
{
    local session=$1 arm=$2 position=$3
    local dir=$p6_work/s${session}-${position}
    local driver_arm=kernel
    [[ "$arm" == M ]] && driver_arm=media
    mkdir -p "$dir"

    p6_setup_ours "$driver_arm"
    p6_ours_pid= p6_ref_pid=
    p6_start_ours "$driver_arm" "$dir/ours-aeron"
    if [[ "$driver_arm" == media ]]; then
        # the PMD's reset bounces the peer link; the address goes back on after
        # the media line (E3's pitfall 1)
        ip link set "$(p6_peer_iface_name)" up
        ip addr replace "$p6_peer_cidr" dev "$(p6_peer_iface_name)"
    fi
    p6_start_reference "$dir/ref-aeron"

    "$p6_work/bin/echo" "$dir/ref-aeron" "aeron:udp?endpoint=$p6_peer_addr:$ECHO_IN_PORT" "$STREAM" \
        "aeron:udp?endpoint=$p6_local_addr:$ECHO_OUT_PORT" "$STREAM" >"$dir/echo.log" 2>&1 &
    local echo_pid=$!
    sleep 1

    local ping_log=$dir/ping.log
    local status=0
    P6_DUMP_RTTS=$dir/rtts.txt "$p6_work/bin/ping" "$dir/ours-aeron" \
        "aeron:udp?endpoint=$p6_peer_addr:$ECHO_IN_PORT" "$STREAM" \
        "aeron:udp?endpoint=$p6_local_addr:$ECHO_OUT_PORT" "$STREAM" "$COUNT" "$WARMUP" \
        >"$ping_log" 2>&1 || status=$?

    kill -TERM "$echo_pid" 2>/dev/null || true
    wait "$echo_pid" 2>/dev/null || true

    local sent received p50 p99 p999 min max packets bytes error missed discard naks gaps
    sent=$(awk '/^SENT / {print $2}' "$ping_log" | tail -1)
    received=$(awk '/^SENT / {print $4}' "$ping_log" | tail -1)
    p50=$(awk '/^P50 / {print $2}' "$ping_log" | tail -1)
    p99=$(awk '/^P50 / {print $4}' "$ping_log" | tail -1)
    p999=$(awk '/^P50 / {print $6}' "$ping_log" | tail -1)
    min=$(awk '/^P50 / {print $8}' "$ping_log" | tail -1)
    max=$(awk '/^P50 / {print $10}' "$ping_log" | tail -1)
    packets=$(p6_counter "$dir/ours-aeron/cnc.dat" "dpdk packets")
    bytes=$(p6_counter "$dir/ours-aeron/cnc.dat" "dpdk bytes")
    error=$(p6_counter "$dir/ours-aeron/cnc.dat" "dpdk error")
    missed=$(p6_counter "$dir/ours-aeron/cnc.dat" "dpdk missed packets")
    discard=$(p6_counter "$dir/ours-aeron/cnc.dat" "dpdk rx receiver discard")
    naks=$(p6_counter "$dir/ref-aeron/cnc.dat" "NAKs sent")
    gaps=$(p6_counter "$dir/ref-aeron/cnc.dat" "Loss gap fills")

    if [[ "$status" -ne 0 || "${sent:-0}" -ne $((COUNT + WARMUP)) || "${received:-0}" -ne $((COUNT + WARMUP)) ||
        -z "$p50" ]]; then
        fail "cell $session/$position ($arm): not a reading ($(tail -2 "$ping_log" | tr '\n' ' '))"
    elif [[ "$arm" == M && ( "${error:-1}" != 0 || "${missed:-1}" != 0 || "${discard:-1}" != 0 ) ]]; then
        fail "cell $session/$position ($arm): media counters error=${error:-?} missed=${missed:-?} discard=${discard:-?}"
    elif [[ "$arm" == K && -n "${packets}" ]]; then
        fail "cell $session/$position (kernel): the driver has dpdk counters"
    else
        pass "cell $session/$position ($arm): p50 $p50 ns, p99 $p99 ns, p999 $p999 ns"
    fi
    say "     packets=${packets:-<none>} bytes=${bytes:-<none>} error=${error:-<none>} missed=${missed:-<none>} discard=${discard:-<none>} domain NAKs=${naks:-?} gaps=${gaps:-?}"
    csv+="$session,$position,$arm,$p50,$p99,$p999,$min,$max,$sent,$received,${packets:-},${error:-},${missed:-},${discard:-},${naks:-},${gaps:-}"$'\n'

    kill -TERM "$p6_ours_pid" "$p6_ref_pid" 2>/dev/null || true
    p6_wait_pid "$p6_ours_pid" 20 2>/dev/null || true
    p6_wait_pid "$p6_ref_pid" 20 2>/dev/null || true
    p6_ours_pid= p6_ref_pid=
}

main()
{
    p6_preflight
    p6_peer_iface=$(p6_resolve_peer_iface)
    [[ -n "$p6_peer_iface" ]] || { echo "cannot name the peer port" >&2; exit 2; }
    p6_work=$(mktemp -d "/tmp/axc-p6-perf-XXXXXX")
    trap p6_teardown EXIT
    say "P6 perf: $p6_pci, $p6_local_cidr vs reference on $p6_peer_iface $p6_peer_cidr; N=$COUNT warmup=$WARMUP sessions=$SESSIONS [$p6_work]"

    export AERON_RECEIVER_IDLE_STRATEGY=${P6_IDLE_STRATEGY:-spin}
    export AERON_SENDER_IDLE_STRATEGY=${P6_IDLE_STRATEGY:-spin}

    p6_compile_probes
    p6_setup_peer

    csv=$'session,position,arm,p50_ns,p99_ns,p999_ns,min_ns,max_ns,sent,received,packets,error,missed,discard,naks,gaps\n'
    local session position arm
    for session in $(seq 1 "$SESSIONS"); do
        for position in 1 2 3 4; do
            case $position in
                1 | 4) arm=K ;;
                2 | 3) arm=M ;;
            esac
            cell "$session" "$arm" "$position"
        done
    done

    printf '%s' "$csv" >"$p6_work/perf.csv"
    say ""
    say "session,position,arm,p50_ns,p99_ns,p999_ns,min_ns,max_ns"
    awk -F, 'NR > 1 && $4 != "" {print $1","$2","$3","$4","$5","$6","$7","$8}' <<<"$csv"

    # Same-session judgement: floor from the two same-arm cells, then M/K.
    local s k1 k2 m1 m2 floor_k floor_m ratio1 ratio2
    for s in $(seq 1 "$SESSIONS"); do
        k1=$(awk -F, -v s="$s" '$1 == s && $2 == 1 && $4 != "" {print $4}' <<<"$csv")
        k2=$(awk -F, -v s="$s" '$1 == s && $2 == 4 && $4 != "" {print $4}' <<<"$csv")
        m1=$(awk -F, -v s="$s" '$1 == s && $2 == 2 && $4 != "" {print $4}' <<<"$csv")
        m2=$(awk -F, -v s="$s" '$1 == s && $2 == 3 && $4 != "" {print $4}' <<<"$csv")
        if [[ -z "$k1" || -z "$k2" || -z "$m1" || -z "$m2" ]]; then
            fail "session $s: not all four cells produced a p50"
            continue
        fi
        floor_k=$(awk -v a="$k1" -v b="$k2" 'BEGIN {print (a > b ? a / b : b / a)}')
        floor_m=$(awk -v a="$m1" -v b="$m2" 'BEGIN {print (a > b ? a / b : b / a)}')
        ratio1=$(awk -v m="$m1" -v k="$k1" 'BEGIN {printf "%.3f", m / k}')
        ratio2=$(awk -v m="$m2" -v k="$k2" 'BEGIN {printf "%.3f", m / k}')
        say "session $s: floor K $(printf '%.3f' "$floor_k") M $(printf '%.3f' "$floor_m"); M/K $ratio1 / $ratio2"
        local outside
        outside=$(awk -v r1="$ratio1" -v r2="$ratio2" -v fk="$floor_k" -v fm="$floor_m" \
            'BEGIN {print ((r1 < 1 / fk && r2 < 1 / fk) || (r1 > fk && r2 > fk) || (r1 < 1 / fm && r2 < 1 / fm) || (r1 > fm && r2 > fm)) ? 1 : 0}')
        if [[ "$outside" == 1 ]]; then
            pass "session $s: both M/K ratios are outside the same-arm floor"
        else
            fail "session $s: M/K $ratio1 / $ratio2 is inside the floor (K $floor_k, M $floor_m) -- no signal"
        fi
    done

    say ""
    say "csv: $p6_work/perf.csv"
    if [[ "$failures" -eq 0 ]]; then
        say "P6 perf: all criteria passed ($p6_work)"
        exit 0
    fi
    say "P6 perf: $failures criterion(s) failed ($p6_work)"
    exit 1
}

main
