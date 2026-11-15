#!/usr/bin/env bash
# P6 / T5: the reference driver and the reference clients, over the cable, on
# our DPDK media.
#
#   sudo bench/dpdk-p6/run-interop.sh
#
# Judged (plan-dpdk-p6.md §2): I1 ARP, I2 reference -> us, I3 us -> reference,
# I4 both at once, I5 counters. I6 (the archive interop suite) is
# run-archive-suite.sh. Every criterion prints PASS/FAIL with the reading it
# judged; the script exits non-zero if any failed.
set -euo pipefail

p6_repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
# shellcheck source=/dev/null
source "$p6_repo/bench/dpdk-p6/lib.sh"

failures=0
say() { printf '%s\n' "$*"; }
fail() { printf 'FAIL %s\n' "$*"; failures=$((failures + 1)); }
pass() { printf 'PASS %s\n' "$*"; }

INGRESS_PORT=24620
EGRESS_PORT=24621
SIMUL_INGRESS_PORT=24622
SIMUL_EGRESS_PORT=24623
INGRESS_STREAM=1101
EGRESS_STREAM=1102
COUNT=10

ours_dir=
ref_dir=

java_sample() # java_sample <main-class> <args...>
{
    java "${p6_java_opens[@]}" -cp "$p6_jar" "$@"
}

run_reference_publisher() # run_reference_publisher <port> <stream> <count> <log>
{
    local port=$1 stream=$2 count=$3 log=$4
    java_sample "-Daeron.dir=$ref_dir" "-Daeron.sample.channel=aeron:udp?endpoint=$p6_local_addr:$port" \
        "-Daeron.sample.streamId=$stream" "-Daeron.sample.messages=$count" "-Daeron.sample.lingerTimeout=500" \
        io.aeron.samples.BasicPublisher >"$log" 2>&1
}

run_reference_subscriber() # run_reference_subscriber <port> <stream> <log> -> pid
{
    local port=$1 stream=$2 log=$3
    java_sample "-Daeron.dir=$ref_dir" "-Daeron.sample.channel=aeron:udp?endpoint=$p6_peer_addr:$port" \
        "-Daeron.sample.streamId=$stream" io.aeron.samples.BasicSubscriber >"$log" 2>&1 &
    printf '%s' "$!"
}

main()
{
    p6_preflight
    p6_peer_iface=$(p6_resolve_peer_iface)
    [[ -n "$p6_peer_iface" ]] || { echo "cannot name the peer port" >&2; exit 2; }
    p6_work=$(mktemp -d "/tmp/axc-p6-interop-XXXXXX")
    trap p6_teardown EXIT
    ours_dir=$p6_work/ours-aeron
    ref_dir=$p6_work/ref-aeron
    say "P6 interop: $p6_pci (vfio) $p6_local_cidr, reference on $p6_peer_iface $p6_peer_cidr [$p6_work]"

    p6_compile_probes
    p6_setup_ours media
    p6_setup_peer
    p6_start_ours media "$ours_dir"
    p6_start_reference "$ref_dir"
    say "media: $(grep 'dpdk media on' "$p6_work/ours.log" | tail -1)"

    # ---- I1: our stack answers ARP, and the kernel can reach it ----
    local arp_miss_before arp_miss_after
    arp_miss_before=$(p6_counter "$ours_dir/cnc.dat" "dpdk arp miss")
    if ping -c 1 -W 2 "$p6_local_addr" >/dev/null 2>&1; then
        pass "I1: ping reached $p6_local_addr"
    else
        fail "I1: ping to $p6_local_addr failed"
    fi
    if ip neigh show "$p6_local_addr" | grep -q "lladdr"; then
        pass "I1: $(ip neigh show "$p6_local_addr" | head -1)"
    else
        fail "I1: no neighbour entry for $p6_local_addr"
    fi
    arp_miss_after=$(p6_counter "$ours_dir/cnc.dat" "dpdk arp miss")
    say "     dpdk arp miss $arp_miss_before -> $arp_miss_after"

    # ---- I2: the reference publisher, our subscriber ----
    local recv_log=$p6_work/i2-recv.log pub_log=$p6_work/i2-pub.log
    "$p6_work/bin/recv" "$ours_dir" "aeron:udp?endpoint=$p6_local_addr:$INGRESS_PORT" "$INGRESS_STREAM" \
        "$COUNT" "Hello World!" 60 >"$recv_log" 2>&1 &
    local recv_pid=$!
    run_reference_publisher "$INGRESS_PORT" "$INGRESS_STREAM" "$COUNT" "$pub_log" || true
    p6_wait_pid "$recv_pid" 60 || true
    if grep -q "RECEIVED $COUNT BAD 0" "$recv_log"; then
        pass "I2: $(grep RECEIVED "$recv_log" | tail -1) (reference publisher, our subscriber, payload byte-for-byte)"
    else
        fail "I2: $(tail -3 "$recv_log" | tr '\n' ' ')"
    fi

    # ---- I3: our publisher, the reference subscriber ----
    local sub_log=$p6_work/i3-sub.log send_log=$p6_work/i3-send.log
    local sub_pid
    sub_pid=$(run_reference_subscriber "$EGRESS_PORT" "$EGRESS_STREAM" "$sub_log")
    sleep 1
    "$p6_work/bin/send" "$ours_dir" "aeron:udp?endpoint=$p6_peer_addr:$EGRESS_PORT" "$EGRESS_STREAM" \
        "$COUNT" "P6 Hello" >"$send_log" 2>&1 || true
    sleep 1
    kill -TERM "$sub_pid" 2>/dev/null || true
    wait "$sub_pid" 2>/dev/null || true
    local seen
    seen=$(grep -c "P6 Hello" "$sub_log" || true)
    if [[ "$seen" -ge "$COUNT" ]] && grep -q "SENT $COUNT" "$send_log"; then
        pass "I3: the reference subscriber printed $seen of $COUNT payloads"
    else
        fail "I3: reference saw $seen of $COUNT; $(tail -2 "$sub_log" | tr '\n' ' ') / $(tail -2 "$send_log" | tr '\n' ' ')"
    fi

    # ---- I4: both directions at once ----
    local simul_recv=$p6_work/i4-recv.log simul_pub=$p6_work/i4-pub.log
    local simul_sub=$p6_work/i4-sub.log simul_send=$p6_work/i4-send.log
    "$p6_work/bin/recv" "$ours_dir" "aeron:udp?endpoint=$p6_local_addr:$SIMUL_INGRESS_PORT" "$INGRESS_STREAM" \
        "$COUNT" "Hello World!" 60 >"$simul_recv" 2>&1 &
    local simul_recv_pid=$!
    local simul_sub_pid
    simul_sub_pid=$(run_reference_subscriber "$SIMUL_EGRESS_PORT" "$EGRESS_STREAM" "$simul_sub")
    run_reference_publisher "$SIMUL_INGRESS_PORT" "$INGRESS_STREAM" "$COUNT" "$simul_pub" &
    local simul_pub_pid=$!
    sleep 1
    "$p6_work/bin/send" "$ours_dir" "aeron:udp?endpoint=$p6_peer_addr:$SIMUL_EGRESS_PORT" "$EGRESS_STREAM" \
        "$COUNT" "P6 Hello" >"$simul_send" 2>&1 || true
    wait "$simul_pub_pid" || true
    p6_wait_pid "$simul_recv_pid" 60 || true
    sleep 1
    kill -TERM "$simul_sub_pid" 2>/dev/null || true
    wait "$simul_sub_pid" 2>/dev/null || true
    local simul_seen
    simul_seen=$(grep -c "P6 Hello" "$simul_sub" || true)
    if grep -q "RECEIVED $COUNT BAD 0" "$simul_recv" && [[ "$simul_seen" -ge "$COUNT" ]]; then
        pass "I4: both directions at once ($(grep RECEIVED "$simul_recv" | tail -1); reference saw $simul_seen)"
    else
        fail "I4: ours $(tail -1 "$simul_recv") / reference $simul_seen"
    fi

    # ---- I5: counters ----
    local packets error discard bytes
    packets=$(p6_counter "$ours_dir/cnc.dat" "dpdk packets")
    bytes=$(p6_counter "$ours_dir/cnc.dat" "dpdk bytes")
    error=$(p6_counter "$ours_dir/cnc.dat" "dpdk error")
    discard=$(p6_counter "$ours_dir/cnc.dat" "dpdk rx receiver discard")
    if [[ "${packets:-0}" -gt 0 && "${error:-1}" == 0 && "${discard:-1}" == 0 ]]; then
        pass "I5: dpdk packets $packets, bytes $bytes, error 0, rx receiver discard 0"
    else
        fail "I5: packets=${packets:-?} error=${error:-?} discard=${discard:-?}"
    fi
    local ref_naks ref_gaps
    ref_naks=$(p6_counter "$ref_dir/cnc.dat" "NAKs sent")
    ref_gaps=$(p6_counter "$ref_dir/cnc.dat" "Loss gap fills")
    say "     reference: NAKs sent ${ref_naks:-?}, loss gap fills ${ref_gaps:-?}"
    if [[ "${ref_naks:-1}" == 0 ]]; then
        pass "I5: the reference never had to NAK"
    else
        fail "I5: the reference sent ${ref_naks:-?} NAKs"
    fi

    say ""
    if [[ "$failures" -eq 0 ]]; then
        say "P6 interop: all criteria passed ($p6_work)"
        exit 0
    fi
    say "P6 interop: $failures criterion(s) failed ($p6_work)"
    exit 1
}

main
