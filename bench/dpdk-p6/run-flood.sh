#!/usr/bin/env bash
# P6 / the flood arm (arm B of the lost-frame card): a dense one-way run of
# frames from our DPDK media to the reference driver's subscriber.
#
#   sudo bench/dpdk-p6/run-flood.sh
#
# The ping grid sends ~44,000 frames per session, and the loss this arm exists
# for shows up a handful of times per 20,000 at worst: a one-way flood raises
# the frame count by ~50x and removes the round trip from the measurement, so
# "a frame the driver counted as sent never reached the peer" stops being a
# once-an-hour event. Both ends' counters are bracketed with the drivers
# stopped, and the peer's namespace UDP counters are read too (the earlier NAK
# round read only the socket-buffer counter).
#
# Judged: the sender offered `FLOOD_COUNT`, the subscriber received all of them
# with no bad payload, and neither end recorded an error or a drop. A frame our
# NIC never sent shows up as `RECEIVED < FLOOD_COUNT` (the reference's NAK
# recovery covers it, so the count alone would not fail) and as a peer-NIC
# counter that did not move; a frame the peer's kernel dropped shows in its UDP
# counters. Exits non-zero if any check fails.
set -euo pipefail

p6_repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
# shellcheck source=/dev/null
source "$p6_repo/bench/dpdk-p6/lib.sh"

COUNT=${FLOOD_COUNT:-1000000}
PORT=${FLOOD_PORT:-24640}
STREAM=${FLOOD_STREAM:-1201}
PREFIX=${FLOOD_PREFIX:-"P6 Flood"}
RECV_TIMEOUT=${FLOOD_TIMEOUT:-600}

failures=0
fail() { printf 'FAIL %s\n' "$*"; failures=$((failures + 1)); }
pass() { printf 'PASS %s\n' "$*"; }

delta() # delta <file-before> <file-after> <name>
{
    local before after name
    before=$(grep -E "^[[:space:]]*$3:" "$1" 2>/dev/null | awk '{print $2}' | head -1)
    after=$(grep -E "^[[:space:]]*$3:" "$2" 2>/dev/null | awk '{print $2}' | head -1)
    printf '%s' "$(( ${after:-0} - ${before:-0} ))"
}

main()
{
    p6_preflight
    p6_peer_iface=$(p6_resolve_peer_iface)
    [[ -n "$p6_peer_iface" ]] || { echo "cannot name the peer port" >&2; exit 2; }
    p6_work=$(mktemp -d "/tmp/axc-p6-flood-XXXXXX")
    trap p6_teardown EXIT
    local ours_dir=$p6_work/ours-aeron ref_dir=$p6_work/ref-aeron
    printf 'flood: %s frames of %s, %s -> %s:%s stream %s [%s]\n' "$COUNT" "$PREFIX" "$p6_local_addr" \
        "$p6_peer_addr" "$PORT" "$STREAM" "$p6_work"

    p6_compile_probes
    p6_setup_ours media
    p6_setup_peer
    p6_start_ours media "$ours_dir"
    p6_start_reference "$ref_dir"
    grep -m1 'dpdk media on' "$p6_work/ours.log" || true

    p6_peer_counters "$p6_work/peer-before.txt"

    p6_on_peer "$p6_work/bin/recv" "$ref_dir" "aeron:udp?endpoint=$p6_peer_addr:$PORT" "$STREAM" "$COUNT" \
        "$PREFIX" "$RECV_TIMEOUT" >"$p6_work/recv.log" 2>&1 &
    local recv_pid=$!
    sleep 2

    local started=$SECONDS
    "$p6_work/bin/send" "$ours_dir" "aeron:udp?endpoint=$p6_peer_addr:$PORT" "$STREAM" "$COUNT" "$PREFIX" \
        >"$p6_work/send.log" 2>&1 || true
    local send_seconds=$((SECONDS - started))

    local deadline=$((SECONDS + RECV_TIMEOUT))
    while kill -0 "$recv_pid" 2>/dev/null && ((SECONDS < deadline)); do
        sleep 1
    done
    kill -TERM "$recv_pid" 2>/dev/null || true
    wait "$recv_pid" 2>/dev/null || true

    p6_peer_counters "$p6_work/peer-after.txt"

    printf 'send (%ss): %s\n' "$send_seconds" "$(grep -E 'SENT' "$p6_work/send.log" | tail -1)"
    printf 'recv (%ss): %s\n' "$((SECONDS - started - send_seconds))" \
        "$(grep -E 'RECEIVED' "$p6_work/recv.log" | tail -1)"
    printf 'peer rx_packets: +%s, Udp InDatagrams: +%s, Udp InErrors: +%s\n' \
        "$(delta "$p6_work/peer-before.txt" "$p6_work/peer-after.txt" rx_packets)" \
        "$(delta "$p6_work/peer-before.txt" "$p6_work/peer-after.txt" udp_InDatagrams)" \
        "$(delta "$p6_work/peer-before.txt" "$p6_work/peer-after.txt" udp_InErrors)"

    grep -q "SENT $COUNT" "$p6_work/send.log" && pass "the publisher offered $COUNT" ||
        fail "the publisher did not offer $COUNT: $(tail -2 "$p6_work/send.log" | tr '\n' ' ')"
    grep -q "RECEIVED $COUNT BAD 0" "$p6_work/recv.log" && pass "the subscriber received $COUNT, payload intact" ||
        fail "the subscriber did not: $(tail -2 "$p6_work/recv.log" | tr '\n' ' ')"

    local checked=0
    for name in rx_errors rx_crc_errors rx_csum_err rx_no_buffer_count rx_missed_errors rx_dropped \
        udp_InErrors udp_RcvbufErrors; do
        local d
        d=$(delta "$p6_work/peer-before.txt" "$p6_work/peer-after.txt" "$name")
        checked=$((checked + 1))
        [[ "$d" -eq 0 ]] || fail "the peer's $name moved: +$d"
    done
    [[ "$checked" -gt 0 ]] && pass "the peer's $checked error/drop counters did not move"

    if ((failures > 0)); then
        printf '%s criteria failed [%s]\n' "$failures" "$p6_work"
        exit 1
    fi
    printf 'P6 flood: all criteria passed [%s]\n' "$p6_work"
}

main "$@"
