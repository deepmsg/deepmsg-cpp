#!/usr/bin/env bash
# E3: the cluster on an external driver -- over the DPDK media, or over the
# kernel, with the same binaries and the same cluster configuration.
#
#   sudo bench/dpdk-e3/run-smoke.sh media     # deepmsg.dpdk.* in driver.properties
#   sudo bench/dpdk-e3/run-smoke.sh kernel    # the same driver binary, no media
#
# The four criteria are `plan-dpdk-bench-e2e.md` W4/E3: echo, snapshot +
# restart, clean termination, and the kernel arm agreeing with the media arm.
# Every one prints PASS/FAIL with the reading it judged, and the script exits
# non-zero if any of them failed -- an arm that ran and did nothing is not a
# green arm.
#
# Real card, so root and the bench wiring from
# `handoff-2026-11-15-dpdk-on-bench.md` are assumed:
#
#   03:00.0  the media port (vfio-pci on the media arm, ixgbe on the kernel arm)
#   03:00.1  the peer, kernel, 10.10.0.2/24 (the client side)
#
# The media arm keeps the client in the root netns: 10.10.0.1 is not a kernel
# address there, so the kernel cannot short-circuit the route. The kernel arm
# has to put the peer port in a netns -- both addresses on one host would take
# the kernel's local shortcut and the arm would not cross the cable at all
# (`dpdk-bench-e2e.md` §4, the two void rounds).
#
# Environment (all optional):
#   AXC_BUILD_DIR        build directory (default <repo>/build/dpdk)
#   AXC_AERON_JAR        aeron-all jar
#   AXC_AERON_TESTS      the fork's checkout; its `deepmsg-tests/java` probes
#                        are required (the echo client is one of them)
#   AXC_DPDK_BENCH_PCI   media function (0000:03:00.0)
#   AXC_DPDK_BENCH_LOCAL the media address (10.10.0.1/24)
#   AXC_DPDK_BENCH_PEER  the peer address (10.10.0.2/24)
#   E3_COUNT             messages per echo probe (10)
#   E3_BASE_PORT         first cluster port (24600; ports +0/+1/+2/+3 and +10)
set -euo pipefail

arm=${1:-}
case "$arm" in
    media|kernel) ;;
    *)
        echo "usage: $0 media|kernel" >&2
        exit 2
        ;;
esac

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
build=${AXC_BUILD_DIR:-$repo/build/dpdk}
jar=${AXC_AERON_JAR:-$repo/third_party/aeron/aeron-all/build/libs/aeron-all-1.54.0.jar}
tests_root=${AXC_AERON_TESTS:-}
pci=${AXC_DPDK_BENCH_PCI:-0000:03:00.0}
peer_bdf=$(printf '%s' "$pci" | sed -E 's/([0-9a-f]{2}:[0-9a-f]{2})\.[0-9a-f]$/\1.1/')
local_cidr=${AXC_DPDK_BENCH_LOCAL:-10.10.0.1/24}
peer_cidr=${AXC_DPDK_BENCH_PEER:-10.10.0.2/24}
count=${E3_COUNT:-10}
base_port=${E3_BASE_PORT:-24600}
ingress_port=$base_port
consensus_port=$((base_port + 1))
log_port=$((base_port + 2))
catchup_port=$((base_port + 3))
egress_port=$((base_port + 10))
netns=axc-e3-peer
client_ns=

local_addr=${local_cidr%%/*}
peer_addr=${peer_cidr%%/*}
work=
peer_iface=
driver_pid=
client_driver_pid=
node_pid=
container_pid=
failures=0

JO=("--add-opens" "java.base/jdk.internal.misc=ALL-UNNAMED" "--add-opens" "java.base/sun.nio.ch=ALL-UNNAMED")

say() { printf '%s\n' "$*"; }
fail() { printf 'FAIL %s\n' "$*"; failures=$((failures + 1)); }
pass() { printf 'PASS %s\n' "$*"; }

root_iface()
{
    cat "/sys/bus/pci/devices/$pci"/net/*/uevent 2>/dev/null | sed -n 's/^INTERFACE=//p' | head -1
}

resolve_peer_iface()
{
    cat "/sys/bus/pci/devices/$peer_bdf"/net/*/uevent 2>/dev/null |
        sed -n 's/^INTERFACE=//p' | head -1
}

peer_iface_name()
{
    # Resolved once in `main`: once the port is in the netns its name is not in
    # the root netns's sysfs view any more, and a command substitution cannot
    # fill a cache in the caller anyway.
    printf '%s' "$peer_iface"
}

peer_mac()
{
    cat "/sys/bus/pci/devices/$peer_bdf/net/$(peer_iface_name)/address"
}

cleanup()
{
    set +e
    for pid in "$container_pid" "$node_pid" "$client_driver_pid" "$driver_pid"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null
        fi
    done
    sleep 1
    if ip netns list 2>/dev/null | grep -q "^$netns"; then
        ip -n "$netns" link set "$(peer_iface_name)" netns 1 2>/dev/null
        ip netns del "$netns" 2>/dev/null
    fi
    ip addr del "$local_cidr" dev "$(root_iface)" 2>/dev/null
    ip addr del "$peer_cidr" dev "$(peer_iface_name)" 2>/dev/null
    nmcli device set "$(root_iface)" managed yes 2>/dev/null
    nmcli device set "$(peer_iface_name)" managed yes 2>/dev/null
    if [[ "$arm" == media ]]; then
        dpdk-devbind.py --bind=ixgbe "$pci" >/dev/null 2>&1
        sleep 2
    fi
    rm -f /dev/hugepages/rtemap_* 2>/dev/null
    set -e
}

preflight()
{
    [[ $(id -u) -eq 0 ]] || { echo "run as root: sudo $0 $arm" >&2; exit 2; }
    for binary in deepmsg-media-driver deepmsg-cluster-node deepmsg-clustered-service-container; do
        [[ -x "$build/apps/$binary/$binary" ]] ||
            { echo "missing $build/apps/$binary/$binary (build the dpdk preset)" >&2; exit 2; }
    done
    [[ -f "$jar" ]] || { echo "missing aeron-all jar: $jar (set AXC_AERON_JAR)" >&2; exit 2; }
    command -v java >/dev/null && command -v javac >/dev/null || { echo "java/javac not found" >&2; exit 2; }
    [[ -d "/sys/bus/pci/devices/$pci" ]] || { echo "no PCI device $pci" >&2; exit 2; }
    [[ -d "/sys/bus/pci/devices/$peer_bdf" ]] || { echo "no peer PCI device $peer_bdf" >&2; exit 2; }
    [[ -n "$tests_root" ]] ||
        { echo "set AXC_AERON_TESTS to the fork's checkout (the probes live there)" >&2; exit 2; }
    [[ -f "$tests_root/deepmsg-tests/java/ClusterEchoClientProbe.java" ]] ||
        { echo "probe not found under $tests_root/deepmsg-tests/java" >&2; exit 2; }
    if [[ "$arm" == media ]]; then
        local free_pages
        free_pages=$(awk '/^HugePages_Free:/ {print $2}' /proc/meminfo)
        [[ "${free_pages:-0}" -gt 0 ]] || { echo "no free huge pages (/proc/meminfo)" >&2; exit 2; }
    fi
    local port
    for port in "$ingress_port" "$consensus_port" "$log_port" "$catchup_port" "$egress_port"; do
        if ss -lun 2>/dev/null | awk '{print $4}' | grep -q ":$port\$"; then
            echo "port $port is already bound" >&2
            exit 2
        fi
    done
}

start() # start <pid-variable> <log> <command...>
{
    local name=$1 log=$2
    shift 2
    "$@" >"$log" 2>&1 &
    printf -v "$name" '%s' "$!"
}

wait_until() # wait_until <seconds> <command...>
{
    local seconds=$1
    shift
    local deadline=$((SECONDS + seconds))
    while ((SECONDS < deadline)); do
        if "$@"; then
            return 0
        fi
        sleep 0.1
    done
    return 1
}

wait_pid() # wait_pid <pid> <seconds>; the child's status, or 124 on timeout
{
    local pid=$1 deadline=$((SECONDS + $2))
    while kill -0 "$pid" 2>/dev/null; do
        if ((SECONDS >= deadline)); then
            return 124
        fi
        sleep 0.1
    done
    wait "$pid"
}

cluster_read() # cluster_read <label-prefix>
{
    local out
    out=$(java "${JO[@]}" -cp "$jar" "$repo/bench/tools/ClusterToggle.java" "$work/aeron/cnc.dat" "$1") || return 1
    printf '%s' "$(cut -f2 <<<"$out")"
}

cluster_toggle() # cluster_toggle <value>
{
    java "${JO[@]}" -cp "$jar" "$repo/bench/tools/ClusterToggle.java" "$work/aeron/cnc.dat" \
        "Cluster control toggle" "$1" >/dev/null
}

counter_dump()
{
    java "${JO[@]}" -cp "$jar" "$repo/bench/tools/CounterDump.java" "$work/aeron/cnc.dat" 2>/dev/null
}

counter_value() # counter_value <label>
{
    counter_dump | awk -F'\t' -v label="$1" '$1 == label {print $2; exit}'
}

interface_packets() # interface_packets <iface> [netns] -> "<rx> <tx>"
{
    local iface=$1 ns=${2:-} stats
    if [[ -n "$ns" ]]; then
        stats=$(ip -n "$ns" -s link show "$iface")
    else
        stats=$(ip -s link show "$iface")
    fi
    awk '/RX:/ {getline; rx=$2} /TX:/ {getline; tx=$2} END {print rx, tx}' <<<"$stats"
}

root_iface_ready()
{
    [[ -n "$(root_iface)" ]]
}

peer_iface_ready()
{
    [[ -n "$(peer_iface_name)" ]]
}

iface_has_carrier() # iface_has_carrier <iface> [netns]
{
    local iface=$1 ns=${2:-}
    if [[ -n "$ns" ]]; then
        ip -n "$ns" link show "$iface" 2>/dev/null | grep -q LOWER_UP
    else
        ip link show "$iface" 2>/dev/null | grep -q LOWER_UP
    fi
}

state_is_active()
{
    [[ "$(cluster_read "Consensus Module state")" == 1 ]]
}

counters_exist()
{
    cluster_read "Consensus Module state" >/dev/null
}

snapshot_above() # snapshot_above <n>
{
    local value
    value=$(cluster_read "Cluster snapshot count") || return 1
    [[ "$value" -gt "$1" ]]
}

properties()
{
    local aeron_dir=$1 cluster_dir=$2 archive_dir=$3
    mkdir -p "$aeron_dir" "$cluster_dir" "$archive_dir"

    cat >"$work/driver.properties" <<EOF
aeron.dir=$aeron_dir
aeron.threading.mode=DEDICATED
EOF
    if [[ "$arm" == media ]]; then
        cat >>"$work/driver.properties" <<EOF
deepmsg.dpdk.enabled=true
deepmsg.dpdk.eal.args=-a $pci
deepmsg.dpdk.port=$pci
deepmsg.dpdk.ipv4.address=$local_cidr
deepmsg.dpdk.static.neighbours=$peer_addr=$(peer_mac)
EOF
    fi

    cat >"$work/node.properties" <<EOF
aeron.dir=$aeron_dir
aeron.cluster.dir=$cluster_dir
aeron.archive.dir=$archive_dir
aeron.cluster.id=7
aeron.cluster.member.id=0
aeron.cluster.members=0,$local_addr:$ingress_port,$local_addr:$consensus_port,$local_addr:$log_port,$local_addr:$catchup_port,localhost:0
aeron.cluster.ingress.channel=aeron:udp?term-length=128k|alias=ingress|endpoint=$local_addr:$ingress_port
aeron.cluster.consensus.channel=aeron:udp?alias=consensus
aeron.cluster.log.channel=aeron:udp?endpoint=$local_addr:$log_port
aeron.cluster.replication.channel=aeron:udp
aeron.cluster.control.channel=aeron:ipc?term-length=128k
aeron.cluster.service.count=1
aeron.archive.replication.channel=aeron:udp?endpoint=localhost:0
aeron.cluster.authorisation.service.supplier=ALLOW_ALL
EOF

    cat >"$work/container.properties" <<EOF
aeron.dir=$aeron_dir
aeron.cluster.dir=$cluster_dir
aeron.cluster.id=7
aeron.cluster.service.id=0
aeron.cluster.service.name=clustered-service-7-0
deepmsg.cluster.service=echo
EOF
}

start_cluster()
{
    start node_pid "$work/node.log" \
        "$build/apps/deepmsg-cluster-node/deepmsg-cluster-node" "$work/node.properties"
    # The consensus module allocates its counters before it can go ACTIVE, and
    # it goes ACTIVE only once the service container has joined -- so the
    # container starts after the counters, and the container itself is respawned
    # if it exits early (the reference container does not retry the CM's
    # control publication; `xc2_single_node_test.cpp` does the same).
    wait_until 30 counters_exist ||
        { fail "node: the consensus module never allocated its counters"; return 1; }
    local attempt
    for attempt in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
        start container_pid "$work/container.log" \
            "$build/apps/deepmsg-clustered-service-container/deepmsg-clustered-service-container" \
            "$work/container.properties"
        sleep 0.3
        kill -0 "$container_pid" 2>/dev/null && break
        container_pid=
    done
    [[ -n "$container_pid" ]] || { fail "container: never stayed up"; return 1; }
    wait_until 45 state_is_active || { fail "cluster: never became ACTIVE"; return 1; }
}

echo_probe() # echo_probe <aeron-dir>; prints the probe's output, fails unless RECEIVED <count>
{
    local client_dir=$1 output
    if [[ -n "$client_ns" ]]; then
        output=$(ip netns exec "$client_ns" java "${JO[@]}" -cp "$jar:$work/classes" ClusterEchoClientProbe \
            "$client_dir" "$count" 60 "$count" "aeron:udp" "0=$local_addr:$ingress_port" \
            "aeron:udp?endpoint=$peer_addr:$egress_port" 2>&1)
    else
        output=$(java "${JO[@]}" -cp "$jar:$work/classes" ClusterEchoClientProbe \
            "$client_dir" "$count" 60 "$count" "aeron:udp" "0=$local_addr:$ingress_port" \
            "aeron:udp?endpoint=$peer_addr:$egress_port" 2>&1)
    fi
    printf '%s\n' "$output"
    grep -q "RECEIVED $count\$" <<<"$output"
}

start_client_driver()
{
    if [[ -n "$client_ns" ]]; then
        start client_driver_pid "$work/client-driver.log" \
            ip netns exec "$client_ns" java -Daeron.dir="$work/client-aeron" "${JO[@]}" -cp "$jar" \
            io.aeron.driver.MediaDriver
    else
        start client_driver_pid "$work/client-driver.log" \
            java -Daeron.dir="$work/client-aeron" "${JO[@]}" -cp "$jar" io.aeron.driver.MediaDriver
    fi
    wait_until 20 test -f "$work/client-aeron/cnc.dat" ||
        { fail "client: the Java media driver did not start"; tail -5 "$work/client-driver.log"; return 1; }
}

main()
{
    preflight
    peer_iface=$(resolve_peer_iface)
    [[ -n "$peer_iface" ]] || { echo "cannot name the peer port $peer_bdf" >&2; exit 2; }
    work=$(mktemp -d "/tmp/axc-e3-$arm-XXXXXX")
    trap cleanup EXIT
    say "E3 ($arm): $pci, local $local_cidr, peer $peer_cidr on $(peer_iface_name) [$work]"

    mkdir -p "$work/classes"
    javac -proc:none -cp "$jar" -d "$work/classes" \
        "$tests_root/deepmsg-tests/java/ClusterEchoClientProbe.java"

    nmcli device set "$(root_iface)" managed no 2>/dev/null || true
    nmcli device set "$(peer_iface_name)" managed no 2>/dev/null || true

    if [[ "$arm" == media ]]; then
        modprobe vfio-pci
        dpdk-devbind.py --bind=vfio-pci "$pci" >/dev/null
    else
        # A media run leaves the function on vfio with no netdev; a rebind is
        # asynchronous, so wait for the names before using them.
        if [[ "$(basename "$(readlink "/sys/bus/pci/devices/$pci/driver" 2>/dev/null)")" != ixgbe ]]; then
            dpdk-devbind.py --bind=ixgbe "$pci" >/dev/null
        fi
        wait_until 20 root_iface_ready && wait_until 20 peer_iface_ready ||
            { echo "the ixgbe netdevs did not appear" >&2; exit 1; }
        client_ns=$netns
        ip link set "$(root_iface)" up
        ip addr replace "$local_cidr" dev "$(root_iface)"
        ip link set "$(peer_iface_name)" up
        ip netns add "$netns"
        ip link set "$(peer_iface_name)" netns "$netns"
        ip -n "$netns" link set "$(peer_iface_name)" up
        ip -n "$netns" addr replace "$peer_cidr" dev "$(peer_iface_name)"
        wait_until 20 iface_has_carrier "$(peer_iface_name)" "$netns" || say "WARN peer link has no carrier yet"
        wait_until 20 iface_has_carrier "$(root_iface)" || say "WARN cluster link has no carrier yet"
    fi

    properties "$work/aeron" "$work/cluster" "$work/archive"

    start driver_pid "$work/driver.log" \
        "$build/apps/deepmsg-media-driver/deepmsg-media-driver" "$work/driver.properties"
    wait_until 30 test -f "$work/aeron/cnc.dat" ||
        { fail "driver: no CnC file"; tail -5 "$work/driver.log"; exit 1; }

    if [[ "$arm" == media ]]; then
        wait_until 30 grep -q "dpdk media on $pci" "$work/driver.log" ||
            { fail "media: did not open"; tail -10 "$work/driver.log"; exit 1; }
        # The PMD reset bounces the peer link, so the address goes on after the
        # media is up and only once the carrier is back.
        wait_until 20 iface_has_carrier "$(peer_iface_name)" || say "WARN peer link has no carrier yet"
        ip link set "$(peer_iface_name)" up
        ip addr replace "$peer_cidr" dev "$(peer_iface_name)"
        say "media: $(grep 'dpdk media on' "$work/driver.log" | tail -1)"
    fi

    start_client_driver || exit 1
    start_cluster || { tail -n 10 "$work/node.log" "$work/container.log"; exit 1; }

    # ---- criterion 1: the echo goes through, and something carried it ----
    local packets_before packets_after rx_before tx_before rx_after tx_after echo_output
    packets_before=$(counter_value "dpdk packets")
    read -r rx_before tx_before <<<"$(interface_packets "$(peer_iface_name)" "$client_ns")"

    if echo_output=$(echo_probe "$work/client-aeron"); then
        pass "echo: $count/$count answers ($arm arm)"
    else
        fail "echo: $(tail -3 <<<"$echo_output")"
    fi

    if [[ "$arm" == media ]]; then
        # `dpdk packets` is the port's `ipackets` sampled from the driver's
        # statistics (bindings.cpp:174), so it can lag the probe by a sample.
        wait_until 5 test "${packets_before:-0}" -lt "$(counter_value "dpdk packets")" || true
        packets_after=$(counter_value "dpdk packets")
        if [[ -n "$packets_before" && -n "$packets_after" && "$packets_after" -gt "$packets_before" ]]; then
            pass "media rx: dpdk packets $packets_before -> $packets_after"
        else
            fail "media rx: dpdk packets $packets_before -> ${packets_after:-<absent>} (the echo did not cross the card)"
        fi
        if [[ "$(counter_value "dpdk rx receiver discard")" == 0 ]]; then
            pass "media rx: no data-plane frame was discarded"
        else
            fail "media rx: dpdk rx receiver discard is not 0"
        fi
        pass "media: $(grep 'dpdk media on' "$work/driver.log" | tail -1)"
    else
        read -r rx_after tx_after <<<"$(interface_packets "$(peer_iface_name)" "$client_ns")"
        if [[ -n "$rx_after" && -n "$rx_before" && $((rx_after + tx_after)) -gt $((rx_before + tx_before)) ]]; then
            pass "kernel: $(peer_iface_name) $((rx_after + tx_after - rx_before - tx_before)) packets during the echo"
        else
            fail "kernel: no packets crossed $(peer_iface_name)"
        fi
        if counter_dump | grep -q '^dpdk '; then
            fail "kernel: the driver allocated dpdk counters"
        else
            pass "kernel: the same driver binary has no dpdk counters"
        fi
    fi

    # ---- criterion 2: a snapshot, then a restart that replays ----
    local snapshots_before term_before term_after
    snapshots_before=$(cluster_read "Cluster snapshot count" || echo 0)
    term_before=$(cluster_read "Cluster leadership term id" || echo 0)
    cluster_toggle 4
    if wait_until 30 snapshot_above "$snapshots_before"; then
        pass "snapshot: count $snapshots_before -> $(cluster_read "Cluster snapshot count")"
    else
        fail "snapshot: the count did not move"
    fi

    cluster_toggle 5
    if wait_pid "$node_pid" 30; then
        pass "shutdown: the node exited 0"
    else
        fail "shutdown: the node did not exit 0 within 30 s"
    fi
    if wait_pid "$container_pid" 30; then
        pass "shutdown: the container exited 0"
    else
        fail "shutdown: the container did not exit 0 within 30 s"
    fi
    node_pid= container_pid=

    start_cluster || { tail -n 10 "$work/node.log" "$work/container.log"; exit 1; }
    term_after=$(cluster_read "Cluster leadership term id" || echo -1)
    if [[ "$term_after" -gt "$term_before" ]]; then
        pass "restart: leadership term $term_before -> $term_after (the log was replayed)"
    else
        fail "restart: leadership term stayed at $term_after"
    fi
    if echo_output=$(echo_probe "$work/client-aeron"); then
        pass "restart: echo $count/$count after the restart"
    else
        fail "restart: $(tail -3 <<<"$echo_output")"
    fi

    # ---- criterion 3: the last shutdown and the driver's close ----
    cluster_toggle 5
    if wait_pid "$node_pid" 30 && wait_pid "$container_pid" 30; then
        pass "termination: node and container exited 0"
    else
        fail "termination: node or container did not exit 0"
    fi
    node_pid= container_pid=

    kill -TERM "$driver_pid"
    if wait_pid "$driver_pid" 30; then
        pass "termination: the driver exited 0"
    else
        fail "termination: the driver did not exit 0"
    fi
    driver_pid=
    if [[ "$arm" == media ]]; then
        if grep -q "dpdk media closed" "$work/driver.log"; then
            pass "termination: the media says it closed"
        else
            fail "termination: no 'dpdk media closed' line"
        fi
    fi
    if grep -qi "cannot close started device" "$work/driver.log"; then
        fail "termination: 'Cannot close started device' in the driver log"
    else
        pass "termination: no 'Cannot close started device'"
    fi

    say ""
    if [[ "$failures" -eq 0 ]]; then
        say "E3 $arm: all criteria passed ($work)"
        exit 0
    fi
    say "E3 $arm: $failures criterion(s) failed ($work)"
    exit 1
}

main
