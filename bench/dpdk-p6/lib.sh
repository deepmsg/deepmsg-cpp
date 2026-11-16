#!/usr/bin/env bash
# Shared rig for the P6 runs: the bench pair, one media port, one kernel peer.
#
#   03:00.0  our driver's port (vfio on the media arm, ixgbe on the kernel arm)
#   03:00.1  the peer, kernel, 10.10.0.2 -- the reference aeronmd lives here
#
# Sourced by run-interop.sh and run-perf.sh. Every function assumes root (the
# callers check), and `p6_teardown` is what the callers' EXIT trap runs.
# shellcheck shell=bash

p6_repo=${p6_repo:?set p6_repo}
p6_build=${AXC_BUILD_DIR:-$p6_repo/build/dpdk}
p6_jar=${AXC_AERON_JAR:-$p6_repo/third_party/aeron/aeron-all/build/libs/aeron-all-1.54.0.jar}
p6_pci=${AXC_DPDK_BENCH_PCI:-0000:03:00.0}
p6_peer_bdf=$(printf '%s' "$p6_pci" | sed -E 's/([0-9a-f]{2}:[0-9a-f]{2})\.[0-9a-f]$/\1.1/')
p6_local_cidr=${AXC_DPDK_BENCH_LOCAL:-10.10.0.1/24}
p6_peer_cidr=${AXC_DPDK_BENCH_PEER:-10.10.0.2/24}
p6_local_addr=${p6_local_cidr%%/*}
p6_peer_addr=${p6_peer_cidr%%/*}
p6_java_opens=("--add-opens" "java.base/jdk.internal.misc=ALL-UNNAMED" "--add-opens" "java.base/sun.nio.ch=ALL-UNNAMED")

p6_work=
p6_peer_iface=
p6_netns=p6-peer
p6_peer_in_netns=0
p6_ours_pid=
p6_ref_pid=

p6_say() { printf '%s\n' "$*"; }
p6_root_iface()
{
    cat "/sys/bus/pci/devices/$p6_pci"/net/*/uevent 2>/dev/null | sed -n 's/^INTERFACE=//p' | head -1
}
p6_resolve_peer_iface()
{
    cat "/sys/bus/pci/devices/$p6_peer_bdf"/net/*/uevent 2>/dev/null | sed -n 's/^INTERFACE=//p' | head -1
}
p6_peer_iface_name() { printf '%s' "$p6_peer_iface"; }
p6_peer_mac() { cat "/sys/bus/pci/devices/$p6_peer_bdf/net/$(p6_peer_iface_name)/address"; }

p6_preflight()
{
    [[ $(id -u) -eq 0 ]] || { echo "run as root" >&2; exit 2; }
    for binary in deepmsg-media-driver; do
        [[ -x "$p6_build/apps/$binary/$binary" ]] || { echo "missing $p6_build/apps/$binary/$binary" >&2; exit 2; }
    done
    [[ -x "$p6_build/aeron/binaries/aeronmd" ]] || { echo "missing $p6_build/aeron/binaries/aeronmd" >&2; exit 2; }
    [[ -f "$p6_jar" ]] || { echo "missing aeron-all jar: $p6_jar (set AXC_AERON_JAR)" >&2; exit 2; }
    command -v java >/dev/null || { echo "java not found" >&2; exit 2; }
    command -v gcc >/dev/null || { echo "gcc not found" >&2; exit 2; }
    [[ -d "/sys/bus/pci/devices/$p6_pci" ]] || { echo "no PCI device $p6_pci" >&2; exit 2; }
    [[ -d "/sys/bus/pci/devices/$p6_peer_bdf" ]] || { echo "no peer PCI device $p6_peer_bdf" >&2; exit 2; }
}

# What the driver under test was compiled as.
#
# 2026-11-15: the whole DPDK line was measured on a Debug (`-O0`) build by
# accident -- this rig's default build directory is `build/dpdk`, and the
# `dpdk` preset is Debug. A cell's numbers are only comparable with cells of
# the same build type (our own per-frame cost moved 4.3x between the two), so
# every cell records it, and a Debug arm says so out loud.
p6_build_type()
{
    sed -n 's/^CMAKE_BUILD_TYPE:STRING=//p' "$p6_build/CMakeCache.txt" 2>/dev/null | head -1
}

# p6_build_info <file>: what this cell was measured with.
p6_build_info()
{
    local file=$1
    {
        echo "build_dir=$p6_build"
        echo "build_type=$(p6_build_type)"
        echo "segments=$(sed -n 's/^AXC_DPDK_SEGMENTS:BOOL=//p' "$p6_build/CMakeCache.txt" 2>/dev/null | head -1)"
        echo "media_sha256=$(sha256sum "$p6_build/apps/deepmsg-media-driver/deepmsg-media-driver" 2>/dev/null | cut -d' ' -f1)"
        echo "aeronmd_sha256=$(sha256sum "$p6_build/aeron/binaries/aeronmd" 2>/dev/null | cut -d' ' -f1)"
        grep -h '^CXX_FLAGS' "$p6_build/apps/deepmsg-media-driver/CMakeFiles/deepmsg-media-driver.dir/flags.make" 2>/dev/null |
            head -1 | sed 's/^/media_/'
    } >"$file"
}

# A Debug arm is allowed -- the build-type card measures it -- but it must not
# pass for a product reading.
p6_warn_if_debug()
{
    local type
    type=$(p6_build_type)
    [[ "$type" == "Release" ]] && return 0
    p6_say "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    p6_say "!! build type is '${type:-unknown}' in $p6_build"
    p6_say "!! not comparable with Release readings (2026-11-15: the S4b/S2c"
    p6_say "!! 1.3/0.78 us that started a card were this build type's -O0)"
    p6_say "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
}

# p6_peer_counters <file>: the peer NIC's counters, taken while the drivers are
# stopped. Comparing a cell's delta with our own port's opackets says whether a
# frame our driver counted as sent ever left the NIC (2026-11-16: the NAK hunt).
p6_peer_counters()
{
    local file=$1
    ip netns exec "$p6_netns" ethtool -S "$(p6_peer_iface_name)" 2>/dev/null |
        grep -E ' (rx_packets|rx_errors|rx_crc_errors|rx_no_buffer_count|rx_missed_errors|rx_over_errors|rx_fifo_errors|rx_length_errors|rx_dropped|tx_dropped):' >"$file"
}

p6_compile_probes()
{
    local probes=$p6_repo/bench/dpdk-p6/probes
    local out=$p6_work/bin
    mkdir -p "$out"
    local probe
    for probe in echo send recv ping; do
        gcc -std=gnu11 -O2 -I "$p6_repo/third_party/aeron/aeron-client/src/main/c" \
            -o "$out/$probe" "$probes/$probe.c" \
            "$p6_build/aeron/lib/libaeron_static.a" -lpthread -lrt -lm -lbsd -luuid -ldl ||
            { echo "probe $probe did not build" >&2; exit 1; }
    done
}

# p6_setup_peer: the kernel side of the cable, in its own netns.
#
# The netns is not decoration: with both addresses in the host's netns the
# kernel's route to the peer is `local ... dev lo`, so the kernel arm never
# touches the cable (E4 voided two rounds on exactly this). The reference
# driver and its clients run inside the namespace, on both arms, so the far
# side is identical.
p6_setup_peer()
{
    nmcli device set "$(p6_peer_iface_name)" managed no 2>/dev/null || true
    ip link set "$(p6_peer_iface_name)" up
    ip netns add "$p6_netns"
    ip link set "$(p6_peer_iface_name)" netns "$p6_netns"
    ip -n "$p6_netns" link set "$(p6_peer_iface_name)" up
    ip -n "$p6_netns" addr replace "$p6_peer_cidr" dev "$(p6_peer_iface_name)"
    p6_peer_in_netns=1
    local deadline=$((SECONDS + 20))
    while ((SECONDS < deadline)); do
        if ip -n "$p6_netns" link show "$(p6_peer_iface_name)" | grep -q LOWER_UP; then
            break
        fi
        sleep 0.2
    done
}

# Run a command on the far side (inside the peer namespace when there is one).
p6_on_peer()
{
    if [[ "$p6_peer_in_netns" == 1 ]]; then
        ip netns exec "$p6_netns" "$@"
    else
        "$@"
    fi
}

# p6_setup_ours <media|kernel>
p6_setup_ours()
{
    local arm=$1
    nmcli device set "$(p6_root_iface)" managed no 2>/dev/null || true
    if [[ "$arm" == media ]]; then
        modprobe vfio-pci
        # dpdk-devbind refuses an interface the routing table says is in use;
        # a previous kernel-arm cell left the address on it.
        if [[ -n "$(p6_root_iface)" ]]; then
            ip link set "$(p6_root_iface)" down 2>/dev/null || true
            ip addr flush dev "$(p6_root_iface)" 2>/dev/null || true
        fi
        dpdk-devbind.py --bind=vfio-pci "$p6_pci" >/dev/null
    else
        if [[ "$(basename "$(readlink "/sys/bus/pci/devices/$p6_pci/driver" 2>/dev/null)")" != ixgbe ]]; then
            dpdk-devbind.py --bind=ixgbe "$p6_pci" >/dev/null
        fi
        local deadline=$((SECONDS + 20))
        while ((SECONDS < deadline)) && [[ -z "$(p6_root_iface)" ]]; do sleep 0.2; done
        ip link set "$(p6_root_iface)" up
        ip addr replace "$p6_local_cidr" dev "$(p6_root_iface)"
    fi
}

p6_start() # p6_start <pid-var> <log> <command...>
{
    local name=$1 log=$2
    shift 2
    "$@" >"$log" 2>&1 &
    printf -v "$name" '%s' "$!"
}

p6_wait_until() # p6_wait_until <seconds> <command...>
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

p6_wait_pid() # p6_wait_pid <pid> <seconds>
{
    local pid=$1 deadline=$((SECONDS + $2))
    while kill -0 "$pid" 2>/dev/null; do
        ((SECONDS >= deadline)) && return 124
        sleep 0.1
    done
    wait "$pid"
}

# p6_start_ours <arm> <aeron-dir> [extra properties file]
p6_start_ours()
{
    local arm=$1 aeron_dir=$2 extras=${3:-}
    mkdir -p "$aeron_dir"
    {
        echo "aeron.dir=$aeron_dir"
        echo "aeron.threading.mode=DEDICATED"
        if [[ "$arm" == media ]]; then
            echo "deepmsg.dpdk.enabled=true"
            echo "deepmsg.dpdk.eal.args=-a $p6_pci"
            echo "deepmsg.dpdk.port=$p6_pci"
            echo "deepmsg.dpdk.ipv4.address=$p6_local_cidr"
        fi
        [[ -n "$extras" ]] && cat "$extras"
    } >"$p6_work/ours.properties"
    local cell_dir=${aeron_dir%/*}
    local log="$cell_dir/ours.log"
    p6_start p6_ours_pid "$log" "$p6_build/apps/deepmsg-media-driver/deepmsg-media-driver" \
        "$p6_work/ours.properties"
    p6_wait_until 30 test -f "$aeron_dir/cnc.dat" || { echo "our driver did not start" >&2; tail -5 "$log"; exit 1; }
    if [[ "$arm" == media ]]; then
        p6_wait_until 60 grep -q "dpdk media on" "$log" ||
            { echo "the media did not open" >&2; tail -10 "$log"; exit 1; }
    fi
}

p6_start_reference()
{
    local aeron_dir=$1
    mkdir -p "$aeron_dir"
    local cell_dir=${aeron_dir%/*}
    local log="$cell_dir/reference.log"
    if [[ "$p6_peer_in_netns" == 1 ]]; then
        p6_start p6_ref_pid "$log" ip netns exec "$p6_netns" "$p6_build/aeron/binaries/aeronmd" \
            "-Daeron.dir=$aeron_dir"
    else
        p6_start p6_ref_pid "$log" "$p6_build/aeron/binaries/aeronmd" "-Daeron.dir=$aeron_dir"
    fi
    p6_wait_until 30 test -f "$aeron_dir/cnc.dat" ||
        { echo "the reference driver did not start" >&2; tail -5 "$log"; exit 1; }
}

p6_ours_cnc() { printf '%s' "${1}/cnc.dat"; }

p6_counter() # p6_counter <cnc> <label>
{
    java "${p6_java_opens[@]}" -cp "$p6_jar" "$p6_repo/bench/tools/CounterDump.java" "$1" 2>/dev/null |
        awk -F'\t' -v label="$2" '$1 == label {print $2; exit}'
}

p6_teardown()
{
    set +e
    for pid in "$p6_ref_pid" "$p6_ours_pid"; do
        [[ -n "$pid" ]] && kill -TERM "$pid" 2>/dev/null
    done
    sleep 1
    pkill -f "$p6_work/bin/" 2>/dev/null
    if [[ "$p6_peer_in_netns" == 1 ]] && ip netns list 2>/dev/null | grep -q "^$p6_netns"; then
        ip -n "$p6_netns" link set "$(p6_peer_iface_name)" netns 1 2>/dev/null
        ip netns del "$p6_netns" 2>/dev/null
    fi
    [[ -n "$p6_peer_iface" ]] && ip addr del "$p6_peer_cidr" dev "$p6_peer_iface" 2>/dev/null
    ip addr del "$p6_local_cidr" dev "$(p6_root_iface)" 2>/dev/null
    nmcli device set "$(p6_root_iface)" managed yes 2>/dev/null
    nmcli device set "$(p6_peer_iface_name)" managed yes 2>/dev/null
    dpdk-devbind.py --bind=ixgbe "$p6_pci" >/dev/null 2>&1
    rm -f /dev/hugepages/rtemap_* 2>/dev/null
    set -e
}
