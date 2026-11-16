#!/usr/bin/env bash
# T4: both ends of the cable are our media driver.
#
#   sudo bench/dpdk-t4/run-t4.sh
#
#   AXC_DPDK_BENCH_PCI=0000:03:00.0         A's port   (default)
#   AXC_DPDK_BENCH_PEER_PCI=0000:03:00.1    B's port   (default)
#   AXC_DPDK_BENCH_LOCAL=10.10.0.1/24       A's address (default)
#   AXC_DPDK_BENCH_PEER_LOCAL=10.10.0.2/24  B's address (default)
#   AXC_BUILD_DIR=build/dpdk-release        Release, AXC_WITH_DPDK=ON
#
# Binds both ports of the cabled pair to vfio-pci, runs the T4 case with the
# environment it names, and puts both ports back on ixgbe in a trap. The test
# asserts the criteria (`analysis/cpp/plan-dpdk-t4.md` §3); this script only
# puts the machine in the shape the test needs, records what that shape was,
# and keeps the evidence together.
#
# The P6 rig (`bench/dpdk-p6/`) must not run at the same time: its peer is the
# second port, the one this script takes for B.
set -euo pipefail

t4_repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
t4_build=${AXC_BUILD_DIR:-$t4_repo/build/dpdk-release}
t4_test=$t4_build/test/interop/axc_interop_tests
t4_driver_bin=$t4_build/apps/deepmsg-media-driver/deepmsg-media-driver
t4_pci_a=${AXC_DPDK_BENCH_PCI:-0000:03:00.0}
t4_pci_b=${AXC_DPDK_BENCH_PEER_PCI:-0000:03:00.1}
t4_local_a=${AXC_DPDK_BENCH_LOCAL:-10.10.0.1/24}
t4_local_b=${AXC_DPDK_BENCH_PEER_LOCAL:-10.10.0.2/24}
t4_test_cpus=${AXC_DPDK_T4_CPUSET_TEST:-6-7}
t4_work=
t4_bound=(0 0)
t4_iface_a=
t4_iface_b=

say() { printf '%s\n' "$*"; }
fail() { printf 'FAIL %s\n' "$*" >&2; exit 1; }

iface_of() # iface_of <pci>
{
    cat "/sys/bus/pci/devices/$1/net/"*/uevent 2>/dev/null | sed -n 's/^INTERFACE=//p' | head -1
}

driver_of() # driver_of <pci>
{
    basename "$(readlink "/sys/bus/pci/devices/$1/driver" 2>/dev/null)" 2>/dev/null || true
}

port_has_no_kernel_interface() # port_has_no_kernel_interface <pci>
{
    local entries
    entries=$(ls "/sys/bus/pci/devices/$1/net" 2>/dev/null | head -1)
    [[ -z "$entries" ]]
}

preflight()
{
    [[ $(id -u) -eq 0 ]] || fail "run as root (vfio-pci and the media's huge pages need it)"
    [[ -x "$t4_test" ]] || fail "missing $t4_test (build it, or set AXC_BUILD_DIR)"
    [[ -x "$t4_driver_bin" ]] || fail "missing $t4_driver_bin (a build with -DAXC_WITH_DPDK=ON)"
    command -v dpdk-devbind.py >/dev/null || fail "dpdk-devbind.py not found"
    command -v ethtool >/dev/null || fail "ethtool not found"

    local pci
    for pci in "$t4_pci_a" "$t4_pci_b"; do
        [[ -d "/sys/bus/pci/devices/$pci" ]] || fail "no PCI device $pci"
        local current_driver
        current_driver=$(driver_of "$pci")
        [[ "$current_driver" == "ixgbe" ]] ||
            fail "$pci is on '${current_driver:-no driver}' — put both ports back on ixgbe first"
        local iface
        iface=$(iface_of "$pci")
        [[ -n "$iface" ]] || fail "cannot name the interface of $pci"
        ethtool "$iface" 2>/dev/null | grep -q 'Link detected: yes' ||
            fail "$iface has no link — is the Cat6a between $t4_pci_a and $t4_pci_b?"
        if [[ "$pci" == "$t4_pci_a" ]]; then t4_iface_a=$iface; else t4_iface_b=$iface; fi
    done

    if ip -o addr show | grep -qE 'inet 10\.10\.0\.[0-9]+/'; then
        fail "a 10.10.0.x address is already on a kernel interface; T4 needs neither end in the kernel"
    fi

    local port
    for port in 24700 24701 24702 24703; do
        if ss -lun 2>/dev/null | awk -v port=":$port\$" '$4 ~ port {found=1} END {exit !found}'; then
            fail "UDP port $port is already in use"
        fi
    done

    local free_pages
    free_pages=$(awk '/HugePages_Free/ {print $2}' /proc/meminfo)
    [[ -n "$free_pages" && "$free_pages" -gt 0 ]] || fail "no free huge pages (two EALs need them)"
}

build_type()
{
    sed -n 's/^CMAKE_BUILD_TYPE:STRING=//p' "$t4_build/CMakeCache.txt" 2>/dev/null | head -1
}

write_run_info()
{
    {
        echo "run=$t4_work"
        echo "date=$(date -Is)"
        echo "host=$(hostname)"
        echo "kernel=$(uname -r)"
        echo "build_dir=$t4_build"
        echo "build_type=$(build_type)"
        echo "media_sha256=$(sha256sum "$t4_driver_bin" | cut -d' ' -f1)"
        echo "test_sha256=$(sha256sum "$t4_test" | cut -d' ' -f1)"
        echo "a_pci=$t4_pci_a a_iface=$t4_iface_a a_address=$t4_local_a"
        echo "b_pci=$t4_pci_b b_iface=$t4_iface_b b_address=$t4_local_b"
        echo "iommu_group_a=$(basename "$(readlink "/sys/bus/pci/devices/$t4_pci_a/iommu_group")")"
        echo "iommu_group_b=$(basename "$(readlink "/sys/bus/pci/devices/$t4_pci_b/iommu_group")")"
        echo "cpuset_a=${AXC_DPDK_T4_CPUSET_A:-0-2} cpuset_b=${AXC_DPDK_T4_CPUSET_B:-3-5} cpuset_test=$t4_test_cpus"
        echo "hugepages_total=$(awk '/HugePages_Total/ {print $2}' /proc/meminfo) hugepages_free=$(awk '/HugePages_Free/ {print $2}' /proc/meminfo)"
        echo "git=$(git -C "$t4_repo" rev-parse --short HEAD 2>/dev/null) dirty=$(git -C "$t4_repo" status --porcelain | wc -l)"
    } >"$t4_work/RUN-INFO.txt"
}

bind_ports()
{
    local pci
    for pci in "$t4_pci_a" "$t4_pci_b"; do
        local iface
        iface=$(iface_of "$pci")
        nmcli device set "$iface" managed no 2>/dev/null || true
        ip link set "$iface" down
    done
    dpdk-devbind.py --bind=vfio-pci "$t4_pci_a" "$t4_pci_b"
    t4_bound=(1 1)

    for pci in "$t4_pci_a" "$t4_pci_b"; do
        port_has_no_kernel_interface "$pci" ||
            fail "$pci still has a kernel interface after binding; refusing to run with a possible kernel path"
    done
}

run_case()
{
    set +e
    taskset -c "$t4_test_cpus" env \
        AXC_DPDK_BENCH_PCI="$t4_pci_a" AXC_DPDK_BENCH_LOCAL="$t4_local_a" \
        AXC_DPDK_BENCH_PEER_PCI="$t4_pci_b" AXC_DPDK_BENCH_PEER_LOCAL="$t4_local_b" \
        AXC_DPDK_T4_CPUSET_A="${AXC_DPDK_T4_CPUSET_A:-0-2}" \
        AXC_DPDK_T4_CPUSET_B="${AXC_DPDK_T4_CPUSET_B:-3-5}" \
        AXC_DPDK_T4_COUNT="${AXC_DPDK_T4_COUNT:-1000}" \
        AXC_DPDK_T4_WORK="$t4_work/drivers" \
        "$t4_test" --gtest_filter='DpdkTwoDrivers.*' 2>&1 | tee "$t4_work/test.log"
    local status=${PIPESTATUS[0]}
    set -e
    return "$status"
}

leftover_drivers()
{
    # `comm` is truncated to 15 characters by Linux, so `pgrep -x
    # deepmsg-media-driver` matches nothing (found the hard way); `pgrep -f`
    # would match the shell that runs it. Scan /proc and compare the exact
    # truncated name instead.
    local entry pid comm
    for entry in /proc/[0-9]*; do
        [[ -r "$entry/comm" ]] || continue
        comm=$(cat "$entry/comm" 2>/dev/null) || continue
        if [[ "$comm" == "deepmsg-media-d" || "$comm" == "deepmsg-media-driver" ]]; then
            pid=${entry#/proc/}
            printf '%s\n' "$pid"
        fi
    done
}

teardown()
{
    set +e
    # The test terminates its own children; this catches the ones a killed test
    # leaves behind. Never `pkill -f`: the pattern matches the command running it.
    local pid
    for pid in $(leftover_drivers); do
        kill -TERM "$pid" 2>/dev/null
    done
    sleep 1
    for pid in $(leftover_drivers); do
        kill -KILL "$pid" 2>/dev/null
    done

    local pci
    for pci in "$t4_pci_a" "$t4_pci_b"; do
        dpdk-devbind.py --bind=ixgbe "$pci" >/dev/null 2>&1
        local iface
        iface=$(iface_of "$pci")
        if [[ -n "$iface" ]]; then
            ip link set "$iface" up 2>/dev/null
            nmcli device set "$iface" managed yes 2>/dev/null
        fi
    done
    rm -f /dev/hugepages/rtemap_* 2>/dev/null
    rm -rf /var/run/dpdk 2>/dev/null
    set -e
}

main()
{
    preflight
    t4_work=$(mktemp -d "/tmp/axc-t4-run-XXXXXX")
    trap teardown EXIT
    write_run_info
    say "work=$t4_work (RUN-INFO.txt, test.log, drivers/)"

    bind_ports
    say "bound $t4_pci_a and $t4_pci_b to vfio-pci; no kernel interface on either"

    local status=0
    run_case || status=$?

    write_run_info
    if [[ "$status" -eq 0 ]]; then
        say "PASS T4: both ends of the cable were our media driver"
    else
        say "FAIL T4: see $t4_work/test.log (and $t4_work/drivers/*/driver.log)"
    fi
    say "evidence kept in $t4_work — copy it out before the next run"
    return "$status"
}

main "$@"
