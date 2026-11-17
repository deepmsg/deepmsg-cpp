#!/usr/bin/env bash
# The media arm's driver, behind the Java harness's
# `-Daeron.test.system.aeronmd.path`.
#
# `CTestMediaDriver` launches the binary with **no arguments** and its settings
# in the environment (`AERON_DIR` and friends), so the media is configured here,
# from the environment, and every driver gets its own pcap and log keyed by
# `AERON_DIR` (a test may start several drivers at once; one vfio port could not
# serve them, a vdev can).
#
# The harness's own `AERON_THREADING_MODE` is **passed through**, not replaced:
# the media driver serves every mode the reference does, so the media arm runs
# the configuration the suite asks for and the diff against the untouched
# kernel arm is "the media", nothing else. The requested value is written into
# the log before the driver's own output, which is how the arm says which modes
# the suite actually used.
#
#   AXC_T6_DRIVER   the deepmsg-media-driver binary (required)
#   AXC_T6_OUT      where the logs and pcaps go (default /tmp/axc-t6-drivers)
set -euo pipefail

driver=${AXC_T6_DRIVER:?set AXC_T6_DRIVER to the deepmsg-media-driver binary}
out=${AXC_T6_OUT:-/tmp/axc-t6-drivers}
key=$(printf '%s' "${AERON_DIR:-unknown}" | md5sum | cut -c1-8)
mkdir -p "$out"

# The log must live **outside** `AERON_DIR`: the reference driver deletes an
# existing aeron directory on start and the open log would be unlinked.
#
# One variable feeds both the reading and the exec, so a wrapper that forces a
# mode cannot report a different one than the driver got.
effective_mode=${AERON_THREADING_MODE:-}
mode_env=()
[[ -n "$effective_mode" ]] && mode_env+=("AERON_THREADING_MODE=$effective_mode")
printf 'wrapper: AERON_THREADING_MODE=%s\n' "${effective_mode:-<unset>}" \
    >"$out/driver-$key.log"
exec env "${mode_env[@]}" \
    DEEPMSG_DPDK_ENABLED=true \
    "DEEPMSG_DPDK_EAL_ARGS=--no-huge --no-shconf --no-telemetry --vdev=net_pcap0,tx_pcap=$out/driver-$key.pcap" \
    DEEPMSG_DPDK_PORT=net_pcap0 \
    DEEPMSG_DPDK_IPV4_ADDRESS=10.10.0.1/24 \
    "$driver" "$@" >>"$out/driver-$key.log" 2>&1
