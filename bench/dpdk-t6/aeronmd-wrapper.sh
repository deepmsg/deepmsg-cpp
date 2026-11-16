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
# `AERON_THREADING_MODE` is forced to DEDICATED: the launcher runs DEDICATED
# only, and the reference harness asks for SHARED in most classes. The
# `kernel-dedicated` arm measures what that forcing alone changes; this arm must
# be compared with that one, not with the untouched kernel arm.
printf 'wrapper: AERON_THREADING_MODE=DEDICATED (requested %s)\n' "${AERON_THREADING_MODE:-<unset>}" \
    >"$out/driver-$key.log"
exec env AERON_THREADING_MODE=DEDICATED \
    DEEPMSG_DPDK_ENABLED=true \
    "DEEPMSG_DPDK_EAL_ARGS=--no-huge --no-shconf --no-telemetry --vdev=net_pcap0,tx_pcap=$out/driver-$key.pcap" \
    DEEPMSG_DPDK_PORT=net_pcap0 \
    DEEPMSG_DPDK_IPV4_ADDRESS=10.10.0.1/24 \
    "$driver" "$@" >>"$out/driver-$key.log" 2>&1
