#!/usr/bin/env bash
# The kernel-dedicated arm's driver: the reference `aeronmd`, with the C driver
# forced to DEDICATED — the control that separates "the harness asked for
# SHARED" from "the media is on".
#
# The reference harness configures most classes as `AERON_THREADING_MODE=SHARED`
# and `aeronmd` honours it; our launcher runs DEDICATED only, so the media arm
# forces DEDICATED and this arm measures what that forcing alone changes.
#
#   AXC_T6_KERNEL_DRIVER  the reference aeronmd (required)
#   AXC_T6_OUT            where the logs go (default /tmp/axc-t6-drivers/kernel)
set -euo pipefail

binary=${AXC_T6_KERNEL_DRIVER:?set AXC_T6_KERNEL_DRIVER to the reference aeronmd}
out=${AXC_T6_OUT:-/tmp/axc-t6-drivers/kernel}
key=$(printf '%s' "${AERON_DIR:-unknown}" | md5sum | cut -c1-8)
mkdir -p "$out"

printf 'wrapper: AERON_THREADING_MODE=DEDICATED (requested %s)\n' "${AERON_THREADING_MODE:-<unset>}" \
    >"$out/driver-$key.log"
exec env AERON_THREADING_MODE=DEDICATED "$binary" "$@" >>"$out/driver-$key.log" 2>&1
