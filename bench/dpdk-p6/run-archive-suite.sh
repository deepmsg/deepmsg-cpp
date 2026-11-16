#!/usr/bin/env bash
# P6 / I6: the archive interop suite on both arms, compared case by case.
#
#   sudo bench/dpdk-p6/run-archive-suite.sh
#
# Both arms run `axc_archive_interop_tests` with our driver as the media driver
# (`AXC_MEDIA_DRIVER`); the media arm adds `DEEPMSG_DPDK_*`, and the kernel arm
# does not. The criterion is not "everything passes" but "the media arm passes
# what the kernel arm passes": the per-test results are compared and any
# difference is a failure with the two results named.
set -euo pipefail

p6_repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
# shellcheck source=/dev/null
source "$p6_repo/bench/dpdk-p6/lib.sh"

tests_root=${AXC_AERON_TESTS:-}
suite=$p6_build/test/interop/axc_archive_interop_tests

failures=0
say() { printf '%s\n' "$*"; }
fail() { printf 'FAIL %s\n' "$*"; failures=$((failures + 1)); }
pass() { printf 'PASS %s\n' "$*"; }

run_arm() # run_arm <media|kernel> <json-out>
{
    local arm=$1 out=$2
    local -a env_args=(env "AXC_MEDIA_DRIVER=$p6_build/apps/deepmsg-media-driver/deepmsg-media-driver")
    if [[ -n "$tests_root" ]]; then
        env_args+=("AXC_AERON_TESTS=$tests_root")
    fi
    if [[ "$arm" == media ]]; then
        env_args+=("DEEPMSG_DPDK_ENABLED=true" "DEEPMSG_DPDK_EAL_ARGS=-a $p6_pci" "DEEPMSG_DPDK_PORT=$p6_pci"
                   "DEEPMSG_DPDK_IPV4_ADDRESS=$p6_local_cidr")
    fi
    "${env_args[@]}" "$suite" "--gtest_output=json:$out" >"${out%.json}.log" 2>&1 || true
}

main()
{
    p6_preflight
    p6_peer_iface=$(p6_resolve_peer_iface)
    [[ -n "$p6_peer_iface" ]] || { echo "cannot name the peer port" >&2; exit 2; }
    [[ -x "$suite" ]] || { echo "missing $suite" >&2; exit 2; }
    p6_work=$(mktemp -d "/tmp/axc-p6-suite-XXXXXX")
    trap p6_teardown EXIT
    p6_setup_peer
    say "P6 archive suite: $suite [$p6_work]"

    p6_setup_ours kernel
    run_arm kernel "$p6_work/kernel.json"
    say "kernel arm done: $(python3 -c "
import json,sys
d=json.load(open('$p6_work/kernel.json'))
n=sum(len(s.get('testsuite',[])) for s in d.get('testsuites',[d]))
print(n,'cases')" 2>/dev/null || echo '?')"

    p6_setup_ours media
    run_arm media "$p6_work/media.json"
    sleep 2

    python3 - "$p6_work/kernel.json" "$p6_work/media.json" <<'PY'
import json, sys

def results(path):
    out = {}
    try:
        data = json.load(open(path))
    except Exception as error:  # a crashed binary leaves no JSON
        return None, str(error)
    suites = data.get('testsuites', [data])
    for suite in suites:
        for case in suite.get('testsuite', []):
            name = suite.get('name', '') + '.' + case.get('name', '')
            skipped = case.get('status', '').upper() in ('NOTRUN', 'SKIPPED') or bool(case.get('skipped'))
            failed = bool(case.get('failures')) or case.get('result', '').upper() == 'FAILED'
            out[name] = 'skipped' if skipped else ('failed' if failed else 'passed')
    return out, None

kernel, kerr = results(sys.argv[1])
media, merr = results(sys.argv[2])
if kernel is None or media is None:
    print('FAIL one arm produced no JSON: kernel=%s media=%s' % (kerr, merr))
    sys.exit(1)

passed = sum(1 for s in kernel.values() if s == 'passed')
print('kernel: %d cases (%d passed)' % (len(kernel), passed))
print('media:  %d cases (%d passed)' % (len(media), sum(1 for s in media.values() if s == 'passed')))
diff = 0
for name in sorted(set(kernel) | set(media)):
    k = kernel.get(name, 'missing')
    m = media.get(name, 'missing')
    if k != m:
        print('DIFF %s: kernel %s / media %s' % (name, k, m))
        diff += 1
if diff:
    print('FAIL %d case(s) differ between the arms' % diff)
    sys.exit(1)
print('PASS the two arms agree on all %d cases' % len(kernel))
PY
    local status=$?
    say ""
    if [[ "$status" -eq 0 ]]; then
        say "P6 archive suite: the arms agree ($p6_work)"
        exit 0
    fi
    say "P6 archive suite: the arms differ ($p6_work)"
    exit 1
}

main
