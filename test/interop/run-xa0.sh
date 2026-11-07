#!/usr/bin/env bash
# XA-0: run the reference Java archive system tests on the C media driver and
# compare the result with the Java driver baseline.
#
# The switch is `aeron.test.system.aeronmd.path` (build.gradle forwards it to
# the test JVM; TestMediaDriver uses CTestMediaDriver when it is non-empty).
#
# Environment:
#   AXC_AERONMD       path to aeronmd (default build/debug/aeron/binaries/aeronmd)
#   AXC_XA0_OUT       output directory (default /tmp/axc-xa0)
#   AXC_XA0_TEST_CLASS test class (default io.aeron.archive.ArchiveTest)
#
# Exit codes: 0 = identical pass/fail sets, 1 = C driver has a red the Java
# driver does not, 2 = setup problem.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
aeron="$root/third_party/aeron"
aeronmd=${AXC_AERONMD:-"$root/build/debug/aeron/binaries/aeronmd"}
out=${AXC_XA0_OUT:-/tmp/axc-xa0}
test_class=${AXC_XA0_TEST_CLASS:-io.aeron.archive.ArchiveTest}
xml="$aeron/aeron-archive/build/test-results/test/TEST-$test_class.xml"

if [ ! -x "$aeronmd" ]; then
    echo "run-xa0: aeronmd not found or not executable: $aeronmd" >&2
    exit 2
fi

mkdir -p "$out"

run_suite()
{
    local label="$1"
    shift

    rm -rf /dev/shm/aeron-* 2>/dev/null || true

    # The test suite may legitimately fail for the C driver; the comparison below
    # decides, not the gradle exit code.
    ( cd "$aeron" && ./gradlew :aeron-archive:test --tests "$test_class" "$@" --console=plain --rerun-tasks ) \
        >"$out/$label.log" 2>&1 || true

    if [ ! -f "$xml" ]; then
        echo "run-xa0: no test results at $xml (see $out/$label.log)" >&2
        exit 2
    fi

    cp "$xml" "$out/$label.xml"
}

run_suite java
run_suite c-driver "-Daeron.test.system.aeronmd.path=$aeronmd"

python3 - "$out/java.xml" "$out/c-driver.xml" <<'PY'
import sys
import xml.etree.ElementTree as ET


def cases(path):
    root = ET.parse(path).getroot()
    result = {}
    for case in root.findall("testcase"):
        if case.find("failure") is not None or case.find("error") is not None:
            status = "failed"
        elif case.find("skipped") is not None:
            status = "skipped"
        else:
            status = "passed"
        result[case.get("name")] = status
    return root, result


java_root, java = cases(sys.argv[1])
c_root, c = cases(sys.argv[2])

print(f"java driver:  {java_root.get('tests')} tests, "
      f"{java_root.get('failures')} failures, {java_root.get('errors')} errors, "
      f"{java_root.get('skipped')} skipped")
print(f"c driver:     {c_root.get('tests')} tests, "
      f"{c_root.get('failures')} failures, {c_root.get('errors')} errors, "
      f"{c_root.get('skipped')} skipped")

bad = False
for name in sorted(set(java) - set(c)):
    print(f"only in java driver: {name}")
    bad = True
for name in sorted(set(c) - set(java)):
    print(f"only in c driver: {name}")
    bad = True

regressions = [(name, java[name], c[name]) for name in java if name in c and java[name] != c[name]]
for name, before, after in regressions:
    print(f"status differs: {name}: java={before} c={after}")
    if after == "failed":
        bad = True

if bad:
    print("xa0: FAIL (the C driver does not match the Java baseline)")
    sys.exit(1)

print("xa0: PASS (identical pass/fail set)")
PY
