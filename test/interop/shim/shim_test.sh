#!/usr/bin/env bash
# Self-test for the java shim. Run directly or from the interop gtest.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
shim="$here/java"
tmp=$(mktemp -d /tmp/axc-shim-XXXXXX)
trap 'rm -rf "$tmp"' EXIT

failures=0
check()
{
    local name="$1"
    shift
    if "$@"; then
        echo "PASS $name"
    else
        echo "FAIL $name"
        failures=$((failures + 1))
    fi
}

# Fake real java and fake mapped binaries.
cat > "$tmp/real-java" <<'EOF'
#!/usr/bin/env bash
printf 'REALJAVA'
for arg in "$@"; do printf ' [%s]' "$arg"; done
printf '\n'
EOF
cat > "$tmp/fake-server" <<'EOF'
#!/usr/bin/env bash
printf 'FAKESERVER'
for arg in "$@"; do printf ' [%s]' "$arg"; done
printf '\n'
EOF
chmod +x "$tmp/real-java" "$tmp/fake-server"

printf 'com.example.Main\tfake-server\n' > "$tmp/map.tsv"

transparent_output=$(AXC_REAL_JAVA="$tmp/real-java" AXC_SHIM_MODE=transparent "$shim" -cp cp com.example.Main app-arg)
check "transparent passes every argument" test "$transparent_output" = "REALJAVA [-cp] [cp] [com.example.Main] [app-arg]"

mapped_output=$(AXC_REAL_JAVA="$tmp/real-java" AXC_SHIM_MODE=mapped AXC_SHIM_MAP="$tmp/map.tsv" AXC_BIN_DIR="$tmp" \
    "$shim" -Daeron.dir=/tmp/x --add-opens java.base/java.util.zip=ALL-UNNAMED -cp cp com.example.Main app-arg)
check "mapped runs our binary with -D and application args" \
    test "$mapped_output" = "FAKESERVER [-Daeron.dir=/tmp/x] [app-arg]"

set +e
unknown_output=$(AXC_REAL_JAVA="$tmp/real-java" AXC_SHIM_MODE=mapped AXC_SHIM_MAP="$tmp/map.tsv" "$shim" -cp cp com.example.Unknown 2>&1)
unknown_status=$?
set -e
check "unknown main class fails non-zero" test "$unknown_status" -ne 0
check "unknown main class is named" bash -c "[[ '$unknown_output' == *'com.example.Unknown'* ]]"

set +e
jar_output=$(AXC_REAL_JAVA="$tmp/real-java" AXC_SHIM_MODE=mapped AXC_SHIM_MAP="$tmp/map.tsv" "$shim" -jar app.jar 2>&1)
jar_status=$?
set -e
check "mapped -jar is refused loudly" test "$jar_status" -ne 0

set +e
bad_mode=$(AXC_SHIM_MODE=bogus "$shim" -cp cp com.example.Main 2>&1)
bad_mode_status=$?
set -e
check "unknown mode is refused" test "$bad_mode_status" -ne 0

if [ "$failures" -ne 0 ]; then
    echo "shim_test: $failures failure(s)" >&2
    exit 1
fi

echo "shim_test: all checks passed"
