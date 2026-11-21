#!/usr/bin/env bash
# The one gate. CI calls exactly this command (repo-layout L5).
#
#   scripts/check.sh                # debug preset
#   scripts/check.sh debug release  # several presets
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"

# Every unit-test binary has to reach ctest. A target defined without a
# `gtest_discover_tests` line is built and then never run, and nothing says so
# (`axc_cluster_backup_unit_tests` was in that state; the cluster backup tests
# had never executed, 2026-11-21).
unit_cmake=test/unit/CMakeLists.txt
undiscovered=0
while read -r unit_target; do
    if ! grep -q "gtest_discover_tests(${unit_target}[ )]" "$unit_cmake"; then
        echo "check: ${unit_target} is defined in ${unit_cmake} but never discovered by ctest" >&2
        undiscovered=1
    fi
done < <(sed -n 's/^axc_add_executable(\([A-Za-z0-9_]*\).*/\1/p' "$unit_cmake")
if [ "$undiscovered" -ne 0 ]; then
    exit 1
fi

# Config precedence: a properties file beats the environment, and every source
# is insert-if-absent, so an app that loads the environment first has the
# precedence backwards (`PropertySource`'s contract: -D > file > environment).
while read -r app_main; do
    file_line=$(grep -n "load_file(" "$app_main" | head -1 | cut -d: -f1)
    env_line=$(grep -n "load_environment(" "$app_main" | head -1 | cut -d: -f1)
    if [ -n "$file_line" ] && [ -n "$env_line" ] && [ "$env_line" -lt "$file_line" ]; then
        echo "check: ${app_main} loads the environment before its properties file" >&2
        exit 1
    fi
done < <(find apps -name 'main.cpp')

presets=("$@")
if [ ${#presets[@]} -eq 0 ]; then
    presets=(debug)
fi

for preset in "${presets[@]}"; do
    cmake --preset "$preset"
    cmake --build "build/$preset" --parallel "$(nproc)"
    # The interop CDriver fixture needs the reference driver binary; build it so
    # the fixture runs instead of skipping in CI.
    cmake --build "build/$preset" --target aeronmd --parallel "$(nproc)"
    ctest --test-dir "build/$preset" --output-on-failure
done

tools/repo/hotpath-audit.sh "build/${presets[0]}"
tools/repo/check-deps.sh
tools/repo/check-forbidden.sh

# clang-format / clang-tidy must be the same major version as the clang build
# preset (clang-18 cannot see std::expected with libstdc++ 13, so its tidy would
# fail on code that compiles fine).
find_tool()
{
    for candidate in "$1-20" "$1"; do
        if command -v "$candidate" >/dev/null 2>&1; then
            command -v "$candidate"
            return 0
        fi
    done
    return 1
}

clang_format=$(find_tool clang-format || true)
if [ -n "$clang_format" ]; then
    mapfile -t formatted < <(find src apps tools test bench -name '*.cpp' -o -name '*.h')
    "$clang_format" --dry-run --Werror "${formatted[@]}"
else
    echo "clang-format-20/clang-format not found: skipping the format gate"
fi

clang_tidy=$(find_tool clang-tidy || true)
if [ -n "$clang_tidy" ] && [ -f "build/${presets[0]}/compile_commands.json" ]; then
    find src -name '*.cpp' -print0 | xargs -0 -r "$clang_tidy" -p "build/${presets[0]}"
else
    echo "clang-tidy-20/clang-tidy or compile_commands.json not found: skipping the tidy gate"
fi
