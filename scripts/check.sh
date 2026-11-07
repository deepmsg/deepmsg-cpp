#!/usr/bin/env bash
# The one gate. CI calls exactly this command (repo-layout L5).
#
#   scripts/check.sh                # debug preset
#   scripts/check.sh debug release  # several presets
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"

presets=("$@")
if [ ${#presets[@]} -eq 0 ]; then
    presets=(debug)
fi

for preset in "${presets[@]}"; do
    cmake --preset "$preset"
    cmake --build "build/$preset" --parallel "$(nproc)"
    ctest --test-dir "build/$preset" --output-on-failure
done

tools/repo/hotpath-audit.sh "build/${presets[0]}"
tools/repo/check-deps.sh

if command -v clang-format >/dev/null 2>&1; then
    mapfile -t formatted < <(find src apps tools test bench -name '*.cpp' -o -name '*.h')
    clang-format --dry-run --Werror "${formatted[@]}"
else
    echo "clang-format not found: skipping the format gate"
fi

if command -v clang-tidy >/dev/null 2>&1 && [ -f "build/${presets[0]}/compile_commands.json" ]; then
    find src -name '*.cpp' -print0 | xargs -0 -r clang-tidy -p "build/${presets[0]}"
else
    echo "clang-tidy or compile_commands.json not found: skipping the tidy gate"
fi
