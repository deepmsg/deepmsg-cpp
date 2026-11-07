#!/usr/bin/env bash
# Product-code bans (CLAUDE.md non-negotiables 4 and 5).
#
# A grep is used instead of a clang-tidy custom check: banning identifiers is
# not something clang-tidy expresses without a plugin, and a textual check is
# exact, cheap and works for headers as well. clang-tidy still runs for the
# bugprone/performance analysis; the hot-path symbol audit covers allocation.
#
# Scope: src/, apps/, tools/. Tests and benchmarks are allowed to use threads,
# iostream and shared ownership.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$root"

status=0

check()
{
    local pattern="$1"
    local message="$2"
    local allow="${3:-}"

    local hits
    hits=$(grep -rnE "$pattern" src apps tools --include='*.h' --include='*.cpp' 2>/dev/null || true)

    if [ -n "$allow" ]; then
        hits=$(printf '%s\n' "$hits" | grep -vE "$allow" || true)
    fi

    if [ -n "$hits" ]; then
        printf '%s\n' "$hits" >&2
        echo "check-forbidden: $message" >&2
        status=1
    fi
}

check '\bstd::(mutex|recursive_mutex|timed_mutex|condition_variable|condition_variable_any)\b' \
      'locks and condition variables are forbidden; use lock-free structures'

check '\bstd::shared_ptr\b' \
      'std::shared_ptr is forbidden (atomic refcount on the hot path); use unique ownership'

check '\bstd::(async|future|promise|packaged_task)\b' \
      'std::async/future are forbidden; agents are driven by AgentRunner'

check '\bstd::thread\b' \
      'bare std::thread is forbidden; only src/core/agent/agent_runner.h may create a thread' \
      '^src/core/agent/agent_runner\.h:'

check '#include <thread>' \
      'include <thread> outside AgentRunner is forbidden' \
      '^src/core/agent/agent_runner\.h:'

check '#include <iostream>|\bstd::(cout|cerr|clog)\b' \
      'iostream is forbidden in product code; use tracing or the error log'

if [ "$status" -eq 0 ]; then
    echo "check-forbidden: product-code bans OK"
fi

exit "$status"
