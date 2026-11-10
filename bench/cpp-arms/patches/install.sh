#!/usr/bin/env bash
#
# Put "the archive is not mine" into the harness's two archive-aware processes,
# and take it out again.
#
#   install.sh --check     say what is and is not in place (default)
#   install.sh --apply     apply the patches and rebuild the jar they live in
#   install.sh --revert    undo the patches (the jar is left as it is; rebuild)
#
# What goes in, and why:
#
# - **`external-archive.patch`** (S2) — `ArchiveNode` launches an
#   `ArchivingMediaDriver` in its own constructor, so the reference's node can
#   only ever talk to the reference's archive. The arm that says anything about
#   **this build's archive** needs a node that connects to an archive somebody
#   else runs; the patch adds `io.aeron.benchmarks.aeron.archive.external=true`,
#   which skips the launch and connects through `aeron.archive.control.channel`
#   exactly as a client does.
#
#   The three dumps at the end read their directories from the driver the node
#   is a client of rather than from an `ArchivingMediaDriver` it may not own.
#
# - **`external-archive-s1.patch`** (S1) — `LiveRecordingMessageTransceiver`
#   does the same launch, in the client's JVM; the patch is the same one-line
#   switch and the same dump fix. Its recording events already come from the
#   `aeron.archive.recording.events.*` properties the reference's own
#   `live-recording-client` sets, so nothing in the paced receive loop moves.
#
# ★ **The measurement side is untouched in both**: the forwarding/echo loops,
# the subscriptions, the recording-position gate and the answer that is the
# archive's are the reference's own lines. A cell may swap the archive and
# nothing else, and these patches are written so that is true.
#
# ★ **The jar has to be rebuilt**: the harness's classes come out of one shadow
# jar, and a patched source with the old jar runs the old code. `--apply` does the
# rebuild; `--check` reads the property name out of each compiled class, so a
# stale jar is visible rather than silently used.
#
#   BENCHMARKS_DIR    the harness checkout (see bench/lib/checkouts.sh)
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BENCH_DIR=$(dirname "$(dirname "$here")")
# shellcheck source=../../lib/checkouts.sh
. "$BENCH_DIR/lib/checkouts.sh"

action=${1:---check}
jar=$BENCHMARKS_DIR/benchmarks-all/build/libs/benchmarks.jar
property=io.aeron.benchmarks.aeron.archive.external

# patch file | source file | compiled class
entries=(
    "external-archive.patch|benchmarks-aeron/src/main/java/io/aeron/benchmarks/aeron/ArchiveNode.java|io/aeron/benchmarks/aeron/ArchiveNode.class"
    "external-archive-s1.patch|benchmarks-aeron/src/main/java/io/aeron/benchmarks/aeron/LiveRecordingMessageTransceiver.java|io/aeron/benchmarks/aeron/LiveRecordingMessageTransceiver.class"
)

_patch_of() { echo "$here/${1%%|*}"; }
_class_of() { echo "${1##*|}"; }

_in_source() {
    local rest=${1#*|}
    local source=${rest%%|*}
    grep -q 'ARCHIVE_EXTERNAL_PROP_NAME' "$BENCHMARKS_DIR/$source" 2>/dev/null
}

# The property name is a string constant in the compiled class, so the jar can be
# asked the same question the source is: is this switch in there?
_in_jar() {
    [ -f "$jar" ] || return 1
    unzip -p "$jar" "$(_class_of "$1")" 2>/dev/null | grep -a -q "$property"
}

_check() {
    need_dir "harness checkout" "$BENCHMARKS_DIR" || return 1
    printf 'harness          %s\n' "$BENCHMARKS_DIR"
    for entry in "${entries[@]}"; do
        printf 'patch            %s\n' "$(_patch_of "$entry")"
        printf '  in source      %s\n' "$(_in_source "$entry" && echo 'in place' || echo 'MISSING')"
        printf '  in jar         %s\n' "$(_in_jar "$entry" && echo 'in place' || echo 'MISSING — rebuild with --apply')"
    done
}

_apply() {
    local applied=0
    for entry in "${entries[@]}"; do
        local patch
        patch=$(_patch_of "$entry")
        if _in_source "$entry"; then
            echo "already in source: $(basename "$patch")"
        else
            git -C "$BENCHMARKS_DIR" apply --check "$patch"
            git -C "$BENCHMARKS_DIR" apply "$patch"
            echo "applied $(basename "$patch")"
            applied=1
        fi
    done

    local stale=0
    for entry in "${entries[@]}"; do
        _in_jar "$entry" || stale=1
    done
    if [ 0 -eq "$stale" ]; then
        echo "the jar already carries both switches"
    else
        echo "rebuilding the harness (one shadow jar holds its clients and nodes)…"
        ( cd "$BENCHMARKS_DIR" && ./gradlew :benchmarks-all:shadowJar -q )
    fi
    _check
}

_revert() {
    for entry in "${entries[@]}"; do
        local patch
        patch=$(_patch_of "$entry")
        if _in_source "$entry"; then
            git -C "$BENCHMARKS_DIR" apply -R "$patch"
            echo "reverted $(basename "$patch")"
        else
            echo "not in the source: $(basename "$patch")"
        fi
    done
    echo "the jar still carries the switches: rebuild it (./gradlew :benchmarks-all:shadowJar)"
    _check
}

case "$action" in
    --check) _check ;;
    --apply) _apply ;;
    --revert) _revert ;;
    -h|--help) sed -n '3,42p' "${BASH_SOURCE[0]}" ;;
    *) echo "usage: install.sh [--check|--apply|--revert]" >&2; exit 2 ;;
esac
