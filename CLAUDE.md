# deepmsg-cpp — instructions for AI sessions

C++23 transcription of the Aeron archive server and cluster on the official C
client/driver. Reference: Aeron **1.54.0**, pinned as the submodule gitlink
`454da1e4e6` in `third_party/aeron` (our fork `github.com/deepmsg/aeron`).

The plan, per-card plans and delivery records live outside this repo in
`/home/gavin/trader/analysis/cpp/` (work packages: `work-packages-2026-11-07-cpp.md`).

## Non-negotiables

1. **Transcribe, do not redesign.** Follow the Java method by method, keep the
   branch order, and put the Java `file:line` in a comment. If a document and
   the reference code disagree, the code wins and the record says so.
2. **Bytes are the contract.** Wire format, disk format, counter type ids/key
   layouts, configuration names and log contents follow the reference exactly.
   Any new format or key needs a line in `docs/compat.md` plus a golden or
   interop test in the same commit.
3. **Never patch `third_party/aeron`.** The gitlink is the whole contract.
   Test-only branches live in the fork, never on the pinned commit.
4. **C++23 is target-scoped.** Never set `CMAKE_CXX_STANDARD` or
   `CMAKE_C_STANDARD` globally; Aeron keeps C++11/C11.
5. **No hidden synchronisation.** `std::mutex`, `std::condition_variable` and
   bare `std::thread` are forbidden (threads only via `axc::core::AgentRunner`);
   no `std::shared_ptr`; no allocation, `std::string` construction or
   `operator new` on a hot path. Register hot paths in `src/**/HOTPATH`.
6. **Errors are not swallowed.** Functions return `std::expected` and are
   `[[nodiscard]]`; `-Werror=unused-result` is on. A reference exception that
   ends the current `doWork` pass must end ours too.
7. **Every new criterion gets a mutant** that makes it fail, and the reading goes
   into the delivery record.

## Gates

`scripts/check.sh [presets…]` is the one gate — CI calls exactly it:
configure + build + ctest, `hotpath-audit.sh`, `check-deps.sh`, clang-format,
clang-tidy when available. A card is done only when it is green with **gcc 13.3
on all presets** and **clang-20 on `debug-clang`** (the reference policy: one
production compiler plus one clang build so the clang-tidy gate is honest; TSan
for thread handovers once they exist).

## Branches, commits, PRs

- Branch `feat/<card>-<slug>` from `main`; **never commit to `main`**.
- Push, open a PR and merge only when the user explicitly asks. Otherwise stop at
  a local commit and report the branch and commit id.
- PR titles and descriptions are in **English**.

## Reading order for a new task

1. `analysis/cpp/work-packages-2026-11-07-cpp.md` — your card, §0 common DoD.
2. `analysis/cpp/modules-2026-11-07-cpp-interfaces.md` — frozen interfaces only.
3. The card's `impl-*.md` section and the Rust-track plan it names
   (`analysis/archive/…`, `analysis/cluster/…`) — the best line-by-line notes.
