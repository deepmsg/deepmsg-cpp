# Interop tests

Everything here needs a JDK, the reference `aeron-all` jar and the reference
C/Java test code. All of it is delivered by **X0-4**; the directories exist as
placeholders so the layout is fixed.

- `fixture/` — `JavaProcess` (always with a timeout), `CDriver`, `ArchiveImpl{Java,Cpp}`,
  `NodeImpl{Java,Cpp}`, `ServiceImpl{Java,Cpp}`, `CaseTable`, `Golden`.
- `suites/` — gtest filter lists per acceptance line (`xa1-shared.txt`, `xc2-*.txt`, …).
- `cases/` — case denominator tables produced by X0-6, one process per case.
- `shim/` — the java shim (main class → C++ binary). Unknown main class must fail
  loudly. This repo's shim is self-contained; the workspace probe
  `/home/gavin/trader/aeron-shim/java` is only a shape reference.

Reference C/Java test code does **not** live here: it lives in the fork
`deepmsg/aeron` (branch `deepmsg-tests`). Point `AXC_AERON_TESTS` at it; a
missing checkout is SKIP with a printed reason, never a silent pass.
