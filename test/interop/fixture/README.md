# Fixture (X0-4)

Filled by the X0-4 card. Non-negotiables from the plan (`analysis/cpp/`):

1. `JavaProcess` always has a timeout; exit code 143 (SIGTERM) is normal.
2. `CDriver`'s destructor never deletes the shared aeron directory.
3. The shim fails loudly on an unknown main class.
4. One process per case.
5. `AXC_AERON_TESTS` missing ⇒ SKIP with a printed reason.
