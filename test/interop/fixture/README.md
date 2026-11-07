# Fixture (X0-4)

- `process.{h,cpp}` — `run_process` (fork/exec, timeout, combined output) and
  `ChildProcess` (long-running peer, SIGTERM then SIGKILL grace).
- `java_process.{h,cpp}` — `java` launcher with a timeout; exit code 143
  (SIGTERM) is normal, not a failure.
- `c_driver.{h,cpp}` — `aeronmd` on a caller-owned directory. The destructor
  stops the driver but must never delete the directory.

Non-negotiables from the plan:

1. Every Java process has a timeout.
2. The driver fixture never deletes the shared aeron directory.
3. The shim fails loudly on an unknown main class.
4. One process per case (the case table and suite filters land with X0-6/A5b).
5. `AXC_AERON_TESTS` missing ⇒ SKIP with a printed reason.
