# Benchmark batches (A6a / C10)

`run-batch.sh --list/--quick/--out` and `arms/{j,j-c,x-c,x-j}` land with A6a.
Structure follows `deepmsg/bench/archive-vs-java/`.

Fairness rules (perf spec §6.2): same machine, same core map, same idle
strategy, same disk; every run writes `fingerprint.txt` (binary digest,
compiler, -march, PGO) and `RUN-INFO.txt` (arm, fsync, io_uring backend,
recorder threads, every §5 switch). A cell whose instrument was silently
disabled is a failed cell, not a fast one.
