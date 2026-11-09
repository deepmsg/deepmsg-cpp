# Benchmark batches (A6a / C10)

`run-batch.sh --list/--quick/--out/--arms/--scenario/--fsync/--rate/--length/`
`--runs` and the four arms `j / j-c / x-c / x-j` land with A6a. Structure
follows `deepmsg/bench/archive-vs-java/`; the arms and the decision rule are
frozen in `PREREG.md` before the baseline is measured.

    run-batch.sh --quick              # s1, x-c, fsync=0, 25K:32, 5 iterations
    run-batch.sh --list               # every cell the defaults would produce
    run-batch.sh --arms j,j-c,x-c,x-j --fsync 0,2 --rounds 2
    read.py <results-dir> [--stat e_lt_p99|p50|p99.9|…]

Scenarios: `s2` is the reference's live-replay (node archives and replays),
`s1` its live-recording (the client owns the recording and paces itself on the
archive's recording events; the far end only echoes). The archive is always an
external process for the three arms that do not own it: `j` launches the
reference archive in the node's (s2) or client's (s1) own JVM, and
`j-c`/`x-c`/`x-j` start one before the node and the owner connects to it through
`patches/external-archive{,-s1}.patch`. `patches/install.sh --check|--apply|--revert`
is the only way those patches move, and `--check` verifies each source and the
built jar agree.

Fairness rules (perf spec §6.2): same machine, same core map, same idle
strategy, same disk; every batch writes `fingerprint.txt` (binary digests,
toolchain, -march, PGO, harness checkout and its local changes) and every cell
`RUN-INFO.txt` (arm, driver, archive, channels, cores, fsync, every §5 switch).
A cell whose instrument was silently disabled is a failed cell, not a fast one.

Each run also leaves `counters.txt` (the archive's labelled counters at the end
of the run; `tools/counters.sh`) and `archive-du.txt`, which `read.py` uses to
explain a latency difference but never to override one.

The two ends of a measured channel must be told the same thing: `destination`
and `source` are properties of *both* the node and the client. Getting that
wrong looks exactly like a dead archive — both ends sit in `awaitConnected` for
the full 60 s connection timeout (`records/a6a.md` finding 2).
