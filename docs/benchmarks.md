# Benchmarks

Nothing here is a result yet. The rules live in
`/home/gavin/trader/analysis/cpp/spec-2026-11-07-cpp-performance-and-acceptance.md`;
the batch scripts land with A6a in `bench/cpp-arms/`.

Principles, repeated because they are easy to lose:

- only `X-C / J-C` answers "is the C++ server faster than the Java server";
- every cell records `fingerprint.txt` and `RUN-INFO.txt`, including every
  performance switch (io_uring backend, fsync level, recorder threads);
- a cell whose instrument was silently disabled is a failed cell, not a fast one;
- thresholds are pre-registered in `bench/cpp-arms/PREREG.md` before measuring.
