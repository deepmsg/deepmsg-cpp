# Zero-allocation tests

Standalone binaries that replace `operator new` (tagging allocations per agent
thread) and assert zero allocations in steady state: warm up 10⁴ messages,
reset the counter, run 10⁵, expect 0. Delivered with A5b (archive) and C4f
(cluster).
