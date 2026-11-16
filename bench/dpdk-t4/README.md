# The T4 rig: both ends of the cable are our driver

```
03:00.0  vfio-pci → deepmsg-media-driver A, 10.10.0.1/24
03:00.1  vfio-pci → deepmsg-media-driver B, 10.10.0.2/24
         one Cat6a between them; no kernel interface, no namespace
```

`run-t4.sh` binds both ports, runs the `DpdkTwoDrivers` case, and restores both
ports in a trap. The case itself starts the two drivers (one EAL each), drives
two clients, and judges the criteria — see `analysis/cpp/plan-dpdk-t4.md` §3.
Nothing here is a benchmark: no reading from this rig is quoted as a latency.

```bash
# Release + DPDK, or the media's own numbers are -O0 (records/dpdk-release.md)
CM=tools/cmake-4.3.4-linux-x86_64/bin/cmake
$CM -S . -B build/dpdk-release -DCMAKE_BUILD_TYPE=Release -DAXC_WITH_DPDK=ON
$CM --build build/dpdk-release --target axc_interop_tests deepmsg-media-driver -j8

sudo bench/dpdk-t4/run-t4.sh
```

Knobs: `AXC_DPDK_BENCH_PCI` / `AXC_DPDK_BENCH_PEER_PCI` (the two ports),
`AXC_DPDK_BENCH_LOCAL` / `AXC_DPDK_BENCH_PEER_LOCAL` (the two addresses),
`AXC_DPDK_T4_CPUSET_A` (0-2), `AXC_DPDK_T4_CPUSET_B` (3-5),
`AXC_DPDK_T4_CPUSET_TEST` (6-7), `AXC_DPDK_T4_COUNT` (1,000 per direction).

The run keeps everything under one directory: `RUN-INFO.txt` (build type, both
binary hashes, the ports and their IOMMU groups, the CPU sets, huge pages) plus
`test.log` and `drivers/*/driver.log`. Copy it out before the next run.

## Before blaming the product

- **Two EAL processes on one host share the huge page pool.** This is the first
  card that runs two; if the second fails with "not enough huge pages", record
  the reading and raise `HugePages_Total` (or give EAL `--socket-mem`) — that
  is deployment, not product.
- **The two PCI functions must be in different IOMMU groups** (they are on this
  machine: 12 and 13). One group means one process.
- **10GBASE-T takes up to ~20 s to come up after a bind**; the media waits 30 s
  (`media.cpp`, `LINK_TIMEOUT_SECONDS`). The second start is warm.
- **The kernel must own neither port**: the test asserts `/sys/…/net` is empty
  for both before it runs. A kernel interface would leave a send path that is
  not the media.
- The P6 rig (`bench/dpdk-p6/`) and this one cannot run at the same time: P6
  keeps 03:00.1 as its kernel peer.
