# E3: the cluster on an external driver

`run-smoke.sh media|kernel` runs the single-node XC-2 smoke the way
`plan-dpdk-bench-e2e.md` W4 describes it: `deepmsg-media-driver` as the
**external** driver, `deepmsg-cluster-node` in its default (external) form, the
C++ clustered service container, and a **Java** `AeronCluster` client that
reaches the cluster over the wire at `10.10.0.1` from the kernel port at
`10.10.0.2`.

- `media` starts the driver with `deepmsg.dpdk.*` (`-a <pci>`, the media owns
  `10.10.0.1/24`), so the client's offers and the cluster's replies cross
  `03:00.0`.
- `kernel` runs the **same driver binary** with no media configuration, so the
  same cluster configuration travels over `ixgbe` instead. That is the control
  for "the media is not what broke or what worked".

Both arms judge the same four criteria: echo, snapshot + restart (the restarted
node replays and starts a new term), clean termination, and the media arm
showing that the frames reached the port (`dpdk packets` moved, no data-plane
frame discarded) while the kernel arm shows the same binary allocating no DPDK
counters. The script exits non-zero if any criterion failed, and restores the
card and the interfaces on the way out.

## Running it

```bash
sudo env AXC_AERON_TESTS=/path/to/aeron-deepmsg-tests \
    bench/dpdk-e3/run-smoke.sh media
sudo env AXC_AERON_TESTS=/path/to/aeron-deepmsg-tests \
    bench/dpdk-e3/run-smoke.sh kernel
```

`AXC_AERON_TESTS` is required: the echo client is the fork's
`ClusterEchoClientProbe.java`, compiled per run. Everything else has a default
matching the bench machine (`0000:03:00.0`, `10.10.0.1/24`, `10.10.0.2/24`,
ports 24600–24603 and 24610); see the environment list at the top of the script.

Requirements: root, the `dpdk` preset built, huge pages free for the media arm,
`java`/`javac` 21, and NetworkManager present (`nmcli` is used to stop it from
flushing the peer address when the PMD's reset bounces the link).

## Readings

`analysis/cpp/records/dpdk-bench-e2e.md` §5 carries the 2026-11-15 readings and
the pitfalls this harness was built through.
