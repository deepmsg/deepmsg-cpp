# The DPDK media

A userspace implementation of the reference UDP media: the driver's channel
transports and pollers are replaced by a stack that sends and receives through
a DPDK port, so the data path never enters the kernel. It is installed under
the reference driver's own transport-bindings seam
(`aeron_udp_channel_transport_bindings_t`), which is why the driver above it —
this repository's archive server, cluster node and media driver — is unchanged.

What it carries, what it refuses and which names it serves are the byte-level
contract: `docs/compat.md` §"DPDK media". This page is the practical guide:
how to build it, how to turn it on, and how to tell that it is on.

## 1 Build

The media is compiled **only** when both of these hold:

- `AXC_WITH_DPDK=ON` (a CMake option, off by default), and
- `pkg-config --exists libdpdk` — Debian/Ubuntu: `apt install libdpdk-dev`
  (DPDK 23.11 here).

A build without DPDK is a normal build: it has no media, and a configuration
that asks for one is refused at start-up with

```
deepmsg.dpdk.enabled=true but this build has no DPDK media; configure with -DAXC_WITH_DPDK=ON
```

Presets that turn it on:

```bash
cmake --preset dpdk          # Debug + DPDK, the gate's media build
cmake --preset asan          # Debug + AddressSanitizer + DPDK
cmake --build build/dpdk --parallel "$(nproc)"
```

A build meant for numbers is Release:

```bash
cmake -S . -B build/dpdk-release -DCMAKE_BUILD_TYPE=Release -DAXC_WITH_DPDK=ON
cmake --build build/dpdk-release --parallel "$(nproc)"
```

That produces `build/<preset>/apps/deepmsg-media-driver/deepmsg-media-driver`,
the standalone driver, and compiles the `Dpdk*` test cases. The unit cells run
on `net_pcap` vdevs — no root, no huge pages, no card; the cells that need a
real port (the benchmark and two-driver ones) skip when there is none:

```bash
ctest --test-dir build/dpdk -R Dpdk --output-on-failure
```

## 2 Turn it on

Two switches, either one:

| property | meaning |
|---|---|
| `deepmsg.dpdk.enabled=true` | the media's own switch |
| `aeron.udp.channel.transport.bindings.media=dpdk` | the reference-style name; it turns the switch on (and `aeron.print.configuration` still answers `default`) |

Both are read from the ordinary property channels:

- `-Dname=value` on the command line,
- a `*.properties` file passed as an argument,
- the environment: `DEEPMSG_DPDK_*` maps to `deepmsg.dpdk.*` and `AERON_*` to
  `aeron.*` (each `_` becomes `.`, lower-case).

The driver above the media is the same one every binary here uses, so the
archive server and the cluster node take the same properties as the standalone
driver. A minimal unicast configuration:

```properties
# p6.properties
deepmsg.dpdk.enabled=true
deepmsg.dpdk.eal.args=-a 0000:03:00.0
deepmsg.dpdk.port=0000:03:00.0
deepmsg.dpdk.ipv4.address=10.10.0.1/24
aeron.threading.mode=DEDICATED
aeron.sender.idle.strategy=spin
aeron.receiver.idle.strategy=spin
```

```bash
sudo build/dpdk-release/apps/deepmsg-media-driver/deepmsg-media-driver p6.properties
```

The same on the command line:

```bash
sudo build/dpdk-release/apps/deepmsg-media-driver/deepmsg-media-driver \
     -Ddeepmsg.dpdk.enabled=true -Ddeepmsg.dpdk.eal.args="-a 0000:03:00.0" \
     -Ddeepmsg.dpdk.port=0000:03:00.0 -Ddeepmsg.dpdk.ipv4.address=10.10.0.1/24 \
     -Daeron.threading.mode=DEDICATED -Daeron.sender.idle.strategy=spin \
     -Daeron.receiver.idle.strategy=spin
```

### The properties

| name | default | what it does |
|---|---|---|
| `deepmsg.dpdk.enabled` | `false` | the switch (above) |
| `deepmsg.dpdk.eal.args` | empty | the operator's EAL arguments, whitespace-split (`-a 0000:03:00.0`, `--vdev=net_pcap0,...`, `--no-huge`) |
| `deepmsg.dpdk.port` | the first probed port | the EAL port name: a PCI address or a vdev name |
| `deepmsg.dpdk.ipv4.address` | **required when enabled** | this port's address and prefix, e.g. `10.10.0.1/24`; the kernel does not own it, the stack answers ARP for it |
| `deepmsg.dpdk.ipv4.gateway` | none | next hop for destinations off this link |
| `deepmsg.dpdk.static.neighbours` | empty | `address=mac,...` entries that never age out |
| `deepmsg.dpdk.rx.descriptors` / `tx.descriptors` | 1024 / 1024 | port ring sizes (64–4096) |
| `deepmsg.dpdk.mempool.size` / `mempool.cache` | 8191 / 256 | frame pool entries and per-lcore cache |
| `deepmsg.dpdk.rx.burst` | 32 | frames per receive burst (1–64) |
| `deepmsg.dpdk.sender.ring` | 1024 | the sender's ring, power of two |
| `deepmsg.dpdk.ephemeral.ports` | 32768–60999 | the range a transport's own port is taken from |
| `deepmsg.dpdk.arp.timeout.ms` / `arp.retry.ms` | 300000 / 1000 | neighbour ageing and retry |

An **unknown** `deepmsg.dpdk.*` name is refused rather than ignored, and so is
a value out of range. The full table, with the reference `file:line` each name
follows, is in `docs/compat.md`.

EAL notes:

- **Do not give `--lcores` or `-l`.** The media chooses one lcore on the
  process's own CPU set (its `taskset`/cgroup), so a process pinned to four
  CPUs keeps its EAL thread inside them. `--lcores` is refused at start-up.
- `--no-telemetry` is added when absent.
- `--in-memory` and `--no-huge` are mutually exclusive; `--no-huge` also gets
  `--no-shconf`. Either keeps EAL from writing `/var/run/dpdk`, which is what
  lets a second driver on the same host start.

## 3 Host setup

**Huge pages.** The reference configuration is 2 MiB pages; the bench machine
runs 2048 of them (4 GiB). Without any, `--no-huge` also works (slower, and it
is what the tests use).

**A real port.** The port has to be unbound from the kernel and bound to
`vfio-pci` (`dpdk-devbind.py --bind=vfio-pci 0000:03:00.0`); bring the
interface down and flush its addresses first, and tell NetworkManager to leave
it alone (`nmcli device set <if> managed no`), or it will race the bind.
A vdev such as `net_pcap0` needs none of this and no root.

**The address belongs to the media.** Do not `ip addr add` it — the kernel
cannot own an address the media serves; the stack answers ARP itself (or uses
`static.neighbours`). The kernel keeps the *other* interfaces, which is why the
channels the media does not carry still work (below).

## 4 Threading modes

All four of the reference's modes are served. The mode decides how many
transmit queues the port opens:

| mode | TX queues | why |
|---|---|---|
| `DEDICATED` | 2 | sender and receiver are separate agents on separate threads, and a DPDK TX queue is not thread-safe |
| `SHARED`, `SHARED_NETWORK`, `INVOKER` | 1 | the reference runs both agents on one thread in these modes |

`INVOKER` is the mode that cannot start a runner of its own: the caller must
drive the main loop. `deepmsg-media-driver` does that for it (the reference's
`aeronmd` does the same); an application embedding the driver does it with
`StartMode::caller_thread` and `aeron_driver_main_do_work`.

The idle strategies that park a thread (`sleeping`, `yielding`, `backoff`) are
warned about, because a DPDK port has no interrupt with which to wake a parked
receiver: the driver-level round trip measured 1.07 ms with `backoff` and
29.7 us with `spin`. `DEDICATED` on a port that has a single transmit queue
also warns — that is the shape that lost frames on 2026-11-16 before the queues
were split.

## 5 What stays on the kernel

The media is one port, and the driver above it still has its kernel sockets. A
channel whose traffic the port cannot carry is served by the kernel instead,
per channel, at the transport seam:

- **loopback** (`127.0.0.1`, IPC),
- **multicast and broadcast** (the port joins no group and maps no multicast
  address; both directions stay on the kernel),
- **off-link destinations with no gateway** (nothing to resolve — no ARP entry
  to send to),
- **IPv6** (the media is IPv4-only).

This is the "extra NIC" model: the media takes one port and one prefix, and
everything else behaves as it always did. It is also why a machine with only
the one (bound) port cannot serve multicast through the media at all.

## 6 Tell that it is on

One line on stdout when the port opens:

```
dpdk media on 0000:03:00.0 (net_ixgbe), address 10.10.0.1/24, mtu 1500, link up (10000 Mb/s), queues 1 rx 2 tx
```

`queues <rx> rx <tx> tx` is the reading of the threading mode above.

The driver's CnC counters carry the media's own numbers — `dpdk packets`,
`dpdk bytes`, `dpdk error`, `dpdk missed packets`, `dpdk rx receiver discard`,
`dpdk arp miss`, `dpdk tx eagain`, `dpdk nobufs`, and the per-segment timers
when the build has them. A datagram larger than the port's MTU is refused with
`EMSGSIZE` rather than fragmented.

The tests are the smallest complete check (`ctest -R Dpdk`, above): a vdev
opens, frames go out and come back, counters appear, every threading mode
starts, and nothing is delivered by a channel the media should have left
alone.

## 7 Numbers

The driver-level comparison against the kernel is at `docs/benchmarks.md`
(driver-level round trip, M/K ≈ 0.81 at the self-paced rate) and, with the
rate knob and the by-rate medians, in `bench/dpdk-p6/README.md`. The driver
costs about 1.2 us per frame (Release); the round trip is ~95% wire, PHY and
the far side.

## 8 Limits (v1)

No VLAN, no IPv6, no IP options, no fragmentation or reassembly, no multicast
receive and no multicast transmit, no RSS/flow-director distribution, no
receive timestamps. Each of these either falls back to the kernel or is
refused at the channel, and `docs/compat.md` says which.
