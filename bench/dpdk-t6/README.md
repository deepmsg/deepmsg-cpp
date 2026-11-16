# The T6 rig: the media is on and everything falls back

T6 asks one question: with the DPDK media **open** and every channel **falling
back to the kernel**, do the reference Java system tests behave exactly as they
do on the reference `aeronmd`? Per-case diff, no tolerance except a generated
known-red list in which every row carries a mechanism.

```
arm kernel            -Daeron.test.system.aeronmd.path=<reference aeronmd>, untouched
arm kernel-dedicated  → kernel-wrapper.sh: reference aeronmd, AERON_THREADING_MODE=DEDICATED forced
arm media             → aeronmd-wrapper.sh: deepmsg-media-driver on a net_pcap vdev,
                      10.10.0.1/24, media on, AERON_THREADING_MODE=DEDICATED forced
```

**The claim is `kernel-dedicated` vs `media`.** The reference harness asks for
`SHARED` in most classes (`PubAndSubTest` starts 30 drivers, all SHARED) and our
launcher runs DEDICATED only, so both experimental arms force DEDICATED; the
`kernel-dedicated` arm is the control that measures what the forcing alone
changes (reported separately, `diff-control/`). Without it, the diff would
conflate "the media is on" with "the driver threads differently".

## Why a vdev and not the card

- The reference suite starts **two or more drivers in one test** in a large
  family of classes (`ChannelEndpointStatusTest` starts eight across four
  tests). One vfio port can be opened by one process; the second driver never
  creates its CnC (measured: 4/4 red on the card, 4/4 green on a vdev).
- T6 is about the **fallback**, not the wire: endpoints are `localhost`,
  `127.0.0.1` and `224.x`, so the media carries nothing. Each driver writes its
  own pcap, and **every pcap staying at the 24-byte pcap header** is the reading
  that proves it. The card path is covered by T4/T5/T7.
- No root, no huge pages, no machine state, any number of concurrent drivers.

## Running

```bash
# once: the reference system tests' Gradle dependencies (Maven Central)
( cd third_party/aeron && ./gradlew :aeron-system-tests:compileTestJava )

bench/dpdk-t6/run-t6.sh --arm all                   # kernel + control + media, ~2 h
bench/dpdk-t6/run-t6.sh --arm media --area cluster  # one area
bench/dpdk-t6/run-t6.sh --arm all --resume          # continue a killed run
bench/dpdk-t6/run-t6.sh --part a                    # debug vs dpdk ctest, per test
```

Environment: `AXC_BUILD_DIR` (the DPDK build), `AXC_T6_KERNEL_DRIVER`
(`build/debug/aeron/binaries/aeronmd` by default), `AXC_T6_DRIVER` (our media
driver), `AXC_T6_WORK`, `AXC_T6_CLASS_TIMEOUT` (300 s), `AXC_T6_GRADLE_ARGS`
(`--offline` by default, after the one-time fetch).

One work directory holds everything: `<arm>/xml/TEST-*.xml` (the Junit XML, per
class), `<arm>/logs/<class>.log` (Gradle), `<arm>/status.tsv` (class, method,
status, message), `media/drivers/driver-<key>.log` and `.pcap` (one pair per
driver the harness started), `media/driver-checks.txt`, and `diff/deltas.tsv`.

## What fails loudly

- **B1** the arm ran: ≥100 classes and ≥700 cases, or the run is red. (129
  `*Test.java` files exist; 25 produce no tests under the default Gradle task —
  20 `@SlowTest`, `DriverLoggingSystemTest`'s own task, `@TopologyTest`,
  `@BindingsTest`, `@Disabled`, and one helper without `@Test` — so 104 is the
  whole untagged suite.)
- **B2** every driver opened the media: one log per start, each with
  `dpdk media on net_pcap0`; a driver that fell back to the kernel says nothing.
- **B3** the media carried nothing: every pcap is exactly 24 bytes.
- **B4** per-case diff: `kernel-dedicated` vs `media` must have no deltas (each
  surviving row is adjudicated into the record's known-red list with a
  mechanism); `kernel` vs `kernel-dedicated` is the control and is reported,
  not failed.
- **B6** a delta is re-run once before it is called structural.

## Pitfalls that have bitten

- **Gradle needs Maven Central once**; after that `--offline` works. The GitHub
  git channel being down does not affect Maven.
- The wrapper's log must live **outside `AERON_DIR`**: the reference driver
  deletes an existing aeron directory on start and the open log is unlinked.
- A killed class leaves `/dev/shm/aeron-*` behind and the next driver refuses
  the name; the rig removes it before every class.
- `AERON_THREADING_MODE=SHARED` classes are red on our driver by construction
  (`deepmsg-media-driver` runs DEDICATED only). That is the expected known-red
  mechanism; fixing it is a separate card.
