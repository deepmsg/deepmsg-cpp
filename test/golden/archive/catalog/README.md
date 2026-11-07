# catalog golden

`archive.catalog` here was written by the **reference Java implementation**
(Aeron 1.54.0, commit `454da1e4e6`), never by our code. `expected.txt` is the
same run's dump of the file; the C++ test compares against both.

| Item | Value |
|---|---|
| generator | `analysis/cpp/records/a1a/CatalogGoldenGen.java` (to be moved to the fork `deepmsg/aeron`, branch `deepmsg-tests`, `deepmsg-tests/generators/`) |
| reference | Aeron 1.54.0 = `454da1e4e6`; jar `third_party/aeron/aeron-all/build/libs/aeron-all-1.54.0.jar` |
| command | `javac -proc:none -cp <jar> -d /tmp/gen CatalogGoldenGen.java`<br>`java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED -cp <jar>:/tmp/gen io.aeron.archive.CatalogGoldenGen <out-dir>` |
| `archive.catalog` sha256 | `78825ad2f2ce968732b8444e3ebf7cd5f587528525440fef19c42efa7b7afb71` |
| `expected.txt` sha256 | `8d85913011e14c59e58d6e51a62adc52093141455d53717d46d4dcf619e7834b` |

The generator writes a 1024-byte catalog (capacity `1024`, alignment `64`,
version `3.1.0`) holding three recordings with `Checksums.crc32()` enabled: one
stopped (stop position/timestamp set, checksum recomputed), one invalidated
(`RecordingState.INVALID`, still in the file, absent from the index), and one
with a long channel/source. `expected.txt` has one `key=value` per line:
`header.*`, `capacity`, `entryCount`, then `entry.<n>.<field>` for every
descriptor, including the `offset` and the descriptor-header `state`, `length`
and `checksum`.

`test/unit/archive/server/catalog_golden_test.cpp` opens the file read-only and
compares every field. Regenerate only by re-running the reference generator;
never edit either file by hand.
