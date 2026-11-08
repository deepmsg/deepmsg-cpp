# archive mark file golden

`archive-mark.dat` here was written by the **reference Java implementation**
(Aeron 1.54.0, commit `454da1e4e6`), never by our code. `expected.txt` is the
same run's dump of the header; the C++ test compares against both.

| Item | Value |
|---|---|
| generator | `analysis/cpp/records/a1b/MarkFileGoldenGen.java` (to be moved to the fork `deepmsg/aeron`, branch `deepmsg-tests`, `deepmsg-tests/generators/`) |
| reference | Aeron 1.54.0 = `454da1e4e6`; jar `third_party/aeron/aeron-all/build/libs/aeron-all-1.54.0.jar` |
| command | `javac -proc:none -cp <jar> -d /tmp/gen MarkFileGoldenGen.java`<br>`java --add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.util.zip=ALL-UNNAMED -cp <jar>:/tmp/gen io.aeron.archive.MarkFileGoldenGen <out-dir>` |
| `archive-mark.dat` sha256 | `16f88ebe7ba8e5e5e09ed78fad5275622c54e698653d15bb516dd0bd4c07d990` |
| `expected.txt` sha256 | `229967f1b3048998a76b006c9c10c8e9e1bf423c353e181b9cde9e209f8702ae` |

The file is 12288 bytes: an 8-byte SBE message header, the 128-byte
`MarkFileHeader` block, the four var-ascii channel/directory fields, then the
4096-byte error buffer, all inside the 8 KiB header reservation. Every field is
filled (stream ids, archive id, all four channels), the version is signalled as
`3.1.0`, and the activity timestamp is 5555.

The `pid` field carries the generating JVM's pid, so the committed bytes are not
byte-reproducible; the test does not compare it. Everything else is stable.

`test/unit/archive/server/archive_mark_file_golden_test.cpp` opens the file with
the default major-version check and compares every documented field.
